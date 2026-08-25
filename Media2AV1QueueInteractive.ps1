#requires -Version 7.0
# =============================================================================
# Media2AV1Queue-Interactive.ps1
#
# Drag-and-drop front end for Media2AV1Queue.ps1.
#
# Drop one or more files and/or folders onto Media2AV1Queue-Interactive.bat.
# This script expands any dropped folders into the video files they contain,
# asks a single console question about how aggressively to target space
# savings vs. quality for this drop, and then queues the files into the same
# .queue folder used by the standard Media2AV1Queue.bat/.ps1 -- so this can be
# used interchangeably with the original silent drag-drop workflow, and both
# can have jobs in flight / pending at the same time.
#
# Two questions are asked: how to trade size against quality, and which encoder
# lane to use. Both answers apply only to the files in this drop.
#
# The first either nudges the existing AutoCRFOffset dial or sets an explicit
# output-rate target (GiB/hr). The second picks the CPU or Nvidia lane, or
# leaves the choice to the automatic per-file logic. Everything else in the
# main script (preflight estimation, quality measurement, grain scoring, HDR
# handling) still runs exactly as it does today.
#
# This script does not duplicate any encoding logic. It only prompts, expands
# folders into files, and then re-invokes Media2AV1Queue.ps1 in this same
# console window, so both entry points share one queue, one mutex, and one
# log.
# =============================================================================

[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$InputPaths
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScriptDir   = $PSScriptRoot
$MainScript  = Join-Path $ScriptDir "Media2AV1Queue.ps1"

if (-not (Test-Path -LiteralPath $MainScript)) {
    Write-Host ""
    Write-Host "Could not find Media2AV1Queue.ps1 next to this script:" -ForegroundColor Red
    Write-Host "  $MainScript" -ForegroundColor Red
    Write-Host "Make sure Media2AV1Queue-Interactive.ps1 stays in the same folder as Media2AV1Queue.ps1." -ForegroundColor Yellow
    Write-Host ""
    Read-Host "Press Enter to exit"
    exit 1
}

# ------------------------------------------------------------------------
# Folder expansion
# ------------------------------------------------------------------------
# Media2AV1Queue.ps1 only accepts individual files -- dropped folders are
# rejected. This script expands any dropped folder into the video files it
# contains (recursively, so season folders with per-episode subfolders also
# work), while leaving non-video files (subtitles, nfo, artwork, etc.) out of
# the queue.
$VideoExtensions = @(
    '.mkv', '.mp4', '.m4v', '.ts', '.m2ts', '.avi', '.mov', '.wmv', '.webm', '.mpg', '.mpeg', '.vob'
)

function Get-VideoFilesFromDrop {
    param([string[]]$Paths)

    $results = New-Object System.Collections.Generic.List[string]
    $seen    = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)

    foreach ($p in $Paths) {
        if ([string]::IsNullOrWhiteSpace($p)) { continue }

        if (-not (Test-Path -LiteralPath $p)) {
            Write-Warning "Path not found, skipping: $p"
            continue
        }

        $item = Get-Item -LiteralPath $p

        if ($item.PSIsContainer) {
            Write-Host "Expanding folder: $($item.FullName)" -ForegroundColor DarkGray
            $found = @(Get-ChildItem -LiteralPath $item.FullName -File -Recurse -ErrorAction SilentlyContinue |
                Where-Object { $VideoExtensions -contains $_.Extension.ToLowerInvariant() } |
                Sort-Object FullName)

            if ($found.Count -eq 0) {
                Write-Warning "No video files found in folder: $($item.FullName)"
            }

            foreach ($f in $found) {
                if ($seen.Add($f.FullName)) {
                    $results.Add($f.FullName)
                }
            }
        } else {
            if ($VideoExtensions -contains $item.Extension.ToLowerInvariant()) {
                if ($seen.Add($item.FullName)) {
                    $results.Add($item.FullName)
                }
            } else {
                Write-Warning "Skipping non-video file: $($item.FullName)"
            }
        }
    }

    return ,$results
}

$expandedFiles = Get-VideoFilesFromDrop -Paths $InputPaths

if ($expandedFiles.Count -eq 0) {
    Write-Host ""
    Write-Host "No video files were found in what was dropped." -ForegroundColor Yellow
    Write-Host "Drag one or more video files, or folders containing video files, onto this .bat." -ForegroundColor Yellow
    Write-Host ""
    Read-Host "Press Enter to exit"
    exit 0
}

# ------------------------------------------------------------------------
# Console prompt
# ------------------------------------------------------------------------
Write-Host ""
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host " Media2AV1Queue - Interactive Drop" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "$($expandedFiles.Count) video file(s) ready to queue:" -ForegroundColor White
$previewCount = [Math]::Min(10, $expandedFiles.Count)
for ($i = 0; $i -lt $previewCount; $i++) {
    Write-Host "  $([System.IO.Path]::GetFileName($expandedFiles[$i]))" -ForegroundColor Gray
}
if ($expandedFiles.Count -gt $previewCount) {
    Write-Host "  ... and $($expandedFiles.Count - $previewCount) more" -ForegroundColor Gray
}
Write-Host ""
Write-Host "How should this drop be encoded?" -ForegroundColor White
Write-Host ""
Write-Host "  [1] Auto       - let Media2AV1Queue decide everything, same as a normal drag-drop." -ForegroundColor White
Write-Host "  [2] Aggressive - prioritize space savings. Smaller files, more quality given up." -ForegroundColor White
Write-Host "  [3] Balanced   - a middle ground between space savings and quality." -ForegroundColor White
Write-Host "  [4] Quality    - prioritize quality. Larger files, less quality given up." -ForegroundColor White
Write-Host "  [5] Target     - name an exact output rate in GiB/hr for this drop." -ForegroundColor Cyan
Write-Host ""
Write-Host "  Tiers 2-4 nudge the CRF dial. Tier 5 sets the size target directly," -ForegroundColor DarkGray
Write-Host "  which is the more precise control when you have a figure in mind." -ForegroundColor DarkGray
Write-Host ""

$validChoices = @('1', '2', '3', '4', '5')
$choice = $null
while ($null -eq $choice) {
    $response = Read-Host "Enter 1-5 (default 1 / Auto)"
    if ([string]::IsNullOrWhiteSpace($response)) {
        $choice = '1'
        break
    }
    $response = $response.Trim()
    if ($validChoices -contains $response) {
        $choice = $response
        break
    }
    Write-Host "Please enter 1, 2, 3, 4, or 5." -ForegroundColor Yellow
}

# ------------------------------------------------------------------------
# Tier 5: explicit output-rate target
# ------------------------------------------------------------------------
# A CRF nudge is indirect -- it moves quality and lets the size fall where it
# falls. When you already know the size you want, saying so is both simpler and
# more precise: the target feeds straight into the preflight auto-tune loop,
# which then adjusts CRF until the projection lands on it.
#
# GiB/hr is used rather than total GiB so one answer applies to a whole drop of
# mixed-length files.
$targetRate = $null
if ($choice -eq '5') {
    Write-Host ""
    Write-Host "Output rate target, in GiB per hour of runtime." -ForegroundColor White
    Write-Host "  For reference: a 2h movie at 5 GiB/hr lands around 10 GiB." -ForegroundColor DarkGray
    Write-Host "  Typical: 2-4 for 1080p, 4-8 for 2160p SDR, 8-14 for 2160p HDR." -ForegroundColor DarkGray
    Write-Host ""
    while ($null -eq $targetRate) {
        $entered = Read-Host "Target GiB/hr (e.g. 5, or blank to cancel back to Auto)"
        if ([string]::IsNullOrWhiteSpace($entered)) {
            $choice = '1'
            Write-Host "No target entered -- falling back to Auto." -ForegroundColor Yellow
            break
        }
        $parsed = 0.0
        if ([double]::TryParse($entered.Trim(), [System.Globalization.NumberStyles]::Float,
                               [System.Globalization.CultureInfo]::InvariantCulture, [ref]$parsed) -and
            $parsed -gt 0 -and $parsed -le 100) {
            $targetRate = ($parsed.ToString('0.###', [System.Globalization.CultureInfo]::InvariantCulture))
            break
        }
        Write-Host "Enter a positive number up to 100, for example 5 or 4.5." -ForegroundColor Yellow
    }
}

# ------------------------------------------------------------------------
# Encoder lane
# ------------------------------------------------------------------------
# Separate question from the quality tier, because they are genuinely
# independent: wanting a smaller file says nothing about whether you are in a
# hurry. Option 1 sends no override at all, so an unattended drop behaves
# exactly like a plain drag-drop onto Media2AV1Queue.bat.
Write-Host ""
Write-Host "Which encoder should this drop use?" -ForegroundColor White
Write-Host ""
Write-Host "  [1] Auto   - let the built-in logic pick a lane per file." -ForegroundColor White
Write-Host "  [2] CPU    - SVT-AV1. Best compression and quality. Slower." -ForegroundColor White
Write-Host "  [3] NVENC  - av1_nvenc on the GPU. Much faster, larger files at similar quality." -ForegroundColor White
Write-Host ""
Write-Host "  Auto keeps both lanes busy and holds difficult sources back for the CPU," -ForegroundColor DarkGray
Write-Host "  which is usually the best answer for a mixed batch." -ForegroundColor DarkGray
Write-Host ""

$laneChoices = @('1', '2', '3')
$laneChoice = $null
while ($null -eq $laneChoice) {
    $laneResponse = Read-Host "Enter 1-3 (default 1 / Auto)"
    if ([string]::IsNullOrWhiteSpace($laneResponse)) {
        $laneChoice = '1'
        break
    }
    $laneResponse = $laneResponse.Trim()
    if ($laneChoices -contains $laneResponse) {
        $laneChoice = $laneResponse
        break
    }
    Write-Host "Please enter 1, 2, or 3." -ForegroundColor Yellow
}

# Option 1 deliberately sends nothing rather than the string 'Auto'. The main
# script treats an absent override as "use whatever $EncoderPreference is set
# to", so someone who has pinned CPU in the config keeps CPU without having to
# say so again on every drop.
$encoderOverride = switch ($laneChoice) {
    '1' { $null }
    '2' { 'CPU' }
    '3' { 'Nvidia' }
}

$laneName = switch ($laneChoice) {
    '1' { 'Auto (decide per file)' }
    '2' { 'CPU / SVT-AV1' }
    '3' { 'NVENC / av1_nvenc' }
}

$tierName = switch ($choice) {
    '1' { 'Auto' }
    '2' { 'Aggressive' }
    '3' { 'Balanced' }
    '4' { 'Quality' }
    '5' { "Target $targetRate GiB/hr" }
}

# Nudges the existing AutoCRFOffset dial only. All other Auto-mode logic
# (preflight estimation, grain scoring, lane selection) is untouched.
# Positive offset = higher CRF = smaller files / more compression.
# Negative offset = lower CRF = larger files / better quality.
$offsetOverride = switch ($choice) {
    '1' { $null }   # Auto: no override, defer entirely to existing global config
    '2' { '2' }     # Aggressive: bias toward smaller files
    '3' { '0' }     # Balanced: no bias, but explicitly pinned rather than deferring to global
    '4' { '-2' }    # Quality: bias toward better quality
    '5' { $null }   # Target: the rate does the work; no CRF bias on top of it
}

Write-Host ""
Write-Host "Selected: $tierName  |  Encoder: $laneName" -ForegroundColor Green
if ($null -ne $encoderOverride) {
    Write-Host "Encoder lane for this drop: $encoderOverride" -ForegroundColor Green
    Write-Host "If this differs from the EncoderPreference setting, the queue switches to the" -ForegroundColor DarkGray
    Write-Host "automatic lane scheduler so both this drop and any others are honoured." -ForegroundColor DarkGray
}
if ($null -ne $targetRate) {
    Write-Host "Output rate target for this drop: $targetRate GiB/hr" -ForegroundColor Green
    Write-Host "This overrides both the resolution ladder and the source-rate cap." -ForegroundColor DarkGray
} elseif ($null -ne $offsetOverride) {
    Write-Host "AutoCRFOffset override for this drop: $offsetOverride" -ForegroundColor Green
} else {
    Write-Host "No override -- using whatever AutoCRFOffset is configured in Media2AV1Queue.ps1." -ForegroundColor Green
}
Write-Host ""

# ------------------------------------------------------------------------
# Hand off to the main script
# ------------------------------------------------------------------------
# This runs in the same console window/process tree as a normal drag-drop
# onto Media2AV1Queue.bat would. If another worker is already running, the
# main script will queue these files (with this drop's override attached)
# and exit; if not, it becomes the worker and starts processing immediately,
# exactly like today.
#
# Parameters are passed via splatted hashtable (rather than mixing a named
# switch with a positional array splat) so there is no ambiguity in how
# PowerShell's parameter binder assigns the file list to $InputPaths.
$mainScriptArgs = [ordered]@{
    InputPaths = [string[]]$expandedFiles.ToArray()
}
if ($null -ne $offsetOverride) {
    $mainScriptArgs['AutoCRFOffsetOverride'] = $offsetOverride
}
if ($null -ne $targetRate) {
    $mainScriptArgs['TargetGiBPerHourOverride'] = $targetRate
}
if ($null -ne $encoderOverride) {
    $mainScriptArgs['EncoderPreferenceOverride'] = $encoderOverride
}

try {
    & $MainScript @mainScriptArgs
    exit 0
} catch {
    Write-Host ""
    Write-Host "Media2AV1Queue.ps1 reported an error:" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
}
