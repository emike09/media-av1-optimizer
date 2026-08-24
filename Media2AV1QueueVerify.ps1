#requires -Version 7.0
# =============================================================================
# Media2AV1Queue-Verify.ps1
#
# End-to-end smoke test for Media2AV1Queue.ps1.
#
# Everything verified so far has been either isolated function tests or the
# standalone doctor / bench / scan scripts. The main script -- the one with the
# queue, the mutex, the workers, the progress UI, and $ReplaceOriginal = $true --
# has never actually encoded a file since the FFmpeg 9 changes. This closes that
# gap: it runs real files through the real script and then checks the outputs
# against their sources, field by field.
#
# SAFETY
#   Selected sources are COPIED into an isolated sandbox folder and the encode
#   runs against the copies. The originals in your library are never passed to
#   the encoder, so even if $ReplaceOriginal is left at $true -- and even if
#   something in the script misbehaves -- the only files at risk are the copies.
#   The untouched originals then serve as the comparison reference.
#
#   Nothing outside the sandbox folder is written except the encode log that
#   Media2AV1Queue.ps1 maintains itself.
#
# Usage:
#   # pick representatives automatically from a census CSV
#   pwsh -File Media2AV1Queue-Verify.ps1 -CensusCsv .\hdr_census_20260821_152251.csv
#
#   # or name the files yourself
#   pwsh -File Media2AV1Queue-Verify.ps1 -Files "G:\A.mkv","G:\B.mkv"
#
#   # dry run: show what it would test and check disk space, encode nothing
#   pwsh -File Media2AV1Queue-Verify.ps1 -CensusCsv .\census.csv -WhatIfOnly
#
# Recommended: run with $CpuMaxParallel = 1 so a failure is unambiguous.
# =============================================================================

[CmdletBinding()]
param(
    [string]$CensusCsv = '',
    [string[]]$Files = @(),
    [string]$SandboxRoot = '',
    [int]$TrimSeconds = 90,
    [double]$MinCandidateGiB = 0.2,
    [switch]$NoTrim,
    [switch]$WhatIfOnly,
    [switch]$KeepSandbox
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# `pwsh -File` does not bind arrays; the whole argument arrives as one string.
$normalised = New-Object System.Collections.Generic.List[string]
foreach ($raw in @($Files)) {
    if ([string]::IsNullOrWhiteSpace($raw)) { continue }
    foreach ($piece in ([string]$raw -split ',')) {
        $clean = $piece.Trim().Trim('"').Trim("'").Trim()
        if (-not [string]::IsNullOrWhiteSpace($clean)) { $normalised.Add($clean) }
    }
}
$Files = $normalised.ToArray()

function Find-Bin([string]$Name) {
    foreach ($ext in @('.exe','')) {
        $c = Join-Path $PSScriptRoot ($Name + $ext)
        if (Test-Path -LiteralPath $c -PathType Leaf) { return (Get-Item -LiteralPath $c).FullName }
    }
    $cmd = Get-Command $Name -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    return $null
}
$ffmpeg  = Find-Bin 'ffmpeg'
$ffprobe = Find-Bin 'ffprobe'
if (-not $ffmpeg -or -not $ffprobe) { throw "ffmpeg/ffprobe not found next to this script or on PATH." }

$mainScript = Join-Path $PSScriptRoot 'Media2AV1Queue.ps1'
if (-not (Test-Path -LiteralPath $mainScript)) {
    throw "Media2AV1Queue.ps1 was not found next to this script: $mainScript"
}

$pass = 0; $fail = 0; $warn = 0
$findings = New-Object System.Collections.Generic.List[object]

function Say { param([string]$T, [string]$C = 'Gray') Write-Host $T -ForegroundColor $C }
function Head { param([string]$T)
    Write-Host ""
    Write-Host ("=" * 78) -ForegroundColor DarkCyan
    Write-Host " $T" -ForegroundColor Cyan
    Write-Host ("=" * 78) -ForegroundColor DarkCyan
}
function Check {
    param([string]$File, [string]$What, [bool]$Ok, [string]$Detail = '', [switch]$WarnOnly)
    if ($Ok) {
        $script:pass++
        Write-Host ("    PASS  {0}" -f $What) -ForegroundColor Green
    } elseif ($WarnOnly) {
        $script:warn++
        Write-Host ("    WARN  {0}" -f $What) -ForegroundColor Yellow
        if ($Detail) { Write-Host ("          {0}" -f $Detail) -ForegroundColor DarkGray }
    } else {
        $script:fail++
        Write-Host ("    FAIL  {0}" -f $What) -ForegroundColor Red
        if ($Detail) { Write-Host ("          {0}" -f $Detail) -ForegroundColor DarkGray }
    }
    $script:findings.Add([pscustomobject]@{ File = $File; Check = $What; Result = $(if ($Ok) { 'PASS' } elseif ($WarnOnly) { 'WARN' } else { 'FAIL' }); Detail = $Detail })
}

# ---------------------------------------------------------------------------
# Metadata reader: stream level, then frame level. Same two-phase approach the
# main script uses, and for the same reason -- HEVC carries the static HDR10
# payload in SEI, where a stream-level probe cannot see it.
# ---------------------------------------------------------------------------
function Get-MediaFacts {
    param([string]$Path)

    $facts = [ordered]@{
        Exists = (Test-Path -LiteralPath $Path)
        SizeBytes = 0; Duration = 0.0
        VideoCodec = ''; PixFmt = ''
        Primaries = ''; Transfer = ''; Matrix = ''
        HasMastering = $false; Mastering = $null
        MaxCLL = $null; MaxFALL = $null
        HasHDR10Plus = $false; HasDoviRpu = $false; DvProfile = $null
        AudioCount = 0; SubCount = 0; ChapterCount = 0
        AudioCodecs = @()
    }
    if (-not $facts.Exists) { return $facts }
    $facts.SizeBytes = (Get-Item -LiteralPath $Path).Length

    $json = & $ffprobe -v error -print_format json -show_streams -show_format -show_chapters $Path 2>$null
    if (-not $json) { return $facts }
    $p = $json | ConvertFrom-Json -Depth 100

    function Prop($o, $n) {
        if ($null -eq $o) { return $null }
        $pp = $o.PSObject.Properties[$n]
        if ($null -eq $pp -or $null -eq $pp.Value) { return $null }
        return $pp.Value
    }

    $d = Prop $p.format 'duration'
    if ($d) { $facts.Duration = [double]$d }
    $facts.ChapterCount = @(Prop $p 'chapters').Count

    $v = @($p.streams | Where-Object { $_.codec_type -eq 'video' }) | Select-Object -First 1
    $a = @($p.streams | Where-Object { $_.codec_type -eq 'audio' })
    $s = @($p.streams | Where-Object { $_.codec_type -eq 'subtitle' })
    $facts.AudioCount = $a.Count
    $facts.SubCount = $s.Count
    $facts.AudioCodecs = @($a | ForEach-Object { [string](Prop $_ 'codec_name') })

    if ($v) {
        $facts.VideoCodec = [string](Prop $v 'codec_name')
        $facts.PixFmt     = [string](Prop $v 'pix_fmt')
        $facts.Primaries  = [string](Prop $v 'color_primaries')
        $facts.Transfer   = [string](Prop $v 'color_transfer')
        $facts.Matrix     = [string](Prop $v 'color_space')

        $collect = {
            param($SideDataList)
            foreach ($sd in @($SideDataList)) {
                if ($null -eq $sd) { continue }
                $t = [string](Prop $sd 'side_data_type')
                if ($t -match '(?i)mastering\s*display' -and -not $facts.HasMastering) {
                    $vals = @{}
                    foreach ($k in 'green_x','green_y','blue_x','blue_y','red_x','red_y','white_point_x','white_point_y','max_luminance','min_luminance') {
                        $raw = Prop $sd $k
                        if ($null -ne $raw) {
                            if ("$raw" -match '^\s*(-?\d+)\s*/\s*(-?\d+)\s*$' -and [double]$Matches[2] -ne 0) {
                                $vals[$k] = [double]$Matches[1] / [double]$Matches[2]
                            } else {
                                $parsed = 0.0
                                if ([double]::TryParse("$raw", [System.Globalization.NumberStyles]::Float, [System.Globalization.CultureInfo]::InvariantCulture, [ref]$parsed)) { $vals[$k] = $parsed }
                            }
                        }
                    }
                    if ($vals.Count -eq 10) { $facts.HasMastering = $true; $facts.Mastering = $vals }
                } elseif ($t -match '(?i)content\s*light' -and $null -eq $facts.MaxCLL) {
                    $facts.MaxCLL  = Prop $sd 'max_content'
                    $facts.MaxFALL = Prop $sd 'max_average'
                } elseif ($t -match '(?i)HDR10\+|2094-40|Dynamic\s*HDR') {
                    $facts.HasHDR10Plus = $true
                } elseif ($t -match '(?i)DOVI|Dolby\s*Vision') {
                    $facts.HasDoviRpu = $true
                    if ($null -eq $facts.DvProfile) { $facts.DvProfile = Prop $sd 'dv_profile' }
                }
            }
        }

        & $collect (Prop $v 'side_data_list')

        if (-not $facts.HasMastering -or $null -eq $facts.MaxCLL -or -not $facts.HasHDR10Plus) {
            $idx = [string](Prop $v 'index')
            $fj = & $ffprobe -v error -print_format json -show_frames -read_intervals '%+#1' -select_streams "$idx" $Path 2>$null
            if ($fj) {
                try {
                    $fp = $fj | ConvertFrom-Json -Depth 100
                    foreach ($fr in @($fp.frames)) { & $collect (Prop $fr 'side_data_list') }
                } catch { }
            }
        }
    }
    return $facts
}

# ---------------------------------------------------------------------------
Head "Selecting test files"

$selected = New-Object System.Collections.Generic.List[object]

if (-not [string]::IsNullOrWhiteSpace($CensusCsv)) {
    if (-not (Test-Path -LiteralPath $CensusCsv)) { throw "Census CSV not found: $CensusCsv" }
    $census = @(Import-Csv -LiteralPath $CensusCsv)
    Say ("  census: {0} rows from {1}" -f $census.Count, [System.IO.Path]::GetFileName($CensusCsv)) 'DarkGray'

    # One representative per behaviour that has its own code path. These are the
    # classes that can fail independently, so each needs its own sample.
    $wanted = @(
        @{ Key = 'HDR10 with metadata';   Filter = { $_.HdrFormat -eq 'HDR10' -and $_.HasMasteringDisplay -eq 'True' } }
        @{ Key = 'HDR10 without metadata'; Filter = { $_.HdrFormat -like 'HDR10*' -and $_.HasMasteringDisplay -ne 'True' } }
        @{ Key = 'HDR10+';                Filter = { $_.HdrFormat -eq 'HDR10+' } }
        @{ Key = 'HLG';                   Filter = { $_.HdrFormat -eq 'HLG' } }
        @{ Key = 'Dolby Vision P8';       Filter = { $_.HdrFormat -eq 'Dolby Vision' -and $_.DvProfile -eq '8' } }
        @{ Key = 'Dolby Vision P5';       Filter = { $_.HdrFormat -eq 'Dolby Vision' -and $_.DvProfile -eq '5' } }
        @{ Key = 'SDR';                   Filter = { $_.HdrFormat -eq 'SDR' } }
    )

    foreach ($w in $wanted) {
        # Candidate choice matters more than it looks. Preferring the smallest
        # match saves encode time, but naively taking the smallest picks stubs,
        # placeholders and truncated leftovers -- a 2 MB file claiming to be
        # 2160p tests nothing, and a partial file makes the duration and size
        # checks meaningless.
        #
        # So: exclude obvious non-content by name, require a real minimum size,
        # and only then take the smallest qualifying file.
        $eligible = @($census | Where-Object $w.Filter |
                    Where-Object { $_.Name -notmatch '(?i)sample' } |
                    Where-Object { $_.Name -notmatch '(?i)\.tmp\.|\.encoding\.|\.partial\.|\.clean\.tmp' } |
                    Where-Object { $_.SizeGiB -ne '' })

        $bigEnough = @($eligible | Where-Object { [double]$_.SizeGiB -ge $MinCandidateGiB })

        $match = @()
        if ($bigEnough.Count -gt 0) {
            $match = @($bigEnough | Sort-Object { [double]$_.SizeGiB } | Select-Object -First 1)
        } elseif ($eligible.Count -gt 0) {
            # Nothing meets the minimum. Take the LARGEST available rather than
            # the smallest, and say so -- the biggest of a set of small files is
            # the least likely to be a stub.
            $match = @($eligible | Sort-Object { [double]$_.SizeGiB } -Descending | Select-Object -First 1)
            Say ("  {0,-24} no candidate over {1} GiB; using the largest available ({2} GiB) -- may be unrepresentative" -f `
                 $w.Key, $MinCandidateGiB, $match[0].SizeGiB) 'DarkYellow'
            $script:warn++
        }

        if ($match.Count -eq 0) {
            Say ("  {0,-24} no candidate in census -- skipping this class" -f $w.Key) 'DarkYellow'
            continue
        }
        $selected.Add([pscustomobject]@{
            Class = $w.Key
            Path = $match[0].File
            Name = $match[0].Name
            SizeGiB = $match[0].SizeGiB
            ExpectSkip = ($w.Key -eq 'Dolby Vision P5')
        })
        $codecNote = ''
        if ($match[0].PSObject.Properties['Codec'] -and $match[0].Codec -eq 'av1') {
            # Already AV1: the Auto path will likely skip it as already
            # efficient, so it exercises the skip logic rather than the encode.
            $codecNote = '  [already AV1 -- may be skipped as already efficient]'
        }
        Say ("  {0,-24} {1}  ({2} GiB){3}" -f $w.Key, $match[0].Name, $match[0].SizeGiB, $codecNote) $(if ($codecNote) { 'DarkYellow' } else { 'Gray' })
    }
}

foreach ($f in $Files) {
    if (-not (Test-Path -LiteralPath $f)) { Say ("  not found, skipping: {0}" -f $f) 'Yellow'; continue }
    $selected.Add([pscustomobject]@{
        Class = 'explicit'
        Path = (Get-Item -LiteralPath $f).FullName
        Name = [System.IO.Path]::GetFileName($f)
        SizeGiB = [Math]::Round((Get-Item -LiteralPath $f).Length / 1GB, 3)
        ExpectSkip = $false
    })
    Say ("  explicit                 {0}" -f [System.IO.Path]::GetFileName($f)) 'Gray'
}

if ($selected.Count -eq 0) { throw "No test files selected. Pass -CensusCsv or -Files." }

# ---------------------------------------------------------------------------
Head "Sandbox"

if ([string]::IsNullOrWhiteSpace($SandboxRoot)) {
    $SandboxRoot = Join-Path $PSScriptRoot (".verify\{0}" -f (Get-Date -Format 'yyyyMMdd_HHmmss'))
}
$null = New-Item -ItemType Directory -Force -Path $SandboxRoot
Say ("  sandbox: {0}" -f $SandboxRoot) 'DarkGray'
Say  "  originals are never passed to the encoder; only these copies are." 'DarkGray'

# Trimming keeps the test to minutes instead of hours. It is a stream copy, so
# the trimmed clip carries the same codec, colour tags and SEI metadata as the
# source -- which is exactly what is being tested. -NoTrim uses whole files.
if (-not $NoTrim) {
    Say ("  each source trimmed to {0}s by stream copy (metadata preserved)" -f $TrimSeconds) 'DarkGray'
}

$estBytes = 0
foreach ($s in $selected) {
    $srcBytes = (Get-Item -LiteralPath $s.Path).Length
    if ($NoTrim) { $estBytes += $srcBytes * 2 }
    else {
        $srcDur = (Get-MediaFacts -Path $s.Path).Duration
        $frac = if ($srcDur -gt 0) { [Math]::Min(1.0, $TrimSeconds / $srcDur) } else { 1.0 }
        $estBytes += [int64]($srcBytes * $frac * 2)
    }
}
$drive = (Get-Item -LiteralPath $SandboxRoot).PSDrive
$freeBytes = if ($drive) { (Get-PSDrive $drive.Name).Free } else { 0 }
Say ("  estimated sandbox usage: {0:F2} GiB   free on {1}: {2:F2} GiB" -f ($estBytes/1GB), $drive.Name, ($freeBytes/1GB)) 'DarkGray'
if ($freeBytes -gt 0 -and $estBytes -gt ($freeBytes * 0.5)) {
    Say  "  WARNING: the test may use more than half the free space on this drive." 'Yellow'
    $warn++
}

if ($WhatIfOnly) {
    Head "Dry run complete"
    Say  "  Nothing was copied or encoded. Re-run without -WhatIfOnly to execute." 'Cyan'
    Remove-Item -LiteralPath $SandboxRoot -Recurse -Force -ErrorAction SilentlyContinue
    exit 0
}

# --- check the queue is idle, or the test will interleave with real work ----
$queuePending = Join-Path $PSScriptRoot '.queue\pending'
$queueWorking = Join-Path $PSScriptRoot '.queue\working'
foreach ($q in @($queuePending, $queueWorking)) {
    if (Test-Path -LiteralPath $q) {
        $n = @(Get-ChildItem -LiteralPath $q -Filter *.json -File -ErrorAction SilentlyContinue).Count
        if ($n -gt 0) {
            throw "The queue is not idle ($n job(s) in $q). Let it drain before running the verifier, or the test will interleave with real work."
        }
    }
}
Say  "  queue is idle" 'DarkGray'

# --- stage copies ----------------------------------------------------------
Head "Staging copies"
$cases = New-Object System.Collections.Generic.List[object]
$i = 0
foreach ($s in $selected) {
    $i++
    $ext = [System.IO.Path]::GetExtension($s.Path)
    $dest = Join-Path $SandboxRoot ("{0:D2}_{1}{2}" -f $i, ($s.Class -replace '[^\w]+','_'), $ext)
    Write-Host ("  [{0}/{1}] {2} ..." -f $i, $selected.Count, $s.Class) -NoNewline
    try {
        if ($NoTrim) {
            Copy-Item -LiteralPath $s.Path -Destination $dest -Force
        } else {
            & $ffmpeg -hide_banner -loglevel error -nostdin -y -t "$TrimSeconds" -i $s.Path -map 0 -c copy $dest 2>$null
            if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $dest)) {
                # Some containers refuse a full -map 0 copy; fall back to the
                # streams the encoder actually cares about.
                & $ffmpeg -hide_banner -loglevel error -nostdin -y -t "$TrimSeconds" -i $s.Path -map 0:v -map 0:a? -map 0:s? -c copy $dest 2>$null
            }
        }
        if (-not (Test-Path -LiteralPath $dest)) { throw "copy produced no file" }
        $cases.Add([pscustomobject]@{
            Class = $s.Class
            OriginalPath = $s.Path
            StagedPath = $dest
            StagedName = [System.IO.Path]::GetFileName($dest)
            ExpectSkip = $s.ExpectSkip
            SourceFacts = (Get-MediaFacts -Path $dest)   # facts of the STAGED copy
            OutputPath = $null
        })
        Write-Host ("  {0:F2} MiB" -f ((Get-Item -LiteralPath $dest).Length / 1MB)) -ForegroundColor Green
    } catch {
        Write-Host ("  FAILED: {0}" -f $_.Exception.Message) -ForegroundColor Red
        $fail++
    }
}
if ($cases.Count -eq 0) { throw "Nothing was staged; aborting." }

# ---------------------------------------------------------------------------
Head "Running Media2AV1Queue.ps1 against the copies"
Say  "  This is the real script, on the real queue, with real encoding." 'DarkGray'
Say  "  Console output below is the encoder's own." 'DarkGray'
Write-Host ""

$logPath = Join-Path $PSScriptRoot '.queue\encode_log.csv'
$logBefore = if (Test-Path -LiteralPath $logPath) { @(Import-Csv -LiteralPath $logPath).Count } else { 0 }

$stagedPaths = @($cases | ForEach-Object { $_.StagedPath })
$runOk = $true
try {
    & $mainScript -InputPaths $stagedPaths
} catch {
    $runOk = $false
    Say ("  Media2AV1Queue.ps1 threw: {0}" -f $_.Exception.Message) 'Red'
}

Write-Host ""
Check -File '(run)' -What 'Media2AV1Queue.ps1 completed without throwing' -Ok $runOk

# ---------------------------------------------------------------------------
Head "Verifying outputs"

# The script can legitimately decline to encode a file: the Auto path skips
# already-efficient sources, and preflight aborts when the projected output is
# too close to the source size. On a short trimmed clip that second case is
# quite likely, because a 90s sample's copied-stream overhead is proportionally
# larger than a whole film's.
#
# A deliberate skip is not a failure, but it is not a test either -- nothing was
# encoded, so nothing can be checked. Reported as SKIPPED so the distinction is
# visible instead of surfacing as a bogus "produced no output" failure.
$skipStatuses = @('AUTO_SKIPPED_ALREADY_EFFICIENT','PRECHECK_SKIPPED_UNFAVORABLE','SKIPPED_DV')
$logRowsNow = if (Test-Path -LiteralPath $logPath) { @(Import-Csv -LiteralPath $logPath) } else { @() }

function Get-LoggedStatus {
    param([string]$StagedPath)
    $name = [System.IO.Path]::GetFileName($StagedPath)
    $match = @($logRowsNow | Where-Object { $_.InputPath -and ([System.IO.Path]::GetFileName($_.InputPath) -eq $name) })
    if ($match.Count -eq 0) { return $null }
    return $match[-1]
}

# The script renames codec tokens to AV1 and writes <basename>.mkv, so the
# output is discovered rather than assumed.
function Find-Output {
    param($Case)
    $dir = Split-Path -Parent $Case.StagedPath
    $stem = [System.IO.Path]::GetFileNameWithoutExtension($Case.StagedPath)
    $candidates = @(Get-ChildItem -LiteralPath $dir -File -ErrorAction SilentlyContinue |
        Where-Object { $_.Extension -eq '.mkv' -and $_.Name -notlike '*.encoding.tmp.*' })
    # exact stem first (in-place replacement), then any AV1-renamed variant
    $exact = @($candidates | Where-Object { [System.IO.Path]::GetFileNameWithoutExtension($_.Name) -eq $stem })
    if ($exact.Count -gt 0) { return $exact[0].FullName }
    $near = @($candidates | Where-Object { $_.Name -like ("*" + ($stem -replace '^\d+_','') + "*") })
    if ($near.Count -gt 0) { return $near[0].FullName }
    return $null
}

foreach ($c in $cases) {
    Write-Host ""
    Say ("  [{0}]  {1}" -f $c.Class, $c.StagedName) 'White'
    $src = $c.SourceFacts
    $outPath = Find-Output -Case $c
    $c.OutputPath = $outPath

    $logRow = Get-LoggedStatus -StagedPath $c.StagedPath
    $loggedStatus = if ($logRow) { [string]$logRow.Status } else { '(no log row)' }

    # --- expected-skip classes -------------------------------------------
    if ($c.ExpectSkip) {
        $stillOriginalCodec = $false
        if ($outPath) {
            $f = Get-MediaFacts -Path $outPath
            $stillOriginalCodec = ($f.VideoCodec -ne 'av1')
        }
        # "No AV1 output" is necessary but not sufficient evidence of a
        # deliberate refusal -- a crashed encode looks identical from the
        # filesystem. The logged status is what distinguishes them, so a
        # FAILED status here is a real failure, not a pass.
        $noAv1 = ($null -eq $outPath -or $stillOriginalCodec)
        if ($loggedStatus -eq 'FAILED') {
            Check -File $c.StagedName -What 'refused cleanly (Dolby Vision Profile 5)' -Ok $false `
                  -Detail "No AV1 output, but the log says FAILED rather than SKIPPED_DV -- the file was not refused by policy, the job errored. That is a bug, not a refusal."
            if ($logRow -and $logRow.Notes) { Say ("          log: {0}" -f $logRow.Notes) 'DarkGray' }
            continue
        }
        Check -File $c.StagedName -What 'refused rather than converted (Dolby Vision Profile 5)' `
              -Ok $noAv1 `
              -Detail "An AV1 output here would mean a Profile 5 source was converted, which produces wrong colour."
        Check -File $c.StagedName -What 'logged as SKIPPED_DV' -Ok ($loggedStatus -eq 'SKIPPED_DV') `
              -Detail ("log status was '{0}'" -f $loggedStatus)
        if ($logRow -and $logRow.Notes) { Say ("          reason: {0}" -f $logRow.Notes) 'DarkGray' }
        continue
    }

    # --- the script declined to encode this file --------------------------
    if ($skipStatuses -contains $loggedStatus) {
        Say ("    SKIP  the script declined to encode this file: {0}" -f $loggedStatus) 'Cyan'
        if ($logRow.Notes) { Say ("          {0}" -f $logRow.Notes) 'DarkGray' }
        if ($loggedStatus -eq 'PRECHECK_SKIPPED_UNFAVORABLE') {
            Say  "          Common on short trimmed clips: a 90s sample's copied-stream" 'DarkGray'
            Say  "          overhead is proportionally larger than a whole film's, so the" 'DarkGray'
            Say  "          projected saving looks worse than it really is. Re-test this" 'DarkGray'
            Say  "          class with -NoTrim, or a longer -TrimSeconds, to exercise it." 'DarkGray'
        }
        $script:findings.Add([pscustomobject]@{ File = $c.StagedName; Check = 'encode attempted'; Result = 'SKIPPED'; Detail = $loggedStatus })
        continue
    }

    if (-not $outPath) {
        Check -File $c.StagedName -What 'produced an output file' -Ok $false `
              -Detail "No .mkv output found in the sandbox. Check the console output and the encode log."
        continue
    }
    $out = Get-MediaFacts -Path $outPath
    Check -File $c.StagedName -What 'produced an output file' -Ok $true

    # If the encode failed, the file found here is the untouched staged copy --
    # so every metadata check below would be comparing the source against
    # ITSELF and reporting PASS. That is a false pass, and a false pass is worse
    # than a false failure: it says the fix works when it was never exercised.
    #
    # An earlier version of this script did exactly that: it reported
    # "mastering display metadata CARRIED to output  PASS" on files whose
    # encode had failed outright. So: if the output is not AV1, record that as
    # the single failure and stop, rather than emitting a page of meaningless
    # green.
    if ($out.VideoCodec -ne 'av1') {
        Check -File $c.StagedName -What 'output is AV1' -Ok $false `
              -Detail ("got '{0}'. The encode did not produce an AV1 file, so this is the unencoded staged copy. Remaining checks skipped -- comparing the source against itself would report false passes. See the encode log Notes for the cause." -f $out.VideoCodec)
        if ($logRow -and $logRow.Notes) { Say ("          log: {0}" -f ($logRow.Notes -replace '\s+',' ').Substring(0, [Math]::Min(240, ($logRow.Notes -replace '\s+',' ').Length))) 'DarkGray' }
        Say  "    ....  remaining checks skipped (no real output to check)" 'DarkGray'
        continue
    }
    Check -File $c.StagedName -What 'output is AV1' -Ok $true
    Check -File $c.StagedName -What 'output is 10-bit' -Ok ($out.PixFmt -match '10') -Detail ("pix_fmt '{0}'" -f $out.PixFmt)

    # --- duration -----------------------------------------------------------
    $durTol = [Math]::Max(2.0, $src.Duration * 0.02)
    Check -File $c.StagedName -What 'duration preserved' `
          -Ok ([Math]::Abs($out.Duration - $src.Duration) -le $durTol) `
          -Detail ("source {0:F2}s -> output {1:F2}s (tolerance {2:F2}s)" -f $src.Duration, $out.Duration, $durTol)

    # --- streams ------------------------------------------------------------
    Check -File $c.StagedName -What 'at least one audio track kept' -Ok ($out.AudioCount -ge 1) `
          -Detail ("source had {0}, output has {1}" -f $src.AudioCount, $out.AudioCount)
    if ($src.AudioCount -gt 0) {
        Check -File $c.StagedName -What 'audio stream-copied (codec unchanged)' `
              -Ok (@($out.AudioCodecs | Where-Object { $src.AudioCodecs -contains $_ }).Count -ge 1) `
              -Detail ("source [{0}] -> output [{1}]" -f ($src.AudioCodecs -join ','), ($out.AudioCodecs -join ','))
    }
    if ($src.SubCount -gt 0) {
        Check -File $c.StagedName -What 'subtitle track kept' -Ok ($out.SubCount -ge 1) -WarnOnly `
              -Detail ("source had {0}, output has {1}. Selection policy may legitimately drop non-English tracks." -f $src.SubCount, $out.SubCount)
    }
    if ($src.ChapterCount -gt 0) {
        Check -File $c.StagedName -What 'chapters kept' -Ok ($out.ChapterCount -ge 1) -WarnOnly `
              -Detail ("source had {0}, output has {1}" -f $src.ChapterCount, $out.ChapterCount)
    }

    # --- colour signalling --------------------------------------------------
    $srcIsHlg = $src.Transfer -match 'arib-std-b67'
    $srcIsPq  = $src.Transfer -match 'smpte2084'
    $srcIsHdr = $srcIsPq -or $srcIsHlg -or ($src.Primaries -match 'bt2020')

    if ($srcIsHlg) {
        # The specific regression: the old code tagged every HDR source PQ.
        Check -File $c.StagedName -What 'HLG stayed HLG (not mislabelled as PQ)' `
              -Ok ($out.Transfer -match 'arib-std-b67') `
              -Detail ("output transfer '{0}'. smpte2084 here is the pre-fix bug." -f $out.Transfer)
    } elseif ($srcIsHdr) {
        Check -File $c.StagedName -What 'output tagged PQ (smpte2084)' -Ok ($out.Transfer -match 'smpte2084') -Detail ("got '{0}'" -f $out.Transfer)
        Check -File $c.StagedName -What 'output tagged BT.2020 primaries' -Ok ($out.Primaries -match 'bt2020') -Detail ("got '{0}'" -f $out.Primaries)
    } else {
        Check -File $c.StagedName -What 'SDR source not tagged as HDR' `
              -Ok ($out.Transfer -notmatch 'smpte2084|arib-std-b67') -Detail ("got '{0}'" -f $out.Transfer)
    }

    # --- THE headline check: static HDR10 payload --------------------------
    if ($src.HasMastering) {
        Check -File $c.StagedName -What 'mastering display metadata CARRIED to output' -Ok $out.HasMastering `
              -Detail "This is the central fix. Absent here means the payload is still being dropped."
        if ($out.HasMastering) {
            $worst = 0.0; $worstField = ''
            foreach ($k in $src.Mastering.Keys) {
                if (-not $out.Mastering.ContainsKey($k)) { continue }
                # min_luminance is excluded: AV1 quantises it in 1/16384 steps,
                # so 0.0001 legitimately lands on 0.000122.
                if ($k -eq 'min_luminance') { continue }
                $a = $src.Mastering[$k]; $b = $out.Mastering[$k]
                $rel = if ([Math]::Abs($a) -gt 1e-9) { [Math]::Abs($a - $b) / [Math]::Abs($a) } else { 0 }
                if ($rel -gt $worst) { $worst = $rel; $worstField = $k }
            }
            Check -File $c.StagedName -What 'mastering display values match the source' -Ok ($worst -lt 0.01) `
                  -Detail ("worst drift {0:P3} on {1} (1% tolerance; min_luminance excluded by design)" -f $worst, $worstField)
        }
    } elseif ($srcIsHdr -and -not $srcIsHlg) {
        Check -File $c.StagedName -What 'no mastering display invented for a source without one' `
              -Ok (-not $out.HasMastering) `
              -Detail "Fabricated colour volume is worse than none."
    }

    if ($null -ne $src.MaxCLL) {
        Check -File $c.StagedName -What 'MaxCLL / MaxFALL carried to output' `
              -Ok ("$($out.MaxCLL)" -eq "$($src.MaxCLL)" -and "$($out.MaxFALL)" -eq "$($src.MaxFALL)") `
              -Detail ("source {0}/{1} -> output {2}/{3}" -f $src.MaxCLL, $src.MaxFALL, $out.MaxCLL, $out.MaxFALL)
    }

    # --- Dolby Vision -------------------------------------------------------
    if ($src.HasDoviRpu) {
        Check -File $c.StagedName -What 'Dolby Vision RPU not carried into the AV1 output' -Ok (-not $out.HasDoviRpu) `
              -Detail "A retained RPU would make players treat this as DV when the pipeline does not produce a conformant DV stream."
        Check -File $c.StagedName -What 'DV source converted to a valid HDR transfer' `
              -Ok ($out.Transfer -match 'smpte2084|arib-std-b67|bt709') -Detail ("got '{0}'" -f $out.Transfer)
    }

    # --- HDR10+ -------------------------------------------------------------
    if ($src.HasHDR10Plus) {
        if ($out.HasHDR10Plus) {
            Check -File $c.StagedName -What 'HDR10+ dynamic metadata preserved' -Ok $true
        } else {
            Check -File $c.StagedName -What 'HDR10+ dropped, but static HDR10 is correct' -Ok $out.HasMastering -WarnOnly `
                  -Detail "Expected on this toolchain: no AV1-capable hdr10plus_tool and no hdr10plus-json in SVT-AV1. Static HDR10 must still be right."
        }
    }

    # --- size ---------------------------------------------------------------
    if ($src.SizeBytes -gt 0) {
        $ratio = $out.SizeBytes / [double]$src.SizeBytes
        Check -File $c.StagedName -What 'output smaller than source' -Ok ($ratio -lt 1.0) -WarnOnly `
              -Detail ("{0:P1} of source" -f $ratio)
    }
}

# ---------------------------------------------------------------------------
Head "Encode log"

if (-not (Test-Path -LiteralPath $logPath)) {
    Check -File '(log)' -What 'encode log exists' -Ok $false
} else {
    $rows = @(Import-Csv -LiteralPath $logPath)
    Check -File '(log)' -What 'log gained rows for this run' -Ok ($rows.Count -gt $logBefore) `
          -Detail ("{0} rows before, {1} after" -f $logBefore, $rows.Count)

    $header = @($rows[0].PSObject.Properties.Name)
    $newCols = @('SourceHdrFormat','HdrTargetFormat','HdrStaticMetadata','HdrMaxCLL','HdrMaxFALL',
                 'HdrHDR10PlusSource','HdrHDR10PlusOutput','DolbyVisionProfile','DolbyVisionStrategy','HdrPlanSummary')
    $missing = @($newCols | Where-Object { $header -notcontains $_ })
    Check -File '(log)' -What 'new HDR columns present in the log schema' -Ok ($missing.Count -eq 0) `
          -Detail ("missing: {0}" -f ($missing -join ', '))

    $newRows = @($rows | Select-Object -Last ([Math]::Max(1, $rows.Count - $logBefore)))
    $populated = @($newRows | Where-Object { -not [string]::IsNullOrWhiteSpace($_.HdrPlanSummary) })
    Check -File '(log)' -What 'HdrPlanSummary populated on this run''s rows' -Ok ($populated.Count -gt 0) `
          -Detail ("{0} of {1} new rows have it" -f $populated.Count, $newRows.Count)

    Write-Host ""
    Say  "  This run's rows:" 'DarkGray'
    foreach ($r in $newRows) {
        Say ("    {0,-12} {1,-16} {2}" -f $r.Status, $r.HdrTargetFormat, [System.IO.Path]::GetFileName($r.InputPath)) 'DarkGray'
        if ($r.HdrPlanSummary) { Say ("                 {0}" -f $r.HdrPlanSummary) 'DarkGray' }
    }
}

# ---------------------------------------------------------------------------
Head "Result"

$skipped = @($findings | Where-Object { $_.Result -eq 'SKIPPED' }).Count
Say ("  {0} passed   {1} failed   {2} warnings   {3} file(s) skipped by the script" -f $pass, $fail, $warn, $skipped) $(if ($fail -gt 0) { 'Red' } else { 'Green' })
if ($skipped -gt 0) {
    Say  "  Skipped files were not tested -- nothing was encoded for them. Re-run those" 'Cyan'
    Say  "  classes with -NoTrim if you want them covered." 'Cyan'
}

if ($fail -gt 0) {
    Write-Host ""
    Say  "  Failures:" 'Red'
    foreach ($f in @($findings | Where-Object { $_.Result -eq 'FAIL' })) {
        Say ("    {0}: {1}" -f $f.File, $f.Check) 'Red'
        if ($f.Detail) { Say ("      {0}" -f $f.Detail) 'DarkGray' }
    }
    Write-Host ""
    Say  "  DO NOT run this over your library yet." 'Red'
} else {
    Write-Host ""
    Say  "  No failures. The encode path is behaving correctly on every class tested." 'Green'
    Say  "  Reasonable next step: set `$ReplaceOriginal = `$false and run a small real" 'Green'
    Say  "  batch, eyeball a couple of HDR outputs on the TV, then enable replacement." 'Green'
}

$csvOut = Join-Path $SandboxRoot 'verify_results.csv'
$findings | Export-Csv -LiteralPath $csvOut -NoTypeInformation -Encoding UTF8
Write-Host ""
Say ("  detail: {0}" -f $csvOut) 'DarkGray'

if ($KeepSandbox) {
    Say ("  sandbox kept for inspection: {0}" -f $SandboxRoot) 'Cyan'
} else {
    Say  "  (pass -KeepSandbox to keep the encoded outputs for viewing)" 'DarkGray'
    # Results CSV is preserved even when the media is not.
    $keepCsv = Join-Path $PSScriptRoot ("verify_results_{0}.csv" -f (Get-Date -Format 'yyyyMMdd_HHmmss'))
    Copy-Item -LiteralPath $csvOut -Destination $keepCsv -Force -ErrorAction SilentlyContinue
    Say ("  results copied to: {0}" -f $keepCsv) 'DarkGray'
    Remove-Item -LiteralPath $SandboxRoot -Recurse -Force -ErrorAction SilentlyContinue
}
Write-Host ""
exit $(if ($fail -gt 0) { 1 } else { 0 })
