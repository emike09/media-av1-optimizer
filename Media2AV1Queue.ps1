#requires -Version 7.0
# =============================================================================
# PlexAV1Queue.ps1
#
# Drag-and-drop AV1 batch encoder for Plex libraries.
#
# Drop one or more video files onto PlexAV1Queue.bat. Each file is added to a
# filesystem queue under .queue\pending\. A machine-global named mutex ensures
# only one worker runs at a time — if an encoder session is already active,
# newly dropped files are silently appended to the queue and will be picked up
# when the current job finishes.
#
# Per-file workflow:
#   1. ffprobe inspects the source and returns full stream/format metadata.
#   2. Stream selection picks the best English audio, an optional lossy
#      fallback, main English subtitles, and an optional SDH track. All other
#      streams (commentary, foreign, junk) are discarded.
#   3. HDR / Dolby Vision detection decides the colour-space output flags.
#      DV sources are skipped by default to avoid destroying DV metadata.
#   4. ffmpeg re-encodes video to AV1 (libsvtav1) and muxes into MKV. Audio
#      and subtitles are stream-copied. Progress is read from stderr in real
#      time and rendered as a live console UI with a queue sidebar.
#   5. The output is verified (duration check), the original is replaced or
#      backed up, and the result is appended to encode_log.csv.
#
# Requirements:
#   - PowerShell 7.0+  (pwsh.exe)  — ships with .NET 5+
#   - ffmpeg / ffprobe 8.x+        — placed next to the script or on PATH
#
# =============================================================================
[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$InputPaths
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ErrorView = 'NormalView'

# Supports bare `Auto` in the config block so users can write either:
#   $CRF = Auto
# or
#   $CRF = 'Auto'
# In expression context PowerShell otherwise tries to invoke `Auto` as a
# command before our later config validation can normalize it.
function Auto {
    return 'Auto'
}

# =============================================================================
# User-configurable settings
# =============================================================================

# Encoding quality / speed
# Each setting accepts either an integer, bare Auto, or the string 'Auto'.
$CRF       = Auto   # SVT-AV1 CRF. Lower = better quality, larger file. Range 0-63, or 'Auto'.
$Preset    = Auto    # SVT-AV1 preset. Lower = slower encode, higher efficiency. Range 0-13, or 'Auto'.
$AutoCRFOffset = Auto  # Applies only when CRF='Auto'. Integer offset added to the resolved auto CRF, or 'Auto' for 0.

# Film grain synthesis
# AV1 supports storing a compact grain model in the bitstream instead of encoding
# the actual grain pixel-by-pixel. The decoder regenerates perceptually identical
# grain at playback, which can dramatically reduce file size for noisy or grainy
# sources (film grain, heavy ISO noise) with no visible quality loss.
#
# $FilmGrain sets the SVT-AV1 film-grain parameter (0-50):
#   0       Disabled. Grain is encoded literally. Best for clean CGI/animation.
#   4-8     Light grain. Good starting point for modern digital cinema releases.
#   8-15    Moderate grain. Typical for Blu-ray film transfers.
#   15-25   Heavy grain. Use for visibly grainy film sources (e.g. 70s/80s film,
#           high-ISO documentary footage, or notoriously grainy titles like
#           Saving Private Ryan, The Revenant, or Hereditary).
#   25-50   Extreme grain. Rarely needed; use only for severely degraded sources.
#
# When in doubt, start at 8 and adjust based on the source. Setting this too high
# on a clean source introduces artificial noise; too low on a grainy source just
# means the encoder wastes bits trying to reproduce random noise pixel-by-pixel.
#
# This is the single highest-impact setting for oversized encodes of grain-heavy
# content. A title that produces 70 GiB at FilmGrain=0 may produce 20-25 GiB at
# FilmGrain=12 with identical perceptual quality on a calibrated display. 
# Effect on encoding speed: Without film grain synthesis (FilmGrain=0), the encoder 
#tries to preserve every grain pixel. With film grain synthesis (FilmGrain > 0), 
#the encoder removes grain during encoding and stores a compact grain model.
#The decoder later reconstructs this grain. Gains on encoding speed can be up to 30%. 
$FilmGrain = Auto    # 0 = disabled. Range 0-50, or 'Auto'. See notes above for guidance.

# Source handling
$SkipDolbyVisionSources = $true    # Skip DV sources rather than silently destroying DV metadata.
$KeepBackupOriginal     = $false   # $true  -> move original to .queue\backup_originals\ after encode.
                                   # $false -> delete original after a verified successful encode.
$ReplaceOriginal        = $true    # Replace the source with the finished AV1-named .mkv when enabled.

# Stream selection
$KeepEnglishSDH           = $false  # Retain an SDH subtitle track alongside the main subtitle.
$KeepEnglishFallbackAudio = $true  # Retain a secondary lossy English audio track (e.g. stereo AAC)
                                   # when the main track is lossless. Excluded if same codec as main.
# =============================================================================
# End of User-configurable settings
# =============================================================================

# Queue / log paths  (all relative to the script's own directory)
$QueueRoot       = Join-Path $PSScriptRoot ".queue"
$QueuePendingDir = Join-Path $QueueRoot "pending"
$QueueWorkingDir = Join-Path $QueueRoot "working"
$BackupDir       = Join-Path $QueueRoot "backup_originals"
$LogPath         = Join-Path $QueueRoot "encode_log.csv"
$StatePath       = Join-Path $QueueRoot "current_job.json"

$LogColumns = @(
    'Timestamp',
    'Status',
    'InputPath',
    'OutputPath',
    'SourceSizeGiB',
    'OutputSizeGiB',
    'ReductionPercent',
    'SourceDurationSec',
    'OutputDurationSec',
    'ElapsedSec',
    'Profile',
    'HasHDR',
    'HasDV',
    'SelectedAudio',
    'SelectedSubtitles',
    'CRF',
    'Preset',
    'FilmGrain',
    'AutoCRFOffset',
    'ResolvedCRF',
    'ResolvedPreset',
    'ResolvedFilmGrain',
    'AutoReason',
    'BPP',
    'EffectiveVideoBitrate',
    'VideoBitratePerHourGiB',
    'ResolutionTier',
    'CodecClass',
    'GrainClass',
    'GrainScore',
    'WasAutoSkipped',
    'FfmpegPath',
    'FfprobePath',
    'Notes'
)

# Named mutex used to enforce single-worker execution.
# The "Global\" prefix makes it machine-wide so it works across all console
# sessions and UAC boundaries. Each script directory keeps its own .queue
# folder, so two separate copies of this script queue independently but will
# never encode simultaneously.
$MutexName = "Global\PlexAV1QueueMutex"

# =============================================================================
# Tool discovery
# Prefers ffmpeg / ffprobe placed next to the script (portable deployment),
# then falls back to whatever is on PATH.
# =============================================================================
$FfmpegPath  = Join-Path $PSScriptRoot "ffmpeg.exe"
$FfprobePath = Join-Path $PSScriptRoot "ffprobe.exe"

if (-not (Test-Path -LiteralPath $FfmpegPath)) {
    $ffmpegCmd = Get-Command ffmpeg -ErrorAction SilentlyContinue
    if (-not $ffmpegCmd) { throw "ffmpeg.exe not found next to script or in PATH." }
    $FfmpegPath = $ffmpegCmd.Source
}

if (-not (Test-Path -LiteralPath $FfprobePath)) {
    $ffprobeCmd = Get-Command ffprobe -ErrorAction SilentlyContinue
    if (-not $ffprobeCmd) { throw "ffprobe.exe not found next to script or in PATH." }
    $FfprobePath = $ffprobeCmd.Source
}

# =============================================================================
# Queue directory and log initialisation
# Creates the directory tree on first run, and writes the CSV header row if
# the log file does not yet exist.
# =============================================================================
$null = New-Item -ItemType Directory -Force -Path $QueueRoot, $QueuePendingDir, $QueueWorkingDir, $BackupDir

if (-not (Test-Path -LiteralPath $LogPath)) {
    ($LogColumns -join ",") |
        Set-Content -LiteralPath $LogPath -Encoding UTF8
}

# =============================================================================
# FUNCTION: Write-LogRow
#
# Appends one result row to encode_log.csv.
#
# Accepts a flat hashtable whose keys match the CSV columns defined in the
# header above. Values are serialised to quoted CSV fields with internal
# double-quotes escaped per RFC 4180. The [ordered] intermediate ensures
# column order is stable regardless of hashtable insertion order.
# =============================================================================
function Write-LogRow {
    param(
        [hashtable]$Row
    )

    $ordered = [ordered]@{}
    foreach ($column in $LogColumns) {
        $ordered[$column] = if ($Row.ContainsKey($column)) { $Row[$column] } else { "" }
    }

    $line = ($ordered.Values | ForEach-Object {
        $s = [string]$_
        '"' + ($s -replace '"', '""') + '"'
    }) -join ","

    Add-Content -LiteralPath $LogPath -Value $line -Encoding UTF8
}

function Convert-ToInvariantDouble {
    param($Value, [double]$Default = 0.0)
    if ($null -eq $Value) { return $Default }

    $parsed = 0.0
    if ([double]::TryParse(
            ([string]$Value),
            [System.Globalization.NumberStyles]::Any,
            [System.Globalization.CultureInfo]::InvariantCulture,
            [ref]$parsed)) {
        return $parsed
    }

    return $Default
}

function Convert-ToInvariantInt64 {
    param($Value, [int64]$Default = 0)
    if ($null -eq $Value) { return $Default }

    $parsed = 0L
    if ([int64]::TryParse(
            ([string]$Value),
            [System.Globalization.NumberStyles]::Any,
            [System.Globalization.CultureInfo]::InvariantCulture,
            [ref]$parsed)) {
        return $parsed
    }

    return $Default
}

function Resolve-ConfigValue {
    param(
        [string]$Name,
        $Value,
        [int]$Minimum,
        [int]$Maximum
    )

    if ($Value -is [string] -and $Value.Trim().ToLowerInvariant() -eq 'auto') {
        return 'Auto'
    }

    $text = [string]$Value
    $parsed = 0
    if (-not [int]::TryParse($text, [ref]$parsed)) {
        throw "$Name must be an integer from $Minimum to $Maximum, or 'Auto'. Current value: $Value"
    }

    if ($parsed -lt $Minimum -or $parsed -gt $Maximum) {
        throw "$Name must be between $Minimum and $Maximum, or 'Auto'. Current value: $Value"
    }

    return $parsed
}

function Resolve-OffsetConfigValue {
    param(
        [string]$Name,
        $Value
    )

    if ($Value -is [string] -and $Value.Trim().ToLowerInvariant() -eq 'auto') {
        return 'Auto'
    }

    $text = [string]$Value
    $parsed = 0
    if (-not [int]::TryParse($text, [ref]$parsed)) {
        throw "$Name must be an integer or 'Auto'. Current value: $Value"
    }

    return $parsed
}

function Update-LogSchemaIfNeeded {
    if (-not (Test-Path -LiteralPath $LogPath)) { return }

    $expectedHeader = $LogColumns -join ","
    $currentHeader  = Get-Content -LiteralPath $LogPath -TotalCount 1 -ErrorAction SilentlyContinue
    if ($currentHeader -eq $expectedHeader) { return }

    $existingRows = @()
    try {
        $existingRows = @(Import-Csv -LiteralPath $LogPath)
    } catch {
        Write-Warning "Could not migrate existing log schema. Appending with the new schema may misalign old rows. Error: $_"
        return
    }

    $rewritten = [System.Collections.Generic.List[string]]::new()
    $rewritten.Add($expectedHeader)

    foreach ($row in $existingRows) {
        $values = foreach ($column in $LogColumns) {
            $prop = $row.PSObject.Properties[$column]
            $s = if ($null -ne $prop) { [string]$prop.Value } else { "" }
            '"' + ($s -replace '"', '""') + '"'
        }
        $rewritten.Add(($values -join ","))
    }

    Set-Content -LiteralPath $LogPath -Value $rewritten -Encoding UTF8
}

# =============================================================================
# FUNCTION: Get-NormalizedPath
#
# Returns the absolute, canonicalised path for a given input string.
#
# Uses Resolve-Path to expand symlinks and relative components where the path
# already exists on disk, then passes the result through GetFullPath for final
# normalisation. Falls back to GetFullPath alone for paths that do not yet
# exist (e.g. the intended output path before the file is created).
# =============================================================================
function Get-NormalizedPath {
    param([string]$Path)
    try {
        return [System.IO.Path]::GetFullPath((Resolve-Path -LiteralPath $Path).Path)
    } catch {
        return [System.IO.Path]::GetFullPath($Path)
    }
}

# =============================================================================
# FUNCTION: Get-QueueKey
#
# Derives a stable, unique identifier for a file path.
#
# Returns a lowercase hex SHA-256 hash of the lowercased UTF-8 path. The key
# is embedded in the queue JSON filename so duplicate submissions can be
# detected without reading file content. SHA256::HashData() is a .NET 5+
# static method, which is guaranteed by the #requires -Version 7.0 directive.
# =============================================================================
function Get-QueueKey {
    param([string]$FullPath)
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($FullPath.ToLowerInvariant())
    $hash  = [System.Security.Cryptography.SHA256]::HashData($bytes)
    return ([System.BitConverter]::ToString($hash) -replace '-', '').ToLowerInvariant()
}

# =============================================================================
# FUNCTION: Get-ExistingQueuedPaths
#
# Returns a case-insensitive HashSet of all input paths currently tracked by
# the queue system: files in .queue\pending\, files in .queue\working\, and
# the file recorded in current_job.json (the actively-encoding job, if any).
#
# Used by Add-QueueInputs to guard against submitting the same file twice.
# =============================================================================
function Get-ExistingQueuedPaths {
    $paths = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)

    foreach ($dir in @($QueuePendingDir, $QueueWorkingDir)) {
        if (Test-Path -LiteralPath $dir) {
            Get-ChildItem -LiteralPath $dir -Filter *.json -File -ErrorAction SilentlyContinue | ForEach-Object {
                try {
                    $job = Get-Content -LiteralPath $_.FullName -Raw | ConvertFrom-Json
                    if ($job.InputPath) { $null = $paths.Add($job.InputPath) }
                } catch {}
            }
        }
    }

    if (Test-Path -LiteralPath $StatePath) {
        try {
            $state = Get-Content -LiteralPath $StatePath -Raw | ConvertFrom-Json
            if ($state.InputPath) { $null = $paths.Add($state.InputPath) }
        } catch {}
    }

    return ,$paths
}

# =============================================================================
# FUNCTION: Add-QueueInputs
#
# Validates and enqueues one or more input file paths.
#
# For each path:
#   - Skips blank entries, missing paths, and directories.
#   - Resolves to a canonical absolute path.
#   - Skips if already present in pending, working, or the active job.
#   - Writes a small JSON job descriptor to .queue\pending\ with a timestamp-
#     prefixed filename (guarantees FIFO ordering by filesystem creation time)
#     and the SHA-256 queue key embedded for deduplication.
# =============================================================================
function Add-QueueInputs {
    param([string[]]$Paths)

    $existing = Get-ExistingQueuedPaths
	if ($null -eq $existing) {
    $existing = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
}

    foreach ($p in $Paths) {
        if ([string]::IsNullOrWhiteSpace($p)) { continue }
        if (-not (Test-Path -LiteralPath $p)) {
            Write-Warning "Path not found, skipping: $p"
            continue
        }

        $item = Get-Item -LiteralPath $p
        if ($item.PSIsContainer) {
            Write-Warning "Folders are not supported for drag-drop queueing, skipping: $($item.FullName)"
            continue
        }

        $full = Get-NormalizedPath -Path $item.FullName

        if ($existing.Contains($full)) {
            Write-Host "Already queued or currently processing: $full" -ForegroundColor Yellow
            continue
        }

        $key     = Get-QueueKey -FullPath $full
        $jobPath = Join-Path $QueuePendingDir ("{0}_{1}.json" -f (Get-Date -Format "yyyyMMdd_HHmmss_fff"), $key)

        $job = [ordered]@{
            InputPath   = $full
            EnqueuedUtc = [DateTime]::UtcNow.ToString("o")
            QueueKey    = $key
        } | ConvertTo-Json -Depth 4

        Set-Content -LiteralPath $jobPath -Value $job -Encoding UTF8
        $null = $existing.Add($full)

        Write-Host "Queued: $full" -ForegroundColor Cyan
    }
}

# =============================================================================
# FUNCTION: Invoke-FfprobeJson
#
# Runs ffprobe against a file and returns the parsed JSON as a PSCustomObject.
#
# Requests format, stream, and chapter metadata at maximum JSON depth. The
# -v error flag suppresses ffprobe's own progress output so only the JSON
# payload reaches stdout. Arguments are passed as an explicit array so
# PowerShell handles path quoting correctly for all characters in the path.
# =============================================================================
function Invoke-FfprobeJson {
    param([string]$InputPath)

    # Pass arguments as an explicit array so PowerShell handles quoting
    # for the path correctly. The --% stop-parsing token only works inline
    # on the same logical line as the command; it does not survive backtick
    # line continuations and would be passed to ffprobe as a literal string
    # argument, causing an 'Invalid argument' error on the path.
    $ffprobeArgs = @(
        '-v',            'error',
        '-print_format', 'json',
        '-show_format',
        '-show_streams',
        '-show_chapters',
        $InputPath
    )

    $json = & $FfprobePath @ffprobeArgs

    if (-not $json) {
        throw "ffprobe returned no output for: $InputPath"
    }

    return ($json | ConvertFrom-Json -Depth 100)
}

# =============================================================================
# FUNCTION GROUP: Stream property helpers
#
# Small, focused helpers that extract or test a single property of an ffprobe
# stream object. Used throughout Select-Streams to keep the selection logic
# readable. All functions accept a raw stream object from the ffprobe JSON.
#
#   Get-StreamLanguage  - Returns the BCP-47 language tag in lower case,
#                         or an empty string if absent.
#   Get-StreamTitle     - Returns the track title tag, or an empty string.
#   Test-IsEnglish      - True for eng / en / english and untagged streams.
#   Test-IsCommentary   - True if the title contains "commentary".
#   Test-IsSDH          - True for tracks tagged SDH, "hearing impaired",
#                         or the standalone abbreviation "HI".
#   Test-IsForced       - True if the title contains "forced" or the
#                         disposition.forced flag is set.
# =============================================================================
function Get-OptionalProperty {
    param(
        $InputObject,
        [string]$PropertyName,
        $Default = $null
    )

    if ($null -eq $InputObject) { return $Default }

    $prop = $InputObject.PSObject.Properties[$PropertyName]
    if ($null -eq $prop) { return $Default }
    if ($null -eq $prop.Value) { return $Default }
    return $prop.Value
}

function Get-StreamTagValue {
    param(
        $Stream,
        [string]$Name,
        [string]$Default = ''
    )

    $tags = Get-OptionalProperty -InputObject $Stream -PropertyName 'tags' -Default $null
    if ($null -eq $tags) { return $Default }

    $value = Get-OptionalProperty -InputObject $tags -PropertyName $Name -Default $Default
    if ([string]::IsNullOrWhiteSpace([string]$value)) { return $Default }
    return [string]$value
}

function Get-StreamLanguage {
    param($Stream)
    $lang = Get-StreamTagValue -Stream $Stream -Name 'language' -Default ''
    if ([string]::IsNullOrWhiteSpace($lang)) { return "" }
    return $lang.ToLowerInvariant()
}

function Get-StreamTitle {
    param($Stream)
    return (Get-StreamTagValue -Stream $Stream -Name 'title' -Default '')
}

# =============================================================================
# FUNCTION: Get-StreamProp
#
# Safely reads an optional top-level property from an ffprobe stream object.
#
# Set-StrictMode -Version Latest throws a terminating error when code accesses
# a property that does not exist on a PSCustomObject, which is what
# ConvertFrom-Json produces. Many ffprobe stream fields (bit_rate, channels,
# codec_name, codec_tag_string, etc.) are simply absent from the JSON when
# ffprobe has no value to report -- they are not present as null, they are
# missing entirely. The null-coalescing operator (??) cannot help here because
# the throw happens before ?? can evaluate.
#
# This helper uses PSObject.Properties to look up the key without triggering
# strict mode, then returns $Default if the property is absent or null.
# =============================================================================
function Get-StreamProp {
    param($Stream, [string]$Name, $Default = $null)
    return Get-OptionalProperty -InputObject $Stream -PropertyName $Name -Default $Default
}

function Get-StreamBitRate {
    param(
        $Stream,
        [double]$DurationSec = 0.0
    )

    return (Get-StreamBitrateEstimate -Stream $Stream -DurationSec $DurationSec).Bitrate
}

function Get-StreamChannels {
    param($Stream)
    return [int](Convert-ToInvariantInt64 (Get-StreamProp $Stream 'channels' 0) 0)
}

function Get-StreamSideDataList {
    param($Stream)
    $sideData = Get-StreamProp $Stream 'side_data_list' $null
    if ($null -eq $sideData) { return ,@() }
    return ,@($sideData)
}

function Get-StreamBitrateEstimate {
    param(
        $Stream,
        [double]$DurationSec = 0.0
    )

    $streamBitrate = Convert-ToInvariantInt64 (Get-StreamProp $Stream 'bit_rate' $null) 0
    if ($streamBitrate -gt 0) {
        return [ordered]@{
            Bitrate     = $streamBitrate
            Method      = 'stream.bit_rate'
            Approximate = $false
            Reason      = 'Used stream.bit_rate from ffprobe.'
        }
    }

    foreach ($tagName in @('BPS', 'BPS-eng')) {
        $tagBitrate = Convert-ToInvariantInt64 (Get-StreamTagValue $Stream $tagName '') 0
        if ($tagBitrate -gt 0) {
            return [ordered]@{
                Bitrate     = $tagBitrate
                Method      = "stream.tags.$tagName"
                Approximate = $false
                Reason      = "Used stream tag $tagName from ffprobe."
            }
        }
    }

    if ($DurationSec -gt 0) {
        foreach ($tagName in @('NUMBER_OF_BYTES', 'NUMBER_OF_BYTES-eng')) {
            $numBytes = Convert-ToInvariantInt64 (Get-StreamTagValue $Stream $tagName '') 0
            if ($numBytes -gt 0) {
                $tagDerivedBitrate = [int64][Math]::Round(($numBytes * 8.0) / $DurationSec)
                if ($tagDerivedBitrate -gt 0) {
                    return [ordered]@{
                        Bitrate     = $tagDerivedBitrate
                        Method      = "stream.tags.$tagName/duration"
                        Approximate = $false
                        Reason      = "Derived bitrate from stream tag $tagName and container duration."
                    }
                }
            }
        }
    }

    return [ordered]@{
        Bitrate     = 0
        Method      = 'unavailable'
        Approximate = $true
        Reason      = 'No stream-level bitrate metadata was available.'
    }
}

function Get-EffectiveVideoBitrate {
    param(
        $Probe,
        $VideoStream,
        [object[]]$KeptAudioStreams = @()
    )

    $format      = Get-OptionalProperty -InputObject $Probe -PropertyName 'format' -Default ([PSCustomObject]@{})
    $durationSec = Convert-ToInvariantDouble (Get-OptionalProperty $format 'duration' 0) 0.0

    $streamEstimate = Get-StreamBitrateEstimate -Stream $VideoStream -DurationSec $durationSec
    if ($streamEstimate.Bitrate -gt 0) {
        return [ordered]@{
            Bitrate     = $streamEstimate.Bitrate
            Method      = $streamEstimate.Method
            Approximate = $streamEstimate.Approximate
            Reason      = $streamEstimate.Reason
        }
    }

    $formatBitrate = Convert-ToInvariantInt64 (Get-OptionalProperty $format 'bit_rate' $null) 0
    if ($formatBitrate -le 0) {
        return [ordered]@{
            Bitrate     = 0
            Method      = 'unavailable'
            Approximate = $true
            Reason      = 'Could not derive a usable video bitrate from ffprobe metadata.'
        }
    }

    $audioBitrateSum = 0L
    $audioBitrateCount = 0
    foreach ($audioStream in @($KeptAudioStreams | Where-Object { $_ })) {
        $audioEstimate = Get-StreamBitrateEstimate -Stream $audioStream -DurationSec $durationSec
        if ($audioEstimate.Bitrate -gt 0) {
            $audioBitrateSum += [int64]$audioEstimate.Bitrate
            $audioBitrateCount++
        }
    }

    if ($audioBitrateCount -gt 0) {
        return [ordered]@{
            Bitrate     = [int64][Math]::Max(1.0, $formatBitrate - $audioBitrateSum)
            Method      = 'format.bit_rate-minus-kept-audio'
            Approximate = $true
            Reason      = "Used container bit_rate minus $audioBitrateCount kept audio stream bitrate estimate(s)."
        }
    }

    return [ordered]@{
        Bitrate     = $formatBitrate
        Method      = 'format.bit_rate'
        Approximate = $true
        Reason      = 'Used container bit_rate as an approximate video bitrate because stream-level bitrate was unavailable.'
    }
}

function Get-FrameRateValue {
    param([string]$FrameRateText)
    if ([string]::IsNullOrWhiteSpace($FrameRateText)) { return 0.0 }

    $parts = $FrameRateText.Split('/', 2)
    if ($parts.Count -eq 2) {
        $num = Convert-ToInvariantDouble $parts[0] 0.0
        $den = Convert-ToInvariantDouble $parts[1] 0.0
        if ($num -gt 0 -and $den -gt 0) {
            return ($num / $den)
        }
    }

    return Convert-ToInvariantDouble $FrameRateText 0.0
}

function Get-FrameRate {
    param($Stream)

    $avg = Get-FrameRateValue -FrameRateText ([string](Get-StreamProp $Stream 'avg_frame_rate' ''))
    if ($avg -gt 0) { return $avg }

    $raw = Get-FrameRateValue -FrameRateText ([string](Get-StreamProp $Stream 'r_frame_rate' ''))
    if ($raw -gt 0) { return $raw }

    return 0.0
}

function Get-BitsPerPixelPerFrame {
    param(
        [double]$VideoBitrate,
        [int]$Width,
        [int]$Height,
        [double]$FrameRate
    )

    if ($VideoBitrate -le 0 -or $Width -le 0 -or $Height -le 0 -or $FrameRate -le 0) {
        return 0.0
    }

    return ($VideoBitrate / ($Width * $Height * $FrameRate))
}

function Get-ResolutionTier {
    param([int]$Width)
    if ($Width -lt 1280) { return 'SD' }
    if ($Width -lt 2560) { return 'HD' }
    return 'UHD'
}

function Get-CodecClass {
    param($Stream)

    $codec = ([string](Get-StreamProp $Stream 'codec_name' '')).ToLowerInvariant()
    switch ($codec) {
        { $_ -in @('mpeg2video', 'vc1', 'mpeg4', 'msmpeg4v3', 'h263', 'rv40', 'rv30') } { return 'legacy' }
        { $_ -in @('h264', 'avc1') }                                                   { return 'standard' }
        { $_ -in @('hevc', 'h265', 'av1', 'vp9') }                                     { return 'modern' }
        default                                                                        { return 'standard' }
    }
}

function Get-VideoBitratePerHourGiB {
    param([double]$VideoBitrate)
    if ($VideoBitrate -le 0) { return 0.0 }
    return (($VideoBitrate * 3600.0) / 8.0 / 1GB)
}

function Get-VideoBitDepth {
    param($Stream)

    $bitsPerRawSample = Convert-ToInvariantInt64 (Get-StreamProp $Stream 'bits_per_raw_sample' $null) 0
    if ($bitsPerRawSample -gt 0) { return [int]$bitsPerRawSample }

    $pixFmt = ([string](Get-StreamProp $Stream 'pix_fmt' '')).ToLowerInvariant()
    if ($pixFmt -match '12') { return 12 }
    if ($pixFmt -match '10') { return 10 }
    if ($pixFmt -match '9')  { return 9 }
    if ($pixFmt)             { return 8 }

    return 0
}

function Get-ColorPrimariesLabel {
    param([string]$Value)

    switch (($Value ?? '').ToLowerInvariant()) {
        'bt709'     { return 'Rec.709' }
        'bt2020'    { return 'Rec.2020' }
        'smpte170m' { return 'Rec.601' }
        'bt470bg'   { return 'Rec.601' }
        default {
            if ([string]::IsNullOrWhiteSpace($Value)) { return 'Unknown primaries' }
            return $Value
        }
    }
}

function Get-TransferLabel {
    param([string]$Value)

    switch (($Value ?? '').ToLowerInvariant()) {
        'bt709'         { return 'BT.709' }
        'smpte2084'     { return 'PQ' }
        'arib-std-b67'  { return 'HLG' }
        'bt2020-10'     { return 'BT.2020 10-bit' }
        'linear'        { return 'Linear' }
        default {
            if ([string]::IsNullOrWhiteSpace($Value)) { return 'Unknown transfer' }
            return $Value
        }
    }
}

function Get-MatrixLabel {
    param([string]$Value)

    switch (($Value ?? '').ToLowerInvariant()) {
        'bt709'    { return 'Rec.709' }
        'bt2020nc' { return 'Rec.2020 NC' }
        'bt2020c'  { return 'Rec.2020 C' }
        'smpte170m'{ return 'Rec.601' }
        'bt470bg'  { return 'Rec.601' }
        default {
            if ([string]::IsNullOrWhiteSpace($Value)) { return 'Unknown matrix' }
            return $Value
        }
    }
}

function Get-ColorSummary {
    param(
        [int]$BitDepth,
        [string]$DynamicRangeLabel,
        [string]$PrimariesLabel,
        [string]$TransferLabel
    )

    $depthLabel = if ($BitDepth -gt 0) { "$BitDepth-bit" } else { 'Unknown bit depth' }
    return "$depthLabel | $DynamicRangeLabel | $PrimariesLabel / $TransferLabel"
}

function Get-EncodeColorProfile {
    param($SourceProfile)

    $encodeBitDepth = 10
    if ($SourceProfile.HasHDR) {
        $encodeDynamicRange = 'HDR10'
        $encodePrimaries = 'Rec.2020'
        $encodeTransfer = 'PQ'
        $encodeMatrix = 'Rec.2020 NC'
        $note = if ($SourceProfile.SourceHdrFormat -eq 'HDR10+') {
            'Source HDR10+ dynamic metadata is not preserved; encode is labeled HDR10.'
        } elseif ($SourceProfile.SourceHdrFormat -eq 'HLG') {
            'Current encode path tags HLG sources as PQ/HDR10 for AV1 output.'
        } elseif ($SourceProfile.Profile -eq 'DV') {
            'Dolby Vision metadata is not preserved by this AV1 encode path.'
        } else {
            ''
        }
    } else {
        $encodeDynamicRange = 'SDR'
        $encodePrimaries = if ($SourceProfile.SourcePrimariesLabel -and $SourceProfile.SourcePrimariesLabel -ne 'Unknown primaries') {
            $SourceProfile.SourcePrimariesLabel
        } else {
            'Rec.709'
        }
        $encodeTransfer = if ($SourceProfile.SourceTransferLabel -and $SourceProfile.SourceTransferLabel -ne 'Unknown transfer') {
            $SourceProfile.SourceTransferLabel
        } else {
            'BT.709'
        }
        $encodeMatrix = if ($SourceProfile.SourceMatrixLabel -and $SourceProfile.SourceMatrixLabel -ne 'Unknown matrix') {
            $SourceProfile.SourceMatrixLabel
        } else {
            'Rec.709'
        }
        $note = ''
    }

    return [ordered]@{
        BitDepth           = $encodeBitDepth
        DynamicRangeLabel  = $encodeDynamicRange
        PrimariesLabel     = $encodePrimaries
        TransferLabel      = $encodeTransfer
        MatrixLabel        = $encodeMatrix
        Summary            = Get-ColorSummary -BitDepth $encodeBitDepth -DynamicRangeLabel $encodeDynamicRange -PrimariesLabel $encodePrimaries -TransferLabel $encodeTransfer
        Note               = $note
    }
}

function Get-BppTier {
    param([double]$Bpp)

    if ($Bpp -le 0)      { return 'unknown' }
    if ($Bpp -lt 0.06)   { return 'low' }
    if ($Bpp -le 0.15)   { return 'medium' }
    return 'high'
}

function Get-CodecLabel {
    param($Stream)

    $codec = ([string](Get-StreamProp $Stream 'codec_name' '')).ToLowerInvariant()
    switch ($codec) {
        'h264'       { return 'AVC' }
        'avc1'       { return 'AVC' }
        'hevc'       { return 'HEVC' }
        'h265'       { return 'HEVC' }
        'av1'        { return 'AV1' }
        'mpeg2video' { return 'MPEG-2' }
        'vc1'        { return 'VC-1' }
        default {
            if ([string]::IsNullOrWhiteSpace($codec)) { return 'Unknown' }
            return $codec.ToUpperInvariant()
        }
    }
}

function Test-IsEnglish {
    param($Stream)
    $lang = Get-StreamLanguage -Stream $Stream
    return $lang -in @("eng", "en", "english", "")
}

function Test-IsCommentary {
    param($Stream)
    $title = (Get-StreamTitle -Stream $Stream).ToLowerInvariant()
    return $title -match 'commentary'
}

function Test-IsSDH {
    param($Stream)
    # Matches the SDH tag, the phrase "hearing impaired" (with optional
    # separators between the two words), and the standalone abbreviation "HI".
    # Word-boundary anchors prevent false matches on unrelated titles.
    $title = (Get-StreamTitle -Stream $Stream).ToLowerInvariant()
    return $title -match '\bsdh\b|\bhearing[.\s_-]*impaired\b|\bhi\b'
}

function Test-IsForced {
    param($Stream)
    $title = (Get-StreamTitle -Stream $Stream).ToLowerInvariant()
    if ($title -match '\bforced\b') { return $true }
    # disposition and its sub-keys may be absent on some stream types.
    # Use Get-StreamProp to retrieve the disposition object safely, then
    # access .forced via PSObject.Properties to avoid a second strict-mode throw.
    $disp = Get-StreamProp $Stream 'disposition' $null
    if ($null -ne $disp) {
        $forced = $disp.PSObject.Properties['forced']?.Value
        if ($forced -eq 1) { return $true }
    }
    return $false
}

# =============================================================================
# FUNCTION: Get-AudioRank
#
# Returns a numeric quality score for an audio stream used to sort candidates.
#
# Scoring tiers (higher is preferred), with channel count added as a tiebreak:
#   1000+  TrueHD / Atmos
#    900+  DTS-HD MA
#    800+  DTS-HD HRA
#    700+  E-AC-3 Atmos / JOC
#    600+  DTS core
#    500+  E-AC-3
#    400+  AC-3
#    300+  AAC
#    100+  Everything else
# =============================================================================
function Get-AudioRank {
    param($Stream)

    $codec    = (Get-StreamProp $Stream 'codec_name' '').ToLowerInvariant()
    $title    = (Get-StreamTitle -Stream $Stream).ToLowerInvariant()
    $channels = Get-StreamChannels -Stream $Stream

    if ($codec -eq 'truehd' -or $title -match 'atmos')                   { return 1000 + $channels }
    if ($codec -eq 'dts'    -and $title -match 'dts-hd ma|master audio') { return  900 + $channels }
    if ($codec -eq 'dts'    -and $title -match 'dts-hd hra')             { return  800 + $channels }
    if ($codec -eq 'eac3'   -and $title -match 'atmos|joc')              { return  700 + $channels }
    if ($codec -eq 'dts')                                                 { return  600 + $channels }
    if ($codec -eq 'eac3')                                                { return  500 + $channels }
    if ($codec -eq 'ac3')                                                 { return  400 + $channels }
    if ($codec -eq 'aac')                                                 { return  300 + $channels }
    return 100 + $channels
}

# =============================================================================
# FUNCTION: Test-IsLossyAudio
#
# Returns $true if the stream uses a lossy audio codec.
#
# Used to restrict fallback audio candidates to lossy tracks only, ensuring
# the fallback provides a meaningfully different option (e.g. a compatibility-
# oriented stereo AAC) rather than a second lossless track.
# =============================================================================
function Test-IsLossyAudio {
    param($Stream)
    $codec = (Get-StreamProp $Stream 'codec_name' '').ToLowerInvariant()
    return $codec -in @('eac3', 'ac3', 'aac', 'dts', 'mp3', 'opus', 'vorbis')
}

# =============================================================================
# FUNCTION: Select-Streams
#
# Analyses ffprobe output and returns the streams to map into the output file.
#
# Returns an ordered hashtable with keys:
#   Video         - First non-cover-art video stream.
#   MainAudio     - Highest-ranked English audio track (see Get-AudioRank).
#   FallbackAudio - Optional secondary lossy English track with a different
#                   codec to main, for broad player compatibility.
#   MainSub       - Best English non-SDH subtitle (forced or default preferred,
#                   SRT preferred over image-based formats).
#   SdhSub        - Optional English SDH subtitle, kept separately so players
#                   can surface it to users who need it.
#
# Audio fallback: if no English audio exists the function falls back to all
# non-commentary tracks so foreign-language content still gets an audio stream.
#
# Subtitle fallback: if no non-SDH subtitle is found, the first English
# subtitle of any kind is used as the main track.
# =============================================================================
function Select-Streams {
    param($Probe)

    $streams = @($Probe.streams)

    $videoStreams = $streams | Where-Object {
        if ((Get-StreamProp $_ 'codec_type' '') -ne 'video') { return $false }
        # Exclude cover-art streams. disposition may be absent; attached_pic
        # defaults to 0 (not a cover) when the key is missing entirely.
        $disp = Get-StreamProp $_ 'disposition' $null
        $attachedPic = if ($null -ne $disp) { $disp.PSObject.Properties['attached_pic']?.Value } else { 0 }
        -not ($attachedPic -eq 1)
    }

    if (-not $videoStreams) {
        throw "No usable video stream found."
    }

    $video          = $videoStreams | Select-Object -First 1
    $audioStreams    = $streams | Where-Object { (Get-StreamProp $_ 'codec_type' '') -eq 'audio' }
    $subtitleStreams = $streams | Where-Object { (Get-StreamProp $_ 'codec_type' '') -eq 'subtitle' }

    $englishAudio = $audioStreams | Where-Object {
        (Test-IsEnglish $_) -and -not (Test-IsCommentary $_)
    }

    if (-not $englishAudio) {
        $englishAudio = $audioStreams | Where-Object { -not (Test-IsCommentary $_) }
    }

    if (-not $englishAudio) {
        throw "No suitable audio streams found."
    }

    $mainAudio = $englishAudio |
        Sort-Object `
            @{ Expression = { Get-AudioRank $_ };                                    Descending = $true },
            @{ Expression = { [int](Get-StreamProp $_ 'channels' 0) };              Descending = $true },
            @{ Expression = { [int64](Get-StreamProp $_ 'bit_rate' 0) };            Descending = $true } |
        Select-Object -First 1

    $fallbackAudio = $null
    if ($KeepEnglishFallbackAudio) {
        # Only consider tracks with a different codec to main, so we don't end up
        # with two EAC3 tracks at different bitrates serving no practical purpose.
        $mainCodec = (Get-StreamProp $mainAudio 'codec_name' '').ToLowerInvariant()

        $fallbackCandidates = $englishAudio | Where-Object {
            (Get-StreamProp $_ 'index' -1) -ne (Get-StreamProp $mainAudio 'index' -2) -and
            -not (Test-IsCommentary $_) -and
            (Test-IsLossyAudio $_) -and
            (Get-StreamProp $_ 'codec_name' '').ToLowerInvariant() -ne $mainCodec
        }

        if ($fallbackCandidates) {
            $fallbackAudio = $fallbackCandidates |
                Sort-Object `
                    @{ Expression = {
                        $codec = (Get-StreamProp $_ 'codec_name' '').ToLowerInvariant()
                        switch ($codec) {
                            'eac3'  { 500 }
                            'ac3'   { 400 }
                            'aac'   { 300 }
                            'dts'   { 200 }
                            default { 100 }
                        }
                    }; Descending = $true },
                    @{ Expression = { [int](Get-StreamProp $_ 'channels' 0) };   Descending = $true },
                    @{ Expression = { [int64](Get-StreamProp $_ 'bit_rate' 0) }; Descending = $true } |
                Select-Object -First 1
        }
    }

    $englishSubs = $subtitleStreams | Where-Object {
        (Test-IsEnglish $_) -and -not (Test-IsCommentary $_)
    }

    $mainSub = $null
    $sdhSub  = $null

    if ($englishSubs) {
        $mainSub = $englishSubs |
            Where-Object { -not (Test-IsSDH $_) } |
            Sort-Object `
                @{ Expression = { if (Test-IsForced $_)              { 100 } else { 0 } }; Descending = $true },
                @{ Expression = {
                    $disp = Get-StreamProp $_ 'disposition' $null
                    if ($null -ne $disp -and $disp.PSObject.Properties['default']?.Value -eq 1) { 50 } else { 0 }
                }; Descending = $true },
                @{ Expression = { if ((Get-StreamProp $_ 'codec_name' '') -eq 'subrip') { 20 } else { 10 } }; Descending = $true } |
            Select-Object -First 1

        if (-not $mainSub) {
            $mainSub = $englishSubs | Select-Object -First 1
        }

        if ($KeepEnglishSDH) {
            $sdhSub = $englishSubs |
                Where-Object { (Get-StreamProp $_ 'index' -1) -ne (Get-StreamProp $mainSub 'index' -2) -and (Test-IsSDH $_) } |
                Select-Object -First 1
        }
    }

    return [ordered]@{
        Video         = $video
        MainAudio     = $mainAudio
        FallbackAudio = $fallbackAudio
        MainSub       = $mainSub
        SdhSub        = $sdhSub
    }
}

# =============================================================================
# FUNCTION: Get-SourceProfile
#
# Detects the HDR / Dolby Vision profile of the source video stream.
#
# Returns an ordered hashtable: { HasDV, HasHDR, Profile }
# Profile is one of "DV", "HDR", or "SDR".
#
# Dolby Vision detection:
#   Checks codec_name and codec_tag_string for the HEVC DV codec identifiers
#   (dvhe, dvav), and scans side_data_list for DOVI/Dolby Vision RPU blocks.
#   Detection is scoped to stream-level fields only to avoid false positives
#   on filenames or metadata titles that happen to contain "Dolby Vision".
#
# HDR10 / HLG detection:
#   smpte2084    - covers HDR10 and HDR10+ (both use PQ transfer).
#   arib-std-b67 - covers HLG.
#   bt2020-10    - a BT.2020 10-bit transfer indicator written by some encoders
#                  that do not explicitly set smpte2084.
#   bt2020 primaries or colour space are also treated as HDR indicators.
#   When HDR is detected the output is tagged with smpte2084/bt2020nc, which
#   is the correct signalling for HDR10 and HDR10+ in an MKV container.
# =============================================================================
function Get-SourceProfile {
    param($Probe, $VideoStream)

    # Dolby Vision detection. Evaluated per-stream rather than as a single
    # pipeline expression because the side_data_list check requires a variable
    # assignment, which is a statement and cannot appear inside a parenthesised
    # boolean expression in PowerShell.
    $hasDV = $false
    foreach ($dvStream in @($Probe.streams)) {
        if ((Get-StreamProp $dvStream 'codec_name'       '') -match 'dvhe|dvav' -or
            (Get-StreamProp $dvStream 'codec_tag_string' '') -match 'dvhe|dvav') {
            $hasDV = $true
            break
        }
        # side_data_list is absent on streams with no side data; retrieve it
        # safely and scan each entry's side_data_type for DOVI/Dolby Vision.
        $sdList = Get-StreamProp $dvStream 'side_data_list' $null
        if ($null -ne $sdList) {
            foreach ($sd in @($sdList)) {
                if ((Get-StreamProp $sd 'side_data_type' '') -match 'DOVI|Dolby Vision') {
                    $hasDV = $true
                    break
                }
            }
        }
        if ($hasDV) { break }
    }

    $hasHDR   = $false
    $hasHDR10Plus = $false
    $transfer  = [string](Get-StreamProp $VideoStream 'color_transfer'  '')
    $primaries = [string](Get-StreamProp $VideoStream 'color_primaries' '')
    $matrix    = [string](Get-StreamProp $VideoStream 'color_space'     '')
    $bitDepth  = Get-VideoBitDepth -Stream $VideoStream

    $videoSideData = Get-StreamSideDataList -Stream $VideoStream
    if ($videoSideData.Count -gt 0) {
        foreach ($sd in $videoSideData) {
            $sideDataType = [string](Get-StreamProp $sd 'side_data_type' '')
            if ($sideDataType -match 'HDR10\+|SMPTE2094-40|Dynamic HDR') {
                $hasHDR10Plus = $true
                break
            }
        }
    }

    if ($transfer  -match 'smpte2084|arib-std-b67|bt2020-10' -or
        $primaries -match 'bt2020' -or
        $matrix    -match 'bt2020') {
        $hasHDR = $true
    }

    $sourceHdrFormat = if ($hasDV) {
        'Dolby Vision'
    } elseif ($transfer -match 'arib-std-b67') {
        'HLG'
    } elseif ($hasHDR10Plus) {
        'HDR10+'
    } elseif ($transfer -match 'smpte2084|bt2020-10' -or $hasHDR) {
        'HDR10'
    } else {
        'SDR'
    }

    $profile = if ($hasDV) { "DV" } elseif ($hasHDR) { "HDR" } else { "SDR" }
    $sourcePrimariesLabel = Get-ColorPrimariesLabel -Value $primaries
    $sourceTransferLabel = Get-TransferLabel -Value $transfer
    $sourceMatrixLabel = Get-MatrixLabel -Value $matrix

    return [ordered]@{
        HasDV               = $hasDV
        HasHDR              = $hasHDR
        HasHDR10Plus        = $hasHDR10Plus
        Profile             = $profile
        SourceHdrFormat     = $sourceHdrFormat
        SourceBitDepth      = $bitDepth
        SourcePrimaries     = $primaries
        SourceTransfer      = $transfer
        SourceMatrix        = $matrix
        SourcePrimariesLabel= $sourcePrimariesLabel
        SourceTransferLabel = $sourceTransferLabel
        SourceMatrixLabel   = $sourceMatrixLabel
        SourceColorSummary  = Get-ColorSummary -BitDepth $bitDepth -DynamicRangeLabel $sourceHdrFormat -PrimariesLabel $sourcePrimariesLabel -TransferLabel $sourceTransferLabel
    }
}

function Test-ShouldRunGrainPreScan {
    param(
        [double]$DurationSec,
        [double]$Bpp,
        [double]$VideoBitratePerHourGiB,
        [string]$ResolutionTier,
        [string]$CodecClass
    )

    if ($DurationSec -le 0) {
        return [ordered]@{ ShouldRun = $false; Reason = 'pre-scan skipped: source duration unavailable' }
    }

    if ($Bpp -gt 0 -and $Bpp -lt 0.03 -and $VideoBitratePerHourGiB -gt 0 -and $VideoBitratePerHourGiB -lt 3.5) {
        return [ordered]@{ ShouldRun = $false; Reason = 'pre-scan skipped: source already looks heavily bitrate-constrained' }
    }

    if ($ResolutionTier -eq 'SD' -and $VideoBitratePerHourGiB -gt 0 -and $VideoBitratePerHourGiB -lt 3.0 -and $CodecClass -ne 'legacy') {
        return [ordered]@{ ShouldRun = $false; Reason = 'pre-scan skipped: low-density SD source is unlikely to benefit' }
    }

    return [ordered]@{ ShouldRun = $true; Reason = 'pre-scan sampled denoise-vs-original differences across the runtime' }
}

function Get-FallbackAutoFilmGrain {
    param(
        [string]$ResolutionTier,
        [string]$Profile,
        [string]$CodecClass,
        [string]$BppTier,
        [double]$Bpp,
        [double]$VideoBitratePerHourGiB,
        [int]$BitDepth
    )

    if (($Bpp -gt 0 -and $Bpp -lt 0.05) -or ($VideoBitratePerHourGiB -gt 0 -and $VideoBitratePerHourGiB -lt 4.0)) {
        return [ordered]@{
            FilmGrain = 0
            GrainClass = 'none'
            GrainScore = 0.0
            Reason = 'Auto fallback: bitrate-density is already low, so preserve bits for structure instead of synthesized grain.'
        }
    }

    if ($Profile -eq 'HDR' -and $CodecClass -eq 'modern') {
        return [ordered]@{
            FilmGrain = $(if ($BppTier -eq 'high') { 4 } else { 0 })
            GrainClass = $(if ($BppTier -eq 'high') { 'light' } else { 'none' })
            GrainScore = $(if ($BppTier -eq 'high') { 10.0 } else { 0.0 })
            Reason = 'Auto fallback: modern HDR source assumed mostly clean unless pre-scan proves otherwise.'
        }
    }

    if ($CodecClass -eq 'legacy' -and $BppTier -eq 'high') {
        return [ordered]@{
            FilmGrain = 12
            GrainClass = 'heavy'
            GrainScore = 32.0
            Reason = 'Auto fallback: legacy high-density source is likely preserving visible film grain.'
        }
    }

    if ($BppTier -eq 'high') {
        return [ordered]@{
            FilmGrain = 8
            GrainClass = 'moderate'
            GrainScore = 20.0
            Reason = 'Auto fallback: high BPP suggests enough retained texture to preserve moderate grain.'
        }
    }

    if ($BppTier -eq 'medium') {
        return [ordered]@{
            FilmGrain = $(if ($ResolutionTier -eq 'UHD' -or $BitDepth -ge 10) { 4 } else { 8 })
            GrainClass = $(if ($ResolutionTier -eq 'UHD' -or $BitDepth -ge 10) { 'light' } else { 'moderate' })
            GrainScore = $(if ($ResolutionTier -eq 'UHD' -or $BitDepth -ge 10) { 12.0 } else { 18.0 })
            Reason = 'Auto fallback: medium BPP keeps grain conservative without assuming a heavy film transfer.'
        }
    }

    return [ordered]@{
        FilmGrain = 0
        GrainClass = 'none'
        GrainScore = 0.0
        Reason = 'Auto fallback: no strong signal for retained grain.'
    }
}

function Invoke-GrainPreScan {
    param(
        [string]$InputPath,
        $VideoStream,
        [double]$DurationSec
    )

    if ($DurationSec -le 0) {
        return [ordered]@{
            Success    = $false
            GrainScore = 0.0
            GrainClass = 'unknown'
            Reason     = 'Grain pre-scan skipped: source duration unavailable.'
        }
    }

    $videoIndex = [int](Get-StreamProp $VideoStream 'index' 0)
    $sampleFractions = @(0.15, 0.28, 0.41, 0.54, 0.67, 0.80)
    $sampleDuration = 2.0
    $positions = [System.Collections.Generic.List[double]]::new()

    foreach ($fraction in $sampleFractions) {
        $startSec = ($DurationSec * $fraction) - ($sampleDuration / 2.0)
        $positions.Add([Math]::Max(0.0, [Math]::Min($startSec, [Math]::Max(0.0, $DurationSec - $sampleDuration))))
    }

    $ssimValues = [System.Collections.Generic.List[double]]::new()
    foreach ($position in $positions) {
        $scanArgs = @(
            '-hide_banner',
            '-nostats',
            '-v', 'info',
            '-ss', ('{0:F3}' -f $position),
            '-t', ('{0:F3}' -f $sampleDuration),
            '-i', $InputPath,
            '-filter_complex', "[0:$videoIndex]scale=640:-2:flags=bicubic,format=gray,split=2[src][den];[den]hqdn3d=1.5:1.5:6:6[denoised];[src][denoised]ssim",
            '-an',
            '-sn',
            '-dn',
            '-f', 'null',
            'NUL'
        )

        $scanOutput = & $FfmpegPath @scanArgs 2>&1 | Out-String
        $matches = [System.Text.RegularExpressions.Regex]::Matches($scanOutput, 'All:([0-9]+\.[0-9]+)')
        if ($matches.Count -gt 0) {
            $lastValue = Convert-ToInvariantDouble $matches[$matches.Count - 1].Groups[1].Value 0.0
            if ($lastValue -gt 0) {
                $ssimValues.Add($lastValue)
            }
        }
    }

    if ($ssimValues.Count -eq 0) {
        return [ordered]@{
            Success    = $false
            GrainScore = 0.0
            GrainClass = 'unknown'
            Reason     = 'Grain pre-scan could not extract SSIM measurements; falling back to conservative heuristics.'
        }
    }

    $avgSsim = ($ssimValues | Measure-Object -Average).Average
    $grainScore = [Math]::Round([Math]::Min(100.0, [Math]::Max(0.0, (1.0 - $avgSsim) * 1000.0)), 2)

    $grainClass = if     ($grainScore -lt 3.0)  { 'none' }
                  elseif ($grainScore -lt 8.0)  { 'light' }
                  elseif ($grainScore -lt 18.0) { 'moderate' }
                  elseif ($grainScore -lt 30.0) { 'heavy' }
                  else                          { 'extreme' }

    return [ordered]@{
        Success    = $true
        GrainScore = $grainScore
        GrainClass = $grainClass
        Reason     = "Grain pre-scan: 6 x 2s samples, average SSIM after mild denoise = $([Math]::Round($avgSsim, 5))."
    }
}

function Get-AutoEncodeSettings {
    param(
        $Probe,
        $VideoStream,
        $SourceProfile,
        [object[]]$KeptAudioStreams = @(),
        [string]$InputPath,
        $ConfiguredCRF,
        $ConfiguredPreset,
        $ConfiguredFilmGrain,
        $ConfiguredAutoCRFOffset
    )

    $format = Get-OptionalProperty -InputObject $Probe -PropertyName 'format' -Default ([PSCustomObject]@{})
    $durationSec = Convert-ToInvariantDouble (Get-OptionalProperty $format 'duration' 0) 0.0
    $width = [int](Get-StreamProp $VideoStream 'width' 0)
    $height = [int](Get-StreamProp $VideoStream 'height' 0)
    $frameRate = Get-FrameRate -Stream $VideoStream
    $bitDepth = Get-VideoBitDepth -Stream $VideoStream
    $resolutionTier = Get-ResolutionTier -Width $width
    $codecClass = Get-CodecClass -Stream $VideoStream
    $codecLabel = Get-CodecLabel -Stream $VideoStream
    $dynamicRangeClass = if ($SourceProfile.Profile -in @('HDR', 'DV')) { 'HDR' } else { 'SDR' }
    $bitrateInfo = Get-EffectiveVideoBitrate -Probe $Probe -VideoStream $VideoStream -KeptAudioStreams $KeptAudioStreams
    $videoBitrate = [double]$bitrateInfo.Bitrate
    $bpp = Get-BitsPerPixelPerFrame -VideoBitrate $videoBitrate -Width $width -Height $height -FrameRate $frameRate
    $bppTier = Get-BppTier -Bpp $bpp
    $bitratePerHourGiB = Get-VideoBitratePerHourGiB -VideoBitrate $videoBitrate

    $autoEnabled = ($ConfiguredCRF -eq 'Auto' -or $ConfiguredPreset -eq 'Auto' -or $ConfiguredFilmGrain -eq 'Auto')
    $autoSkipAllowed = ($ConfiguredCRF -eq 'Auto')
    $manualReason = ''
    $configuredOffsetValue = if ($ConfiguredAutoCRFOffset -eq 'Auto') { 0 } else { [int]$ConfiguredAutoCRFOffset }
    $appliedAutoCRFOffset = 0

    $grainResult = $null
    if ($ConfiguredFilmGrain -eq 'Auto') {
        $preScanDecision = Test-ShouldRunGrainPreScan `
            -DurationSec $durationSec `
            -Bpp $bpp `
            -VideoBitratePerHourGiB $bitratePerHourGiB `
            -ResolutionTier $resolutionTier `
            -CodecClass $codecClass

        if ($preScanDecision.ShouldRun) {
            try {
                $grainResult = Invoke-GrainPreScan -InputPath $InputPath -VideoStream $VideoStream -DurationSec $durationSec
            } catch {
                $grainResult = [ordered]@{
                    Success    = $false
                    GrainScore = 0.0
                    GrainClass = 'unknown'
                    Reason     = "Grain pre-scan failed: $($_.Exception.Message)"
                }
            }
        } else {
            $grainResult = [ordered]@{
                Success    = $false
                GrainScore = 0.0
                GrainClass = 'unknown'
                Reason     = $preScanDecision.Reason
            }
        }
    }

    $fallbackGrain = Get-FallbackAutoFilmGrain `
        -ResolutionTier $resolutionTier `
        -Profile $dynamicRangeClass `
        -CodecClass $codecClass `
        -BppTier $bppTier `
        -Bpp $bpp `
        -VideoBitratePerHourGiB $bitratePerHourGiB `
        -BitDepth $bitDepth

    $grainClass = $fallbackGrain.GrainClass
    $grainScore = $fallbackGrain.GrainScore
    $filmGrainReason = $fallbackGrain.Reason
    $resolvedFilmGrain = if ($ConfiguredFilmGrain -eq 'Auto') { 0 } else { [int]$ConfiguredFilmGrain }

    if ($ConfiguredFilmGrain -eq 'Auto') {
        if ($grainResult -and $grainResult.Success) {
            $grainClass = $grainResult.GrainClass
            $grainScore = $grainResult.GrainScore
            $resolvedFilmGrain = switch ($grainClass) {
                'none'     { 0 }
                'light'    { 4 }
                'moderate' { 8 }
                'heavy'    { 12 }
                'extreme'  { 16 }
                default    { 0 }
            }
            $filmGrainReason = "$($grainResult.Reason) Class=$grainClass."
        } else {
            $resolvedFilmGrain = [int]$fallbackGrain.FilmGrain
            if ($grainResult) {
                $filmGrainReason = "$($grainResult.Reason) $($fallbackGrain.Reason)"
            }
        }
    }

    $shouldSkip = $false
    $skipReason = ''
    if ($autoEnabled -and $autoSkipAllowed) {
        $grainHeavy = $grainClass -in @('heavy', 'extreme')
        if (($codecClass -in @('standard', 'modern')) -and
            $SourceProfile.Profile -eq 'SDR' -and
            $bpp -gt 0 -and $bpp -lt 0.055 -and
            $bitratePerHourGiB -gt 0 -and $bitratePerHourGiB -lt 8.0 -and
            -not ($resolutionTier -eq 'UHD' -and $SourceProfile.HasHDR) -and
            -not $grainHeavy) {
            $shouldSkip = $true
            $skipReason = "Auto skip: already efficient $resolutionTier $($SourceProfile.Profile) $codecLabel source (BPP $([Math]::Round($bpp, 4)), $([Math]::Round($bitratePerHourGiB, 2)) GiB/hr video)."
        }
    }

    $resolvedCrf = if ($ConfiguredCRF -eq 'Auto') { 0 } else { [int]$ConfiguredCRF }
    $baseAutoCrf = $resolvedCrf
    $crfReason = "Manual: using configured CRF $ConfiguredCRF."
    if ($ConfiguredCRF -ne 'Auto' -and $ConfiguredAutoCRFOffset -ne 'Auto') {
        $crfReason += ' Auto CRF offset ignored because CRF is manual.'
    }
    if ($ConfiguredCRF -eq 'Auto') {
        $crfMatrix = @{
            'SD'  = @{ SDR = 28; HDR = 26 }
            'HD'  = @{
                SDR = @{ low = 26; medium = 24; high = 22; unknown = 24 }
                HDR = @{ low = 22; medium = 20; high = 18; unknown = 20 }
            }
            'UHD' = @{
                SDR = @{ low = 20; medium = 18; high = 16; unknown = 18 }
                HDR = @{ low = 16; medium = 14; high = 12; unknown = 14 }
            }
        }

        $profileKey = $dynamicRangeClass
        if ($resolutionTier -eq 'SD') {
            $baseCrf = $crfMatrix['SD'][$profileKey]
        } else {
            $baseCrf = $crfMatrix[$resolutionTier][$profileKey][$bppTier]
        }

        $codecAdjustment = switch ($codecClass) {
            'legacy' { 2 }
            'modern' { -1 }
            default  { 0 }
        }

        $baseAutoCrf = [Math]::Max(0, [Math]::Min(63, ($baseCrf + $codecAdjustment)))
        $resolvedCrf = $baseAutoCrf
        if ($ConfiguredAutoCRFOffset -ne 'Auto') {
            $appliedAutoCRFOffset = $configuredOffsetValue
            $resolvedCrf = [Math]::Max(0, [Math]::Min(63, ($resolvedCrf + $appliedAutoCRFOffset)))
        }

        $crfReason = "Auto: $resolutionTier / $($SourceProfile.Profile) / $codecLabel / $bppTier BPP"
        if ($bitrateInfo.Approximate) {
            $crfReason += " / approximate bitrate source $($bitrateInfo.Method)"
        } else {
            $crfReason += " / bitrate source $($bitrateInfo.Method)"
        }
        if ($appliedAutoCRFOffset -ne 0) {
            $crfReason += " / offset $(if ($appliedAutoCRFOffset -gt 0) { '+' } else { '' })$appliedAutoCRFOffset"
        }
        $crfReason += '.'
    }

    $resolvedPreset = if ($ConfiguredPreset -eq 'Auto') { 4 } else { [int]$ConfiguredPreset }
    $presetReason = "Manual: using configured preset $ConfiguredPreset."
    if ($ConfiguredPreset -eq 'Auto') {
        if (($resolutionTier -eq 'UHD' -and $dynamicRangeClass -eq 'HDR') -or
            ($codecClass -eq 'modern' -and $bppTier -eq 'high') -or
            ($grainClass -in @('heavy', 'extreme'))) {
            $resolvedPreset = 3
            $presetReason = 'Auto: slower preset for UHD HDR, modern high-density, or heavy-grain sources.'
        } elseif (($resolutionTier -in @('SD', 'HD')) -and ($bppTier -in @('low', 'medium', 'unknown')) -and $dynamicRangeClass -ne 'HDR') {
            if ($grainClass -in @('none', 'light', 'unknown')) {
                $resolvedPreset = 5
                $presetReason = 'Auto: faster preset for SDR SD/HD sources with low-to-moderate compression difficulty and little grain.'
            } else {
                $resolvedPreset = 4
                $presetReason = 'Auto: balanced preset retained because grain argues against the fast path.'
            }
        } else {
            $resolvedPreset = 4
            $presetReason = 'Auto: balanced default preset.'
        }
    }

    if ($ConfiguredFilmGrain -ne 'Auto') {
        $filmGrainReason = "Manual: using configured film grain $ConfiguredFilmGrain."
    }

    if (-not $autoEnabled) {
        $manualReason = 'Manual mode: no Auto settings were enabled.'
    }

    $summaryParts = [System.Collections.Generic.List[string]]::new()
    if ($ConfiguredCRF -eq 'Auto')       { $summaryParts.Add("CRF $resolvedCrf ($crfReason)") }
    if ($ConfiguredPreset -eq 'Auto')    { $summaryParts.Add("Preset $resolvedPreset ($presetReason)") }
    if ($ConfiguredFilmGrain -eq 'Auto') { $summaryParts.Add("FilmGrain $resolvedFilmGrain ($filmGrainReason)") }
    if ($ConfiguredCRF -ne 'Auto' -and $ConfiguredAutoCRFOffset -ne 'Auto') {
        $summaryParts.Add('Auto CRF offset ignored because CRF is manual.')
    }
    if ($shouldSkip)                     { $summaryParts.Add($skipReason) }
    if (-not $autoEnabled)               { $summaryParts.Add($manualReason) }

    return [ordered]@{
        Skip                   = $shouldSkip
        SkipReason             = $skipReason
        CRF                    = $resolvedCrf
        BaseAutoCRF            = $baseAutoCrf
        AutoCRFOffset          = $appliedAutoCRFOffset
        CRFReason              = $crfReason
        Preset                 = $resolvedPreset
        PresetReason           = $presetReason
        FilmGrain              = $resolvedFilmGrain
        FilmGrainReason        = $filmGrainReason
        Reason                 = ($summaryParts -join ' | ')
        ResolutionTier         = $resolutionTier
        BPP                    = $bpp
        BPPTier                = $bppTier
        CodecClass             = $codecClass
        CodecLabel             = $codecLabel
        VideoBitrate           = [int64]$videoBitrate
        VideoBitratePerHourGiB = $bitratePerHourGiB
        BitrateMethod          = $bitrateInfo.Method
        BitrateReason          = $bitrateInfo.Reason
        GrainClass             = $grainClass
        GrainScore             = $grainScore
        GrainPreScan           = $grainResult
        FrameRate              = $frameRate
        BitDepth               = $bitDepth
    }
}

# =============================================================================
# FUNCTION GROUP: Output path helpers
#
#   Convert-CodecTagToAv1Name - Rewrites common source codec tokens in the
#                               basename to AV1 (x264/x265/H.264/H.265/H264/
#                               H265/HEVC -> AV1). If no codec token is found,
#                               the basename is left unchanged.
#
#   Get-TempOutputPath  - Returns the path for the in-progress encode output.
#                         Named <av1-basename>.encoding.tmp.mkv in the source
#                         directory. Using a distinct temp name means a partial
#                         file is never mistaken for a complete encode.
#
#   Get-FinalOutputPath - Returns the intended final output path.
#                         Named <av1-basename>.mkv in the source directory.
#                         If no source codec token exists in the filename, the
#                         basename is preserved as-is.
# =============================================================================
function Convert-CodecTagToAv1Name {
    param([string]$BaseName)

    $codecTokenPattern = '(?i)(?<=^|[._\-\s\[\]\(\)])(?:x264|x265|h264|h265|h\.264|h\.265|hevc)(?=$|[._\-\s\[\]\(\)])'
    return [System.Text.RegularExpressions.Regex]::Replace($BaseName, $codecTokenPattern, 'AV1')
}

function Get-TempOutputPath {
    param([string]$InputPath)
    $dir  = Split-Path -Path $InputPath -Parent
    $name = Convert-CodecTagToAv1Name -BaseName ([System.IO.Path]::GetFileNameWithoutExtension($InputPath))
    return Join-Path $dir ($name + ".encoding.tmp.mkv")
}

function Get-FinalOutputPath {
    param([string]$InputPath)
    $dir  = Split-Path -Path $InputPath -Parent
    $name = Convert-CodecTagToAv1Name -BaseName ([System.IO.Path]::GetFileNameWithoutExtension($InputPath))
    return Join-Path $dir ($name + ".mkv")
}

# =============================================================================
# FUNCTION: Format-StreamSummary
#
# Formats an array of stream objects into a compact human-readable string for
# console output and CSV logging. Null entries are silently filtered so callers
# can pass optional streams (FallbackAudio, SdhSub) without guarding.
#
# Output format per stream:  idx=N;lang=eng;codec=eac3;ch=6;title=Surround 5.1
# Multiple streams are joined with " | ".
# =============================================================================
function Format-StreamSummary {
    param([object[]]$Streams)

    $Streams = @($Streams | Where-Object { $_ })
    if (-not $Streams) { return "" }

    return ($Streams | ForEach-Object {
        $lang  = Get-StreamLanguage $_
        $title = Get-StreamTitle $_
        $codec = Get-StreamProp $_ 'codec_name' ''
        $ch    = Get-StreamProp $_ 'channels'   ''
        $idx   = Get-StreamProp $_ 'index'      ''
        "idx=$idx;lang=$lang;codec=$codec;ch=$ch;title=$title"
    }) -join " | "
}

# =============================================================================
# FUNCTION: Move-ToBackup
#
# Moves the original source file to .queue\backup_originals\ with a timestamp
# prefix to avoid name collisions. Used when $KeepBackupOriginal = $true.
# Returns the destination path for logging purposes.
# =============================================================================
function Move-ToBackup {
    param([string]$OriginalPath)
    $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $name  = [System.IO.Path]::GetFileName($OriginalPath)
    $dest  = Join-Path $BackupDir ("{0}_{1}" -f $stamp, $name)
    Move-Item -LiteralPath $OriginalPath -Destination $dest -Force
    return $dest
}

# =============================================================================
# FUNCTION: Test-SufficientDiskSpace
#
# Verifies that enough free space exists before starting an encode.
#
# Requires $MultiplierRequired x source file size free on the same drive as
# the output directory (default 2x, to hold the temp output and leave
# headroom). Throws a descriptive error rather than letting the encode run for
# hours only to fail mid-way when the disk fills up.
#
# Returns $true silently if free space is adequate, or if the drive letter
# cannot be resolved (e.g. UNC paths).
# =============================================================================
function Test-SufficientDiskSpace {
    param(
        [string]$TargetDirectory,
        [long]  $SourceSizeBytes,
        [double]$MultiplierRequired = 2.0
    )

    $drive = Split-Path -Qualifier $TargetDirectory
    $disk  = Get-PSDrive -Name ($drive.TrimEnd(':')) -ErrorAction SilentlyContinue

    if (-not $disk) { return $true }   # UNC or unmapped drive -- skip check.

    $requiredBytes = [long]($SourceSizeBytes * $MultiplierRequired)
    $freeBytes     = $disk.Free

    if ($freeBytes -lt $requiredBytes) {
        $requiredGiB = [Math]::Round($requiredBytes / 1GB, 2)
        $freeGiB     = [Math]::Round($freeBytes     / 1GB, 2)
        throw ("Insufficient disk space. Required: {0} GiB, Available: {1} GiB on {2}" -f $requiredGiB, $freeGiB, $drive)
    }

    return $true
}

# =============================================================================
# Progress UI
# =============================================================================

# =============================================================================
# FUNCTION: Get-PendingQueueNames
#
# Returns the display filenames of jobs currently in .queue\pending\, sorted
# oldest-first (FIFO). Used by Write-ProgressUI to populate the queue sidebar.
# Falls back to the raw JSON filename if a job descriptor cannot be parsed.
# =============================================================================
function Get-PendingQueueNames {
    if (-not (Test-Path -LiteralPath $QueuePendingDir)) { return @() }

    return @(
        Get-ChildItem -LiteralPath $QueuePendingDir -Filter *.json -File -ErrorAction SilentlyContinue |
            Sort-Object CreationTimeUtc |
            ForEach-Object {
                try {
                    $j = Get-Content -LiteralPath $_.FullName -Raw | ConvertFrom-Json
                    [System.IO.Path]::GetFileName($j.InputPath)
                } catch { $_.Name }
            }
    )
}

# =============================================================================
# FUNCTION: Format-Duration
#
# Formats a duration in fractional seconds as a compact human-readable string.
#
# Output format:
#   >= 1 hour  ->  2h 04m 37s
#   >= 1 min   ->  14m 03s
#   < 1 min    ->  47s
#   <= 0       ->  --:--  (displayed while waiting for the first ffmpeg update)
# =============================================================================
function Format-Duration {
    param([double]$Seconds)
    if ($Seconds -le 0) { return "--:--" }
    $ts = [TimeSpan]::FromSeconds([Math]::Round($Seconds))
    if ($ts.TotalHours   -ge 1) { return "{0}h {1:D2}m {2:D2}s" -f [int]$ts.TotalHours, $ts.Minutes, $ts.Seconds }
    if ($ts.TotalMinutes -ge 1) { return "{0}m {1:D2}s"         -f [int]$ts.TotalMinutes, $ts.Seconds }
    return "{0}s" -f $ts.Seconds
}

# =============================================================================
# FUNCTION: Limit-String
#
# Truncates a string to at most $MaxWidth characters. Replaces the final
# character with the Unicode ellipsis (U+2026) when truncation occurs, so
# the reader can see the string was cut rather than ending cleanly mid-word.
# =============================================================================
function Limit-String {
    param([string]$Value, [int]$MaxWidth)
    if ($Value.Length -le $MaxWidth) { return $Value }
    return $Value.Substring(0, [Math]::Max(0, $MaxWidth - 1)) + [char]0x2026
}

# =============================================================================
# FUNCTION: Write-ProgressUI
#
# Draws (or redraws in-place) a live progress box in the console window.
#
# All output is written via [Console]::Write() using ANSI escape sequences
# rather than Write-Host, so the entire UI block can be redrawn without
# scrolling. On first paint ($UICursorRow = -1) the box is appended to the
# current output. On subsequent calls ESC[{n}A moves the cursor up by the
# number of lines in the box, each line is overwritten, and ESC[K (erase to
# end of line) clears any stale characters if the terminal was resized
# narrower between redraws. The box width adapts to the console on every call.
#
# Layout:
#   +== Encoding ============================================================+
#   |  Movie.Title.2024.mkv                                                  |
#   |  SDR  |  CRF 10  |  Preset 4  |  1h 23m 45s elapsed                  |
#   |                                                                        |
#   |  [XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX...........]  47.3%                |
#   |  Encoded: 12.4 GiB   Speed: 0.82x   ETA: 38m 12s                      |
#   +== Queue (2 pending) ===================================================+
#   |  1. AnotherMovie.mkv                                                   |
#   |  2. YetAnother.mkv                                                     |
#   +========================================================================+
#
# Parameters:
#   FileName          - Filename of the file currently being encoded.
#   Profile           - "SDR", "HDR", or "DV".
#   SourceDurationSec - Total source duration in seconds (from ffprobe).
#   ElapsedSec        - Wall-clock elapsed time from the encode stopwatch.
#   OutTimeSec        - Encoded position in seconds (from ffmpeg out_time_us).
#   OutSizeBytes      - Current output file size in bytes (from ffmpeg total_size).
#   SpeedX            - Encode speed multiplier (from ffmpeg speed, e.g. 0.82).
#   UICursorRow       - Lines previously printed by this function. -1 on first paint.
#
# Returns the number of lines written so the caller can pass it back on the
# next call for the cursor-up calculation.
# =============================================================================
function Write-ProgressUI {
    param(
        [string] $FileName,
        [string] $Profile,
        [string] $EncodeColorLabel,
        [string] $CRFLabel,
        [string] $PresetLabel,
        [double] $SourceDurationSec,
        [double] $ElapsedSec,
        [double] $OutTimeSec   = 0,
        [double] $OutSizeBytes = 0,
        [double] $SpeedX       = 0,
        [int]    $UICursorRow  = -1
    )

    # ── Geometry ──────────────────────────────────────────────────────────────
    $conW  = [Math]::Max(60, $Host.UI.RawUI.WindowSize.Width - 1)
    $inner = $conW - 4   # usable content width inside the border glyphs

    # ── Derived display values ────────────────────────────────────────────────
    $pct = if ($SourceDurationSec -gt 0) {
        [Math]::Min(100.0, ($OutTimeSec / $SourceDurationSec) * 100.0)
    } else { 0.0 }

    $eta = "--"
    if ($SpeedX -gt 0.001 -and $SourceDurationSec -gt 0) {
        $eta = Format-Duration -Seconds (($SourceDurationSec - $OutTimeSec) / $SpeedX)
    }

    $sizeStr  = if ($OutSizeBytes -gt 0) { "{0:F2} GiB" -f ($OutSizeBytes / 1GB) } else { "---" }
    $speedStr = if ($SpeedX -gt 0.001)   { "{0:F2}x"   -f $SpeedX               } else { "---" }
    $elapsStr = Format-Duration -Seconds $ElapsedSec

    # ── Progress bar geometry ─────────────────────────────────────────────────
    # Reserve 9 characters on the right for the "  XX.X%" label.
    $barOuter = $inner - 9
    $barInner = [Math]::Max(4, $barOuter - 2)
    $filled   = [int][Math]::Round($barInner * $pct / 100.0)
    $empty    = $barInner - $filled
    $pctLabel = ("{0,5:F1}%" -f $pct)

    # ── Queue snapshot ────────────────────────────────────────────────────────
    $queueNames = @(Get-PendingQueueNames)   # @() ensures array even when 0 or 1 results

    # ── ANSI colour codes ─────────────────────────────────────────────────────
    $ESC      = [char]27
    $reset    = "${ESC}[0m"
    $cBorder  = "${ESC}[38;5;240m"   # dark grey    -- box lines
    $cTitle   = "${ESC}[1;97m"       # bold white   -- section headings
    $cFile    = "${ESC}[1;96m"       # bold cyan    -- filename
    $cMeta    = "${ESC}[38;5;250m"   # light grey   -- CRF / preset / elapsed
    $cColor   = switch ($Profile) {
        'DV'  { "${ESC}[1;95m" }
        'HDR' { "${ESC}[1;93m" }
        default { "${ESC}[38;5;117m" }
    }
    $cBarDone = "${ESC}[38;5;76m"    # green        -- filled bar blocks
    $cBarTodo = "${ESC}[38;5;238m"   # dark grey    -- empty bar blocks
    $cPct     = "${ESC}[1;92m"       # bright green -- percentage label
    $cStats   = "${ESC}[38;5;250m"   # light grey   -- size / speed / ETA
    $cQueue   = "${ESC}[38;5;245m"   # mid grey     -- queue entries

    # ── Box-drawing characters ────────────────────────────────────────────────
    $TL = [char]0x2554   # top-left corner
    $TR = [char]0x2557   # top-right corner
    $BL = [char]0x255A   # bottom-left corner
    $BR = [char]0x255D   # bottom-right corner
    $HL = [char]0x2550   # horizontal line
    $VL = [char]0x2551   # vertical line
    $LM = [char]0x2560   # left mid-divider
    $RM = [char]0x2563   # right mid-divider

    # ── Inner row helpers ─────────────────────────────────────────────────────
    # Row: pads/truncates $content to exactly $inner chars, wraps in border glyphs.
    function Row ([string]$content, [string]$color = "") {
        $safe = Limit-String -Value $content -MaxWidth $inner
        $pad  = " " * ($inner - $safe.Length)
        "${cBorder}${VL} ${reset}${color}${safe}${reset}${pad} ${cBorder}${VL}${reset}"
    }

    function BlankRow { Row "" }

    # DivRow: full-width divider with a centred label.
    function DivRow ([string]$label) {
        $mid   = " $label "
        $left  = [int][Math]::Floor(($conW - 2 - $mid.Length) / 2)
        $right = $conW - 2 - $left - $mid.Length
        "${cBorder}${LM}$([string]$HL * $left)${cTitle}${mid}${reset}${cBorder}$([string]$HL * $right)${RM}${reset}"
    }

    # ── Build line list ───────────────────────────────────────────────────────
    $titleLabel = " Encoding "
    $tLeft      = [int][Math]::Floor(($conW - 2 - $titleLabel.Length) / 2)
    $tRight     = $conW - 2 - $tLeft - $titleLabel.Length
    $topBorder  = "${cBorder}${TL}$([string]$HL * $tLeft)${cTitle}${titleLabel}${reset}${cBorder}$([string]$HL * $tRight)${TR}${reset}"
    $botBorder  = "${cBorder}${BL}$([string]$HL * ($conW - 2))${BR}${reset}"

    $barContentLen = 2 + $barInner + $pctLabel.Length
    $barPad        = " " * [Math]::Max(0, $inner - $barContentLen)
    $barRow = "${cBorder}${VL} ${reset}[${cBarDone}$([string][char]0x2588 * $filled)${reset}${cBarTodo}$([string][char]0x2591 * $empty)${reset}]${cPct}${pctLabel}${reset}${barPad} ${cBorder}${VL}${reset}"

    $lines = [System.Collections.Generic.List[string]]::new()
    $lines.Add($topBorder)
    $lines.Add((Row (Limit-String $FileName $inner) $cFile))
    $lines.Add((Row "$Profile  |  $EncodeColorLabel" $cColor))
    $lines.Add((Row "CRF $CRFLabel  |  Preset $PresetLabel  |  $elapsStr elapsed" $cMeta))
    $lines.Add($barRow)
    $lines.Add((Row "Encoded: $sizeStr   Speed: $speedStr   ETA: $eta" $cStats))

    if ($queueNames.Count -gt 0) {
        $lines.Add((DivRow "Queue ($($queueNames.Count) pending)"))
        $maxShow = 8
        $shown   = [Math]::Min($queueNames.Count, $maxShow)
        for ($i = 0; $i -lt $shown; $i++) {
            $lines.Add((Row "$($i + 1). $($queueNames[$i])" $cQueue))
        }
        if ($queueNames.Count -gt $maxShow) {
            $lines.Add((Row "  ... and $($queueNames.Count - $maxShow) more" $cQueue))
        }
    }

    $lines.Add($botBorder)

    # ── Render ────────────────────────────────────────────────────────────────
    $lineCount = $lines.Count
    $sb = [System.Text.StringBuilder]::new()

    if ($UICursorRow -ge 0) {
        $null = $sb.Append("${ESC}[${lineCount}A")   # move cursor up $lineCount rows
    }

    foreach ($l in $lines) {
        $null = $sb.Append($l)
        $null = $sb.Append("${ESC}[K")   # erase to end of line (handles terminal resize)
        $null = $sb.Append("`n")
    }

    [Console]::Write($sb.ToString())
    return $lineCount
}

# =============================================================================
# FUNCTION: Invoke-EncodeJob
#
# Executes a single encode job end-to-end for one input file.
#
# Steps:
#   1.  Start stopwatch; measure source file size; verify disk space.
#   2.  Run ffprobe and parse stream metadata.
#   3.  Select streams (Select-Streams) and detect source profile (Get-SourceProfile).
#   4.  Skip DV sources if $SkipDolbyVisionSources is set; log SKIPPED_DV and return.
#   5.  Resolve temp and final output paths; guard against overwriting a prior
#       encode that shares the same base name as the source.
#   6.  Build the ffmpeg argument list:
#         - Explicit -map for each selected stream (video, audio(s), subtitle(s)).
#         - -map_chapters 0 preserves chapter markers.
#         - -map_metadata -1 strips all global metadata; explicit -metadata
#           flags below then re-add only the title and per-stream titles.
#           The clear must precede the explicit metadata args -- ordering matters.
#         - libsvtav1 video encode at the configured CRF and preset.
#         - yuv420p10le pixel format (10-bit, required for HDR passthrough).
#         - BT.2020 / smpte2084 colour tags for HDR sources.
#         - Stream-copy for all audio and subtitle tracks.
#         - Disposition flags: video and first audio set to default; others cleared.
#         - -progress pipe:2 -stats_period 2 instructs ffmpeg to emit machine-
#           readable key=value progress lines to stderr every 2 seconds.
#   7.  Write current_job.json so an interrupted run can be detected and logged
#       on next startup.
#   8.  Print the encode header (filename, profile, selected streams).
#   9.  Launch ffmpeg as a System.Diagnostics.Process with stderr redirected.
#       An async ErrorDataReceived callback parses the key=value progress
#       stream and updates a [hashtable]::Synchronized so the main thread can
#       read values safely across the thread boundary. Non-progress lines
#       (ffmpeg warnings / info) are accumulated in LogLines and only printed
#       to the console if ffmpeg exits non-zero.
#  10.  The main thread polls $proc.HasExited every 500 ms, calling
#       Write-ProgressUI each iteration to redraw the live console box.
#  11.  After WaitForExit(), the final UI frame is painted at 100% on success.
#  12.  Duration sanity check: output must be within max(10s, 2% of source).
#  13.  Delete-then-move is wrapped in its own try/catch. If Move-Item fails
#       after the source has already been deleted, the error message surfaces
#       the temp file path so the user can recover it manually.
#  14.  Log SUCCESS to encode_log.csv.
# =============================================================================
function Invoke-EncodeJob {
    param([string]$InputPath)

    $stopwatch     = [System.Diagnostics.Stopwatch]::StartNew()
    $sourceItem    = Get-Item -LiteralPath $InputPath
    $sourceSizeGiB = [Math]::Round(($sourceItem.Length / 1GB), 3)

    $outputDir = Split-Path -Path $InputPath -Parent
    $null = Test-SufficientDiskSpace -TargetDirectory $outputDir -SourceSizeBytes $sourceItem.Length

    $probe         = Invoke-FfprobeJson -InputPath $InputPath
    $selected      = Select-Streams     -Probe $probe
    $sourceProfile = Get-SourceProfile  -Probe $probe -VideoStream $selected.Video
    $encodeColorProfile = Get-EncodeColorProfile -SourceProfile $sourceProfile
    $sourceFormat  = Get-OptionalProperty -InputObject $probe -PropertyName 'format' -Default ([PSCustomObject]@{})
    $sourceDuration = Convert-ToInvariantDouble (Get-OptionalProperty $sourceFormat 'duration' 0) 0.0
    $sourceResolutionTier = Get-ResolutionTier -Width ([int](Get-StreamProp $selected.Video 'width' 0))
    $sourceCodecClass = Get-CodecClass -Stream $selected.Video
    $selectedAudioSummary = Format-StreamSummary -Streams @($selected.MainAudio, $selected.FallbackAudio)
    $selectedSubtitleSummary = Format-StreamSummary -Streams @($selected.MainSub, $selected.SdhSub)

    if ($sourceProfile.HasDV -and $SkipDolbyVisionSources) {
        Write-Warning "Skipping Dolby Vision source (preserve manually): $InputPath"

        $stopwatch.Stop()
        Write-LogRow @{
            Timestamp         = (Get-Date).ToString("s")
            Status            = "SKIPPED_DV"
            InputPath         = $InputPath
            OutputPath        = ""
            SourceSizeGiB     = $sourceSizeGiB
            OutputSizeGiB     = ""
            ReductionPercent  = ""
            SourceDurationSec = $sourceDuration
            OutputDurationSec = ""
            ElapsedSec        = [Math]::Round($stopwatch.Elapsed.TotalSeconds, 2)
            Profile           = $sourceProfile.Profile
            HasHDR            = $sourceProfile.HasHDR
            HasDV             = $sourceProfile.HasDV
            SelectedAudio     = $selectedAudioSummary
            SelectedSubtitles = $selectedSubtitleSummary
            CRF               = $CRF
            Preset            = $Preset
            FilmGrain         = $FilmGrain
            AutoCRFOffset     = $AutoCRFOffset
            ResolvedCRF       = ""
            ResolvedPreset    = ""
            ResolvedFilmGrain = ""
            AutoReason        = ""
            BPP               = ""
            EffectiveVideoBitrate = ""
            VideoBitratePerHourGiB = ""
            ResolutionTier    = $sourceResolutionTier
            CodecClass        = $sourceCodecClass
            GrainClass        = ""
            GrainScore        = ""
            WasAutoSkipped    = "False"
            FfmpegPath        = $FfmpegPath
            FfprobePath       = $FfprobePath
            Notes             = "Dolby Vision source skipped by policy."
        }
        return
    }

    $autoSettings = Get-AutoEncodeSettings `
        -Probe $probe `
        -VideoStream $selected.Video `
        -SourceProfile $sourceProfile `
        -KeptAudioStreams @($selected.MainAudio, $selected.FallbackAudio) `
        -InputPath $InputPath `
        -ConfiguredCRF $CRF `
        -ConfiguredPreset $Preset `
        -ConfiguredFilmGrain $FilmGrain `
        -ConfiguredAutoCRFOffset $AutoCRFOffset

    $resolvedCRF = [int]$autoSettings.CRF
    $resolvedPreset = [int]$autoSettings.Preset
    $resolvedFilmGrain = [int]$autoSettings.FilmGrain
    $resolvedCRFLabel = [string]$resolvedCRF
    $resolvedPresetLabel = [string]$resolvedPreset
    $encodeColorLabel = $encodeColorProfile.Summary

    if ($autoSettings.Skip) {
        Write-Host ""
        Write-Host "Auto Skip: $($autoSettings.SkipReason)" -ForegroundColor Yellow
        Write-Host ""

        $stopwatch.Stop()
        Write-LogRow @{
            Timestamp         = (Get-Date).ToString("s")
            Status            = "AUTO_SKIPPED_ALREADY_EFFICIENT"
            InputPath         = $InputPath
            OutputPath        = ""
            SourceSizeGiB     = $sourceSizeGiB
            OutputSizeGiB     = ""
            ReductionPercent  = ""
            SourceDurationSec = [Math]::Round($sourceDuration, 3)
            OutputDurationSec = ""
            ElapsedSec        = [Math]::Round($stopwatch.Elapsed.TotalSeconds, 2)
            Profile           = $sourceProfile.Profile
            HasHDR            = $sourceProfile.HasHDR
            HasDV             = $sourceProfile.HasDV
            SelectedAudio     = $selectedAudioSummary
            SelectedSubtitles = $selectedSubtitleSummary
            CRF               = $CRF
            Preset            = $Preset
            FilmGrain         = $FilmGrain
            AutoCRFOffset     = $AutoCRFOffset
            ResolvedCRF       = $resolvedCRF
            ResolvedPreset    = $resolvedPreset
            ResolvedFilmGrain = $resolvedFilmGrain
            AutoReason        = $autoSettings.SkipReason
            BPP               = [Math]::Round($autoSettings.BPP, 6)
            EffectiveVideoBitrate = $autoSettings.VideoBitrate
            VideoBitratePerHourGiB = [Math]::Round($autoSettings.VideoBitratePerHourGiB, 3)
            ResolutionTier    = $autoSettings.ResolutionTier
            CodecClass        = $autoSettings.CodecClass
            GrainClass        = $autoSettings.GrainClass
            GrainScore        = $autoSettings.GrainScore
            WasAutoSkipped    = "True"
            FfmpegPath        = $FfmpegPath
            FfprobePath       = $FfprobePath
            Notes             = $autoSettings.BitrateReason
        }
        return
    }

    $tempOutput  = Get-TempOutputPath  -InputPath $InputPath
    $finalOutput = Get-FinalOutputPath -InputPath $InputPath
    $displayOutputName = [System.IO.Path]::GetFileName($finalOutput)

    # Guard against silently overwriting a prior encode that has the same base
    # name as the source when the source is not itself an MKV.
    if ((Test-Path -LiteralPath $finalOutput) -and
        (-not [string]::Equals(
            (Get-NormalizedPath $finalOutput),
            (Get-NormalizedPath $InputPath),
            [System.StringComparison]::OrdinalIgnoreCase))) {
        throw "Final output path already exists and is not the source file: $finalOutput. Remove it manually before re-encoding."
    }

    if (Test-Path -LiteralPath $tempOutput) {
        Remove-Item -LiteralPath $tempOutput -Force
    }

    # ── Build ffmpeg argument list ────────────────────────────────────────────
    $ffArgs = New-Object System.Collections.Generic.List[string]
    $ffArgs.AddRange([string[]]@(
        "-hide_banner",
        "-y",
        "-i", $InputPath,
        "-map", "0:$($selected.Video.index)",
        "-map", "0:$($selected.MainAudio.index)"
    ))

    if ($selected.FallbackAudio) { $ffArgs.AddRange([string[]]@("-map", "0:$($selected.FallbackAudio.index)")) }
    if ($selected.MainSub)       { $ffArgs.AddRange([string[]]@("-map", "0:$($selected.MainSub.index)")) }
    if ($selected.SdhSub)        { $ffArgs.AddRange([string[]]@("-map", "0:$($selected.SdhSub.index)")) }

    $ffArgs.AddRange([string[]]@(
        "-map_chapters",  "0",
        # -map_metadata -1 clears all global container metadata first. The explicit
        # -metadata flags below then re-add only what we want. Ordering matters --
        # the clear must precede all metadata write arguments.
        "-map_metadata",  "-1",
        "-max_muxing_queue_size", "4096",
        "-c:v",     "libsvtav1",
        "-preset",  "$resolvedPreset",
        "-crf",     "$resolvedCRF",
        "-pix_fmt", "yuv420p10le"
    ))

    # Film grain synthesis: pass the parameter to SVT-AV1 only when enabled.
    # -svtav1-params is a catch-all for encoder-specific options not exposed as
    # top-level ffmpeg flags. Multiple params can be chained with colons, e.g.
    # "film-grain=10:film-grain-denoise=0". film-grain-denoise=0 tells the
    # encoder to synthesise grain at decode time WITHOUT pre-denoising the source
    # first -- generally preferred when the source grain is already well-behaved
    # and you do not want to alter the underlying image texture.
    if ($resolvedFilmGrain -gt 0) {
        $ffArgs.AddRange([string[]]@("-svtav1-params", "film-grain=$resolvedFilmGrain`:film-grain-denoise=0"))
    }

    if ($sourceProfile.HasHDR) {
        # smpte2084 (PQ) is the correct transfer function for both HDR10 and HDR10+.
        # HLG sources are also flagged HasHDR; tagging them smpte2084 is a known
        # trade-off when repackaging into AV1/MKV without tone-mapping.
        $ffArgs.AddRange([string[]]@(
            "-color_primaries", "bt2020",
            "-color_trc",       "smpte2084",
            "-colorspace",      "bt2020nc"
        ))
    }

    $ffArgs.AddRange([string[]]@("-c:a", "copy"))

    if ($selected.MainSub -or $selected.SdhSub) { $ffArgs.AddRange([string[]]@("-c:s", "copy")) }

    $ffArgs.AddRange([string[]]@(
        "-disposition:v:0", "default",
        "-disposition:a:0", "default"
    ))

    if ($selected.FallbackAudio) { $ffArgs.AddRange([string[]]@("-disposition:a:1", "0")) }

    if ($selected.MainSub) { $ffArgs.AddRange([string[]]@("-disposition:s:0", "default")) }

    if ($selected.SdhSub) {
        $subIndex = if ($selected.MainSub) { 1 } else { 0 }
        $ffArgs.AddRange([string[]]@("-disposition:s:$subIndex", "0"))
    }

    $baseTitle  = [System.IO.Path]::GetFileNameWithoutExtension($InputPath)
    $videoTitle = "AV1 $($encodeColorProfile.DynamicRangeLabel) $($encodeColorProfile.BitDepth)-bit"

    $ffArgs.AddRange([string[]]@(
        "-metadata",       "title=$baseTitle",
        "-metadata:s:v:0", "title=$videoTitle",
        "-metadata:s:a:0", "title=$(Get-StreamTitle $selected.MainAudio)"
    ))

    if ($selected.FallbackAudio) {
        $ffArgs.AddRange([string[]]@("-metadata:s:a:1", "title=$(Get-StreamTitle $selected.FallbackAudio)"))
    }

    if ($selected.MainSub) {
        $ffArgs.AddRange([string[]]@("-metadata:s:s:0", "title=$(Get-StreamTitle $selected.MainSub)"))
    }

    if ($selected.SdhSub) {
        $subIndex = if ($selected.MainSub) { 1 } else { 0 }
        $ffArgs.AddRange([string[]]@("-metadata:s:s:$subIndex", "title=$(Get-StreamTitle $selected.SdhSub)"))
    }

    # Direct ffmpeg to emit machine-readable key=value progress to stderr every
    # 2 seconds. Stderr is fully redirected; the async callback below parses it.
    $ffArgs.AddRange([string[]]@("-progress", "pipe:2", "-stats_period", "2"))
    $ffArgs.Add($tempOutput)

    # ── Write state file ──────────────────────────────────────────────────────
    # current_job.json records enough context to detect an interrupted encode on
    # the next run. It is deleted in the finally block after the job completes.
    $currentState = [ordered]@{
        InputPath    = $InputPath
        TempOutput   = $tempOutput
        FinalOutput  = $finalOutput
        StartedLocal = (Get-Date).ToString("s")
        Profile      = $sourceProfile.Profile
        HasHDR       = $sourceProfile.HasHDR
        HasDV        = $sourceProfile.HasDV
        CRF          = $CRF
        Preset       = $Preset
        FilmGrain    = $FilmGrain
        AutoCRFOffset = $AutoCRFOffset
        ResolvedCRF  = $resolvedCRF
        ResolvedPreset = $resolvedPreset
        ResolvedFilmGrain = $resolvedFilmGrain
        AutoReason   = $autoSettings.Reason
        BPP          = [Math]::Round($autoSettings.BPP, 6)
        EffectiveVideoBitrate = $autoSettings.VideoBitrate
        VideoBitratePerHourGiB = [Math]::Round($autoSettings.VideoBitratePerHourGiB, 3)
        ResolutionTier = $autoSettings.ResolutionTier
        CodecClass   = $autoSettings.CodecClass
        GrainClass   = $autoSettings.GrainClass
        GrainScore   = $autoSettings.GrainScore
        WasAutoSkipped = $false
    } | ConvertTo-Json -Depth 8

    Set-Content -LiteralPath $StatePath -Value $currentState -Encoding UTF8

    # ── Print encode header ───────────────────────────────────────────────────
    Write-Host ""
    Write-Host "------------------------------------------------------------" -ForegroundColor DarkGray
    Write-Host "Source   : $InputPath"                                          -ForegroundColor Green
    Write-Host "Encoding : $displayOutputName"                                  -ForegroundColor Green
    Write-Host "Profile : $($sourceProfile.Profile)"                           -ForegroundColor Green
    Write-Host "Source Color: $($sourceProfile.SourceColorSummary)"            -ForegroundColor Green
    Write-Host "Encode Color: $($encodeColorProfile.Summary)"                  -ForegroundColor Green
    if (-not [string]::IsNullOrWhiteSpace($encodeColorProfile.Note)) {
        Write-Host "Color Note : $($encodeColorProfile.Note)"                   -ForegroundColor Yellow
    }
    if ($CRF -eq 'Auto') {
        Write-Host "Auto CRF    : $resolvedCRF ($($autoSettings.CRFReason))"   -ForegroundColor Green
    }
    if ($Preset -eq 'Auto') {
        Write-Host "Auto Preset : $resolvedPreset ($($autoSettings.PresetReason))" -ForegroundColor Green
    }
    if ($FilmGrain -eq 'Auto') {
        Write-Host "Auto Grain  : $resolvedFilmGrain ($($autoSettings.FilmGrainReason))" -ForegroundColor Green
    } elseif ($resolvedFilmGrain -gt 0) {
        Write-Host "Grain       : film-grain=$resolvedFilmGrain (manual)"       -ForegroundColor Green
    }
    if ($autoSettings.VideoBitrate -gt 0) {
        Write-Host "Signals     : $($autoSettings.ResolutionTier) / $($sourceProfile.Profile) / $($autoSettings.CodecLabel) / BPP $([Math]::Round($autoSettings.BPP, 4)) / $([Math]::Round($autoSettings.VideoBitratePerHourGiB, 2)) GiB/hr" -ForegroundColor Green
    }
    Write-Host "Audio   : $selectedAudioSummary" -ForegroundColor Green
    Write-Host "Subs    : $selectedSubtitleSummary" -ForegroundColor Green
    Write-Host "------------------------------------------------------------" -ForegroundColor DarkGray
    Write-Host ""

    # ── Launch ffmpeg with redirected stderr ──────────────────────────────────
    $psi                       = [System.Diagnostics.ProcessStartInfo]::new()
    $psi.FileName              = $FfmpegPath
    $psi.RedirectStandardError = $true
    $psi.RedirectStandardInput = $false
    $psi.UseShellExecute       = $false
    $psi.CreateNoWindow        = $false

    foreach ($a in $ffArgs) { $psi.ArgumentList.Add($a) }

    $proc           = [System.Diagnostics.Process]::new()
    $proc.StartInfo = $psi

    # Shared state between the async stderr callback and the main UI loop.
    # [hashtable]::Synchronized wraps every read/write in a monitor lock so
    # the threadpool callback and the main thread cannot race on these values.
    $shared = [hashtable]::Synchronized(@{
        OutTimeSec   = 0.0
        OutSizeBytes = 0.0
        SpeedX       = 0.0
        LogLines     = [System.Collections.Generic.List[string]]::new()
    })

    # ── Background runspace: reads stderr from ffmpeg synchronously ───────────
    # PS scriptblocks cannot run on arbitrary .NET threadpool threads because
    # those threads have no PowerShell runspace attached. Using add_ErrorDataReceived
    # with a scriptblock therefore crashes the process with a PSInvalidOperationException.
    #
    # The correct pattern is a dedicated PowerShell instance running in its own
    # Runspace on a background thread. It reads stderr line by line in a blocking
    # loop, parses the -progress pipe:2 key=value output, and writes results into
    # the synchronized hashtable. The main thread reads from that hashtable to
    # drive the progress UI without any thread-safety issues.
    $stderrRunspace = [System.Management.Automation.Runspaces.RunspaceFactory]::CreateRunspace()
    $stderrRunspace.Open()
    $stderrRunspace.SessionStateProxy.SetVariable('shared', $shared)
    $stderrRunspace.SessionStateProxy.SetVariable('proc',   $proc)

    $stderrPs = [System.Management.Automation.PowerShell]::Create()
    $stderrPs.Runspace = $stderrRunspace
    $null = $stderrPs.AddScript({
        try {
            while ($true) {
                $line = $proc.StandardError.ReadLine()
                if ($null -eq $line) { break }
                if ([string]::IsNullOrEmpty($line)) { continue }

                if ($line -match '^([a-z_]+)=(.+)$') {
                    $k = $Matches[1]; $v = $Matches[2]
                    switch ($k) {
                        'out_time_us' {
                            $us = 0L
                            if ([long]::TryParse($v, [ref]$us)) {
                                $shared.OutTimeSec = [Math]::Max(0.0, $us / 1000000.0)
                            }
                        }
                        'total_size' {
                            $sz = 0L
                            if ([long]::TryParse($v, [ref]$sz)) {
                                $shared.OutSizeBytes = [Math]::Max(0.0, [double]$sz)
                            }
                        }
                        'speed' {
                            $sp = 0.0
                            if ([double]::TryParse(($v -replace 'x',''),
                                    [Globalization.NumberStyles]::Any,
                                    [Globalization.CultureInfo]::InvariantCulture,
                                    [ref]$sp)) {
                                $shared.SpeedX = [Math]::Max(0.0, $sp)
                            }
                        }
                    }
                } else {
                    $shared.LogLines.Add($line)
                }
            }
        } catch {}
    })

    $null = $proc.Start()
    $stderrAsync = $stderrPs.BeginInvoke()

    # ── Live UI loop ──────────────────────────────────────────────────────────
    $uiFileName     = $displayOutputName
    $uiLineCount    = -1   # -1 signals first paint; no cursor-up on first call
    while (-not $proc.HasExited) {
        $uiLineCount = Write-ProgressUI `
            -FileName          $uiFileName `
            -Profile           $sourceProfile.Profile `
            -EncodeColorLabel  $encodeColorLabel `
            -CRFLabel          $resolvedCRFLabel `
            -PresetLabel       $resolvedPresetLabel `
            -SourceDurationSec $sourceDuration `
            -ElapsedSec        $stopwatch.Elapsed.TotalSeconds `
            -OutTimeSec        $shared.OutTimeSec `
            -OutSizeBytes      $shared.OutSizeBytes `
            -SpeedX            $shared.SpeedX `
            -UICursorRow       $uiLineCount

        Start-Sleep -Milliseconds 500
    }

    $proc.WaitForExit()
    $ffExit = $proc.ExitCode
    $proc.Dispose()

    # Wait for the stderr reader to finish draining, then tear down its runspace.
    $null = $stderrPs.EndInvoke($stderrAsync)
    $stderrPs.Dispose()
    $stderrRunspace.Close()
    $stderrRunspace.Dispose()

    # Final paint: snap to 100% on success, leave at actual position on failure.
    $null = Write-ProgressUI `
        -FileName          $uiFileName `
        -Profile           $sourceProfile.Profile `
        -EncodeColorLabel  $encodeColorLabel `
        -CRFLabel          $resolvedCRFLabel `
        -PresetLabel       $resolvedPresetLabel `
        -SourceDurationSec $sourceDuration `
        -ElapsedSec        $stopwatch.Elapsed.TotalSeconds `
        -OutTimeSec        $(if ($ffExit -eq 0) { $sourceDuration } else { $shared.OutTimeSec }) `
        -OutSizeBytes      $shared.OutSizeBytes `
        -SpeedX            $shared.SpeedX `
        -UICursorRow       $uiLineCount

    Write-Host ""

    if ($ffExit -ne 0) {
        if ($shared.LogLines.Count -gt 0) {
            Write-Host "-- ffmpeg output -----------------------------------------------" -ForegroundColor DarkGray
            $shared.LogLines | ForEach-Object { Write-Host $_ -ForegroundColor DarkYellow }
            Write-Host "----------------------------------------------------------------" -ForegroundColor DarkGray
        }
        throw "ffmpeg exited with code $ffExit"
    }

    if (-not (Test-Path -LiteralPath $tempOutput)) {
        throw "Temporary output was not created: $tempOutput"
    }

    # ── Duration sanity check ─────────────────────────────────────────────────
    # Uses a flat 10-second floor or 2% of source duration, whichever is larger.
    # A flat percentage alone would reject valid short clips where muxer rounding
    # differences are proportionally significant.
    $outProbe       = Invoke-FfprobeJson -InputPath $tempOutput
    $outputDuration = [double](Get-StreamProp (Get-StreamProp $outProbe 'format' ([PSCustomObject]@{})) 'duration' 0)

    if ($sourceDuration -gt 0) {
        $allowedDelta = [Math]::Max(10.0, $sourceDuration * 0.02)
        if ($outputDuration -lt ($sourceDuration - $allowedDelta)) {
            throw ("Output duration check failed. Source={0:F3}s  Output={1:F3}s  AllowedDelta={2:F3}s" -f $sourceDuration, $outputDuration, $allowedDelta)
        }
    }

    $outItem       = Get-Item -LiteralPath $tempOutput
    $outputSizeGiB = [Math]::Round(($outItem.Length / 1GB), 3)
    $reduction     = if ($sourceItem.Length -gt 0) {
        [Math]::Round((1 - ($outItem.Length / [double]$sourceItem.Length)) * 100, 2)
    } else { 0 }

    # ── Replace original ──────────────────────────────────────────────────────
    # The delete and move are wrapped together so that if Move-Item fails after
    # the source has been removed, the error message surfaces the temp file path
    # for manual recovery rather than leaving the user with neither file.
    $outputPathForLog = $finalOutput
    if ($ReplaceOriginal) {
        try {
            if ($KeepBackupOriginal) {
                $backupPath = Move-ToBackup -OriginalPath $InputPath
                Write-Host "Moved original to backup: $backupPath" -ForegroundColor Yellow
            } else {
                Remove-Item -LiteralPath $InputPath -Force
            }
            Move-Item -LiteralPath $tempOutput -Destination $finalOutput -Force
        } catch {
            $tempStillExists = Test-Path -LiteralPath $tempOutput
            $recovery = if ($tempStillExists) {
                "Encoded temp file still exists and can be recovered: $tempOutput"
            } else {
                "Encoded temp file is also missing. Check disk for partial writes."
            }
            throw "Post-encode file management failed: $_`n$recovery"
        }
    } else {
        Move-Item -LiteralPath $tempOutput -Destination $finalOutput -Force
    }

    $stopwatch.Stop()

    Write-LogRow @{
        Timestamp         = (Get-Date).ToString("s")
        Status            = "SUCCESS"
        InputPath         = $InputPath
        OutputPath        = $outputPathForLog
        SourceSizeGiB     = $sourceSizeGiB
        OutputSizeGiB     = $outputSizeGiB
        ReductionPercent  = $reduction
        SourceDurationSec = [Math]::Round($sourceDuration, 3)
        OutputDurationSec = [Math]::Round($outputDuration, 3)
        ElapsedSec        = [Math]::Round($stopwatch.Elapsed.TotalSeconds, 2)
        Profile           = $sourceProfile.Profile
        HasHDR            = $sourceProfile.HasHDR
        HasDV             = $sourceProfile.HasDV
        SelectedAudio     = $selectedAudioSummary
        SelectedSubtitles = $selectedSubtitleSummary
        CRF               = $CRF
        Preset            = $Preset
        FilmGrain         = $FilmGrain
        AutoCRFOffset     = $AutoCRFOffset
        ResolvedCRF       = $resolvedCRF
        ResolvedPreset    = $resolvedPreset
        ResolvedFilmGrain = $resolvedFilmGrain
        AutoReason        = $autoSettings.Reason
        BPP               = [Math]::Round($autoSettings.BPP, 6)
        EffectiveVideoBitrate = $autoSettings.VideoBitrate
        VideoBitratePerHourGiB = [Math]::Round($autoSettings.VideoBitratePerHourGiB, 3)
        ResolutionTier    = $autoSettings.ResolutionTier
        CodecClass        = $autoSettings.CodecClass
        GrainClass        = $autoSettings.GrainClass
        GrainScore        = $autoSettings.GrainScore
        WasAutoSkipped    = "False"
        FfmpegPath        = $FfmpegPath
        FfprobePath       = $FfprobePath
        Notes             = $autoSettings.BitrateReason
    }

    Write-Host ""
    Write-Host "Done: $outputPathForLog" -ForegroundColor Cyan
    Write-Host ("Source: {0} GiB  ->  Output: {1} GiB  ({2}% smaller)" -f $sourceSizeGiB, $outputSizeGiB, $reduction) -ForegroundColor Cyan
    Write-Host ""
}

# =============================================================================
# FUNCTION: Invoke-QueueProcessing
#
# Drives the encode loop: dequeues and processes jobs until the pending
# directory is empty.
#
# Startup -- interrupted job detection:
#   If current_job.json exists when this function is called it means the
#   previous run was killed before it could clean up (e.g. system shutdown or
#   Ctrl+C during a file replace). The state file is read and an INTERRUPTED
#   row is written to the log. If the temp output file still exists on disk a
#   warning is printed so the user can inspect it -- it may be a complete or
#   near-complete encode that can be renamed and used directly.
#
# Main loop:
#   1.  Find the oldest JSON file in .queue\pending\ (FIFO by creation time).
#   2.  Atomically move it to .queue\working\. The working directory acts as a
#       lock token; the pending-to-working move prevents a second instance from
#       picking up the same job even if the mutex somehow fails.
#   3.  Call Invoke-EncodeJob. On failure the error is printed and a FAILED row
#       is logged before the loop continues with the next job.
#   4.  The working file and state file are both cleaned up in the finally
#       block. The state file is deleted here rather than at the top of the
#       loop so that only a genuine crash leaves it on disk for detection.
# =============================================================================
function Invoke-QueueProcessing {

    if (Test-Path -LiteralPath $StatePath) {
        try {
            $interrupted = Get-Content -LiteralPath $StatePath -Raw | ConvertFrom-Json
            $tempPath    = $interrupted.TempOutput

            Write-Warning "Detected interrupted job from previous run: $($interrupted.InputPath)"

            if ($tempPath -and (Test-Path -LiteralPath $tempPath)) {
                Write-Warning "Encoded temp file still exists: $tempPath"
                Write-Warning "Inspect manually -- it may be a complete encode. Move to final path or delete before re-queuing."
            }

            Write-LogRow @{
                Timestamp         = (Get-Date).ToString("s")
                Status            = "INTERRUPTED"
                InputPath         = $interrupted.InputPath
                OutputPath        = $interrupted.FinalOutput
                SourceSizeGiB     = ""
                OutputSizeGiB     = ""
                ReductionPercent  = ""
                SourceDurationSec = ""
                OutputDurationSec = ""
                ElapsedSec        = ""
                Profile           = $interrupted.Profile
                HasHDR            = $interrupted.HasHDR
                HasDV             = $interrupted.HasDV
                SelectedAudio     = ""
                SelectedSubtitles = ""
                CRF               = $interrupted.CRF
                Preset            = $interrupted.Preset
                FilmGrain         = $interrupted.FilmGrain
                AutoCRFOffset     = Get-OptionalProperty $interrupted 'AutoCRFOffset' ''
                ResolvedCRF       = Get-OptionalProperty $interrupted 'ResolvedCRF' ''
                ResolvedPreset    = Get-OptionalProperty $interrupted 'ResolvedPreset' ''
                ResolvedFilmGrain = Get-OptionalProperty $interrupted 'ResolvedFilmGrain' ''
                AutoReason        = Get-OptionalProperty $interrupted 'AutoReason' ''
                BPP               = Get-OptionalProperty $interrupted 'BPP' ''
                EffectiveVideoBitrate = Get-OptionalProperty $interrupted 'EffectiveVideoBitrate' ''
                VideoBitratePerHourGiB = Get-OptionalProperty $interrupted 'VideoBitratePerHourGiB' ''
                ResolutionTier    = Get-OptionalProperty $interrupted 'ResolutionTier' ''
                CodecClass        = Get-OptionalProperty $interrupted 'CodecClass' ''
                GrainClass        = Get-OptionalProperty $interrupted 'GrainClass' ''
                GrainScore        = Get-OptionalProperty $interrupted 'GrainScore' ''
                WasAutoSkipped    = Get-OptionalProperty $interrupted 'WasAutoSkipped' 'False'
                FfmpegPath        = $FfmpegPath
                FfprobePath       = $FfprobePath
                Notes             = "Process was interrupted. Temp output may exist at: $tempPath"
            }
        } catch {
            Write-Warning "Could not parse interrupted state file: $_"
        }

        Remove-Item -LiteralPath $StatePath -Force -ErrorAction SilentlyContinue
    }

    while ($true) {
        $nextJob = Get-ChildItem -LiteralPath $QueuePendingDir -Filter *.json -File |
            Sort-Object CreationTimeUtc |
            Select-Object -First 1

        if (-not $nextJob) { break }

        $workingJobPath = Join-Path $QueueWorkingDir $nextJob.Name
        Move-Item -LiteralPath $nextJob.FullName -Destination $workingJobPath -Force

        try {
            $job = Get-Content -LiteralPath $workingJobPath -Raw | ConvertFrom-Json
            Invoke-EncodeJob -InputPath $job.InputPath
        }
        catch {
            $message = $_.Exception.Message
            $position = $_.InvocationInfo.PositionMessage
            $stack = $_.ScriptStackTrace
            $state = $null

            if (Test-Path -LiteralPath $StatePath) {
                try {
                    $state = Get-Content -LiteralPath $StatePath -Raw | ConvertFrom-Json
                } catch {}
            }

            Write-Host "FAILED: $message" -ForegroundColor Red

            if ($position) {
                Write-Host ""
                Write-Host $position -ForegroundColor Yellow
            }

            if ($stack) {
                Write-Host ""
                Write-Host "Stack trace:" -ForegroundColor DarkYellow
                Write-Host $stack -ForegroundColor DarkYellow
            }

            Write-LogRow @{
                Timestamp         = (Get-Date).ToString("s")
                Status            = "FAILED"
                InputPath         = $job.InputPath
                OutputPath        = ""
                SourceSizeGiB     = ""
                OutputSizeGiB     = ""
                ReductionPercent  = ""
                SourceDurationSec = ""
                OutputDurationSec = ""
                ElapsedSec        = ""
                Profile           = if ($state) { Get-OptionalProperty $state 'Profile' '' } else { "" }
                HasHDR            = if ($state) { Get-OptionalProperty $state 'HasHDR' '' } else { "" }
                HasDV             = if ($state) { Get-OptionalProperty $state 'HasDV' '' } else { "" }
                SelectedAudio     = ""
                SelectedSubtitles = ""
                CRF               = if ($state) { Get-OptionalProperty $state 'CRF' $CRF } else { $CRF }
                Preset            = if ($state) { Get-OptionalProperty $state 'Preset' $Preset } else { $Preset }
                FilmGrain         = if ($state) { Get-OptionalProperty $state 'FilmGrain' $FilmGrain } else { $FilmGrain }
                AutoCRFOffset     = if ($state) { Get-OptionalProperty $state 'AutoCRFOffset' $AutoCRFOffset } else { $AutoCRFOffset }
                ResolvedCRF       = if ($state) { Get-OptionalProperty $state 'ResolvedCRF' '' } else { "" }
                ResolvedPreset    = if ($state) { Get-OptionalProperty $state 'ResolvedPreset' '' } else { "" }
                ResolvedFilmGrain = if ($state) { Get-OptionalProperty $state 'ResolvedFilmGrain' '' } else { "" }
                AutoReason        = if ($state) { Get-OptionalProperty $state 'AutoReason' '' } else { "" }
                BPP               = if ($state) { Get-OptionalProperty $state 'BPP' '' } else { "" }
                EffectiveVideoBitrate = if ($state) { Get-OptionalProperty $state 'EffectiveVideoBitrate' '' } else { "" }
                VideoBitratePerHourGiB = if ($state) { Get-OptionalProperty $state 'VideoBitratePerHourGiB' '' } else { "" }
                ResolutionTier    = if ($state) { Get-OptionalProperty $state 'ResolutionTier' '' } else { "" }
                CodecClass        = if ($state) { Get-OptionalProperty $state 'CodecClass' '' } else { "" }
                GrainClass        = if ($state) { Get-OptionalProperty $state 'GrainClass' '' } else { "" }
                GrainScore        = if ($state) { Get-OptionalProperty $state 'GrainScore' '' } else { "" }
                WasAutoSkipped    = if ($state) { Get-OptionalProperty $state 'WasAutoSkipped' 'False' } else { "False" }
                FfmpegPath        = $FfmpegPath
                FfprobePath       = $FfprobePath
                Notes             = ($message + " | " + $position)
            }
        }
        finally {
            if (Test-Path -LiteralPath $workingJobPath) {
                Remove-Item -LiteralPath $workingJobPath -Force -ErrorAction SilentlyContinue
            }

            # Delete the state file only after the job has concluded (success or
            # handled failure). A crash between encode completion and this line
            # leaves the state file on disk for detection on the next run.
            if (Test-Path -LiteralPath $StatePath) {
                Remove-Item -LiteralPath $StatePath -Force -ErrorAction SilentlyContinue
            }
        }
    }
}

# =============================================================================
# Entry point
#
# 1.  Validate that at least one input path was provided.
# 2.  Call Add-QueueInputs to validate and enqueue all inputs.
# 3.  Attempt to acquire the machine-global named mutex with a 0 ms timeout.
#     WaitOne(0) returns $true if this process got the lock, $false if another
#     instance already holds it.
# 4.  If another worker is running, exit cleanly -- files are already queued
#     and will be processed when that worker's loop reaches them.
# 5.  If this process holds the lock, call Invoke-QueueProcessing to drain
#     the queue. The mutex is released in the finally block regardless of
#     whether processing succeeds or throws.
# =============================================================================
$CRF       = Resolve-ConfigValue -Name 'CRF'       -Value $CRF       -Minimum 0 -Maximum 63
$Preset    = Resolve-ConfigValue -Name 'Preset'    -Value $Preset    -Minimum 0 -Maximum 13
$FilmGrain = Resolve-ConfigValue -Name 'FilmGrain' -Value $FilmGrain -Minimum 0 -Maximum 50
$AutoCRFOffset = Resolve-OffsetConfigValue -Name 'AutoCRFOffset' -Value $AutoCRFOffset

Update-LogSchemaIfNeeded

if (-not $InputPaths -or $InputPaths.Count -eq 0) {
    Write-Host "Drag one or more files onto the .bat launcher, or call this script with file paths." -ForegroundColor Yellow
    exit 1
}

Add-QueueInputs -Paths $InputPaths

$createdNew = $false
$mutex      = [System.Threading.Mutex]::new($false, $MutexName, [ref]$createdNew)

$hasLock = $false
try {
    $hasLock = $mutex.WaitOne(0)
    if (-not $hasLock) {
        Write-Host "Another encode worker is already running. Files were added to queue." -ForegroundColor Yellow
        exit 0
    }

    Invoke-QueueProcessing
}
finally {
    if ($hasLock) { $mutex.ReleaseMutex() | Out-Null }
    $mutex.Dispose()
}
