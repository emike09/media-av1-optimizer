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
#   - ffmpeg / ffprobe 6.x+        — placed next to the script or on PATH
#
# =============================================================================
[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$InputPaths
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# =============================================================================
# User-configurable settings
# =============================================================================

# Encoding quality / speed
$CRF    = 10    # SVT-AV1 CRF. Lower = better quality, larger file. Range 0-63.
$Preset = 4     # SVT-AV1 preset. Lower = slower encode, higher efficiency. Range 0-13.

# Source handling
$SkipDolbyVisionSources = $true    # Skip DV sources rather than silently destroying DV metadata.
$KeepBackupOriginal     = $false   # $true  -> move original to .queue\backup_originals\ after encode.
                                   # $false -> delete original after a verified successful encode.
$ReplaceOriginal        = $true    # Rename the finished .mkv to the original file's base name.

# Stream selection
$KeepEnglishSDH           = $true  # Retain an SDH subtitle track alongside the main subtitle.
$KeepEnglishFallbackAudio = $true  # Retain a secondary lossy English audio track (e.g. stereo AAC)
                                   # when the main track is lossless. Excluded if same codec as main.

# Queue / log paths  (all relative to the script's own directory)
$QueueRoot       = Join-Path $PSScriptRoot ".queue"
$QueuePendingDir = Join-Path $QueueRoot "pending"
$QueueWorkingDir = Join-Path $QueueRoot "working"
$BackupDir       = Join-Path $QueueRoot "backup_originals"
$LogPath         = Join-Path $QueueRoot "encode_log.csv"
$StatePath       = Join-Path $QueueRoot "current_job.json"

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
    "Timestamp,Status,InputPath,OutputPath,SourceSizeGiB,OutputSizeGiB,ReductionPercent,SourceDurationSec,OutputDurationSec,ElapsedSec,Profile,HasHDR,HasDV,SelectedAudio,SelectedSubtitles,CRF,Preset,FfmpegPath,FfprobePath,Notes" |
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

    $ordered = [ordered]@{
        Timestamp         = $Row.Timestamp
        Status            = $Row.Status
        InputPath         = $Row.InputPath
        OutputPath        = $Row.OutputPath
        SourceSizeGiB     = $Row.SourceSizeGiB
        OutputSizeGiB     = $Row.OutputSizeGiB
        ReductionPercent  = $Row.ReductionPercent
        SourceDurationSec = $Row.SourceDurationSec
        OutputDurationSec = $Row.OutputDurationSec
        ElapsedSec        = $Row.ElapsedSec
        Profile           = $Row.Profile
        HasHDR            = $Row.HasHDR
        HasDV             = $Row.HasDV
        SelectedAudio     = $Row.SelectedAudio
        SelectedSubtitles = $Row.SelectedSubtitles
        CRF               = $Row.CRF
        Preset            = $Row.Preset
        FfmpegPath        = $Row.FfmpegPath
        FfprobePath       = $Row.FfprobePath
        Notes             = $Row.Notes
    }

    $line = ($ordered.Values | ForEach-Object {
        $s = [string]$_
        '"' + ($s -replace '"', '""') + '"'
    }) -join ","

    Add-Content -LiteralPath $LogPath -Value $line -Encoding UTF8
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

    return $paths
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
# payload reaches stdout. The --% stop-parsing token passes the input path
# through without PowerShell re-interpreting any special characters in it.
# =============================================================================
function Invoke-FfprobeJson {
    param([string]$InputPath)

    $json = & $FfprobePath `
        -v error `
        -print_format json `
        -show_format `
        -show_streams `
        -show_chapters `
        --% "$InputPath"

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
function Get-StreamLanguage {
    param($Stream)
    $lang = $Stream.tags.language
    if ([string]::IsNullOrWhiteSpace($lang)) { return "" }
    return $lang.ToLowerInvariant()
}

function Get-StreamTitle {
    param($Stream)
    $title = $Stream.tags.title
    if ([string]::IsNullOrWhiteSpace($title)) { return "" }
    return $title
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
    try {
        if ($Stream.disposition.forced -eq 1) { return $true }
    } catch {}
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

    $codec    = ($Stream.codec_name ?? "").ToLowerInvariant()
    $title    = (Get-StreamTitle -Stream $Stream).ToLowerInvariant()
    $channels = [int]($Stream.channels ?? 0)

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
    $codec = ($Stream.codec_name ?? "").ToLowerInvariant()
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
        $_.codec_type -eq 'video' -and
        -not ($_.disposition.attached_pic -eq 1)
    }

    if (-not $videoStreams) {
        throw "No usable video stream found."
    }

    $video          = $videoStreams | Select-Object -First 1
    $audioStreams    = $streams | Where-Object { $_.codec_type -eq 'audio' }
    $subtitleStreams = $streams | Where-Object { $_.codec_type -eq 'subtitle' }

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
            @{ Expression = { Get-AudioRank $_ };            Descending = $true },
            @{ Expression = { [int]($_.channels  ?? 0) };   Descending = $true },
            @{ Expression = { [int64]($_.bit_rate ?? 0) };  Descending = $true } |
        Select-Object -First 1

    $fallbackAudio = $null
    if ($KeepEnglishFallbackAudio) {
        # Only consider tracks with a different codec to main, so we don't end up
        # with two EAC3 tracks at different bitrates serving no practical purpose.
        $mainCodec = ($mainAudio.codec_name ?? "").ToLowerInvariant()

        $fallbackCandidates = $englishAudio | Where-Object {
            $_.index -ne $mainAudio.index -and
            -not (Test-IsCommentary $_) -and
            (Test-IsLossyAudio $_) -and
            ($_.codec_name ?? "").ToLowerInvariant() -ne $mainCodec
        }

        if ($fallbackCandidates) {
            $fallbackAudio = $fallbackCandidates |
                Sort-Object `
                    @{ Expression = {
                        $codec = ($_.codec_name ?? "").ToLowerInvariant()
                        switch ($codec) {
                            'eac3'  { 500 }
                            'ac3'   { 400 }
                            'aac'   { 300 }
                            'dts'   { 200 }
                            default { 100 }
                        }
                    }; Descending = $true },
                    @{ Expression = { [int]($_.channels  ?? 0) };  Descending = $true },
                    @{ Expression = { [int64]($_.bit_rate ?? 0) }; Descending = $true } |
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
                @{ Expression = { if ($_.disposition.default -eq 1) {  50 } else { 0 } }; Descending = $true },
                @{ Expression = { if ($_.codec_name -eq 'subrip')   {  20 } else { 10 } }; Descending = $true } |
            Select-Object -First 1

        if (-not $mainSub) {
            $mainSub = $englishSubs | Select-Object -First 1
        }

        if ($KeepEnglishSDH) {
            $sdhSub = $englishSubs |
                Where-Object { $_.index -ne $mainSub.index -and (Test-IsSDH $_) } |
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

    $hasDV = [bool](
        @($Probe.streams) | Where-Object {
            ($_.codec_name       ?? "") -match 'dvhe|dvav' -or
            ($_.codec_tag_string ?? "") -match 'dvhe|dvav' -or
            (
                $_.side_data_list -and
                @($_.side_data_list) | Where-Object { ($_.side_data_type ?? "") -match 'DOVI|Dolby Vision' }
            )
        }
    )

    $hasHDR  = $false
    $transfer = [string]($VideoStream.color_transfer  ?? "")
    $primaries = [string]($VideoStream.color_primaries ?? "")
    $matrix   = [string]($VideoStream.color_space     ?? "")

    if ($transfer  -match 'smpte2084|arib-std-b67|bt2020-10' -or
        $primaries -match 'bt2020' -or
        $matrix    -match 'bt2020') {
        $hasHDR = $true
    }

    $profile = if ($hasDV) { "DV" } elseif ($hasHDR) { "HDR" } else { "SDR" }

    return [ordered]@{
        HasDV   = $hasDV
        HasHDR  = $hasHDR
        Profile = $profile
    }
}

# =============================================================================
# FUNCTION GROUP: Output path helpers
#
#   Get-TempOutputPath  - Returns the path for the in-progress encode output.
#                         Named <basename>.encoding.tmp.mkv in the source
#                         directory. Using a distinct temp name means a partial
#                         file is never mistaken for a complete encode.
#
#   Get-FinalOutputPath - Returns the intended final output path.
#                         Named <basename>.mkv in the source directory.
#                         When the source is already an MKV this resolves to
#                         the same path as the source; the original is deleted
#                         or moved before the temp file is renamed into place.
# =============================================================================
function Get-TempOutputPath {
    param([string]$InputPath)
    $dir  = Split-Path -LiteralPath $InputPath -Parent
    $name = [System.IO.Path]::GetFileNameWithoutExtension($InputPath)
    return Join-Path $dir ($name + ".encoding.tmp.mkv")
}

function Get-FinalOutputPath {
    param([string]$InputPath)
    $dir  = Split-Path -LiteralPath $InputPath -Parent
    $name = [System.IO.Path]::GetFileNameWithoutExtension($InputPath)
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
        $codec = ($_.codec_name ?? "")
        $ch    = ($_.channels   ?? "")
        "idx=$($_.index);lang=$lang;codec=$codec;ch=$ch;title=$title"
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
    $queueNames = Get-PendingQueueNames

    # ── ANSI colour codes ─────────────────────────────────────────────────────
    $ESC      = [char]27
    $reset    = "${ESC}[0m"
    $cBorder  = "${ESC}[38;5;240m"   # dark grey    -- box lines
    $cTitle   = "${ESC}[1;97m"       # bold white   -- section headings
    $cFile    = "${ESC}[1;96m"       # bold cyan    -- filename
    $cMeta    = "${ESC}[38;5;250m"   # light grey   -- CRF / preset / elapsed
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
    $lines.Add((Row "$Profile  |  CRF $CRF  |  Preset $Preset  |  $elapsStr elapsed" $cMeta))
    $lines.Add((BlankRow))
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

    $outputDir = Split-Path -LiteralPath $InputPath -Parent
    $null = Test-SufficientDiskSpace -TargetDirectory $outputDir -SourceSizeBytes $sourceItem.Length

    $probe         = Invoke-FfprobeJson -InputPath $InputPath
    $selected      = Select-Streams     -Probe $probe
    $sourceProfile = Get-SourceProfile  -Probe $probe -VideoStream $selected.Video

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
            SourceDurationSec = [double]($probe.format.duration ?? 0)
            OutputDurationSec = ""
            ElapsedSec        = [Math]::Round($stopwatch.Elapsed.TotalSeconds, 2)
            Profile           = $sourceProfile.Profile
            HasHDR            = $sourceProfile.HasHDR
            HasDV             = $sourceProfile.HasDV
            SelectedAudio     = Format-StreamSummary -Streams @($selected.MainAudio, $selected.FallbackAudio)
            SelectedSubtitles = Format-StreamSummary -Streams @($selected.MainSub, $selected.SdhSub)
            CRF               = $CRF
            Preset            = $Preset
            FfmpegPath        = $FfmpegPath
            FfprobePath       = $FfprobePath
            Notes             = "Dolby Vision source skipped by policy."
        }
        return
    }

    $tempOutput  = Get-TempOutputPath  -InputPath $InputPath
    $finalOutput = Get-FinalOutputPath -InputPath $InputPath

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
    $ffArgs.AddRange(@(
        "-hide_banner",
        "-y",
        "-i", $InputPath,
        "-map", "0:$($selected.Video.index)",
        "-map", "0:$($selected.MainAudio.index)"
    ))

    if ($selected.FallbackAudio) { $ffArgs.AddRange(@("-map", "0:$($selected.FallbackAudio.index)")) }
    if ($selected.MainSub)       { $ffArgs.AddRange(@("-map", "0:$($selected.MainSub.index)")) }
    if ($selected.SdhSub)        { $ffArgs.AddRange(@("-map", "0:$($selected.SdhSub.index)")) }

    $ffArgs.AddRange(@(
        "-map_chapters",  "0",
        # -map_metadata -1 clears all global container metadata first. The explicit
        # -metadata flags below then re-add only what we want. Ordering matters --
        # the clear must precede all metadata write arguments.
        "-map_metadata",  "-1",
        "-max_muxing_queue_size", "4096",
        "-c:v",     "libsvtav1",
        "-preset",  "$Preset",
        "-crf",     "$CRF",
        "-pix_fmt", "yuv420p10le"
    ))

    if ($sourceProfile.HasHDR) {
        # smpte2084 (PQ) is the correct transfer function for both HDR10 and HDR10+.
        # HLG sources are also flagged HasHDR; tagging them smpte2084 is a known
        # trade-off when repackaging into AV1/MKV without tone-mapping.
        $ffArgs.AddRange(@(
            "-color_primaries", "bt2020",
            "-color_trc",       "smpte2084",
            "-colorspace",      "bt2020nc"
        ))
    }

    $ffArgs.AddRange(@("-c:a", "copy"))

    if ($selected.MainSub -or $selected.SdhSub) { $ffArgs.AddRange(@("-c:s", "copy")) }

    $ffArgs.AddRange(@(
        "-disposition:v:0", "default",
        "-disposition:a:0", "default"
    ))

    if ($selected.FallbackAudio) { $ffArgs.AddRange(@("-disposition:a:1", "0")) }

    if ($selected.MainSub) { $ffArgs.AddRange(@("-disposition:s:0", "default")) }

    if ($selected.SdhSub) {
        $subIndex = if ($selected.MainSub) { 1 } else { 0 }
        $ffArgs.AddRange(@("-disposition:s:$subIndex", "0"))
    }

    $baseTitle  = [System.IO.Path]::GetFileNameWithoutExtension($InputPath)
    $videoTitle = if ($sourceProfile.HasHDR) { "AV1 HDR10" } else { "AV1 SDR" }

    $ffArgs.AddRange(@(
        "-metadata",       "title=$baseTitle",
        "-metadata:s:v:0", "title=$videoTitle",
        "-metadata:s:a:0", "title=$(Get-StreamTitle $selected.MainAudio)"
    ))

    if ($selected.FallbackAudio) {
        $ffArgs.AddRange(@("-metadata:s:a:1", "title=$(Get-StreamTitle $selected.FallbackAudio)"))
    }

    if ($selected.MainSub) {
        $ffArgs.AddRange(@("-metadata:s:s:0", "title=$(Get-StreamTitle $selected.MainSub)"))
    }

    if ($selected.SdhSub) {
        $subIndex = if ($selected.MainSub) { 1 } else { 0 }
        $ffArgs.AddRange(@("-metadata:s:s:$subIndex", "title=$(Get-StreamTitle $selected.SdhSub)"))
    }

    # Direct ffmpeg to emit machine-readable key=value progress to stderr every
    # 2 seconds. Stderr is fully redirected; the async callback below parses it.
    $ffArgs.AddRange(@("-progress", "pipe:2", "-stats_period", "2"))
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
    } | ConvertTo-Json -Depth 8

    Set-Content -LiteralPath $StatePath -Value $currentState -Encoding UTF8

    # ── Print encode header ───────────────────────────────────────────────────
    Write-Host ""
    Write-Host "------------------------------------------------------------" -ForegroundColor DarkGray
    Write-Host "Encoding: $InputPath"                                          -ForegroundColor Green
    Write-Host "Profile : $($sourceProfile.Profile)"                           -ForegroundColor Green
    Write-Host "Audio   : $(Format-StreamSummary @($selected.MainAudio, $selected.FallbackAudio))" -ForegroundColor Green
    Write-Host "Subs    : $(Format-StreamSummary @($selected.MainSub, $selected.SdhSub))"          -ForegroundColor Green
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

    # Async stderr reader: fires on a threadpool thread for each line received.
    # Lines matching the key=value pattern are progress data from -progress pipe:2.
    # Everything else is a normal ffmpeg log line, accumulated for display on failure.
    $errAction = {
        param($sender, $e)
        $line = $e.Data
        if ([string]::IsNullOrEmpty($line)) { return }

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
                    if ([double]::TryParse(($v -replace 'x', ''),
                            [Globalization.NumberStyles]::Any,
                            [Globalization.CultureInfo]::InvariantCulture, [ref]$sp)) {
                        $shared.SpeedX = [Math]::Max(0.0, $sp)
                    }
                }
            }
        } else {
            $shared.LogLines.Add($line)
        }
    }

    $proc.add_ErrorDataReceived($errAction)
    $null = $proc.Start()
    $proc.BeginErrorReadLine()

    # ── Live UI loop ──────────────────────────────────────────────────────────
    $uiFileName     = [System.IO.Path]::GetFileName($InputPath)
    $uiLineCount    = -1   # -1 signals first paint; no cursor-up on first call
    $sourceDuration = [double]($probe.format.duration ?? 0)

    while (-not $proc.HasExited) {
        $uiLineCount = Write-ProgressUI `
            -FileName          $uiFileName `
            -Profile           $sourceProfile.Profile `
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

    # Final paint: snap to 100% on success, leave at actual position on failure.
    $null = Write-ProgressUI `
        -FileName          $uiFileName `
        -Profile           $sourceProfile.Profile `
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
    $outputDuration = [double]($outProbe.format.duration ?? 0)

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
        SelectedAudio     = Format-StreamSummary -Streams @($selected.MainAudio, $selected.FallbackAudio)
        SelectedSubtitles = Format-StreamSummary -Streams @($selected.MainSub, $selected.SdhSub)
        CRF               = $CRF
        Preset            = $Preset
        FfmpegPath        = $FfmpegPath
        FfprobePath       = $FfprobePath
        Notes             = ""
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
            Write-Host "FAILED: $message" -ForegroundColor Red

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
                Profile           = ""
                HasHDR            = ""
                HasDV             = ""
                SelectedAudio     = ""
                SelectedSubtitles = ""
                CRF               = $CRF
                Preset            = $Preset
                FfmpegPath        = $FfmpegPath
                FfprobePath       = $FfprobePath
                Notes             = $message
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
