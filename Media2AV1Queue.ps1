#requires -Version 7.0
# =============================================================================
# Media2AV1Queue.ps1
#
# Queue-based AV1 encoder for video libraries.
#
# Edit settings in the "User-configurable settings" section below.
#
# Requirements:
# - PowerShell 7+
# - FFmpeg / FFprobe 8.1 full build
# - FFmpeg 6.x / 7.x / stripped/basic builds are not supported
# =============================================================================

# Do not change the following lines.
[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$InputPaths
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ErrorView = 'NormalView'
$script:ResolvedScriptProcessPriority = 'Normal'
$script:QueueShutdownRequested = $false
$script:QueueShutdownMessageShown = $false
$script:QueueShutdownSentinel = '__QUEUE_SHUTDOWN__'
$script:OriginalTreatControlCAsInput = $null
$script:HeldForCpuAnnouncements = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
$script:SessionLogPath = $null
$script:QueuePaused = $false
$script:SoftExitRequested = $false
$script:ShowHelpOverlay = $false
$script:ConsoleCommandContext = [pscustomobject]@{ Kind = ''; Target = ''; ExpiresAt = $null }
$script:ConsoleStatus = [pscustomobject]@{ Message = ''; Level = 'Info'; ExpiresAt = $null }
$script:ThreadControlInteropLoaded = $false
$script:TestAutoShutdownAt = $null
$script:TestAutoShutdownSeconds = 0

# Supports bare Auto in config, e.g.:
#   $CRF = Auto
# or
#   $CRF = 'Auto'
# Do not change this function.
function Auto {
    return 'Auto'
}

# =============================================================================
# User-configurable settings
# =============================================================================
# Quick start:
# - Best quality/compression: $EncoderPreference = 'CPU'
# - Best speed:               $EncoderPreference = 'Nvidia'
# - Best automation:          $EncoderPreference = 'Auto'
# - Most users should leave CRF / Preset / FilmGrain on Auto
# =============================================================================

# ------------------------------------------------------------------------
# Quality
# ------------------------------------------------------------------------
$CRF = Auto                       # 0-63, recommend 10-28, or Auto. Lower = better quality / larger file.
$Preset = Auto                    # Software: 0-13, recommend 3-6, or Auto. Lower = slower / better compression.
$AutoCRFOffset = Auto             # Integer or Auto. Recommended -2 to +2. Auto = 0.
$FilmGrain = Auto                 # 0-50, recommend 0-16, or Auto. See notes below.

# ------------------------------------------------------------------------
# Encoder lanes
# ------------------------------------------------------------------------
$EncoderPreference = 'Auto'       # Auto | CPU | Nvidia
$SoftwareEncodePriority = 'BelowNormal' # Idle | BelowNormal | Normal | AboveNormal
$HardwareEncodePriority = 'Normal'      # Idle | BelowNormal | Normal | AboveNormal
$ScriptProcessPriority  = 'Normal'      # Idle | BelowNormal | Normal | AboveNormal
$ApplyProcessPriority   = $true         # $true = apply priority settings above

# ------------------------------------------------------------------------
# NVENC
# ------------------------------------------------------------------------
$NvencMaxParallel = Auto          # 1-8, recommend Auto, or Auto. Auto uses GPU model lookup.
$NvencCQ = Auto                   # 0-51, recommend 18-28, or Auto. Lower = better quality / larger file.
$NvencPreset = Auto               # p1-p7 or Auto. Recommended p5-p6 for quality-focused NVENC.
$NvencDecode = Auto               # Auto | cpu | cuda
$NvencTune = 'auto'               # auto | hq | ll | ull | $null
$NvencAllowSplitFrame = $false    # $true = allow split-frame if supported. Leave off unless testing.

# ------------------------------------------------------------------------
# Preflight estimation & auto-tuning
# ------------------------------------------------------------------------
$EnablePreflightEstimate = $true              # Run sample-based estimate before full encode.
$PreflightSampleCount = 4                     # 1-12, recommend 3-6.
$PreflightSampleDurationSec = 30              # 5-120, recommend 15-30.
$PreflightWarnIfEstimatedPctOfSource = 95     # 1-1000, recommend 90-100.
$PreflightAbortIfEstimatedPctOfSource = 100   # 1-1000, recommend 95-110.
$EnablePreflightAutoTune = $true              # Allow preflight to retune Auto settings.
$EnableSecondPreflightPass = $true            # Run one more preflight after major retuning.
$PreflightAutoTuneQuality = 'High'            # Low | Medium | High

# Tiny-output safety check
# Helps catch cases where Auto mode may compress too aggressively.
$PreflightTinyOutputPctThreshold = 35         # 1-100, recommend 25-50.
$PreflightTinyOutputAbsoluteGiBThreshold = 1.0 # 0.1-100.0, recommend 0.5-2.0.

# Live size estimate
$EnableLiveSizeEstimate = $true               # Show estimated final size during encode.
$LiveEstimateStartPercent = 3                 # 1-100, recommend 3-15.
$LiveEstimateSmoothingFactor = 0.30           # 0.01-1.00, recommend 0.20-0.40.

# Advanced preflight overrides
# Leave these at $null unless you specifically want manual GiB/hr control.
$PreflightAutoTuneCustomTargetGiBPerHour = $null # Decimal or $null. Recommended 1.0-20.0 depending on source.
$PreflightAutoTuneCustomUpperGiBPerHour = $null  # Decimal or $null. Recommended target + 1 to +4.
$PreflightAutoTuneCustomLowerGiBPerHour = $null  # Decimal or $null. Recommended target - 1 to -4.

# ------------------------------------------------------------------------
# Source handling
# ------------------------------------------------------------------------
$SkipDolbyVisionSources = $true   # $true = skip DV sources by default.
$KeepBackupOriginal = $false      # $true = move original to backup folder after success.
$ReplaceOriginal = $true          # $true = replace source with finished AV1 output on success.

# ------------------------------------------------------------------------
# Stream selection
# ------------------------------------------------------------------------
$KeepEnglishSDH = $false          # Keep an English SDH subtitle track.
$KeepEnglishFallbackAudio = $true # Keep a secondary lossy English audio track when useful.

# =============================================================================
# End of user-configurable settings
# =============================================================================

# =============================================================================
# Notes
# =============================================================================
# FilmGrain guide:
# - 0     = disabled / clean CGI / animation
# - 4-8   = light grain
# - 8-15  = typical Blu-ray film grain
# - 15-25 = heavy grain
# - 25+   = extreme / degraded sources
#
# NVENC notes:
# - Faster than software SVT-AV1
# - Lower compression efficiency at similar visual quality
# - Film grain synthesis may not be available in the NVENC AV1 path
#
# Preflight notes:
# - Runs short sample encodes before the full encode starts
# - Helps avoid wasting hours on files that would end up too large
# - Auto mode may use preflight to retune CRF / FilmGrain before the main encode

# Queue / log paths  (all relative to the script's own directory)
$QueueRoot       = Join-Path $PSScriptRoot ".queue"
$QueuePendingDir = Join-Path $QueueRoot "pending"
$QueueWorkingDir = Join-Path $QueueRoot "working"
$BackupDir       = Join-Path $QueueRoot "backup_originals"
$PreflightDir    = Join-Path $QueueRoot "preflight"
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
    'EstimatedFinalSizeGiB',
    'EstimatedSavingsPercent',
    'EstimatedOutputGiBPerHour',
    'InitialResolvedCRF',
    'InitialResolvedPreset',
    'InitialResolvedFilmGrain',
    'PreflightPassCount',
    'Preflight1EstimatedFinalGiB',
    'Preflight1EstimatedSavingsPercent',
    'Preflight1EstimatedGiBPerHour',
    'Preflight2EstimatedFinalGiB',
    'Preflight2EstimatedSavingsPercent',
    'Preflight2EstimatedGiBPerHour',
    'FinalResolvedCRF',
    'FinalResolvedPreset',
    'FinalResolvedFilmGrain',
    'PreflightAutoTuneReason',
    'WasPreflightRetuned',
    'WasSkippedByPreflight',
    'CRF',
    'Preset',
    'FilmGrain',
    'AutoCRFOffset',
    'EncoderPreference',
    'ResolvedEncodeLane',
    'LaneSelectionReason',
    'LaneSuitability',
    'CpuOnlyReason',
    'NvidiaFallbackAllowed',
    'HeldForCpuLane',
    'WorkerProcessPriority',
    'ScriptProcessPriority',
    'EncodeMode',
    'ResolvedCRF',
    'ResolvedPreset',
    'ResolvedFilmGrain',
    'ResolvedCQ',
    'ResolvedNvencPreset',
    'ResolvedNvencTune',
    'ResolvedDecodePath',
    'AutoReason',
    'BPP',
    'EffectiveVideoBitrate',
    'VideoBitratePerHourGiB',
    'ResolutionTier',
    'CodecClass',
    'GrainClass',
    'GrainScore',
    'WasAutoSkipped',
    'NvencWorkerCountAtStart',
    'NvencEngineCountDetected',
    'NvencCapacitySource',
    'DetectedGpuName',
    'FilmGrainDisabledReason',
    'FfmpegPath',
    'FfprobePath',
    'Notes'
)

# Named mutex used to enforce a single queue-manager instance.
# The "Global\" prefix makes it machine-wide so it works across all console
# sessions and UAC boundaries. Each script directory keeps its own .queue
# folder, so two separate copies of this script queue independently but will
# never drive the same queue at the same time.
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
$null = New-Item -ItemType Directory -Force -Path $QueueRoot, $QueuePendingDir, $QueueWorkingDir, $BackupDir, $PreflightDir

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

    if ($script:SessionLogPath) {
        Write-SessionTextLogEntry -Row $ordered
    }
}

function Resolve-SessionTextLogPath {
    if (-not [string]::IsNullOrWhiteSpace($script:SessionLogPath) -and (Test-Path -LiteralPath $script:SessionLogPath)) {
        return $script:SessionLogPath
    }

    $hasActiveQueueSession = $false
    try {
        $hasActiveQueueSession = (@(Get-ChildItem -LiteralPath $QueueWorkingDir -Filter *.json -File -ErrorAction SilentlyContinue).Count -gt 0) -or
            (Test-Path -LiteralPath $StatePath)
    } catch {}
    if (-not $hasActiveQueueSession) { return $null }

    try {
        $latest = @(Get-ChildItem -LiteralPath $QueueRoot -Filter *.log -File -ErrorAction SilentlyContinue |
            Sort-Object LastWriteTimeUtc -Descending |
            Select-Object -First 1)
        if ($latest.Count -gt 0) {
            return $latest[0].FullName
        }
    } catch {}

    return $null
}

function Write-SessionTextLogMessage {
    param(
        [string]$Message,
        [ValidateSet('Info', 'Warn', 'Err')]
        [string]$Level = 'Info'
    )

    $logPath = Resolve-SessionTextLogPath
    if ([string]::IsNullOrWhiteSpace($logPath)) { return }
    $stamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    Add-Content -LiteralPath $logPath -Value ("[{0}] [{1}] {2}" -f $stamp, $Level, $Message) -Encoding UTF8
}

function Write-SessionTextLogEntry {
    param($Row)

    if ([string]::IsNullOrWhiteSpace($script:SessionLogPath) -or $null -eq $Row) { return }

    $parts = [System.Collections.Generic.List[string]]::new()
    $level = switch -Regex ([string]$Row.Status) {
        '^FAILED' { 'Err'; break }
        '^SUCCESS' { 'Info'; break }
        '^SKIPPED|^AUTO_SKIPPED|^PRECHECK_SKIPPED' { 'Warn'; break }
        default { 'Info' }
    }
    $parts.Add("Status $($Row.Status)")

    $inputName = if (-not [string]::IsNullOrWhiteSpace([string]$Row.InputPath)) {
        [System.IO.Path]::GetFileName([string]$Row.InputPath)
    } else {
        ''
    }
    if ($inputName) { $parts.Add($inputName) }

    if (-not [string]::IsNullOrWhiteSpace([string]$Row.ResolvedEncodeLane)) { $parts.Add("Lane $($Row.ResolvedEncodeLane)") }
    if (-not [string]::IsNullOrWhiteSpace([string]$Row.EncodeMode))         { $parts.Add("Mode $($Row.EncodeMode)") }
    if (-not [string]::IsNullOrWhiteSpace([string]$Row.Profile))            { $parts.Add("Profile $($Row.Profile)") }
    if (-not [string]::IsNullOrWhiteSpace([string]$Row.OutputSizeGiB))      { $parts.Add("Output $($Row.OutputSizeGiB) GiB") }
    if (-not [string]::IsNullOrWhiteSpace([string]$Row.ReductionPercent))   { $parts.Add("Savings $($Row.ReductionPercent)%") }
    if (-not [string]::IsNullOrWhiteSpace([string]$Row.ResolvedCRF))        { $parts.Add("CRF $($Row.ResolvedCRF)") }
    if (-not [string]::IsNullOrWhiteSpace([string]$Row.ResolvedCQ))         { $parts.Add("CQ $($Row.ResolvedCQ)") }
    if (-not [string]::IsNullOrWhiteSpace([string]$Row.ResolvedPreset))     { $parts.Add("Preset $($Row.ResolvedPreset)") }
    if (-not [string]::IsNullOrWhiteSpace([string]$Row.ResolvedNvencPreset)){ $parts.Add("NVENC $($Row.ResolvedNvencPreset)") }

    $reason = if (-not [string]::IsNullOrWhiteSpace([string]$Row.LaneSelectionReason)) {
        [string]$Row.LaneSelectionReason
    } elseif (-not [string]::IsNullOrWhiteSpace([string]$Row.AutoReason)) {
        [string]$Row.AutoReason
    } else {
        [string]$Row.Notes
    }
    if (-not [string]::IsNullOrWhiteSpace($reason)) { $parts.Add("Reason $reason") }

    Write-SessionTextLogMessage -Level $level -Message ($parts -join ' | ')
}

function Write-SessionEncodeStart {
    param($Init)

    if ($null -eq $Init) { return }

    $modeLabel = if ($Init.ResolvedEncodeLane -eq 'Nvidia') { 'NVENC' } else { 'SVT-AV1' }
    Write-SessionTextLogMessage -Level Info -Message ("Starting | {0} -> {1} | Lane {2} | Mode {3}" -f $Init.DisplayInputName, $Init.DisplayOutputName, $Init.ResolvedEncodeLane, $modeLabel)
    Write-SessionTextLogMessage -Level Info -Message ("Lane decision | {0}" -f $Init.LaneSelectionReason)

    $sourceColor = Get-OptionalProperty -InputObject $Init.SourceProfile -PropertyName 'SourceColorSummary' -Default ''
    $encodeColor = Get-OptionalProperty -InputObject $Init.EncodeColorProfile -PropertyName 'Summary' -Default ''
    if (-not [string]::IsNullOrWhiteSpace($sourceColor) -or -not [string]::IsNullOrWhiteSpace($encodeColor)) {
        Write-SessionTextLogMessage -Level Info -Message ("Color | Source {0} -> Output {1}" -f $sourceColor, $encodeColor)
    }

    if ($Init.ResolvedEncodeLane -eq 'Nvidia' -and $Init.NvencSettings) {
        Write-SessionTextLogMessage -Level Info -Message ("Output settings | CQ {0} | NVENC {1} | Tune {2} | Decode {3} | Priority {4}" -f $Init.NvencSettings.CQ, $Init.NvencSettings.Preset, $Init.NvencSettings.TuneDisplay, $Init.NvencSettings.DecodePath, $Init.WorkerProcessPriority)
    } else {
        Write-SessionTextLogMessage -Level Info -Message ("Output settings | CRF {0} | Preset {1} | FilmGrain {2} | Priority {3}" -f $Init.PreflightWorkflow.FinalResolvedCRF, $Init.PreflightWorkflow.FinalResolvedPreset, $Init.EffectiveFilmGrain, $Init.WorkerProcessPriority)
    }

    $autoReason = if ($Init.PreflightWorkflow -and -not [string]::IsNullOrWhiteSpace($Init.PreflightWorkflow.PreflightAutoTuneReason)) {
        $Init.PreflightWorkflow.PreflightAutoTuneReason
    } else {
        Get-OptionalProperty -InputObject $Init.AutoSettings -PropertyName 'Reason' -Default ''
    }
    if (-not [string]::IsNullOrWhiteSpace($autoReason)) {
        Write-SessionTextLogMessage -Level Info -Message ("Auto reason | {0}" -f $autoReason)
    }

    $signalLine = "Signals | {0} | {1} | {2} | BPP {3}" -f `
        (Get-OptionalProperty -InputObject $Init.AutoSettings -PropertyName 'ResolutionTier' -Default ''), `
        (Get-OptionalProperty -InputObject $Init.SourceProfile -PropertyName 'Profile' -Default ''), `
        (Get-OptionalProperty -InputObject $Init.AutoSettings -PropertyName 'CodecLabel' -Default ''), `
        ([Math]::Round((Convert-ToInvariantDouble (Get-OptionalProperty -InputObject $Init.AutoSettings -PropertyName 'BPP' -Default 0.0) 0.0), 4))
    Write-SessionTextLogMessage -Level Info -Message $signalLine
}

function Start-SessionTextLog {
    $stamp = Get-Date -Format 'HH-mm-yyyy-MM-dd'
    $candidatePath = Join-Path $QueueRoot ("{0}.log" -f $stamp)
    $suffix = 1
    while (Test-Path -LiteralPath $candidatePath) {
        $suffix++
        $candidatePath = Join-Path $QueueRoot ("{0}_{1}.log" -f $stamp, $suffix)
    }

    $script:SessionLogPath = $candidatePath
    Set-Content -LiteralPath $script:SessionLogPath -Value ("Media2AV1Queue session log`r`nStarted: {0}`r`n" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss')) -Encoding UTF8

    Write-SessionTextLogMessage -Level Info -Message ("EncoderPreference={0}" -f $EncoderPreference)
    if ($script:NvencEnvironment) {
        Write-SessionTextLogMessage -Level Info -Message ("GPU={0} | NVENC engines={1} | Capacity={2} ({3})" -f $script:NvencEnvironment.GpuName, $script:NvencEnvironment.NvencEngineCount, $script:NvencEnvironment.MaxParallel, $script:NvencEnvironment.CapacitySource)
    }
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

function Resolve-BooleanConfigValue {
    param(
        [string]$Name,
        $Value
    )

    if ($Value -is [bool]) { return $Value }

    $text = ([string]$Value).Trim().ToLowerInvariant()
    if ($text -in @('true', '$true', '1', 'yes', 'on')) { return $true }
    if ($text -in @('false', '$false', '0', 'no', 'off')) { return $false }

    throw "$Name must be `$true or `$false. Current value: $Value"
}

function Resolve-EncoderPreferenceConfigValue {
    param(
        [string]$Name,
        $Value
    )

    if ($null -eq $Value) {
        throw "$Name must be one of: Auto, CPU, Nvidia."
    }

    $text = ([string]$Value).Trim()
    if ([string]::IsNullOrWhiteSpace($text)) {
        throw "$Name must be one of: Auto, CPU, Nvidia."
    }

    switch ($text.ToLowerInvariant()) {
        'auto'   { return 'Auto' }
        'cpu'    { return 'CPU' }
        'nvidia' { return 'Nvidia' }
        default  { throw "$Name must be one of: Auto, CPU, Nvidia. Current value: $Value" }
    }
}

function Resolve-ProcessPriorityConfigValue {
    param(
        [string]$Name,
        $Value
    )

    if ($null -eq $Value) {
        throw "$Name must be one of: Idle, BelowNormal, Normal, AboveNormal."
    }

    $text = ([string]$Value).Trim()
    switch ($text.ToLowerInvariant()) {
        'idle'        { return 'Idle' }
        'belownormal' { return 'BelowNormal' }
        'normal'      { return 'Normal' }
        'abovenormal' { return 'AboveNormal' }
        default       { throw "$Name must be one of: Idle, BelowNormal, Normal, AboveNormal. Current value: $Value" }
    }
}

function Resolve-DoubleRangeConfigValue {
    param(
        [string]$Name,
        $Value,
        [double]$Minimum,
        [double]$Maximum
    )

    $parsed = Convert-ToInvariantDouble $Value ([double]::NaN)
    if ([double]::IsNaN($parsed) -or $parsed -lt $Minimum -or $parsed -gt $Maximum) {
        throw "$Name must be a number from $Minimum to $Maximum. Current value: $Value"
    }

    return $parsed
}

function Resolve-NullableDoubleRangeConfigValue {
    param(
        [string]$Name,
        $Value,
        [double]$Minimum,
        [double]$Maximum
    )

    if ($null -eq $Value) { return $null }

    $text = ([string]$Value).Trim()
    if ([string]::IsNullOrWhiteSpace($text)) { return $null }

    return Resolve-DoubleRangeConfigValue -Name $Name -Value $Value -Minimum $Minimum -Maximum $Maximum
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

function Resolve-NvencPresetConfigValue {
    param(
        [string]$Name,
        $Value
    )

    if ($Value -is [string] -and $Value.Trim().ToLowerInvariant() -eq 'auto') {
        return 'Auto'
    }

    $text = ([string]$Value).Trim().ToLowerInvariant()
    if ($text -match '^p[1-7]$') {
        return $text
    }

    throw "$Name must be one of p1-p7 or 'Auto'. Current value: $Value"
}

function Resolve-NvencDecodeConfigValue {
    param(
        [string]$Name,
        $Value
    )

    $text = ([string]$Value).Trim().ToLowerInvariant()
    if ($text -in @('auto', 'cpu', 'cuda')) {
        if ($text -eq 'auto') { return 'Auto' }
        return $text
    }

    throw "$Name must be 'Auto', 'cpu', or 'cuda'. Current value: $Value"
}

function Resolve-NvencTuneConfigValue {
    param(
        [string]$Name,
        $Value
    )

    if ($null -eq $Value) { return $null }

    $text = ([string]$Value).Trim()
    if ([string]::IsNullOrWhiteSpace($text)) { return $null }

    $normalized = $text.ToLowerInvariant()
    if ($normalized -eq 'auto') { return 'Auto' }
    if ($normalized -in @('hq', 'll', 'ull')) { return $normalized }

    throw "$Name must be one of 'Auto', 'hq', 'll', 'ull', or `$null. Current value: $Value"
}

function Resolve-PreflightAutoTuneQualityConfigValue {
    param(
        [string]$Name,
        $Value
    )

    $text = ([string]$Value).Trim()
    if ([string]::IsNullOrWhiteSpace($text)) {
        throw "$Name must be 'Low', 'Medium', or 'High'. Current value: $Value"
    }

    switch ($text.ToLowerInvariant()) {
        'low' { return 'Low' }
        'medium' { return 'Medium' }
        'high' { return 'High' }
        default { throw "$Name must be 'Low', 'Medium', or 'High'. Current value: $Value" }
    }
}

function Test-RequiredFfmpegBuild {
    param([string]$ExecutablePath)

    $versionText = (& $ExecutablePath -hide_banner -version | Out-String)
    if ([string]::IsNullOrWhiteSpace($versionText)) {
        throw "Unable to inspect FFmpeg version information. This script requires a full FFmpeg 8.1 build."
    }

    $versionLine = (($versionText -split "\r?\n")[0]).Trim()

    if ($versionText -match '(?im)^ffmpeg version [^\r\n]*\b(?:n?6(?:\.\d+)?|n?7(?:\.\d+)?)\b') {
        throw "This script requires a full FFmpeg 8.1 build. FFmpeg 6.x / 7.x builds are unsupported. Detected: $versionLine"
    }

    if ($versionText -match '(?i)\b(?:essentials|basic|minimal|lite)(?:[_ -]?build)?\b') {
        throw "This script requires a full FFmpeg 8.1 build. Stripped/basic FFmpeg builds are unsupported. Detected: $versionLine"
    }

    if ($versionText -notmatch '(?i)\b(?:n?8\.1|8\.1)\b' -and $versionText -notmatch '(?i)full_build') {
        Write-Warning "This script is designed for a full FFmpeg 8.1 build. Older FFmpeg 6.x / 7.x and stripped/basic builds are unsupported. Detected: $versionLine. NVENC tune support will still be checked against the local build."
    }

    return [ordered]@{
        VersionLine = $versionLine
        VersionText = $versionText
    }
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

function Test-TextContainsOption {
    param(
        [string]$Text,
        [string]$OptionName
    )

    return ($Text -match "(?m)^\s+-$([Regex]::Escape($OptionName))\b")
}

function Test-TextContainsValue {
    param(
        [string]$Text,
        [string]$Value
    )

    return ($Text -match "(?m)^\s+$([Regex]::Escape($Value))\b")
}

function Test-NvencTuneSupported {
    param([string]$EncoderHelpText)

    return Test-TextContainsOption -Text $EncoderHelpText -OptionName 'tune'
}

function Get-NvidiaSmiPath {
    $cmd = Get-Command nvidia-smi -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }

    $defaultPath = Join-Path ${env:ProgramFiles} 'NVIDIA Corporation\NVSMI\nvidia-smi.exe'
    if (Test-Path -LiteralPath $defaultPath) { return $defaultPath }

    return $null
}

function Get-NvencEngineCountFromGpuName {
    param([string]$GpuName)

    $normalized = ([string]$GpuName).ToUpperInvariant().Trim()
    $lookup = [ordered]@{
        'RTX 5090 LAPTOP GPU'                 = 1
        'RTX 5080 LAPTOP GPU'                 = 1
        'RTX 5070 TI LAPTOP GPU'              = 1
        'RTX 5070 LAPTOP GPU'                 = 1
        'RTX 5060 LAPTOP GPU'                 = 1
        'RTX 4090 LAPTOP GPU'                 = 1
        'RTX 4080 LAPTOP GPU'                 = 1
        'RTX 4070 LAPTOP GPU'                 = 1
        'RTX 4060 LAPTOP GPU'                 = 1
        'RTX 4050 LAPTOP GPU'                 = 1
        'RTX 5090'                            = 3
        'RTX 5080'                            = 2
        'RTX 5070 TI'                         = 1
        'RTX 5070'                            = 1
        'RTX 5060 TI'                         = 1
        'RTX 5060'                            = 1
        'RTX 4090'                            = 2
        'RTX 4080 SUPER'                      = 1
        'RTX 4080'                            = 1
        'RTX 4070 TI SUPER'                   = 1
        'RTX 4070 TI'                         = 1
        'RTX 4070 SUPER'                      = 1
        'RTX 4070'                            = 1
        'RTX 4060 TI'                         = 1
        'RTX 4060'                            = 1
        'RTX 6000 ADA GENERATION'             = 3
        'RTX 5000 ADA GENERATION'             = 2
        'RTX 4500 ADA GENERATION'             = 1
        'RTX 4000 ADA GENERATION'             = 1
        'RTX 4000 SFF ADA GENERATION'         = 1
        'RTX 3500 ADA GENERATION LAPTOP GPU'  = 1
        'RTX 3000 ADA GENERATION LAPTOP GPU'  = 1
        'RTX 2000 ADA GENERATION LAPTOP GPU'  = 1
    }

    foreach ($key in $lookup.Keys) {
        if ($normalized -like "*$key*") {
            return [ordered]@{
                EngineCount = [int]$lookup[$key]
                Source      = 'matrix'
                Warning     = ''
            }
        }
    }

    $genericAdaOrBlackwell = $normalized -match 'RTX 4\d{3}|RTX 5\d{3}|ADA'
    $warning = if ($genericAdaOrBlackwell) {
        "GPU model '$GpuName' was not found in the curated NVENC lookup table. Defaulting to 1 NVENC engine conservatively."
    } else {
        "GPU model '$GpuName' is not recognized as an AV1-capable NVIDIA model. Defaulting to 1 NVENC engine."
    }

    return [ordered]@{
        EngineCount = 1
        Source      = 'fallback'
        Warning     = $warning
    }
}

function Get-NvencEnvironment {
    $encodersText = (& $FfmpegPath -hide_banner -encoders | Out-String)
    if ($encodersText -notmatch '(?m)\bav1_nvenc\b') {
        throw "NVENC mode requested, but this FFmpeg build does not expose av1_nvenc."
    }

    $encoderHelpText = (& $FfmpegPath -hide_banner -h encoder=av1_nvenc | Out-String)
    if (-not $encoderHelpText) {
        throw "NVENC mode requested, but FFmpeg could not describe encoder=av1_nvenc."
    }

    $nvidiaSmiPath = Get-NvidiaSmiPath
    if (-not $nvidiaSmiPath) {
        throw "NVENC mode requested, but nvidia-smi was not found."
    }

    $gpuNames = @(& $nvidiaSmiPath --query-gpu=name --format=csv,noheader 2>$null | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if (-not $gpuNames -or $gpuNames.Count -eq 0) {
        throw "NVENC mode requested, but no usable NVIDIA GPU was reported by nvidia-smi."
    }

    $primaryGpuName = [string]$gpuNames[0]
    $engineInfo = Get-NvencEngineCountFromGpuName -GpuName $primaryGpuName
    if ($engineInfo.Warning) {
        Write-Warning $engineInfo.Warning
    }

    $hwaccelsText = (& $FfmpegPath -hide_banner -hwaccels | Out-String)
    $supportsCudaHwaccel = ($hwaccelsText -match '(?m)^\s*cuda\s*$')

    $maxParallel = if ($NvencMaxParallel -eq 'Auto') { [int]$engineInfo.EngineCount } else { [int]$NvencMaxParallel }
    $capacitySource = if ($NvencMaxParallel -eq 'Auto') { $engineInfo.Source } else { 'overridden' }

    return [ordered]@{
        Available             = $true
        EncoderHelpText       = $encoderHelpText
        SupportsPreset        = Test-TextContainsOption -Text $encoderHelpText -OptionName 'preset'
        SupportsTune          = Test-NvencTuneSupported -EncoderHelpText $encoderHelpText
        SupportsRc            = Test-TextContainsOption -Text $encoderHelpText -OptionName 'rc'
        SupportsCQ            = Test-TextContainsOption -Text $encoderHelpText -OptionName 'cq'
        SupportsLookahead     = Test-TextContainsOption -Text $encoderHelpText -OptionName 'rc-lookahead'
        SupportsSpatialAQ     = Test-TextContainsOption -Text $encoderHelpText -OptionName 'spatial-aq'
        SupportsTemporalAQ    = Test-TextContainsOption -Text $encoderHelpText -OptionName 'temporal-aq'
        SupportsAQStrength    = Test-TextContainsOption -Text $encoderHelpText -OptionName 'aq-strength'
        SupportsBRefMode      = Test-TextContainsOption -Text $encoderHelpText -OptionName 'b_ref_mode'
        SupportsMultipass     = Test-TextContainsOption -Text $encoderHelpText -OptionName 'multipass'
        SupportsHighBitDepth  = Test-TextContainsOption -Text $encoderHelpText -OptionName 'highbitdepth'
        SupportsSplitEncode   = Test-TextContainsOption -Text $encoderHelpText -OptionName 'split_encode_mode'
        SupportsP4            = Test-TextContainsValue -Text $encoderHelpText -Value 'p4'
        SupportsP5            = Test-TextContainsValue -Text $encoderHelpText -Value 'p5'
        SupportsP6            = Test-TextContainsValue -Text $encoderHelpText -Value 'p6'
        SupportsP7            = Test-TextContainsValue -Text $encoderHelpText -Value 'p7'
        SupportsTuneHQ        = Test-TextContainsValue -Text $encoderHelpText -Value 'hq'
        SupportsTuneLL        = Test-TextContainsValue -Text $encoderHelpText -Value 'll'
        SupportsTuneULL       = Test-TextContainsValue -Text $encoderHelpText -Value 'ull'
        SupportsCudaHwaccel   = $supportsCudaHwaccel
        GpuName               = $primaryGpuName
        AllGpuNames           = $gpuNames
        NvencEngineCount      = [int]$engineInfo.EngineCount
        MaxParallel           = [Math]::Max(1, $maxParallel)
        CapacitySource        = $capacitySource
        NvidaSmiPath          = $nvidiaSmiPath
    }
}

function Resolve-NvencTune {
    param(
        [AllowNull()][string]$ConfiguredNvencTune,
        $NvencEnvironment
    )

    if ([string]::IsNullOrWhiteSpace([string]$ConfiguredNvencTune)) {
        return [ordered]@{
            Tune    = $null
            Reason  = 'NVENC Tune: disabled (user setting empty/null)'
            Warning = ''
        }
    }

    if ($ConfiguredNvencTune -eq 'Auto') {
        if ($NvencEnvironment.SupportsTune -and $NvencEnvironment.SupportsTuneHQ) {
            return [ordered]@{
                Tune    = 'hq'
                Reason  = 'NVENC Tune: hq (supported by local FFmpeg build)'
                Warning = ''
            }
        }

        return [ordered]@{
            Tune    = $null
            Reason  = 'NVENC Tune: disabled (not supported by local FFmpeg build)'
            Warning = ''
        }
    }

    if (-not $NvencEnvironment.SupportsTune) {
        return [ordered]@{
            Tune    = $null
            Reason  = 'NVENC Tune: disabled (not supported by local FFmpeg build)'
            Warning = "NVENC tune '$ConfiguredNvencTune' was requested, but this FFmpeg build does not expose -tune for av1_nvenc. Tune has been disabled."
        }
    }

    $supportKey = switch ($ConfiguredNvencTune) {
        'hq'  { 'SupportsTuneHQ' }
        'll'  { 'SupportsTuneLL' }
        'ull' { 'SupportsTuneULL' }
        default { '' }
    }

    if ([string]::IsNullOrWhiteSpace($supportKey) -or -not $NvencEnvironment[$supportKey]) {
        return [ordered]@{
            Tune    = $null
            Reason  = "NVENC Tune: disabled (local FFmpeg build does not expose tune '$ConfiguredNvencTune')"
            Warning = "NVENC tune '$ConfiguredNvencTune' was requested, but this FFmpeg build does not list it for av1_nvenc. Tune has been disabled."
        }
    }

    return [ordered]@{
        Tune    = $ConfiguredNvencTune
        Reason  = "NVENC Tune: $ConfiguredNvencTune (supported by local FFmpeg build)"
        Warning = ''
    }
}

function Convert-SoftwareQualityToNvencSettings {
    param(
        $AutoSettings,
        $SourceProfile,
        [string]$ConfiguredNvencPreset,
        $ConfiguredNvencCQ,
        [string]$ConfiguredNvencTune,
        [string]$ConfiguredNvencDecode,
        $NvencEnvironment
    )

    $softwareCrf = [int]$AutoSettings.CRF
    $cq = if ($ConfiguredNvencCQ -eq 'Auto') {
        [Math]::Max(0, [Math]::Min(63, ($softwareCrf + 8)))
    } else {
        [int]$ConfiguredNvencCQ
    }

    $preset = $ConfiguredNvencPreset
    $presetReason = "Manual: using configured NVENC preset $ConfiguredNvencPreset."
    if ($ConfiguredNvencPreset -eq 'Auto') {
        if (($AutoSettings.ResolutionTier -eq 'UHD' -and $SourceProfile.HasHDR) -or
            ($AutoSettings.GrainClass -in @('heavy', 'extreme')) -or
            ($AutoSettings.BPPTier -eq 'high')) {
            $preset = if ($NvencEnvironment.SupportsP6) { 'p6' } elseif ($NvencEnvironment.SupportsP5) { 'p5' } else { 'p4' }
            $presetReason = 'Auto: higher-quality NVENC preset for difficult HDR/UHD or heavy-grain content.'
        } elseif (($AutoSettings.ResolutionTier -in @('SD', 'HD')) -and
                  ($SourceProfile.Profile -eq 'SDR') -and
                  ($AutoSettings.BPPTier -in @('low', 'medium')) -and
                  ($AutoSettings.GrainClass -in @('none', 'light', 'unknown'))) {
            $preset = if ($NvencEnvironment.SupportsP4) { 'p4' } else { 'p5' }
            $presetReason = 'Auto: speed-favored NVENC preset for easier SDR content.'
        } else {
            $preset = if ($NvencEnvironment.SupportsP5) { 'p5' } else { 'p4' }
            $presetReason = 'Auto: balanced NVENC preset.'
        }
    }

    $tuneResolution = Resolve-NvencTune -ConfiguredNvencTune $ConfiguredNvencTune -NvencEnvironment $NvencEnvironment
    $tune = $tuneResolution.Tune
    $tuneReason = $tuneResolution.Reason

    $decodePath = $ConfiguredNvencDecode
    $decodeReason = "Manual: using configured decode path $ConfiguredNvencDecode."
    if ($ConfiguredNvencDecode -eq 'Auto') {
        $decodePath = 'cpu'
        $decodeReason = 'Auto: CPU decode selected for maximum compatibility and filter-path reliability.'
    } elseif ($ConfiguredNvencDecode -eq 'cuda' -and -not $NvencEnvironment.SupportsCudaHwaccel) {
        $decodePath = 'cpu'
        $decodeReason = 'Requested CUDA decode is not supported by this FFmpeg build; falling back to CPU decode.'
    }

    $pixFmt = if ($SourceProfile.HasHDR -or $AutoSettings.BitDepth -ge 10) { 'p010le' } else { 'yuv420p' }
    $bitDepth = if ($pixFmt -eq 'p010le') { 10 } else { 8 }

    return [ordered]@{
        CQ             = $cq
        Preset         = $preset
        PresetReason   = $presetReason
        Tune           = $tune
        TuneReason     = $tuneReason
        TuneWarning    = $tuneResolution.Warning
        TuneDisplay    = if ([string]::IsNullOrWhiteSpace([string]$tune)) { 'disabled' } else { $tune }
        DecodePath     = $decodePath
        DecodeReason   = $decodeReason
        PixFmt         = $pixFmt
        BitDepth       = $bitDepth
        Reason         = "Mapped software-style Auto CRF $softwareCrf to NVENC CQ $cq."
    }
}

function Get-ResolvedEncodeLaneName {
    param([string]$EncodeMode)

    if ($EncodeMode -eq 'nvenc') { return 'Nvidia' }
    return 'CPU'
}

function Get-AutoLaneHint {
    param(
        $SourceProfile,
        $AutoSettings
    )

    $preflightTargets = Resolve-PreflightAutoTuneTargets -QualityProfile $PreflightAutoTuneQuality -ResolutionTier $AutoSettings.ResolutionTier -SourceProfile $SourceProfile

    if ($SourceProfile.HasHDR -and $AutoSettings.ResolutionTier -eq 'UHD') {
        return [pscustomobject][ordered]@{
            Lane   = 'CPU'
            Reason = 'modern UHD HDR source; software lane favored for compression efficiency'
        }
    }

    if (($AutoSettings.CodecClass -eq 'modern' -and $AutoSettings.BPPTier -in @('medium', 'high')) -or
        ($AutoSettings.VideoBitratePerHourGiB -ge $preflightTargets.UpperGiBPerHour)) {
        return [pscustomobject][ordered]@{
            Lane   = 'CPU'
            Reason = 'modern or high-density source; software lane favored'
        }
    }

    if (($SourceProfile.Profile -eq 'SDR') -and
        ($AutoSettings.ResolutionTier -in @('SD', 'HD')) -and
        ($AutoSettings.CodecClass -in @('legacy', 'standard')) -and
        ($AutoSettings.BPPTier -in @('low', 'medium'))) {
        return [pscustomobject][ordered]@{
            Lane   = 'Nvidia'
            Reason = 'SDR HD/SD source with lower-risk compression profile; Nvidia lane favored for throughput'
        }
    }

    return [pscustomobject][ordered]@{
        Lane   = 'CPU'
        Reason = 'quality-first default; software lane favored when source complexity is uncertain'
    }
}

function Get-EncoderLaneSuitability {
    param(
        $SourceProfile,
        $AutoSettings
    )

    $cpuOnlyScore = 0
    $cpuReasons = [System.Collections.Generic.List[string]]::new()
    $nvidiaPreferredScore = 0
    $nvidiaReasons = [System.Collections.Generic.List[string]]::new()
    $preflightTargets = Resolve-PreflightAutoTuneTargets -QualityProfile $PreflightAutoTuneQuality -ResolutionTier $AutoSettings.ResolutionTier -SourceProfile $SourceProfile

    if ($AutoSettings.ResolutionTier -eq 'UHD' -and $SourceProfile.HasHDR) {
        $cpuOnlyScore += 1
        $cpuReasons.Add('UHD HDR source')
    } elseif ($SourceProfile.HasHDR) {
        $cpuOnlyScore += 1
        $cpuReasons.Add('HDR source')
    }

    switch ($AutoSettings.GrainClass) {
        'extreme' {
            $cpuOnlyScore += 3
            $cpuReasons.Add('extreme grain')
        }
        'heavy' {
            $cpuOnlyScore += 3
            $cpuReasons.Add('heavy grain')
        }
        'moderate' {
            $cpuOnlyScore += 0
            $cpuReasons.Add('moderate grain')
        }
    }

    if ($AutoSettings.CodecClass -eq 'modern') {
        $cpuOnlyScore += 1
        $cpuReasons.Add('modern source codec')
    } elseif ($AutoSettings.CodecClass -eq 'standard' -and $AutoSettings.BPPTier -eq 'high') {
        $cpuOnlyScore += 1
        $cpuReasons.Add('high-density AVC source')
    }

    if ($AutoSettings.BPPTier -eq 'high') {
        $cpuOnlyScore += 1
        $cpuReasons.Add('high BPP content')
    }

    if ($AutoSettings.VideoBitratePerHourGiB -ge $preflightTargets.UpperGiBPerHour) {
        $cpuOnlyScore += 1
        $cpuReasons.Add('high projected density')
    }

    if ([int]$AutoSettings.FilmGrain -ge 12 -or $AutoSettings.GrainClass -in @('heavy', 'extreme')) {
        $cpuOnlyScore += 1
        $cpuReasons.Add('software film-grain tools matter')
    }

    if (($AutoSettings.ResolutionTier -eq 'UHD') -and
        $SourceProfile.HasHDR -and
        ($AutoSettings.GrainClass -in @('heavy', 'extreme')) -and
        ($AutoSettings.CodecClass -eq 'modern')) {
        $cpuOnlyScore += 2
        $cpuReasons.Add('quality-first worst-case NVENC candidate')
    }

    if (($SourceProfile.Profile -eq 'SDR') -and
        ($AutoSettings.ResolutionTier -in @('SD', 'HD')) -and
        ($AutoSettings.CodecClass -in @('legacy', 'standard')) -and
        ($AutoSettings.BPPTier -in @('low', 'medium')) -and
        ($AutoSettings.GrainClass -in @('none', 'light', 'unknown'))) {
        $nvidiaPreferredScore += 4
        $nvidiaReasons.Add('SDR HD/SD source with lower-risk compression profile')
    }

    if (($SourceProfile.Profile -eq 'SDR') -and ($AutoSettings.BPPTier -eq 'low')) {
        $nvidiaPreferredScore += 1
        $nvidiaReasons.Add('low-density SDR content')
    }

    if ($AutoSettings.CodecClass -eq 'legacy') {
        $nvidiaPreferredScore += 1
        $nvidiaReasons.Add('legacy source codec')
    }

    if ($cpuOnlyScore -ge 7) {
        $reason = '{0}; software encoding strongly preferred' -f (($cpuReasons | Select-Object -Unique) -join ', ')
        return [pscustomobject][ordered]@{
            Suitability           = 'CpuOnly'
            PreferredLane         = 'CPU'
            Reason                = $reason
            CpuOnlyReason         = $reason
            NvidiaFallbackAllowed = $false
        }
    }

    if ($nvidiaPreferredScore -ge 4 -and $cpuOnlyScore -le 1) {
        $reason = if ($nvidiaReasons.Count -gt 0) {
            '{0}; Nvidia lane favored for throughput' -f (($nvidiaReasons | Select-Object -Unique) -join ', ')
        } else {
            'Nvidia lane favored for throughput'
        }

        return [pscustomobject][ordered]@{
            Suitability           = 'NvidiaPreferred'
            PreferredLane         = 'Nvidia'
            Reason                = $reason
            CpuOnlyReason         = ''
            NvidiaFallbackAllowed = $true
        }
    }

    $fallbackReason = if ($cpuReasons.Count -gt 0) {
        '{0}; software lane preferred but Nvidia fallback allowed' -f (($cpuReasons | Select-Object -Unique) -join ', ')
    } else {
        'quality-first default; software lane preferred but Nvidia fallback allowed'
    }

    return [pscustomobject][ordered]@{
        Suitability           = 'NvidiaAllowedFallback'
        PreferredLane         = 'CPU'
        Reason                = $fallbackReason
        CpuOnlyReason         = ''
        NvidiaFallbackAllowed = $true
    }
}

function Test-NvencFallbackSuitable {
    param(
        $LaneSuitability,
        $Init
    )

    if ($null -eq $Init) {
        return [pscustomobject][ordered]@{
            Allowed = $true
            Reason  = ''
        }
    }

    if ($LaneSuitability -and $LaneSuitability.Suitability -eq 'CpuOnly') {
        return [pscustomobject][ordered]@{
            Allowed = $false
            Reason  = if ($LaneSuitability.CpuOnlyReason) { $LaneSuitability.CpuOnlyReason } else { 'NVENC not recommended for this source.' }
        }
    }

    if ($Init.ResolvedEncodeLane -ne 'Nvidia') {
        return [pscustomobject][ordered]@{
            Allowed = $true
            Reason  = ''
        }
    }

    $preflight = Get-OptionalProperty -InputObject $Init -PropertyName 'PreflightEstimate' -Default $null
    $isHdr = [bool](Get-OptionalProperty -InputObject $Init.SourceProfile -PropertyName 'HasHDR' -Default $false)
    $resolutionTier = [string](Get-OptionalProperty -InputObject $Init.AutoSettings -PropertyName 'ResolutionTier' -Default '')
    $codecClass = [string](Get-OptionalProperty -InputObject $Init.AutoSettings -PropertyName 'CodecClass' -Default '')
    $grainClass = [string](Get-OptionalProperty -InputObject $Init.AutoSettings -PropertyName 'GrainClass' -Default '')
    $preflightTargets = Resolve-PreflightAutoTuneTargets -QualityProfile $PreflightAutoTuneQuality -ResolutionTier $resolutionTier -SourceProfile $Init.SourceProfile

    if ($preflight -and $preflight.Ran) {
        $pctOfSource = Convert-ToInvariantDouble (Get-OptionalProperty -InputObject $preflight -PropertyName 'EstimatedPctOfSource' -Default 0.0) 0.0
        $gibPerHour = Convert-ToInvariantDouble (Get-OptionalProperty -InputObject $preflight -PropertyName 'EstimatedOutputGiBPerHour' -Default 0.0) 0.0
        $savingsPct = Convert-ToInvariantDouble (Get-OptionalProperty -InputObject $preflight -PropertyName 'EstimatedSavingsPercent' -Default 0.0) 0.0

        if ($pctOfSource -ge 90.0) {
            return [pscustomobject][ordered]@{
                Allowed = $false
                Reason  = 'NVENC fallback held for CPU because preflight projects near-source-size output.'
            }
        }

        if ($gibPerHour -gt $preflightTargets.UpperGiBPerHour -and $savingsPct -lt 25.0) {
            return [pscustomobject][ordered]@{
                Allowed = $false
                Reason  = 'NVENC fallback held for CPU because preflight projects weak compression efficiency.'
            }
        }

        if ($isHdr -and $resolutionTier -eq 'UHD' -and $codecClass -eq 'modern' -and $savingsPct -lt 20.0) {
            return [pscustomobject][ordered]@{
                Allowed = $false
                Reason  = 'NVENC fallback held for CPU because modern UHD HDR content projected weak savings.'
            }
        }
    }

    if ($isHdr -and $resolutionTier -eq 'UHD' -and $codecClass -eq 'modern' -and $grainClass -in @('heavy', 'extreme')) {
        return [pscustomobject][ordered]@{
            Allowed = $false
            Reason  = 'NVENC fallback held for CPU because heavy grain on modern UHD HDR content favors software encoding.'
        }
    }

    return [pscustomobject][ordered]@{
        Allowed = $true
        Reason  = ''
    }
}

function Try-Get-NvencEnvironment {
    try {
        return Get-NvencEnvironment
    } catch {
        Write-Warning $_.Exception.Message
        return $null
    }
}

function Get-WorkerProcessPriorityName {
    param([string]$EncodeMode)

    if ($EncodeMode -eq 'nvenc') { return $HardwareEncodePriority }
    return $SoftwareEncodePriority
}

function Set-TrackedProcessPriority {
    param(
        [System.Diagnostics.Process]$Process,
        [string]$PriorityName
    )

    $currentName = try { $Process.PriorityClass.ToString() } catch { 'Normal' }

    if (-not $ApplyProcessPriority) {
        return [pscustomobject][ordered]@{
            AppliedPriority = $currentName
            Warning         = ''
            Reason          = 'Process priority handling disabled; leaving worker at OS default priority.'
        }
    }

    try {
        $desiredPriority = [System.Diagnostics.ProcessPriorityClass]::$PriorityName
        $Process.PriorityClass = $desiredPriority
        return [pscustomobject][ordered]@{
            AppliedPriority = $Process.PriorityClass.ToString()
            Warning         = ''
            Reason          = "Process priority: $($Process.PriorityClass)"
        }
    } catch {
        return [pscustomobject][ordered]@{
            AppliedPriority = $currentName
            Warning         = "Could not set process priority to ${PriorityName}: $($_.Exception.Message)"
            Reason          = "Process priority: $currentName (priority change failed)"
        }
    }
}

function Initialize-ConsoleShutdownHandling {
    try {
        $script:OriginalTreatControlCAsInput = [Console]::TreatControlCAsInput
        [Console]::TreatControlCAsInput = $true
    } catch {
        $script:OriginalTreatControlCAsInput = $null
    }
}

function Restore-ConsoleShutdownHandling {
    try {
        if ($null -ne $script:OriginalTreatControlCAsInput) {
            [Console]::TreatControlCAsInput = [bool]$script:OriginalTreatControlCAsInput
        }
    } catch {}
}

function Initialize-TestHooks {
    $envValue = [Environment]::GetEnvironmentVariable('MEDIA2AV1QUEUE_TEST_AUTO_SHUTDOWN_SEC')
    if ([string]::IsNullOrWhiteSpace($envValue)) { return }

    $seconds = 0
    if (-not [int]::TryParse($envValue, [ref]$seconds)) { return }
    if ($seconds -le 0) { return }

    $script:TestAutoShutdownSeconds = $seconds
    $script:TestAutoShutdownAt = (Get-Date).AddSeconds($seconds)
}

function Invoke-TestAutoShutdownIfDue {
    if ($script:QueueShutdownRequested) { return $true }
    if ($null -eq $script:TestAutoShutdownAt) { return $false }
    if ((Get-Date) -lt $script:TestAutoShutdownAt) { return $false }

    $script:QueueShutdownRequested = $true
    $script:TestAutoShutdownAt = $null
    if (-not $script:QueueShutdownMessageShown) {
        Write-Host "Shutting down (test hook). Restart by running Media2AV1Queue.bat." -ForegroundColor Yellow
        Write-SessionTextLogMessage -Level Warn -Message ("Shutdown requested by test hook after {0}s." -f $script:TestAutoShutdownSeconds)
        $script:QueueShutdownMessageShown = $true
    }
    Clear-ConsoleCommandContext
    return $true
}

function Set-ConsoleStatusMessage {
    param(
        [string]$Message,
        [ValidateSet('Info', 'Warn', 'Err')]
        [string]$Level = 'Info',
        [int]$DurationSec = 5,
        [bool]$Log = $false
    )

    $script:ConsoleStatus = [pscustomobject]@{
        Message = $Message
        Level = $Level
        ExpiresAt = (Get-Date).AddSeconds([Math]::Max(1, $DurationSec))
    }

    if ($Log -and -not [string]::IsNullOrWhiteSpace($Message)) {
        Write-SessionTextLogMessage -Level $Level -Message $Message
    }
}

function Get-ConsoleStatusMessage {
    if ($null -eq $script:ConsoleStatus) { return '' }
    if ([string]::IsNullOrWhiteSpace($script:ConsoleStatus.Message)) { return '' }
    if ($script:ConsoleStatus.ExpiresAt -and (Get-Date) -gt $script:ConsoleStatus.ExpiresAt) {
        $script:ConsoleStatus = [pscustomobject]@{ Message = ''; Level = 'Info'; ExpiresAt = $null }
        return ''
    }
    return [string]$script:ConsoleStatus.Message
}

function Clear-ConsoleCommandContext {
    $script:ConsoleCommandContext = [pscustomobject]@{
        Kind = ''
        Target = ''
        ExpiresAt = $null
    }
}

function Set-ConsoleCommandContext {
    param(
        [string]$Kind,
        [string]$Target = '',
        [int]$TimeoutSec = 10
    )

    $script:ConsoleCommandContext = [pscustomobject]@{
        Kind = $Kind
        Target = $Target
        ExpiresAt = (Get-Date).AddSeconds([Math]::Max(1, $TimeoutSec))
    }
}

function Get-ConsoleCommandPrompt {
    $ctx = $script:ConsoleCommandContext
    if ($null -eq $ctx -or [string]::IsNullOrWhiteSpace($ctx.Kind)) { return '' }

    if ($ctx.ExpiresAt -and (Get-Date) -gt $ctx.ExpiresAt) {
        Clear-ConsoleCommandContext
        Set-ConsoleStatusMessage -Message 'Command timed out.' -Level Warn
        return ''
    }

    switch ($ctx.Kind) {
        'Worker' { return "Command: worker $($ctx.Target) selected (waiting for p/r/s, 10s timeout)" }
        'Queue'  { return 'Command: queue selected (waiting for p/r/c, 10s timeout)' }
        default  { return '' }
    }
}

function Get-QueueControlStateText {
    $queueState = if ($script:QueuePaused) { 'Paused' } else { 'Running' }
    $softExitState = if ($script:SoftExitRequested) { 'Armed' } else { 'Off' }
    return "Queue: $queueState  |  Soft exit: $softExitState"
}

function Get-ConsoleHelpLines {
    return @(
        'Controls:',
        '[1-9] Select worker  |  [q] Queue controls  |  [x] Finish active jobs, then exit  |  [h] Toggle help',
        'Worker:',
        '[p] Pause selected worker  |  [r] Resume selected worker  |  [s] Stop selected worker',
        'Queue:',
        '[q] then [p] Pause queue  |  [q] then [r] Resume queue  |  [q] then [c] Clear pending queue',
        'Notes:',
        'Pause suspends the current ffmpeg process. Stop cancels the job and holds the worker.'
    )
}

function Confirm-ConsoleAction {
    param([string]$Prompt)

    Write-Host ""
    Write-Host "$Prompt Y/N" -ForegroundColor Yellow
    while ($true) {
        try {
            $answer = [Console]::ReadKey($true)
        } catch {
            return $false
        }

        switch ($answer.Key) {
            ([ConsoleKey]::Y) { return $true }
            ([ConsoleKey]::N) { return $false }
        }
    }
}

function Ensure-ProcessThreadControlInterop {
    if ($script:ThreadControlInteropLoaded) { return }

    Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;

public static class Media2Av1ThreadControl {
    [DllImport("kernel32.dll", SetLastError=true)]
    public static extern IntPtr OpenThread(uint dwDesiredAccess, bool bInheritHandle, uint dwThreadId);

    [DllImport("kernel32.dll", SetLastError=true)]
    public static extern uint SuspendThread(IntPtr hThread);

    [DllImport("kernel32.dll", SetLastError=true)]
    public static extern uint ResumeThread(IntPtr hThread);

    [DllImport("kernel32.dll", SetLastError=true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    public static extern bool CloseHandle(IntPtr hObject);
}
"@ -ErrorAction SilentlyContinue

    $script:ThreadControlInteropLoaded = $true
}

function Suspend-ProcessThreads {
    param([System.Diagnostics.Process]$Process)

    if ($null -eq $Process) { return }
    Ensure-ProcessThreadControlInterop

    $threadAccess = 0x0002
    foreach ($thread in @($Process.Threads)) {
        $handle = [Media2Av1ThreadControl]::OpenThread($threadAccess, $false, [uint32]$thread.Id)
        if ($handle -eq [IntPtr]::Zero) { continue }
        try {
            [void][Media2Av1ThreadControl]::SuspendThread($handle)
        } finally {
            [void][Media2Av1ThreadControl]::CloseHandle($handle)
        }
    }
}

function Resume-ProcessThreads {
    param([System.Diagnostics.Process]$Process)

    if ($null -eq $Process) { return }
    Ensure-ProcessThreadControlInterop

    $threadAccess = 0x0002
    foreach ($thread in @($Process.Threads)) {
        $handle = [Media2Av1ThreadControl]::OpenThread($threadAccess, $false, [uint32]$thread.Id)
        if ($handle -eq [IntPtr]::Zero) { continue }
        try {
            while ($true) {
                $resumeResult = [Media2Av1ThreadControl]::ResumeThread($handle)
                if ($resumeResult -eq 0 -or $resumeResult -eq 0xFFFFFFFF) { break }
            }
        } finally {
            [void][Media2Av1ThreadControl]::CloseHandle($handle)
        }
    }
}

function Get-WorkerStateLabel {
    param($Worker)

    $state = [string](Get-OptionalProperty -InputObject $Worker -PropertyName 'WorkerState' -Default '')
    if (-not [string]::IsNullOrWhiteSpace($state)) { return $state }
    return 'Running'
}

function Set-WorkerState {
    param(
        $Worker,
        [string]$State
    )

    if ($null -eq $Worker) { return }
    if ($Worker.PSObject.Properties['WorkerState']) {
        $Worker.WorkerState = $State
    } else {
        $Worker | Add-Member -NotePropertyName WorkerState -NotePropertyValue $State
    }
}

function Get-WorkerBySlot {
    param(
        [object[]]$Workers,
        [string]$Slot
    )

    return @($Workers | Where-Object { "$($_.SlotNumber)" -eq "$Slot" } | Select-Object -First 1)[0]
}

function Pause-WorkerByUser {
    param($Worker)

    if ($null -eq $Worker) { return }
    $state = Get-WorkerStateLabel -Worker $Worker
    if ($state -ne 'Running') {
        Set-ConsoleStatusMessage -Message ("Worker {0} is not running." -f $Worker.SlotNumber) -Level Warn
        return
    }

    try {
        Suspend-ProcessThreads -Process $Worker.TrackedProcess.Process
        try { $Worker.Stopwatch.Stop() } catch {}
        Set-WorkerState -Worker $Worker -State 'Paused'
        Set-ConsoleStatusMessage -Message ("Worker {0} paused." -f $Worker.SlotNumber) -Level Info -Log $true
    } catch {
        Set-ConsoleStatusMessage -Message ("Could not pause worker {0}: {1}" -f $Worker.SlotNumber, $_.Exception.Message) -Level Err -Log $true
    }
}

function Resume-WorkerByUser {
    param($Worker)

    if ($null -eq $Worker) { return }
    $state = Get-WorkerStateLabel -Worker $Worker

    if ($state -eq 'Paused') {
        try {
            Resume-ProcessThreads -Process $Worker.TrackedProcess.Process
            try { $Worker.Stopwatch.Start() } catch {}
            Set-WorkerState -Worker $Worker -State 'Running'
            Set-ConsoleStatusMessage -Message ("Worker {0} resumed." -f $Worker.SlotNumber) -Level Info -Log $true
        } catch {
            Set-ConsoleStatusMessage -Message ("Could not resume worker {0}: {1}" -f $Worker.SlotNumber, $_.Exception.Message) -Level Err -Log $true
        }
        return
    }

    if ($state -eq 'Held') {
        if ($Worker.PSObject.Properties['PendingResumeRequested']) {
            $Worker.PendingResumeRequested = $true
        } else {
            $Worker | Add-Member -NotePropertyName PendingResumeRequested -NotePropertyValue $true
        }
        Set-ConsoleStatusMessage -Message ("Worker {0} restart requested." -f $Worker.SlotNumber) -Level Info -Log $true
        return
    }

    Set-ConsoleStatusMessage -Message ("Worker {0} is not paused or held." -f $Worker.SlotNumber) -Level Warn
}

function Stop-WorkerByUser {
    param($Worker)

    if ($null -eq $Worker) { return }

    $state = Get-WorkerStateLabel -Worker $Worker
    if ($state -notin @('Running', 'Paused')) {
        Set-ConsoleStatusMessage -Message ("Worker {0} is not running." -f $Worker.SlotNumber) -Level Warn
        return
    }

    if (-not (Confirm-ConsoleAction -Prompt ("Stop worker {0} and hold it?" -f $Worker.SlotNumber))) {
        Set-ConsoleStatusMessage -Message ("Worker {0} stop cancelled." -f $Worker.SlotNumber) -Level Info -Log $true
        return
    }

    if ($state -eq 'Paused') {
        try { Resume-ProcessThreads -Process $Worker.TrackedProcess.Process } catch {}
    }

    if ($Worker.PSObject.Properties['ManualStopRequested']) {
        $Worker.ManualStopRequested = $true
    } else {
        $Worker | Add-Member -NotePropertyName ManualStopRequested -NotePropertyValue $true
    }

    Set-WorkerState -Worker $Worker -State 'Stopping'
    Set-ConsoleStatusMessage -Message ("Worker {0} stop requested." -f $Worker.SlotNumber) -Level Warn -Log $true
    try {
        $Worker.TrackedProcess.Process.Kill()
    } catch {
        Set-ConsoleStatusMessage -Message ("Could not stop worker {0}: {1}" -f $Worker.SlotNumber, $_.Exception.Message) -Level Err -Log $true
    }
}

function Clear-PendingQueueByUser {
    if (-not (Confirm-ConsoleAction -Prompt 'Clear pending queue?')) {
        Set-ConsoleStatusMessage -Message 'Pending queue clear cancelled.' -Level Info -Log $true
        return
    }

    $removed = 0
    foreach ($job in @(Get-ChildItem -LiteralPath $QueuePendingDir -Filter *.json -File -ErrorAction SilentlyContinue)) {
        try {
            Remove-Item -LiteralPath $job.FullName -Force -ErrorAction SilentlyContinue
            $removed++
        } catch {}
    }

    Set-ConsoleStatusMessage -Message ("Pending queue cleared: {0} item(s) removed." -f $removed) -Level Warn -Log $true
}

function Handle-QueueCommand {
    param([ConsoleKeyInfo]$KeyInfo)

    switch ($KeyInfo.Key) {
        ([ConsoleKey]::P) {
            if ($script:QueuePaused) {
                Set-ConsoleStatusMessage -Message 'Queue is already paused.' -Level Warn
            } else {
                $script:QueuePaused = $true
                Set-ConsoleStatusMessage -Message 'Queue paused by user.' -Level Info -Log $true
            }
        }
        ([ConsoleKey]::R) {
            if (-not $script:QueuePaused) {
                Set-ConsoleStatusMessage -Message 'Queue is already running.' -Level Warn
            } else {
                $script:QueuePaused = $false
                Set-ConsoleStatusMessage -Message 'Queue resumed by user.' -Level Info -Log $true
            }
        }
        ([ConsoleKey]::C) {
            Clear-PendingQueueByUser
        }
        default {
            Set-ConsoleStatusMessage -Message 'Queue command cancelled.' -Level Warn
        }
    }
}

function Try-RestartHeldWorker {
    param(
        $Worker,
        $NvencEnvironment = $null
    )

    if ($null -eq $Worker) { return $false }
    if (-not (Get-OptionalProperty -InputObject $Worker -PropertyName 'PendingResumeRequested' -Default $false)) { return $false }

    $Worker.PendingResumeRequested = $false
    $heldInputPath = Get-OptionalProperty -InputObject $Worker -PropertyName 'HeldInputPath' -Default ''
    $heldEncodeMode = Get-OptionalProperty -InputObject $Worker -PropertyName 'HeldEncodeMode' -Default ''
    if ([string]::IsNullOrWhiteSpace($heldInputPath) -or [string]::IsNullOrWhiteSpace($heldEncodeMode)) {
        Set-ConsoleStatusMessage -Message ("Worker {0} has no held job to restart." -f $Worker.SlotNumber) -Level Warn -Log $true
        return $false
    }

    if (-not (Test-Path -LiteralPath $heldInputPath)) {
        Write-SessionTextLogMessage -Level Err -Message ("Held worker restart failed | worker {0} | source missing | {1}" -f $Worker.SlotNumber, $heldInputPath)
        Set-ConsoleStatusMessage -Message ("Worker {0} restart failed: source missing." -f $Worker.SlotNumber) -Level Err -Log $true
        return $false
    }

    Write-SessionTextLogMessage -Level Info -Message ("Held worker restart requested | worker {0} | {1}" -f $Worker.SlotNumber, $heldInputPath)

    try {
        $newInit = Get-EncodeInitialization -InputPath $heldInputPath -EncodeMode $heldEncodeMode -NvencEnvironment $NvencEnvironment -EncoderPreferenceValue $Worker.Init.EncoderPreference -LaneSelectionReason ((Get-OptionalProperty -InputObject $Worker -PropertyName 'HeldRestartReason' -Default '') ?? $Worker.Init.LaneSelectionReason) -LaneSuitability $Worker.Init.LaneSuitability -CpuOnlyReason $Worker.Init.CpuOnlyReason -NvidiaFallbackAllowed $Worker.Init.NvidiaFallbackAllowed
        if ($newInit.EarlyExit) {
            $row = $newInit.Row
            if ($heldEncodeMode -eq 'nvenc' -and $NvencEnvironment) {
                $row.NvencWorkerCountAtStart = $NvencEnvironment.MaxParallel
            }
            Write-LogRow $row
            Set-ConsoleStatusMessage -Message ("Worker {0} restart could not continue." -f $Worker.SlotNumber) -Level Warn -Log $true
            return $false
        }

        if (Test-Path -LiteralPath $newInit.TempOutput) {
            Remove-Item -LiteralPath $newInit.TempOutput -Force -ErrorAction SilentlyContinue
        }

        $ffArgs = if ($newInit.ResolvedEncodeLane -eq 'Nvidia') {
            Build-NvencFfmpegArgs -Init $newInit -NvencEnvironment $NvencEnvironment
        } else {
            Build-SoftwareFfmpegArgs -Init $newInit
        }
        $tracked = Start-TrackedFfmpegProcess -Arguments $ffArgs -PriorityName $newInit.WorkerProcessPriority
        $newInit.WorkerProcessPriority = $tracked.WorkerProcessPriority
        Write-SessionEncodeStart -Init $newInit

        $Worker.Init = $newInit
        $Worker.TrackedProcess = $tracked
        $Worker.Stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
        $Worker.WorkerProcessPriority = $tracked.WorkerProcessPriority
        $Worker.ShutdownRequestedAt = $null
        $Worker.ManualStopRequested = $false
        $Worker.HeldInputPath = ''
        $Worker.HeldEncodeMode = ''
        $Worker.HeldRestartReason = ''
        Set-WorkerState -Worker $Worker -State 'Running'
        Set-ConsoleStatusMessage -Message ("Worker {0} restarted from scratch." -f $Worker.SlotNumber) -Level Info -Log $true
        return $true
    } catch {
        Write-SessionTextLogMessage -Level Err -Message ("Held worker restart failed | worker {0} | {1}" -f $Worker.SlotNumber, $_.Exception.Message)
        Set-ConsoleStatusMessage -Message ("Worker {0} restart failed: {1}" -f $Worker.SlotNumber, $_.Exception.Message) -Level Err -Log $true
        return $false
    }
}

function Get-BlockingWorkerCount {
    param([object[]]$Workers)

    return @($Workers | Where-Object { (Get-WorkerStateLabel -Worker $_) -in @('Running', 'Paused', 'Starting', 'Stopping') }).Count
}

function Handle-LiveConsoleInput {
    param(
        [object[]]$Workers = @(),
        $NvencEnvironment = $null
    )

    if ($script:QueueShutdownRequested) { return $true }
    $null = Get-ConsoleCommandPrompt
    $ctx = $script:ConsoleCommandContext

    try {
        while ([Console]::KeyAvailable) {
            $key = [Console]::ReadKey($true)
            $isCtrlC = (($key.Modifiers -band [ConsoleModifiers]::Control) -and $key.Key -eq [ConsoleKey]::C) -or ([int][char]$key.KeyChar -eq 3)
            if ($isCtrlC) {
                Write-Host ""
                if (Confirm-ConsoleAction -Prompt 'Cancel current queue?') {
                    $script:QueueShutdownRequested = $true
                    if (-not $script:QueueShutdownMessageShown) {
                        Write-Host "Shutting down. Restart by running Media2AV1Queue.bat." -ForegroundColor Yellow
                        Write-SessionTextLogMessage -Level Warn -Message 'Shutdown requested by user. Restart by running Media2AV1Queue.bat.'
                        $script:QueueShutdownMessageShown = $true
                    }
                    Clear-ConsoleCommandContext
                    return $true
                }

                Set-ConsoleStatusMessage -Message 'Continuing queue.' -Level Info
                Clear-ConsoleCommandContext
                continue
            }

            $ctx = $script:ConsoleCommandContext
            if ($ctx -and -not [string]::IsNullOrWhiteSpace($ctx.Kind)) {
                switch ($ctx.Kind) {
                    'Worker' {
                        $worker = Get-WorkerBySlot -Workers $Workers -Slot $ctx.Target
                        if ($null -eq $worker) {
                            Set-ConsoleStatusMessage -Message ("Worker {0} is no longer available." -f $ctx.Target) -Level Warn
                            Clear-ConsoleCommandContext
                            continue
                        }

                        switch ($key.Key) {
                            ([ConsoleKey]::P) { Pause-WorkerByUser -Worker $worker }
                            ([ConsoleKey]::R) { Resume-WorkerByUser -Worker $worker }
                            ([ConsoleKey]::S) { Stop-WorkerByUser -Worker $worker }
                            default { Set-ConsoleStatusMessage -Message 'Worker command cancelled.' -Level Warn }
                        }
                        Clear-ConsoleCommandContext
                        continue
                    }
                    'Queue' {
                        Handle-QueueCommand -KeyInfo $key
                        Clear-ConsoleCommandContext
                        continue
                    }
                }
            }

            if ($key.KeyChar -match '^[1-9]$') {
                $slot = [string]$key.KeyChar
                $worker = Get-WorkerBySlot -Workers $Workers -Slot $slot
                if ($null -eq $worker) {
                    Set-ConsoleStatusMessage -Message ("Worker {0} is not active in this session." -f $slot) -Level Warn
                } else {
                    Set-ConsoleCommandContext -Kind 'Worker' -Target $slot
                }
                continue
            }

            switch ($key.Key) {
                ([ConsoleKey]::Q) {
                    Set-ConsoleCommandContext -Kind 'Queue'
                }
                ([ConsoleKey]::H) {
                    $script:ShowHelpOverlay = -not $script:ShowHelpOverlay
                    Set-ConsoleStatusMessage -Message ("Help overlay {0}." -f $(if ($script:ShowHelpOverlay) { 'shown' } else { 'hidden' })) -Level Info
                }
                ([ConsoleKey]::X) {
                    if (Confirm-ConsoleAction -Prompt 'Finish active jobs, then exit?') {
                        $script:SoftExitRequested = $true
                        $script:QueuePaused = $true
                        Set-ConsoleStatusMessage -Message 'Soft exit armed. No new jobs will start.' -Level Warn -Log $true
                    } else {
                        Set-ConsoleStatusMessage -Message 'Soft exit cancelled.' -Level Info -Log $true
                    }
                }
            }
        }
    } catch {}

    return $script:QueueShutdownRequested
}

function Test-QueueShutdownRequested {
    param(
        [object[]]$Workers = @(),
        $NvencEnvironment = $null
    )

    if (Invoke-TestAutoShutdownIfDue) { return $true }
    return (Handle-LiveConsoleInput -Workers $Workers -NvencEnvironment $NvencEnvironment)
}


function Request-FfmpegProcessQuit {
    param([System.Diagnostics.Process]$Process)

    if ($null -eq $Process) { return }
    try {
        if ($Process.HasExited) { return }
    } catch {
        return
    }

    try {
        $Process.StandardInput.WriteLine('q')
        $Process.StandardInput.Flush()
    } catch {}
}

function Get-TrackedWorkerProcess {
    param($Worker)

    $trackedProcess = Get-OptionalProperty -InputObject $Worker -PropertyName 'TrackedProcess' -Default $null
    $process = Get-OptionalProperty -InputObject $trackedProcess -PropertyName 'Process' -Default $null
    if ($process -is [System.Diagnostics.Process]) { return $process }
    return $null
}

function Test-TrackedWorkerProcessExited {
    param($Worker)

    $process = Get-TrackedWorkerProcess -Worker $Worker
    if ($null -eq $process) { return $true }
    try {
        return $process.HasExited
    } catch {
        return $true
    }
}

function Request-WorkerShutdown {
    param($Worker)

    if ($null -eq $Worker) { return }

    $process = Get-TrackedWorkerProcess -Worker $Worker

    $shutdownRequestedAt = Get-OptionalProperty -InputObject $Worker -PropertyName 'ShutdownRequestedAt' -Default $null
    if ($null -ne $shutdownRequestedAt -and $shutdownRequestedAt -isnot [datetime]) {
        $shutdownRequestedAt = $null
        try {
            if ($Worker.PSObject.Properties['ShutdownRequestedAt']) {
                $Worker.ShutdownRequestedAt = $null
            }
        } catch {}
    }

    if ($null -eq $shutdownRequestedAt) {
        $slot = Get-OptionalProperty -InputObject $Worker -PropertyName 'SlotNumber' -Default '?'
        $lane = Get-OptionalProperty -InputObject $Worker.Init -PropertyName 'ResolvedEncodeLane' -Default 'Unknown'
        $name = Get-OptionalProperty -InputObject $Worker.Init -PropertyName 'DisplayInputName' -Default ''
        Write-Host ("Shutdown: requesting worker {0} ({1}) to stop gracefully{2}" -f $slot, $lane, $(if ($name) { " - $name" } else { '' })) -ForegroundColor DarkYellow
        Write-SessionTextLogMessage -Level Warn -Message ("Shutdown | requesting worker {0} ({1}) to stop gracefully{2}" -f $slot, $lane, $(if ($name) { " - $name" } else { '' }))
        if ($null -ne $process) {
            Request-FfmpegProcessQuit -Process $process
        }
        try {
            if ($Worker.PSObject.Properties['ShutdownRequestedAt']) {
                $Worker.ShutdownRequestedAt = Get-Date
            } else {
                $Worker | Add-Member -NotePropertyName ShutdownRequestedAt -NotePropertyValue (Get-Date)
            }
        } catch {}
        return
    }

    try {
        if ($null -ne $process -and -not $process.HasExited -and ((Get-Date) - $shutdownRequestedAt).TotalSeconds -ge 20) {
            $slot = Get-OptionalProperty -InputObject $Worker -PropertyName 'SlotNumber' -Default '?'
            Write-Host ("Shutdown: worker {0} did not exit in time; terminating ffmpeg." -f $slot) -ForegroundColor Yellow
            Write-SessionTextLogMessage -Level Warn -Message ("Shutdown | worker {0} did not exit in time; terminating ffmpeg." -f $slot)
            $process.Kill()
            if ($Worker.PSObject.Properties['ShutdownRequestedAt']) {
                $Worker.ShutdownRequestedAt = (Get-Date).AddYears(50)
            } else {
                $Worker | Add-Member -NotePropertyName ShutdownRequestedAt -NotePropertyValue ((Get-Date).AddYears(50))
            }
        }
    } catch {}
}

function Requeue-WorkingJob {
    param([string]$WorkingJobPath)

    if ([string]::IsNullOrWhiteSpace($WorkingJobPath) -or -not (Test-Path -LiteralPath $WorkingJobPath)) {
        return
    }

    $pendingPath = Join-Path $QueuePendingDir ([System.IO.Path]::GetFileName($WorkingJobPath))
    if (Test-Path -LiteralPath $pendingPath) {
        Remove-Item -LiteralPath $WorkingJobPath -Force -ErrorAction SilentlyContinue
        return
    }

    Move-Item -LiteralPath $WorkingJobPath -Destination $pendingPath -Force
}

function Show-NoWorkToResumeMessage {
    Write-Host "No queued or interrupted jobs were found." -ForegroundColor Yellow
    Write-Host "Press any key to continue..." -ForegroundColor DarkGray
    try { $null = [Console]::ReadKey($true) } catch {}
}

function Recover-StaleQueueArtifactsForEnqueue {
    if (Test-Path -LiteralPath $StatePath) {
        Remove-Item -LiteralPath $StatePath -Force -ErrorAction SilentlyContinue
    }

    foreach ($staleJob in @(Get-ChildItem -LiteralPath $QueueWorkingDir -Filter *.json -File -ErrorAction SilentlyContinue)) {
        try {
            Requeue-WorkingJob -WorkingJobPath $staleJob.FullName
        } catch {}
    }
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
            Write-SessionTextLogMessage -Level Warn -Message ("Queue add skipped | path not found | {0}" -f $p)
            continue
        }

        $item = Get-Item -LiteralPath $p
        if ($item.PSIsContainer) {
            Write-Warning "Folders are not supported for drag-drop queueing, skipping: $($item.FullName)"
            Write-SessionTextLogMessage -Level Warn -Message ("Queue add skipped | folders are not supported | {0}" -f $item.FullName)
            continue
        }

        $full = Get-NormalizedPath -Path $item.FullName

        if ($existing.Contains($full)) {
            Write-Host "Already queued or currently processing: $full" -ForegroundColor Yellow
            Write-SessionTextLogMessage -Level Warn -Message ("Queue add skipped | already queued or processing | {0}" -f $full)
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
        Write-SessionTextLogMessage -Level Info -Message ("Queued | {0}" -f $full)
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

    if ($InputObject -is [System.Collections.IDictionary]) {
        if (-not $InputObject.Contains($PropertyName)) { return $Default }
        $value = $InputObject[$PropertyName]
        if ($null -eq $value) { return $Default }
        return $value
    }

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

function Get-StreamEstimatedSizeBytes {
    param(
        $Stream,
        [double]$DurationSec = 0.0
    )

    foreach ($tagName in @('NUMBER_OF_BYTES', 'NUMBER_OF_BYTES-eng')) {
        $numBytes = Convert-ToInvariantInt64 (Get-StreamTagValue $Stream $tagName '') 0
        if ($numBytes -gt 0) { return $numBytes }
    }

    $bitrateEstimate = Get-StreamBitrateEstimate -Stream $Stream -DurationSec $DurationSec
    if ($bitrateEstimate.Bitrate -gt 0 -and $DurationSec -gt 0) {
        return [int64][Math]::Round(($bitrateEstimate.Bitrate * $DurationSec) / 8.0)
    }

    return 0L
}

function Get-CopiedStreamsSizeEstimate {
    param(
        [object[]]$Streams = @(),
        [double]$DurationSec = 0.0
    )

    $totalBytes = 0L
    $usedCount = 0

    foreach ($stream in @($Streams | Where-Object { $_ })) {
        $streamBytes = Get-StreamEstimatedSizeBytes -Stream $stream -DurationSec $DurationSec
        if ($streamBytes -gt 0) {
            $totalBytes += [int64]$streamBytes
            $usedCount++
        }
    }

    return [ordered]@{
        Bytes      = $totalBytes
        StreamCount = $usedCount
        Reason     = if ($usedCount -gt 0) {
            "Estimated $usedCount copied stream size(s) from ffprobe metadata."
        } else {
            'No reliable copied-stream size estimate was available.'
        }
    }
}

function Update-LiveEstimateState {
    param(
        $State,
        [double]$SourceDurationSec,
        [int64]$SourceSizeBytes
    )

    $result = [pscustomobject][ordered]@{
        Enabled             = $EnableLiveSizeEstimate
        Ready               = $false
        Status              = 'starting'
        ProgressPercent     = 0.0
        EstimatedFinalBytes = 0.0
        EstimatedFinalSizeGiB = 0.0
        EstimatedSavingsPercent = 0.0
        EstimatedOutputGiBPerHour = 0.0
    }

    if (-not $EnableLiveSizeEstimate) {
        $result.Status = 'disabled'
        return $result
    }

    if ($SourceDurationSec -le 0 -or $SourceSizeBytes -le 0) {
        return $result
    }

    $encodedSec = Convert-ToInvariantDouble (Get-OptionalProperty $State 'OutTimeSec' 0.0) 0.0
    $outputBytes = Convert-ToInvariantDouble (Get-OptionalProperty $State 'OutSizeBytes' 0.0) 0.0
    $progressPercent = if ($SourceDurationSec -gt 0) { ($encodedSec / $SourceDurationSec) * 100.0 } else { 0.0 }
    $result.ProgressPercent = $progressPercent

    if ($encodedSec -le 0.0 -or $outputBytes -le 0.0) {
        $result.Status = 'starting'
        $State.EstimateReady = $false
        return $result
    }

    if ($progressPercent -lt $LiveEstimateStartPercent) {
        $result.Status = 'warming up'
        $State.EstimateReady = $false
        return $result
    }

    $rawEstimatedFinalBytes = ($outputBytes / $encodedSec) * $SourceDurationSec
    if ($rawEstimatedFinalBytes -le 0.0) {
        $State.EstimateReady = $false
        return $result
    }

    $previousSmoothed = Convert-ToInvariantDouble (Get-OptionalProperty $State 'SmoothedEstimatedFinalBytes' 0.0) 0.0
    $smoothed = if ($previousSmoothed -gt 0.0) {
        ($previousSmoothed * (1.0 - $LiveEstimateSmoothingFactor)) + ($rawEstimatedFinalBytes * $LiveEstimateSmoothingFactor)
    } else {
        $rawEstimatedFinalBytes
    }

    $savingsPercent = 100.0 * (1.0 - ($smoothed / $SourceSizeBytes))
    $outputGiBPerHour = if ($SourceDurationSec -gt 0) { ($smoothed / 1GB) / ($SourceDurationSec / 3600.0) } else { 0.0 }

    $State.SmoothedEstimatedFinalBytes = $smoothed
    $State.LastRawEstimatedFinalBytes = $rawEstimatedFinalBytes
    $State.EstimateReady = $true
    $State.EstimatedSavingsPercent = $savingsPercent
    $State.EstimatedOutputGiBPerHour = $outputGiBPerHour

    $result.Ready = $true
    $result.Status = 'ready'
    $result.EstimatedFinalBytes = $smoothed
    $result.EstimatedFinalSizeGiB = ($smoothed / 1GB)
    $result.EstimatedSavingsPercent = $savingsPercent
    $result.EstimatedOutputGiBPerHour = $outputGiBPerHour
    return $result
}

function Get-AnimatedDotsText {
    param([int]$MaximumDots = 10)

    $secondsTick = [int64][Math]::Floor([DateTime]::UtcNow.Ticks / 10000000.0)
    $dotCount = [Math]::Max(1, [Math]::Min($MaximumDots, [int](($secondsTick % $MaximumDots) + 1)))
    return ('.' * $dotCount)
}

function Get-LiveEstimateSummaryText {
    param($Estimate)

    if (-not $EnableLiveSizeEstimate) { return '' }
    if ($null -eq $Estimate -or -not $Estimate.Ready) {
        if ($Estimate -and $Estimate.Status -eq 'starting') {
            return ("Starting up{0}" -f (Get-AnimatedDotsText))
        }

        if ($Estimate -and $Estimate.Status -eq 'warming up') {
            return ("Est. final size: warming up ({0:F1}% complete)" -f $Estimate.ProgressPercent)
        }

        return 'Est. final size: warming up'
    }

    return ("Est. final: {0:F2} GiB  |  Est. savings: {1:F1}%  |  Est. rate: {2:F2} GiB/hr" -f
        $Estimate.EstimatedFinalSizeGiB,
        $Estimate.EstimatedSavingsPercent,
        $Estimate.EstimatedOutputGiBPerHour)
}

function Remove-AnsiDisplayFormatting {
    param([string]$Value)

    if ([string]::IsNullOrEmpty($Value)) { return '' }

    $escape = [regex]::Escape([string][char]27)
    return [regex]::Replace($Value, "${escape}\[[0-9;?]*[ -/]*[@-~]", '')
}

function Add-RainbowHdrHighlights {
    param(
        [string]$Text,
        [string]$BaseColor = ''
    )

    if ([string]::IsNullOrEmpty($Text)) { return $Text }

    $ESC = [char]27
    $reset = "${ESC}[0m"
    $rainbowCodes = @('196', '208', '226', '82', '45', '93')

    function Convert-HdrTokenToRainbow {
        param([string]$Token)

        $builder = [System.Text.StringBuilder]::new()
        for ($i = 0; $i -lt $Token.Length; $i++) {
            $code = $rainbowCodes[$i % $rainbowCodes.Count]
            $null = $builder.Append("${ESC}[1;38;5;${code}m")
            $null = $builder.Append($Token[$i])
        }

        $null = $builder.Append($reset)
        if (-not [string]::IsNullOrEmpty($BaseColor)) {
            $null = $builder.Append($BaseColor)
        }

        return $builder.ToString()
    }

    $highlighted = [regex]::Replace($Text, 'HDR10\+', { param($m) Convert-HdrTokenToRainbow -Token $m.Value })
    $highlighted = [regex]::Replace($highlighted, 'HDR10', { param($m) Convert-HdrTokenToRainbow -Token $m.Value })
    $highlighted = [regex]::Replace($highlighted, '(?<![A-Za-z0-9])HDR(?![A-Za-z0-9+])', { param($m) Convert-HdrTokenToRainbow -Token $m.Value })

    return $highlighted
}

function Invoke-FfmpegSync {
    param([string[]]$Arguments)

    $tracked = Start-TrackedFfmpegProcess -Arguments $Arguments -PriorityName 'Normal'
    try {
        while (-not $tracked.Process.HasExited) {
            if (Test-QueueShutdownRequested) {
                Request-FfmpegProcessQuit -Process $tracked.Process
                $deadline = (Get-Date).AddSeconds(20)
                while (-not $tracked.Process.HasExited -and (Get-Date) -lt $deadline) {
                    Start-Sleep -Milliseconds 200
                }
                if (-not $tracked.Process.HasExited) {
                    try { $tracked.Process.Kill() } catch {}
                }
                throw $script:QueueShutdownSentinel
            }

            Start-Sleep -Milliseconds 200
        }

        $stderr = ($tracked.Shared.LogLines -join "`n")
        return [pscustomobject][ordered]@{
            ExitCode = $tracked.Process.ExitCode
            Stderr   = $stderr
            LogLines = @(($stderr -split "\r?\n") | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        }
    } finally {
        Stop-TrackedFfmpegProcess -TrackedProcess $tracked
    }
}

function Invoke-PreflightEstimate {
    param(
        [string]$InputPath,
        $Selected,
        $SourceProfile,
        [string]$EncodeMode,
        [double]$SourceDurationSec,
        [int64]$SourceSizeBytes,
        [int]$ResolvedCRF,
        [int]$ResolvedPreset,
        [int]$ResolvedFilmGrain,
        [int]$PassNumber = 1,
        [string]$SettingsLabel = '',
        $NvencSettings = $null,
        $NvencEnvironment = $null
    )

    if (-not $EnablePreflightEstimate) {
        return [pscustomobject][ordered]@{
            Ran = $false
            ShouldSkip = $false
            Reason = 'Preflight estimate disabled by configuration.'
        }
    }

    if ($SourceDurationSec -le 0) {
        return [pscustomobject][ordered]@{
            Ran = $false
            ShouldSkip = $false
            Reason = 'Preflight estimate skipped because the source duration is unavailable.'
        }
    }

    $maxSamplesBySpan = [int][Math]::Floor(($SourceDurationSec * 0.80) / [Math]::Max(1, $PreflightSampleDurationSec))
    $sampleCount = [Math]::Min([int]$PreflightSampleCount, [Math]::Max(0, $maxSamplesBySpan))
    if ($sampleCount -lt 1) {
        return [pscustomobject][ordered]@{
            Ran = $false
            ShouldSkip = $false
            Reason = 'Preflight estimate skipped because the source is too short for the configured sample spacing.'
        }
    }

    $copiedEstimate = Get-CopiedStreamsSizeEstimate -Streams @($Selected.MainAudio, $Selected.FallbackAudio, $Selected.MainSub, $Selected.SdhSub) -DurationSec $SourceDurationSec
    $sampleBytesPerSec = [System.Collections.Generic.List[double]]::new()
    $sampleFailures = [System.Collections.Generic.List[string]]::new()

    Write-Host ("Preflight pass {0}" -f $PassNumber) -ForegroundColor DarkCyan
    Write-SessionTextLogMessage -Level Info -Message ("Preflight pass {0} | {1}" -f $PassNumber, [System.IO.Path]::GetFileName($InputPath))
    if (-not [string]::IsNullOrWhiteSpace($SettingsLabel)) {
        Write-Host ("Settings: {0}" -f $SettingsLabel) -ForegroundColor DarkCyan
        Write-SessionTextLogMessage -Level Info -Message ("Preflight settings | {0}" -f $SettingsLabel)
    }
    Write-Host ("Running {0} sample encodes of {1}s each..." -f $sampleCount, $PreflightSampleDurationSec) -ForegroundColor DarkCyan
    Write-SessionTextLogMessage -Level Info -Message ("Preflight samples | count={0} | duration={1}s" -f $sampleCount, $PreflightSampleDurationSec)

    for ($i = 0; $i -lt $sampleCount; $i++) {
        $fraction = 0.10 + (0.80 * (($i + 0.5) / $sampleCount))
        $centerSec = $SourceDurationSec * $fraction
        $startSec = [Math]::Max(0.0, [Math]::Min($SourceDurationSec - $PreflightSampleDurationSec, $centerSec - ($PreflightSampleDurationSec / 2.0)))
        $sampleOutput = Join-Path $PreflightDir ("{0}_{1}_{2}.mkv" -f ([System.IO.Path]::GetFileNameWithoutExtension($InputPath)), [Guid]::NewGuid().ToString('N'), $i)

        try {
            $ffArgs = New-Object System.Collections.Generic.List[string]
            $ffArgs.AddRange([string[]]@('-hide_banner', '-y', '-ss', ("{0:0.###}" -f $startSec), '-t', "$PreflightSampleDurationSec"))

            if ($EncodeMode -eq 'nvenc' -and $NvencSettings -and $NvencSettings.DecodePath -eq 'cuda') {
                $ffArgs.AddRange([string[]]@('-hwaccel', 'cuda', '-hwaccel_output_format', 'cuda'))
            }

            $ffArgs.AddRange([string[]]@('-i', $InputPath, '-map', "0:$($Selected.Video.index)", '-an', '-sn', '-dn'))

            if ($EncodeMode -eq 'nvenc') {
                $ffArgs.AddRange([string[]]@('-c:v', 'av1_nvenc'))
                if ($NvencEnvironment.SupportsPreset -and -not [string]::IsNullOrWhiteSpace($NvencSettings.Preset)) {
                    $ffArgs.AddRange([string[]]@('-preset', $NvencSettings.Preset))
                }
                if ($NvencEnvironment.SupportsTune -and -not [string]::IsNullOrWhiteSpace($NvencSettings.Tune)) {
                    $ffArgs.AddRange([string[]]@('-tune', $NvencSettings.Tune))
                }
                if ($NvencEnvironment.SupportsRc) { $ffArgs.AddRange([string[]]@('-rc', 'vbr')) }
                if ($NvencEnvironment.SupportsCQ) { $ffArgs.AddRange([string[]]@('-cq', "$($NvencSettings.CQ)")) }
                if ($NvencEnvironment.SupportsLookahead) { $ffArgs.AddRange([string[]]@('-rc-lookahead', '32')) }
                if ($NvencEnvironment.SupportsSpatialAQ)  { $ffArgs.AddRange([string[]]@('-spatial-aq', '1')) }
                if ($NvencEnvironment.SupportsTemporalAQ) { $ffArgs.AddRange([string[]]@('-temporal-aq', '1')) }
                if ($NvencEnvironment.SupportsAQStrength) { $ffArgs.AddRange([string[]]@('-aq-strength', '8')) }
                if ($NvencEnvironment.SupportsBRefMode)   { $ffArgs.AddRange([string[]]@('-b_ref_mode', 'middle')) }
                if ($NvencEnvironment.SupportsMultipass)  { $ffArgs.AddRange([string[]]@('-multipass', 'fullres')) }
                if ($NvencEnvironment.SupportsSplitEncode -and -not $NvencAllowSplitFrame) {
                    $ffArgs.AddRange([string[]]@('-split_encode_mode', 'disabled'))
                }
                $ffArgs.AddRange([string[]]@('-pix_fmt', $NvencSettings.PixFmt))
                if ($NvencEnvironment.SupportsHighBitDepth -and $NvencSettings.BitDepth -ge 10) {
                    $ffArgs.AddRange([string[]]@('-highbitdepth', '1'))
                }
            } else {
                $ffArgs.AddRange([string[]]@('-c:v', 'libsvtav1', '-preset', "$ResolvedPreset", '-crf', "$ResolvedCRF", '-pix_fmt', 'yuv420p10le'))
                if ($ResolvedFilmGrain -gt 0) {
                    $ffArgs.AddRange([string[]]@('-svtav1-params', "film-grain=$ResolvedFilmGrain`:film-grain-denoise=0"))
                }
            }

            if ($SourceProfile.HasHDR) {
                $ffArgs.AddRange([string[]]@('-color_primaries', 'bt2020', '-color_trc', 'smpte2084', '-colorspace', 'bt2020nc'))
            }

            $ffArgs.Add($sampleOutput)

            $sampleResult = Invoke-FfmpegSync -Arguments $ffArgs.ToArray()
            if ($sampleResult.ExitCode -ne 0) {
                $sampleFailures.Add(("sample {0} failed: {1}" -f ($i + 1), (($sampleResult.LogLines | Select-Object -Last 3) -join ' || ')))
                continue
            }

            if (-not (Test-Path -LiteralPath $sampleOutput)) {
                $sampleFailures.Add(("sample {0} did not create an output file" -f ($i + 1)))
                continue
            }

            $sampleItem = Get-Item -LiteralPath $sampleOutput
            if ($sampleItem.Length -gt 0) {
                $sampleBytesPerSec.Add(($sampleItem.Length / [double]$PreflightSampleDurationSec))
            }
        } finally {
            if (Test-Path -LiteralPath $sampleOutput) {
                Remove-Item -LiteralPath $sampleOutput -Force -ErrorAction SilentlyContinue
            }
        }
    }

    if ($sampleBytesPerSec.Count -eq 0) {
        $failureReason = if ($sampleFailures.Count -gt 0) {
            "Preflight estimate failed; continuing with full encode. " + (($sampleFailures | Select-Object -First 3) -join ' | ')
        } else {
            'Preflight estimate failed; continuing with full encode.'
        }
        Write-SessionTextLogMessage -Level Warn -Message $failureReason
        return [pscustomobject][ordered]@{
            Ran = $false
            ShouldSkip = $false
            Reason = $failureReason
        }
    }

    $sortedRates = @($sampleBytesPerSec | Sort-Object)
    $medianRate = if ($sortedRates.Count % 2 -eq 1) {
        $sortedRates[[int][Math]::Floor($sortedRates.Count / 2)]
    } else {
        ($sortedRates[($sortedRates.Count / 2) - 1] + $sortedRates[$sortedRates.Count / 2]) / 2.0
    }

    $estimatedVideoBytes = $medianRate * $SourceDurationSec
    $estimatedFinalBytes = $estimatedVideoBytes + $copiedEstimate.Bytes
    $estimatedPctOfSource = if ($SourceSizeBytes -gt 0) { ($estimatedFinalBytes / $SourceSizeBytes) * 100.0 } else { 0.0 }
    $estimatedSavingsPercent = if ($SourceSizeBytes -gt 0) { 100.0 * (1.0 - ($estimatedFinalBytes / $SourceSizeBytes)) } else { 0.0 }
    $estimatedOutputGiBPerHour = if ($SourceDurationSec -gt 0) { ($estimatedFinalBytes / 1GB) / ($SourceDurationSec / 3600.0) } else { 0.0 }

    $shouldSkip = $estimatedPctOfSource -ge $PreflightAbortIfEstimatedPctOfSource
    $reason = "Used median bytes/sec from $($sampleBytesPerSec.Count) sample encode(s)."
    if ($copiedEstimate.Bytes -gt 0) {
        $reason += " Added copied stream estimate ($([Math]::Round($copiedEstimate.Bytes / 1MB, 2)) MiB)."
    }
    Write-SessionTextLogMessage -Level Info -Message ("Preflight estimate | {0:F2} GiB | savings {1:F1}% | rate {2:F2} GiB/hr" -f ($estimatedFinalBytes / 1GB), $estimatedSavingsPercent, $estimatedOutputGiBPerHour)

    return [pscustomobject][ordered]@{
        Ran = $true
        ShouldSkip = $shouldSkip
        EstimatedFinalBytes = $estimatedFinalBytes
        EstimatedFinalSizeGiB = ($estimatedFinalBytes / 1GB)
        EstimatedSavingsPercent = $estimatedSavingsPercent
        EstimatedOutputGiBPerHour = $estimatedOutputGiBPerHour
        EstimatedPctOfSource = $estimatedPctOfSource
        SampleCountUsed = $sampleBytesPerSec.Count
        WarningTriggered = ($estimatedPctOfSource -ge $PreflightWarnIfEstimatedPctOfSource)
        Reason = $reason
    }
}

function Format-PreflightSettingsLabel {
    param(
        [string]$EncodeMode,
        [int]$CRF,
        [int]$Preset,
        [int]$FilmGrain,
        $NvencSettings = $null
    )

    if ($EncodeMode -eq 'nvenc' -and $NvencSettings) {
        return "CRF $CRF / Preset $Preset / FilmGrain $FilmGrain / CQ $($NvencSettings.CQ) / NVENC $($NvencSettings.Preset)"
    }

    return "CRF $CRF / Preset $Preset / FilmGrain $FilmGrain"
}

function Resolve-PreflightAutoTuneTargets {
    param(
        [string]$QualityProfile,
        [string]$ResolutionTier,
        $SourceProfile
    )

    $target = 4.0
    $lower = 3.0
    $upper = 5.0
    $profileLabel = if ([string]::IsNullOrWhiteSpace($QualityProfile)) { 'High' } else { $QualityProfile }
    $isHdr = [bool](Get-OptionalProperty -InputObject $SourceProfile -PropertyName 'HasHDR' -Default $false)
    $tier = if ([string]::IsNullOrWhiteSpace($ResolutionTier)) { 'HD' } else { $ResolutionTier }

    switch ("$tier|$($isHdr)") {
        'UHD|True' {
            switch ($profileLabel) {
                'Low'    { $target = 6.0;  $lower = 4.0;  $upper = 8.0 }
                'Medium' { $target = 9.0;  $lower = 7.0;  $upper = 11.0 }
                default  { $target = 12.0; $lower = 10.0; $upper = 14.0 }
            }
        }
        'UHD|False' {
            switch ($profileLabel) {
                'Low'    { $target = 5.0;  $lower = 3.0;  $upper = 7.0 }
                'Medium' { $target = 8.0;  $lower = 6.0;  $upper = 10.0 }
                default  { $target = 10.0; $lower = 8.0;  $upper = 12.0 }
            }
        }
        'HD|True' {
            switch ($profileLabel) {
                'Low'    { $target = 3.0; $lower = 2.0; $upper = 4.0 }
                'Medium' { $target = 4.0; $lower = 3.0; $upper = 5.0 }
                default  { $target = 5.0; $lower = 4.0; $upper = 6.0 }
            }
        }
        'HD|False' {
            switch ($profileLabel) {
                'Low'    { $target = 2.0; $lower = 1.0; $upper = 3.0 }
                'Medium' { $target = 3.0; $lower = 2.0; $upper = 4.0 }
                default  { $target = 4.0; $lower = 3.0; $upper = 5.0 }
            }
        }
        default {
            switch ($profileLabel) {
                'Low'    { $target = 1.0; $lower = 0.0; $upper = 2.0 }
                'Medium' { $target = 1.5; $lower = 0.5; $upper = 2.5 }
                default  { $target = 2.0; $lower = 1.0; $upper = 3.0 }
            }
        }
    }

    if ($null -ne $PreflightAutoTuneCustomTargetGiBPerHour) { $target = [double]$PreflightAutoTuneCustomTargetGiBPerHour }
    if ($null -ne $PreflightAutoTuneCustomUpperGiBPerHour)  { $upper  = [double]$PreflightAutoTuneCustomUpperGiBPerHour }
    if ($null -ne $PreflightAutoTuneCustomLowerGiBPerHour)  { $lower  = [double]$PreflightAutoTuneCustomLowerGiBPerHour }

    $sourceLabel = switch ($tier) {
        'UHD' { if ($isHdr) { 'UHD HDR' } else { 'UHD SDR' } }
        'HD'  { if ($isHdr) { 'HD HDR' } else { 'HD SDR' } }
        default { 'SD / 720p SDR' }
    }

    return [pscustomobject][ordered]@{
        QualityProfile = $profileLabel
        TargetGiBPerHour = $target
        LowerGiBPerHour = $lower
        UpperGiBPerHour = $upper
        TargetReason = "{0} {1}, range {2}-{3}" -f $sourceLabel, $profileLabel, $lower, $upper
    }
}

function Test-PreflightOversized {
    param(
        $PreflightResult,
        $Targets
    )

    if (-not $PreflightResult -or -not $PreflightResult.Ran) { return $false }
    return ($PreflightResult.EstimatedOutputGiBPerHour -gt $Targets.UpperGiBPerHour -or
            $PreflightResult.EstimatedPctOfSource -ge $PreflightWarnIfEstimatedPctOfSource)
}

function Test-PreflightSuspiciouslyTiny {
    param(
        $PreflightResult,
        $Targets
    )

    if (-not $PreflightResult -or -not $PreflightResult.Ran) { return $false }
    return ($PreflightResult.EstimatedPctOfSource -le $PreflightTinyOutputPctThreshold -and
            ($PreflightResult.EstimatedFinalSizeGiB -lt $PreflightTinyOutputAbsoluteGiBThreshold -or
             $PreflightResult.EstimatedOutputGiBPerHour -lt $Targets.LowerGiBPerHour))
}

function Get-PreflightAutoTuneAdjustment {
    param(
        $PreflightResult,
        $PreflightTargets,
        $AutoSettings,
        [string]$EncodeMode,
        [int]$CurrentCRF,
        [int]$CurrentPreset,
        [int]$CurrentFilmGrain,
        [bool]$AllowCrfTune,
        [bool]$AllowPresetTune,
        [bool]$AllowFilmGrainTune
    )

    $newCrf = $CurrentCRF
    $newPreset = $CurrentPreset
    $newFilmGrain = $CurrentFilmGrain
    $reasons = [System.Collections.Generic.List[string]]::new()

    if (Test-PreflightOversized -PreflightResult $PreflightResult -Targets $PreflightTargets) {
        if ($AllowCrfTune) {
            $adjustedCrf = [Math]::Max(0, [Math]::Min(63, ($CurrentCRF + 2)))
            if ($adjustedCrf -ne $newCrf) {
                $reasons.Add("Auto-tune: CRF $CurrentCRF -> $adjustedCrf (oversized preflight)")
                $newCrf = $adjustedCrf
            }
        }

        if ($AllowFilmGrainTune -and $EncodeMode -eq 'software' -and ($AutoSettings.GrainClass -in @('moderate', 'heavy', 'extreme')) -and $newFilmGrain -lt 16) {
            $adjustedFilmGrain = [Math]::Min(16, ($newFilmGrain + 4))
            if ($adjustedFilmGrain -ne $newFilmGrain) {
                $reasons.Add("Auto-tune: FilmGrain $newFilmGrain -> $adjustedFilmGrain (grain-aware oversized preflight)")
                $newFilmGrain = $adjustedFilmGrain
            }
        }
    } elseif (Test-PreflightSuspiciouslyTiny -PreflightResult $PreflightResult -Targets $PreflightTargets) {
        if ($AllowCrfTune) {
            $adjustedCrf = [Math]::Max(0, [Math]::Min(63, ($CurrentCRF - 1)))
            if ($adjustedCrf -ne $newCrf) {
                $reasons.Add("Auto-tune: CRF $CurrentCRF -> $adjustedCrf (suspiciously tiny preflight)")
                $newCrf = $adjustedCrf
            }
        }

        if ($AllowFilmGrainTune -and $EncodeMode -eq 'software' -and ($AutoSettings.GrainClass -in @('none', 'light', 'unknown')) -and $newFilmGrain -gt 0) {
            $adjustedFilmGrain = [Math]::Max(0, ($newFilmGrain - 4))
            if ($adjustedFilmGrain -ne $newFilmGrain) {
                $reasons.Add("Auto-tune: FilmGrain $newFilmGrain -> $adjustedFilmGrain (suspiciously tiny clean-source preflight)")
                $newFilmGrain = $adjustedFilmGrain
            }
        }
    }

    # Preset is intentionally left untouched here unless future evidence shows
    # it helps more than it harms. In the current quality-first architecture,
    # CRF and film grain are the clearer tuning levers.
    return [pscustomobject][ordered]@{
        CRF = $newCrf
        Preset = $newPreset
        FilmGrain = $newFilmGrain
        MaterialChange = ($newCrf -ne $CurrentCRF -or $newPreset -ne $CurrentPreset -or $newFilmGrain -ne $CurrentFilmGrain)
        Reasons = @($reasons)
    }
}

function Invoke-PreflightAutoTuneWorkflow {
    param(
        [string]$InputPath,
        $Selected,
        $SourceProfile,
        [string]$EncodeMode,
        [double]$SourceDurationSec,
        [int64]$SourceSizeBytes,
        $AutoSettings,
        [int]$InitialResolvedCRF,
        [int]$InitialResolvedPreset,
        [int]$InitialResolvedFilmGrain,
        $NvencEnvironment = $null
    )

    $workflow = [pscustomobject][ordered]@{
        InitialResolvedCRF = $InitialResolvedCRF
        InitialResolvedPreset = $InitialResolvedPreset
        InitialResolvedFilmGrain = $InitialResolvedFilmGrain
        FinalResolvedCRF = $InitialResolvedCRF
        FinalResolvedPreset = $InitialResolvedPreset
        FinalResolvedFilmGrain = $InitialResolvedFilmGrain
        FinalNvencSettings = $null
        PreflightPassCount = 0
        Preflight1 = $null
        Preflight2 = $null
        FinalPreflight = [pscustomobject][ordered]@{ Ran = $false; ShouldSkip = $false; Reason = 'Preflight estimate not run.' }
        PreflightAutoTuneReason = ''
        WasPreflightRetuned = $false
        WasSkippedByPreflight = $false
        SkipStatus = ''
    }

    if ($EncodeMode -eq 'nvenc') {
        $baseNvencAuto = [ordered]@{}
        foreach ($prop in $AutoSettings.Keys) { $baseNvencAuto[$prop] = $AutoSettings[$prop] }
        $baseNvencAuto.CRF = $InitialResolvedCRF
        $workflow.FinalNvencSettings = Convert-SoftwareQualityToNvencSettings `
            -AutoSettings $baseNvencAuto `
            -SourceProfile $SourceProfile `
            -ConfiguredNvencPreset $NvencPreset `
            -ConfiguredNvencCQ $NvencCQ `
            -ConfiguredNvencTune $NvencTune `
            -ConfiguredNvencDecode $NvencDecode `
            -NvencEnvironment $NvencEnvironment
    }

    if (-not $EnablePreflightEstimate) {
        $workflow.PreflightAutoTuneReason = 'Preflight estimate disabled by configuration.'
        return $workflow
    }

    $allowCrfTune = ($CRF -eq 'Auto')
    $allowPresetTune = ($Preset -eq 'Auto' -and $EncodeMode -eq 'software')
    $allowFilmGrainTune = ($FilmGrain -eq 'Auto' -and $EncodeMode -eq 'software')
    $preflightTargets = Resolve-PreflightAutoTuneTargets -QualityProfile $PreflightAutoTuneQuality -ResolutionTier $AutoSettings.ResolutionTier -SourceProfile $SourceProfile
    $reasons = [System.Collections.Generic.List[string]]::new()
    $reasons.Add("Initial Auto: CRF $InitialResolvedCRF / Preset $InitialResolvedPreset / FilmGrain $InitialResolvedFilmGrain ($($AutoSettings.ResolutionTier) / $($SourceProfile.Profile) / $($AutoSettings.CodecLabel) / $($AutoSettings.BPPTier) BPP)")
    $reasons.Add(("Preflight target: {0} GiB/hr ({1})" -f $preflightTargets.TargetGiBPerHour, $preflightTargets.TargetReason))

    $currentCrf = $InitialResolvedCRF
    $currentPreset = $InitialResolvedPreset
    $currentFilmGrain = $InitialResolvedFilmGrain

    $currentNvencSettings = $workflow.FinalNvencSettings
    Write-Host ("Preflight target profile: {0}" -f $preflightTargets.QualityProfile) -ForegroundColor DarkCyan
    Write-Host ("Resolved target: {0} GiB/hr ({1})" -f $preflightTargets.TargetGiBPerHour, $preflightTargets.TargetReason) -ForegroundColor DarkCyan
    Write-SessionTextLogMessage -Level Info -Message ("Preflight target | profile {0} | {1} GiB/hr ({2})" -f $preflightTargets.QualityProfile, $preflightTargets.TargetGiBPerHour, $preflightTargets.TargetReason)
    $workflow.Preflight1 = Invoke-PreflightEstimate `
        -InputPath $InputPath `
        -Selected $Selected `
        -SourceProfile $SourceProfile `
        -EncodeMode $EncodeMode `
        -SourceDurationSec $SourceDurationSec `
        -SourceSizeBytes $SourceSizeBytes `
        -ResolvedCRF $currentCrf `
        -ResolvedPreset $currentPreset `
        -ResolvedFilmGrain $currentFilmGrain `
        -PassNumber 1 `
        -SettingsLabel (Format-PreflightSettingsLabel -EncodeMode $EncodeMode -CRF $currentCrf -Preset $currentPreset -FilmGrain $currentFilmGrain -NvencSettings $currentNvencSettings) `
        -NvencSettings $currentNvencSettings `
        -NvencEnvironment $NvencEnvironment

    if ($workflow.Preflight1.Ran) {
        $workflow.PreflightPassCount = 1
        $reasons.Add(("Preflight 1: projected {0:F2} GiB/hr, {1:F1}% of source" -f $workflow.Preflight1.EstimatedOutputGiBPerHour, $workflow.Preflight1.EstimatedPctOfSource))
    } else {
        $workflow.PreflightAutoTuneReason = $workflow.Preflight1.Reason
        $workflow.FinalPreflight = $workflow.Preflight1
        return $workflow
    }

    if ($EnablePreflightAutoTune -and ($allowCrfTune -or $allowPresetTune -or $allowFilmGrainTune)) {
        $adjustment = Get-PreflightAutoTuneAdjustment `
            -PreflightResult $workflow.Preflight1 `
            -PreflightTargets $preflightTargets `
            -AutoSettings $AutoSettings `
            -EncodeMode $EncodeMode `
            -CurrentCRF $currentCrf `
            -CurrentPreset $currentPreset `
            -CurrentFilmGrain $currentFilmGrain `
            -AllowCrfTune $allowCrfTune `
            -AllowPresetTune $allowPresetTune `
            -AllowFilmGrainTune $allowFilmGrainTune

        if ($adjustment.MaterialChange) {
            $workflow.WasPreflightRetuned = $true
            foreach ($adjustmentReason in @($adjustment.Reasons)) {
                if (-not [string]::IsNullOrWhiteSpace([string]$adjustmentReason)) {
                    $reasons.Add([string]$adjustmentReason)
                }
            }
            $currentCrf = $adjustment.CRF
            $currentPreset = $adjustment.Preset
            $currentFilmGrain = $adjustment.FilmGrain
            $workflow.FinalResolvedCRF = $currentCrf
            $workflow.FinalResolvedPreset = $currentPreset
            $workflow.FinalResolvedFilmGrain = $currentFilmGrain

            if ($EncodeMode -eq 'nvenc') {
                $retunedNvencAuto = [ordered]@{}
                foreach ($prop in $AutoSettings.Keys) { $retunedNvencAuto[$prop] = $AutoSettings[$prop] }
                $retunedNvencAuto.CRF = $currentCrf
                $workflow.FinalNvencSettings = Convert-SoftwareQualityToNvencSettings `
                    -AutoSettings $retunedNvencAuto `
                    -SourceProfile $SourceProfile `
                    -ConfiguredNvencPreset $NvencPreset `
                    -ConfiguredNvencCQ $NvencCQ `
                    -ConfiguredNvencTune $NvencTune `
                    -ConfiguredNvencDecode $NvencDecode `
                    -NvencEnvironment $NvencEnvironment
            }

            if ($EnableSecondPreflightPass) {
                $currentNvencSettings = if ($EncodeMode -eq 'nvenc') { $workflow.FinalNvencSettings } else { $null }
                $workflow.Preflight2 = Invoke-PreflightEstimate `
                    -InputPath $InputPath `
                    -Selected $Selected `
                    -SourceProfile $SourceProfile `
                    -EncodeMode $EncodeMode `
                    -SourceDurationSec $SourceDurationSec `
                    -SourceSizeBytes $SourceSizeBytes `
                    -ResolvedCRF $currentCrf `
                    -ResolvedPreset $currentPreset `
                    -ResolvedFilmGrain $currentFilmGrain `
                    -PassNumber 2 `
                    -SettingsLabel (Format-PreflightSettingsLabel -EncodeMode $EncodeMode -CRF $currentCrf -Preset $currentPreset -FilmGrain $currentFilmGrain -NvencSettings $currentNvencSettings) `
                    -NvencSettings $currentNvencSettings `
                    -NvencEnvironment $NvencEnvironment

                if ($workflow.Preflight2.Ran) {
                    $workflow.PreflightPassCount = 2
                    $reasons.Add(("Preflight 2: projected {0:F2} GiB/hr, {1:F1}% of source" -f $workflow.Preflight2.EstimatedOutputGiBPerHour, $workflow.Preflight2.EstimatedPctOfSource))
                }
            }
        }
    }

    $workflow.FinalPreflight = if ($workflow.Preflight2 -and $workflow.Preflight2.Ran) { $workflow.Preflight2 } else { $workflow.Preflight1 }
    $workflow | Add-Member -NotePropertyName PreflightTargets -NotePropertyValue $preflightTargets -Force
    if ($workflow.FinalPreflight -and $workflow.FinalPreflight.Ran -and $workflow.FinalPreflight.EstimatedPctOfSource -ge $PreflightAbortIfEstimatedPctOfSource) {
        $workflow.WasSkippedByPreflight = $true
        $workflow.SkipStatus = 'PRECHECK_SKIPPED_UNFAVORABLE'
        $reasons.Add('Decision: skipped (estimated output exceeds threshold)')
    } elseif ($workflow.FinalPreflight -and $workflow.FinalPreflight.Ran) {
        $reasons.Add('Proceeding with tuned settings')
    }

    $workflow.PreflightAutoTuneReason = ($reasons -join ' | ')
    return $workflow
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

function Get-EncodeInitialization {
    param(
        [string]$InputPath,
        [string]$EncodeMode = 'software',
        $NvencEnvironment = $null,
        [string]$EncoderPreferenceValue = $EncoderPreference,
        [string]$LaneSelectionReason = '',
        [string]$LaneSuitability = '',
        [string]$CpuOnlyReason = '',
        [bool]$NvidiaFallbackAllowed = $true,
        [bool]$HeldForCpuLane = $false
    )

    $sourceItem = Get-Item -LiteralPath $InputPath
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
    $copiedStreamsEstimate = Get-CopiedStreamsSizeEstimate -Streams @($selected.MainAudio, $selected.FallbackAudio, $selected.MainSub, $selected.SdhSub) -DurationSec $sourceDuration
    $copiedStreamsEstimate = Get-CopiedStreamsSizeEstimate -Streams @($selected.MainAudio, $selected.FallbackAudio, $selected.MainSub, $selected.SdhSub) -DurationSec $sourceDuration

    $tempOutput  = Get-TempOutputPath  -InputPath $InputPath
    $finalOutput = Get-FinalOutputPath -InputPath $InputPath
    $displayOutputName = [System.IO.Path]::GetFileName($finalOutput)
    $displayInputName = [System.IO.Path]::GetFileName($InputPath)
    $resolvedEncodeLane = Get-ResolvedEncodeLaneName -EncodeMode $EncodeMode
    if ([string]::IsNullOrWhiteSpace($LaneSelectionReason)) {
        $LaneSelectionReason = if ($resolvedEncodeLane -eq 'Nvidia') {
            'Encoder preference selected the Nvidia lane.'
        } else {
            'Encoder preference selected the CPU lane.'
        }
    }

    if ((Test-Path -LiteralPath $finalOutput) -and
        (-not [string]::Equals(
            (Get-NormalizedPath $finalOutput),
            (Get-NormalizedPath $InputPath),
            [System.StringComparison]::OrdinalIgnoreCase))) {
        throw "Final output path already exists and is not the source file: $finalOutput. Remove it manually before re-encoding."
    }

    if ($sourceProfile.HasDV -and $SkipDolbyVisionSources) {
        return [ordered]@{
            EarlyExit = 'SKIPPED_DV'
            Row = @{
                Timestamp         = (Get-Date).ToString("s")
                Status            = "SKIPPED_DV"
                InputPath         = $InputPath
                OutputPath        = ""
                SourceSizeGiB     = $sourceSizeGiB
                OutputSizeGiB     = ""
                ReductionPercent  = ""
                SourceDurationSec = $sourceDuration
                OutputDurationSec = ""
                ElapsedSec        = ""
                Profile           = $sourceProfile.Profile
                HasHDR            = $sourceProfile.HasHDR
                HasDV             = $sourceProfile.HasDV
                SelectedAudio     = $selectedAudioSummary
                SelectedSubtitles = $selectedSubtitleSummary
                CRF               = $CRF
                Preset            = $Preset
                FilmGrain         = $FilmGrain
                AutoCRFOffset     = $AutoCRFOffset
                EncoderPreference = $EncoderPreferenceValue
                ResolvedEncodeLane = $resolvedEncodeLane
                LaneSelectionReason = $LaneSelectionReason
                LaneSuitability  = $LaneSuitability
                CpuOnlyReason    = $CpuOnlyReason
                NvidiaFallbackAllowed = "$NvidiaFallbackAllowed"
                HeldForCpuLane   = "$HeldForCpuLane"
                WorkerProcessPriority = Get-WorkerProcessPriorityName -EncodeMode $EncodeMode
                ScriptProcessPriority = $script:ResolvedScriptProcessPriority
                EncodeMode        = $EncodeMode
                ResolvedCRF       = ""
                ResolvedPreset    = ""
                ResolvedFilmGrain = ""
                ResolvedCQ        = ""
                ResolvedNvencPreset = ""
                ResolvedNvencTune = ""
                ResolvedDecodePath = ""
                AutoReason        = ""
                BPP               = ""
                EffectiveVideoBitrate = ""
                VideoBitratePerHourGiB = ""
                ResolutionTier    = $sourceResolutionTier
                CodecClass        = $sourceCodecClass
                GrainClass        = ""
                GrainScore        = ""
                WasAutoSkipped    = "False"
                NvencWorkerCountAtStart = ""
                NvencEngineCountDetected = if ($NvencEnvironment) { $NvencEnvironment.NvencEngineCount } else { "" }
                NvencCapacitySource = if ($NvencEnvironment) { $NvencEnvironment.CapacitySource } else { "" }
                DetectedGpuName   = if ($NvencEnvironment) { $NvencEnvironment.GpuName } else { "" }
                FilmGrainDisabledReason = ""
                FfmpegPath        = $FfmpegPath
                FfprobePath       = $FfprobePath
                Notes             = "Dolby Vision source skipped by policy."
            }
        }
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

    if ($autoSettings.Skip) {
        return [ordered]@{
            EarlyExit = 'AUTO_SKIPPED_ALREADY_EFFICIENT'
            Row = @{
                Timestamp         = (Get-Date).ToString("s")
                Status            = "AUTO_SKIPPED_ALREADY_EFFICIENT"
                InputPath         = $InputPath
                OutputPath        = ""
                SourceSizeGiB     = $sourceSizeGiB
                OutputSizeGiB     = ""
                ReductionPercent  = ""
                SourceDurationSec = [Math]::Round($sourceDuration, 3)
                OutputDurationSec = ""
                ElapsedSec        = ""
                Profile           = $sourceProfile.Profile
                HasHDR            = $sourceProfile.HasHDR
                HasDV             = $sourceProfile.HasDV
                SelectedAudio     = $selectedAudioSummary
                SelectedSubtitles = $selectedSubtitleSummary
                CRF               = $CRF
                Preset            = $Preset
                FilmGrain         = $FilmGrain
                AutoCRFOffset     = $AutoCRFOffset
                EncoderPreference = $EncoderPreferenceValue
                ResolvedEncodeLane = $resolvedEncodeLane
                LaneSelectionReason = if ($LaneSelectionReason) { $LaneSelectionReason } else { $autoSettings.SkipReason }
                LaneSuitability  = $LaneSuitability
                CpuOnlyReason    = $CpuOnlyReason
                NvidiaFallbackAllowed = "$NvidiaFallbackAllowed"
                HeldForCpuLane   = "$HeldForCpuLane"
                WorkerProcessPriority = Get-WorkerProcessPriorityName -EncodeMode $EncodeMode
                ScriptProcessPriority = $script:ResolvedScriptProcessPriority
                EncodeMode        = $EncodeMode
                ResolvedCRF       = $autoSettings.CRF
                ResolvedPreset    = $autoSettings.Preset
                ResolvedFilmGrain = $autoSettings.FilmGrain
                ResolvedCQ        = ""
                ResolvedNvencPreset = ""
                ResolvedNvencTune = ""
                ResolvedDecodePath = ""
                AutoReason        = $autoSettings.SkipReason
                BPP               = [Math]::Round($autoSettings.BPP, 6)
                EffectiveVideoBitrate = $autoSettings.VideoBitrate
                VideoBitratePerHourGiB = [Math]::Round($autoSettings.VideoBitratePerHourGiB, 3)
                ResolutionTier    = $autoSettings.ResolutionTier
                CodecClass        = $autoSettings.CodecClass
                GrainClass        = $autoSettings.GrainClass
                GrainScore        = $autoSettings.GrainScore
                WasAutoSkipped    = "True"
                NvencWorkerCountAtStart = ""
                NvencEngineCountDetected = if ($NvencEnvironment) { $NvencEnvironment.NvencEngineCount } else { "" }
                NvencCapacitySource = if ($NvencEnvironment) { $NvencEnvironment.CapacitySource } else { "" }
                DetectedGpuName   = if ($NvencEnvironment) { $NvencEnvironment.GpuName } else { "" }
                FilmGrainDisabledReason = ""
                FfmpegPath        = $FfmpegPath
                FfprobePath       = $FfprobePath
                Notes             = $autoSettings.BitrateReason
            }
        }
    }

    $nvencSettings = $null
    $filmGrainDisabledReason = ''
    $effectiveFilmGrain = [int]$autoSettings.FilmGrain
    $preflightWorkflow = [pscustomobject][ordered]@{
        InitialResolvedCRF = [int]$autoSettings.CRF
        InitialResolvedPreset = [int]$autoSettings.Preset
        InitialResolvedFilmGrain = $effectiveFilmGrain
        FinalResolvedCRF = [int]$autoSettings.CRF
        FinalResolvedPreset = [int]$autoSettings.Preset
        FinalResolvedFilmGrain = $effectiveFilmGrain
        FinalNvencSettings = $nvencSettings
        PreflightPassCount = 0
        Preflight1 = $null
        Preflight2 = $null
        FinalPreflight = [pscustomobject][ordered]@{ Ran = $false; ShouldSkip = $false; Reason = '' }
        PreflightAutoTuneReason = ''
        WasPreflightRetuned = $false
        WasSkippedByPreflight = $false
        SkipStatus = ''
    }
    if ($EncodeMode -eq 'nvenc') {
        if (-not $NvencEnvironment) {
            throw "NVENC initialization requested without a detected NVENC environment."
        }

        $nvencSettings = Convert-SoftwareQualityToNvencSettings `
            -AutoSettings $autoSettings `
            -SourceProfile $sourceProfile `
            -ConfiguredNvencPreset $NvencPreset `
            -ConfiguredNvencCQ $NvencCQ `
            -ConfiguredNvencTune $NvencTune `
            -ConfiguredNvencDecode $NvencDecode `
            -NvencEnvironment $NvencEnvironment

        if ([int]$autoSettings.FilmGrain -gt 0) {
            $effectiveFilmGrain = 0
            $filmGrainDisabledReason = 'FFmpeg av1_nvenc does not expose AV1 film grain synthesis in this build; FilmGrain was forced to 0.'
        }
    }

    $preflightWorkflow = Invoke-PreflightAutoTuneWorkflow `
        -InputPath $InputPath `
        -Selected $selected `
        -SourceProfile $sourceProfile `
        -EncodeMode $EncodeMode `
        -SourceDurationSec $sourceDuration `
        -SourceSizeBytes $sourceItem.Length `
        -AutoSettings $autoSettings `
        -InitialResolvedCRF ([int]$autoSettings.CRF) `
        -InitialResolvedPreset ([int]$autoSettings.Preset) `
        -InitialResolvedFilmGrain $effectiveFilmGrain `
        -NvencEnvironment $NvencEnvironment

    $preflightEstimate = $preflightWorkflow.FinalPreflight
    $effectiveFilmGrain = [int]$preflightWorkflow.FinalResolvedFilmGrain
    if ($EncodeMode -eq 'nvenc' -and $preflightWorkflow.FinalNvencSettings) {
        $nvencSettings = $preflightWorkflow.FinalNvencSettings
    }

    if ($preflightEstimate.Ran) {
        Write-Host ("Preflight estimate: {0:F2} GiB (projected savings {1:F1}%)" -f $preflightEstimate.EstimatedFinalSizeGiB, $preflightEstimate.EstimatedSavingsPercent) -ForegroundColor DarkCyan
        Write-SessionTextLogMessage -Level Info -Message ("Preflight estimate | {0} | {1:F2} GiB | savings {2:F1}%" -f $displayInputName, $preflightEstimate.EstimatedFinalSizeGiB, $preflightEstimate.EstimatedSavingsPercent)
        if ($preflightWorkflow.WasSkippedByPreflight) {
            Write-Host "Decision: skipped (estimated output exceeds threshold)" -ForegroundColor Yellow
            Write-SessionTextLogMessage -Level Warn -Message ("Preflight decision | skipped | {0} | estimated output exceeds threshold" -f $displayInputName)
            return [ordered]@{
                EarlyExit = 'PRECHECK_SKIPPED_UNFAVORABLE'
                Row = @{
                    Timestamp         = (Get-Date).ToString("s")
                    Status            = "PRECHECK_SKIPPED_UNFAVORABLE"
                    InputPath         = $InputPath
                    OutputPath        = ""
                    SourceSizeGiB     = $sourceSizeGiB
                    OutputSizeGiB     = ""
                    ReductionPercent  = ""
                    SourceDurationSec = [Math]::Round($sourceDuration, 3)
                    OutputDurationSec = ""
                    ElapsedSec        = ""
                    Profile           = $sourceProfile.Profile
                    HasHDR            = $sourceProfile.HasHDR
                    HasDV             = $sourceProfile.HasDV
                    SelectedAudio     = $selectedAudioSummary
                    SelectedSubtitles = $selectedSubtitleSummary
                    EstimatedFinalSizeGiB = [Math]::Round($preflightEstimate.EstimatedFinalSizeGiB, 3)
                    EstimatedSavingsPercent = [Math]::Round($preflightEstimate.EstimatedSavingsPercent, 2)
                    EstimatedOutputGiBPerHour = [Math]::Round($preflightEstimate.EstimatedOutputGiBPerHour, 3)
                    InitialResolvedCRF = $preflightWorkflow.InitialResolvedCRF
                    InitialResolvedPreset = $preflightWorkflow.InitialResolvedPreset
                    InitialResolvedFilmGrain = $preflightWorkflow.InitialResolvedFilmGrain
                    PreflightPassCount = $preflightWorkflow.PreflightPassCount
                    Preflight1EstimatedFinalGiB = if ($preflightWorkflow.Preflight1 -and $preflightWorkflow.Preflight1.Ran) { [Math]::Round($preflightWorkflow.Preflight1.EstimatedFinalSizeGiB, 3) } else { "" }
                    Preflight1EstimatedSavingsPercent = if ($preflightWorkflow.Preflight1 -and $preflightWorkflow.Preflight1.Ran) { [Math]::Round($preflightWorkflow.Preflight1.EstimatedSavingsPercent, 2) } else { "" }
                    Preflight1EstimatedGiBPerHour = if ($preflightWorkflow.Preflight1 -and $preflightWorkflow.Preflight1.Ran) { [Math]::Round($preflightWorkflow.Preflight1.EstimatedOutputGiBPerHour, 3) } else { "" }
                    Preflight2EstimatedFinalGiB = if ($preflightWorkflow.Preflight2 -and $preflightWorkflow.Preflight2.Ran) { [Math]::Round($preflightWorkflow.Preflight2.EstimatedFinalSizeGiB, 3) } else { "" }
                    Preflight2EstimatedSavingsPercent = if ($preflightWorkflow.Preflight2 -and $preflightWorkflow.Preflight2.Ran) { [Math]::Round($preflightWorkflow.Preflight2.EstimatedSavingsPercent, 2) } else { "" }
                    Preflight2EstimatedGiBPerHour = if ($preflightWorkflow.Preflight2 -and $preflightWorkflow.Preflight2.Ran) { [Math]::Round($preflightWorkflow.Preflight2.EstimatedOutputGiBPerHour, 3) } else { "" }
                    FinalResolvedCRF = $preflightWorkflow.FinalResolvedCRF
                    FinalResolvedPreset = $preflightWorkflow.FinalResolvedPreset
                    FinalResolvedFilmGrain = $preflightWorkflow.FinalResolvedFilmGrain
                    PreflightAutoTuneReason = $preflightWorkflow.PreflightAutoTuneReason
                    WasPreflightRetuned = "$($preflightWorkflow.WasPreflightRetuned)"
                    WasSkippedByPreflight = 'True'
                    CRF               = $CRF
                    Preset            = $Preset
                    FilmGrain         = $FilmGrain
                    AutoCRFOffset     = $AutoCRFOffset
                    EncoderPreference = $EncoderPreferenceValue
                    ResolvedEncodeLane = $resolvedEncodeLane
                    LaneSelectionReason = $LaneSelectionReason
                    LaneSuitability  = $LaneSuitability
                    CpuOnlyReason    = $CpuOnlyReason
                    NvidiaFallbackAllowed = "$NvidiaFallbackAllowed"
                    HeldForCpuLane   = "$HeldForCpuLane"
                    WorkerProcessPriority = Get-WorkerProcessPriorityName -EncodeMode $EncodeMode
                    ScriptProcessPriority = $script:ResolvedScriptProcessPriority
                    EncodeMode        = $EncodeMode
                    ResolvedCRF       = $preflightWorkflow.FinalResolvedCRF
                    ResolvedPreset    = $preflightWorkflow.FinalResolvedPreset
                    ResolvedFilmGrain = $effectiveFilmGrain
                    ResolvedCQ        = if ($nvencSettings) { $nvencSettings.CQ } else { "" }
                    ResolvedNvencPreset = if ($nvencSettings) { $nvencSettings.Preset } else { "" }
                    ResolvedNvencTune = if ($nvencSettings) { $nvencSettings.Tune } else { "" }
                    ResolvedDecodePath = if ($nvencSettings) { $nvencSettings.DecodePath } else { "" }
                    AutoReason        = $preflightWorkflow.PreflightAutoTuneReason
                    BPP               = [Math]::Round($autoSettings.BPP, 6)
                    EffectiveVideoBitrate = $autoSettings.VideoBitrate
                    VideoBitratePerHourGiB = [Math]::Round($autoSettings.VideoBitratePerHourGiB, 3)
                    ResolutionTier    = $autoSettings.ResolutionTier
                    CodecClass        = $autoSettings.CodecClass
                    GrainClass        = $autoSettings.GrainClass
                    GrainScore        = $autoSettings.GrainScore
                    WasAutoSkipped    = "False"
                    NvencWorkerCountAtStart = ""
                    NvencEngineCountDetected = if ($NvencEnvironment) { $NvencEnvironment.NvencEngineCount } else { "" }
                    NvencCapacitySource = if ($NvencEnvironment) { $NvencEnvironment.CapacitySource } else { "" }
                    DetectedGpuName   = if ($NvencEnvironment) { $NvencEnvironment.GpuName } else { "" }
                    FilmGrainDisabledReason = $filmGrainDisabledReason
                    FfmpegPath        = $FfmpegPath
                    FfprobePath       = $FfprobePath
                    Notes             = $preflightWorkflow.PreflightAutoTuneReason
                }
            }
        }

        if ($preflightEstimate.WarningTriggered) {
            Write-Host ("Warning: projected output is {0:F1}% of source size." -f $preflightEstimate.EstimatedPctOfSource) -ForegroundColor Yellow
            Write-SessionTextLogMessage -Level Warn -Message ("Preflight warning | {0} | projected output is {1:F1}% of source size" -f $displayInputName, $preflightEstimate.EstimatedPctOfSource)
        }
        Write-Host "Proceeding with full encode" -ForegroundColor DarkCyan
        Write-SessionTextLogMessage -Level Info -Message ("Preflight decision | proceed | {0}" -f $displayInputName)
    } elseif ($EnablePreflightEstimate -and -not [string]::IsNullOrWhiteSpace($preflightEstimate.Reason)) {
        Write-Warning $preflightEstimate.Reason
        Write-SessionTextLogMessage -Level Warn -Message ("Preflight warning | {0} | {1}" -f $displayInputName, $preflightEstimate.Reason)
    }

    return [ordered]@{
        EarlyExit               = ''
        InputPath               = $InputPath
        SourceItem              = $sourceItem
        SourceSizeGiB           = $sourceSizeGiB
        SourceDurationSec       = $sourceDuration
        Probe                   = $probe
        Selected                = $selected
        SourceProfile           = $sourceProfile
        EncodeColorProfile      = $encodeColorProfile
        SelectedAudioSummary    = $selectedAudioSummary
        SelectedSubtitleSummary = $selectedSubtitleSummary
        CopiedStreamsEstimate   = $copiedStreamsEstimate
        SourceResolutionTier    = $sourceResolutionTier
        SourceCodecClass        = $sourceCodecClass
        AutoSettings            = $autoSettings
        NvencSettings           = $nvencSettings
        PreflightEstimate       = $preflightEstimate
        PreflightWorkflow       = $preflightWorkflow
        FilmGrainDisabledReason = $filmGrainDisabledReason
        EffectiveFilmGrain      = $effectiveFilmGrain
        TempOutput              = $tempOutput
        FinalOutput             = $finalOutput
        DisplayOutputName       = $displayOutputName
        DisplayInputName        = $displayInputName
        OutputDir               = $outputDir
        EncoderPreference       = $EncoderPreferenceValue
        ResolvedEncodeLane      = $resolvedEncodeLane
        LaneSelectionReason     = $LaneSelectionReason
        LaneSuitability         = $LaneSuitability
        CpuOnlyReason           = $CpuOnlyReason
        NvidiaFallbackAllowed   = $NvidiaFallbackAllowed
        HeldForCpuLane          = $HeldForCpuLane
        WorkerProcessPriority   = Get-WorkerProcessPriorityName -EncodeMode $EncodeMode
        ScriptProcessPriority   = $script:ResolvedScriptProcessPriority
        EncodeMode              = $EncodeMode
    }
}

function Resolve-EncoderLane {
    param(
        [string]$InputPath,
        [string]$EncoderPreferenceValue,
        [bool]$CpuLaneAvailable = $true,
        [bool]$NvidiaLaneAvailable = $false,
        $NvencEnvironment = $null
    )

    if ($EncoderPreferenceValue -eq 'CPU') {
        if (-not $CpuLaneAvailable) {
            return [pscustomobject][ordered]@{
                Ready  = $false
                Reason = 'CPU lane is currently busy.'
                Init   = $null
            }
        }

        return [pscustomobject][ordered]@{
            Ready  = $true
            Reason = 'Encoder preference forced the CPU lane.'
            Init   = (Get-EncodeInitialization -InputPath $InputPath -EncodeMode 'software' -EncoderPreferenceValue $EncoderPreferenceValue -LaneSelectionReason 'forced CPU lane by encoder preference')
        }
    }

    if ($EncoderPreferenceValue -eq 'Nvidia') {
        if (-not $NvencEnvironment) {
            throw "EncoderPreference='Nvidia' requires a usable NVIDIA AV1 NVENC environment."
        }
        if (-not $NvidiaLaneAvailable) {
            return [pscustomobject][ordered]@{
                Ready  = $false
                Reason = 'Nvidia lane is currently at capacity.'
                Init   = $null
            }
        }

        return [pscustomobject][ordered]@{
            Ready  = $true
            Reason = 'Encoder preference forced the Nvidia lane.'
            Init   = (Get-EncodeInitialization -InputPath $InputPath -EncodeMode 'nvenc' -NvencEnvironment $NvencEnvironment -EncoderPreferenceValue $EncoderPreferenceValue -LaneSelectionReason 'forced Nvidia lane by encoder preference')
        }
    }

    if (-not $NvencEnvironment) {
        if (-not $CpuLaneAvailable) {
            return [pscustomobject][ordered]@{
                Ready  = $false
                Reason = 'Nvidia lane is unavailable and the CPU lane is currently busy.'
                Init   = $null
            }
        }

        return [pscustomobject][ordered]@{
            Ready  = $true
            Reason = 'Nvidia lane unavailable; using CPU lane.'
            Init   = (Get-EncodeInitialization -InputPath $InputPath -EncodeMode 'software' -EncoderPreferenceValue $EncoderPreferenceValue -LaneSelectionReason 'Nvidia lane unavailable; using CPU lane')
        }
    }

    $probe = Invoke-FfprobeJson -InputPath $InputPath
    $selected = Select-Streams -Probe $probe
    $sourceProfile = Get-SourceProfile -Probe $probe -VideoStream $selected.Video
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
    $laneSuitability = Get-EncoderLaneSuitability -SourceProfile $sourceProfile -AutoSettings $autoSettings
    $hint = [pscustomobject][ordered]@{
        Lane   = $laneSuitability.PreferredLane
        Reason = $laneSuitability.Reason
    }
    $preferredLane = $hint.Lane
    $preferredLaneAvailable = if ($preferredLane -eq 'Nvidia') { $NvidiaLaneAvailable } else { $CpuLaneAvailable }
    $alternateLane = if ($preferredLane -eq 'Nvidia') { 'CPU' } else { 'Nvidia' }
    $alternateLaneAvailable = if ($alternateLane -eq 'Nvidia') { $NvidiaLaneAvailable } else { $CpuLaneAvailable }

    if ($laneSuitability.Suitability -eq 'CpuOnly' -and -not $CpuLaneAvailable) {
        return [pscustomobject][ordered]@{
            Ready                 = $false
            Reason                = "Queued for CPU: $($laneSuitability.CpuOnlyReason). NVENC not recommended for this source."
            Init                  = $null
            HeldForCpuLane        = $true
            LaneSuitability       = $laneSuitability.Suitability
            CpuOnlyReason         = $laneSuitability.CpuOnlyReason
            NvidiaFallbackAllowed = $laneSuitability.NvidiaFallbackAllowed
        }
    }

    if (-not $preferredLaneAvailable) {
        if (-not $alternateLaneAvailable) {
            return [pscustomobject][ordered]@{
                Ready                 = $false
                Reason                = "$preferredLane lane preferred but not currently available."
                Init                  = $null
                HeldForCpuLane        = $false
                LaneSuitability       = $laneSuitability.Suitability
                CpuOnlyReason         = $laneSuitability.CpuOnlyReason
                NvidiaFallbackAllowed = $laneSuitability.NvidiaFallbackAllowed
            }
        }

        if ($alternateLane -eq 'Nvidia' -and -not $laneSuitability.NvidiaFallbackAllowed) {
            return [pscustomobject][ordered]@{
                Ready                 = $false
                Reason                = "CPU-only decision: Nvidia fallback disabled for this file. $($laneSuitability.CpuOnlyReason)."
                Init                  = $null
                HeldForCpuLane        = $true
                LaneSuitability       = $laneSuitability.Suitability
                CpuOnlyReason         = $laneSuitability.CpuOnlyReason
                NvidiaFallbackAllowed = $laneSuitability.NvidiaFallbackAllowed
            }
        }

        $alternateMode = if ($alternateLane -eq 'Nvidia') { 'nvenc' } else { 'software' }
        $alternateReason = "$($hint.Reason); preferred $preferredLane lane busy, evaluating $alternateLane lane to keep workers active"
        $alternateInit = Get-EncodeInitialization `
            -InputPath $InputPath `
            -EncodeMode $alternateMode `
            -NvencEnvironment $NvencEnvironment `
            -EncoderPreferenceValue $EncoderPreferenceValue `
            -LaneSelectionReason $alternateReason `
            -LaneSuitability $laneSuitability.Suitability `
            -CpuOnlyReason $laneSuitability.CpuOnlyReason `
            -NvidiaFallbackAllowed $laneSuitability.NvidiaFallbackAllowed

        if ($alternateLane -eq 'Nvidia') {
            $nvencFallback = Test-NvencFallbackSuitable -LaneSuitability $laneSuitability -Init $alternateInit
            if (-not $nvencFallback.Allowed) {
                return [pscustomobject][ordered]@{
                    Ready                 = $false
                    Reason                = "Queued for CPU: $($nvencFallback.Reason)"
                    Init                  = $null
                    HeldForCpuLane        = $true
                    LaneSuitability       = $laneSuitability.Suitability
                    CpuOnlyReason         = if ($laneSuitability.CpuOnlyReason) { $laneSuitability.CpuOnlyReason } else { $nvencFallback.Reason }
                    NvidiaFallbackAllowed = $false
                }
            }
        }

        if ($alternateInit.EarlyExit -eq 'PRECHECK_SKIPPED_UNFAVORABLE') {
            return [pscustomobject][ordered]@{
                Ready                 = $false
                Reason                = "$alternateReason; alternate $alternateLane lane preflight was unfavorable, waiting for preferred $preferredLane lane"
                Init                  = $null
                HeldForCpuLane        = ($preferredLane -eq 'CPU')
                LaneSuitability       = $laneSuitability.Suitability
                CpuOnlyReason         = $laneSuitability.CpuOnlyReason
                NvidiaFallbackAllowed = $laneSuitability.NvidiaFallbackAllowed
            }
        }

        return [pscustomobject][ordered]@{
            Ready                 = $true
            Reason                = $alternateReason
            Init                  = $alternateInit
            HeldForCpuLane        = $false
            LaneSuitability       = $laneSuitability.Suitability
            CpuOnlyReason         = $laneSuitability.CpuOnlyReason
            NvidiaFallbackAllowed = $laneSuitability.NvidiaFallbackAllowed
        }
    }

    $firstMode = if ($preferredLane -eq 'Nvidia') { 'nvenc' } else { 'software' }
    $firstInit = Get-EncodeInitialization `
        -InputPath $InputPath `
        -EncodeMode $firstMode `
        -NvencEnvironment $NvencEnvironment `
        -EncoderPreferenceValue $EncoderPreferenceValue `
        -LaneSelectionReason $hint.Reason `
        -LaneSuitability $laneSuitability.Suitability `
        -CpuOnlyReason $laneSuitability.CpuOnlyReason `
        -NvidiaFallbackAllowed $laneSuitability.NvidiaFallbackAllowed

    if ($firstInit.EarlyExit -ne 'PRECHECK_SKIPPED_UNFAVORABLE') {
        return [pscustomobject][ordered]@{
            Ready                 = $true
            Reason                = $hint.Reason
            Init                  = $firstInit
            HeldForCpuLane        = $false
            LaneSuitability       = $laneSuitability.Suitability
            CpuOnlyReason         = $laneSuitability.CpuOnlyReason
            NvidiaFallbackAllowed = $laneSuitability.NvidiaFallbackAllowed
        }
    }

    if (-not $alternateLaneAvailable) {
        $holdReason = "$($hint.Reason); alternate $alternateLane lane unavailable after unfavorable preflight."
        if ($alternateLane -eq 'CPU') {
            return [pscustomobject][ordered]@{
                Ready                 = $false
                Reason                = "Queued for CPU: NVENC not recommended for this source. $holdReason"
                Init                  = $null
                HeldForCpuLane        = $true
                LaneSuitability       = $laneSuitability.Suitability
                CpuOnlyReason         = $laneSuitability.CpuOnlyReason
                NvidiaFallbackAllowed = $laneSuitability.NvidiaFallbackAllowed
            }
        }

        $firstInit.Row.LaneSelectionReason = $holdReason
        return [pscustomobject][ordered]@{
            Ready                 = $true
            Reason                = $firstInit.Row.LaneSelectionReason
            Init                  = $firstInit
            HeldForCpuLane        = $false
            LaneSuitability       = $laneSuitability.Suitability
            CpuOnlyReason         = $laneSuitability.CpuOnlyReason
            NvidiaFallbackAllowed = $laneSuitability.NvidiaFallbackAllowed
        }
    }

    $alternateMode = if ($alternateLane -eq 'Nvidia') { 'nvenc' } else { 'software' }
    $alternateReason = "preferred $preferredLane lane preflight was unfavorable; trying $alternateLane lane"
    $alternateInit = Get-EncodeInitialization `
        -InputPath $InputPath `
        -EncodeMode $alternateMode `
        -NvencEnvironment $NvencEnvironment `
        -EncoderPreferenceValue $EncoderPreferenceValue `
        -LaneSelectionReason $alternateReason `
        -LaneSuitability $laneSuitability.Suitability `
        -CpuOnlyReason $laneSuitability.CpuOnlyReason `
        -NvidiaFallbackAllowed $laneSuitability.NvidiaFallbackAllowed

    if ($alternateLane -eq 'Nvidia') {
        $nvencFallback = Test-NvencFallbackSuitable -LaneSuitability $laneSuitability -Init $alternateInit
        if (-not $nvencFallback.Allowed) {
            return [pscustomobject][ordered]@{
                Ready                 = $false
                Reason                = "Queued for CPU: $($nvencFallback.Reason)"
                Init                  = $null
                HeldForCpuLane        = $true
                LaneSuitability       = $laneSuitability.Suitability
                CpuOnlyReason         = if ($laneSuitability.CpuOnlyReason) { $laneSuitability.CpuOnlyReason } else { $nvencFallback.Reason }
                NvidiaFallbackAllowed = $false
            }
        }
    }

    if ($alternateInit.EarlyExit -eq 'PRECHECK_SKIPPED_UNFAVORABLE') {
        if ($alternateLane -eq 'Nvidia') {
            return [pscustomobject][ordered]@{
                Ready                 = $false
                Reason                = "Queued for CPU: NVENC not recommended for this source. $alternateReason; Nvidia preflight was unfavorable."
                Init                  = $null
                HeldForCpuLane        = $true
                LaneSuitability       = $laneSuitability.Suitability
                CpuOnlyReason         = $laneSuitability.CpuOnlyReason
                NvidiaFallbackAllowed = $laneSuitability.NvidiaFallbackAllowed
            }
        }

        $alternateInit.Row.LaneSelectionReason = "$alternateReason; both lanes were unfavorable"
    }

    return [pscustomobject][ordered]@{
        Ready                 = $true
        Reason                = $alternateInit.Row.LaneSelectionReason
        Init                  = $alternateInit
        HeldForCpuLane        = $false
        LaneSuitability       = $laneSuitability.Suitability
        CpuOnlyReason         = $laneSuitability.CpuOnlyReason
        NvidiaFallbackAllowed = $laneSuitability.NvidiaFallbackAllowed
    }
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
        [int64]  $SourceSizeBytes = 0,
        [double] $ElapsedSec,
        [double] $OutTimeSec   = 0,
        [double] $OutSizeBytes = 0,
        [double] $SpeedX       = 0,
        $EstimateState = $null,
        [int]    $UICursorRow  = -1
    )

    # ── Geometry ──────────────────────────────────────────────────────────────
    # Keep a small right margin so Windows console hosts do not soft-wrap
    # full-width box lines onto the next row during repaint.
    $conW  = [Math]::Max(60, $Host.UI.RawUI.WindowSize.Width - 4)
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
    $estimate = if ($EstimateState) {
        Update-LiveEstimateState -State $EstimateState -SourceDurationSec $SourceDurationSec -SourceSizeBytes $SourceSizeBytes
    } else {
        $null
    }

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
        $visible = Remove-AnsiDisplayFormatting $content
        $safe = if ($visible.Length -gt $inner) {
            Limit-String -Value $visible -MaxWidth $inner
        } else {
            $content
        }
        $pad  = " " * [Math]::Max(0, $inner - (Remove-AnsiDisplayFormatting $safe).Length)
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
    $colorSummaryLine = Add-RainbowHdrHighlights -Text "$Profile  |  $EncodeColorLabel" -BaseColor $cColor
    $lines.Add((Row $colorSummaryLine $cColor))
    $lines.Add((Row "CRF $CRFLabel  |  Preset $PresetLabel  |  $elapsStr elapsed" $cMeta))
    $lines.Add($barRow)
    $lines.Add((Row "Encoded: $sizeStr   Speed: $speedStr   ETA: $eta" $cStats))
    if ($EnableLiveSizeEstimate) {
        $lines.Add((Row (Get-LiveEstimateSummaryText -Estimate $estimate) $cStats))
    }

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
        $null = $sb.Append("${ESC}[${UICursorRow}A")   # move cursor up by the previously rendered frame height
        $null = $sb.Append("`r")
    }

    foreach ($l in $lines) {
        $null = $sb.Append("`r")
        $null = $sb.Append($l)
        $null = $sb.Append("${ESC}[K")   # erase to end of line (handles terminal resize)
        $null = $sb.Append("`r`n")
    }

    $staleLineCount = [Math]::Max(0, $UICursorRow - $lineCount)
    for ($i = 0; $i -lt $staleLineCount; $i++) {
        $null = $sb.Append("`r")
        $null = $sb.Append("${ESC}[K")
        $null = $sb.Append("`r`n")
    }
    if ($staleLineCount -gt 0) {
        $null = $sb.Append("${ESC}[${staleLineCount}A")
        $null = $sb.Append("`r")
    }

    [Console]::Write($sb.ToString())
    return $lineCount
}

function Start-TrackedFfmpegProcess {
    param(
        [string[]]$Arguments,
        [string]$PriorityName = 'Normal'
    )

    $psi                       = [System.Diagnostics.ProcessStartInfo]::new()
    $psi.FileName              = $FfmpegPath
    $psi.RedirectStandardError = $true
    $psi.RedirectStandardInput = $true
    $psi.UseShellExecute       = $false
    $psi.CreateNoWindow        = $false

    foreach ($a in $Arguments) { $psi.ArgumentList.Add($a) }

    $proc           = [System.Diagnostics.Process]::new()
    $proc.StartInfo = $psi

    $shared = [hashtable]::Synchronized(@{
        OutTimeSec                  = 0.0
        OutSizeBytes                = 0.0
        SpeedX                      = 0.0
        LogLines                    = [System.Collections.Generic.List[string]]::new()
        SmoothedEstimatedFinalBytes = 0.0
        LastRawEstimatedFinalBytes  = 0.0
        EstimatedSavingsPercent     = 0.0
        EstimatedOutputGiBPerHour   = 0.0
        EstimateReady               = $false
    })

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
    $priorityResolution = Set-TrackedProcessPriority -Process $proc -PriorityName $PriorityName
    if ($priorityResolution.Warning) {
        Write-Warning $priorityResolution.Warning
    }
    $stderrAsync = $stderrPs.BeginInvoke()

    return [pscustomobject][ordered]@{
        Process        = $proc
        Shared         = $shared
        StderrRunspace = $stderrRunspace
        StderrPs       = $stderrPs
        StderrAsync    = $stderrAsync
        WorkerProcessPriority = $priorityResolution.AppliedPriority
        WorkerPriorityReason  = $priorityResolution.Reason
    }
}

function Stop-TrackedFfmpegProcess {
    param($TrackedProcess)

    try {
        $TrackedProcess.Process.WaitForExit()
    } catch {}

    try {
        $null = $TrackedProcess.StderrPs.EndInvoke($TrackedProcess.StderrAsync)
    } catch {}

    try { $TrackedProcess.StderrPs.Dispose() } catch {}
    try { $TrackedProcess.StderrRunspace.Close() } catch {}
    try { $TrackedProcess.StderrRunspace.Dispose() } catch {}
    try { $TrackedProcess.Process.Dispose() } catch {}
}

function Build-SoftwareFfmpegArgs {
    param($Init)

    $selected = $Init.Selected
    $sourceProfile = $Init.SourceProfile
    $encodeColorProfile = $Init.EncodeColorProfile

    $ffArgs = New-Object System.Collections.Generic.List[string]
    $ffArgs.AddRange([string[]]@(
        '-hide_banner',
        '-y',
        '-i', $Init.InputPath,
        '-map', "0:$($selected.Video.index)",
        '-map', "0:$($selected.MainAudio.index)"
    ))

    if ($selected.FallbackAudio) { $ffArgs.AddRange([string[]]@('-map', "0:$($selected.FallbackAudio.index)")) }
    if ($selected.MainSub)       { $ffArgs.AddRange([string[]]@('-map', "0:$($selected.MainSub.index)")) }
    if ($selected.SdhSub)        { $ffArgs.AddRange([string[]]@('-map', "0:$($selected.SdhSub.index)")) }

    $ffArgs.AddRange([string[]]@(
        '-map_chapters', '0',
        '-map_metadata', '-1',
        '-max_muxing_queue_size', '4096',
        '-c:v', 'libsvtav1',
        '-preset', "$($Init.PreflightWorkflow.FinalResolvedPreset)",
        '-crf', "$($Init.PreflightWorkflow.FinalResolvedCRF)",
        '-pix_fmt', 'yuv420p10le'
    ))

    if ([int]$Init.EffectiveFilmGrain -gt 0) {
        $ffArgs.AddRange([string[]]@('-svtav1-params', "film-grain=$($Init.EffectiveFilmGrain)`:film-grain-denoise=0"))
    }

    if ($sourceProfile.HasHDR) {
        $ffArgs.AddRange([string[]]@(
            '-color_primaries', 'bt2020',
            '-color_trc', 'smpte2084',
            '-colorspace', 'bt2020nc'
        ))
    }

    $ffArgs.AddRange([string[]]@('-c:a', 'copy'))
    if ($selected.MainSub -or $selected.SdhSub) { $ffArgs.AddRange([string[]]@('-c:s', 'copy')) }

    $ffArgs.AddRange([string[]]@(
        '-disposition:v:0', 'default',
        '-disposition:a:0', 'default'
    ))
    if ($selected.FallbackAudio) { $ffArgs.AddRange([string[]]@('-disposition:a:1', '0')) }
    if ($selected.MainSub)       { $ffArgs.AddRange([string[]]@('-disposition:s:0', 'default')) }
    if ($selected.SdhSub) {
        $subIndex = if ($selected.MainSub) { 1 } else { 0 }
        $ffArgs.AddRange([string[]]@("-disposition:s:$subIndex", '0'))
    }

    $baseTitle = [System.IO.Path]::GetFileNameWithoutExtension($Init.InputPath)
    $videoTitle = "AV1 $($encodeColorProfile.DynamicRangeLabel) $($encodeColorProfile.BitDepth)-bit"
    $ffArgs.AddRange([string[]]@(
        '-metadata', "title=$baseTitle",
        '-metadata:s:v:0', "title=$videoTitle",
        '-metadata:s:a:0', "title=$(Get-StreamTitle $selected.MainAudio)"
    ))
    if ($selected.FallbackAudio) {
        $ffArgs.AddRange([string[]]@('-metadata:s:a:1', "title=$(Get-StreamTitle $selected.FallbackAudio)"))
    }
    if ($selected.MainSub) {
        $ffArgs.AddRange([string[]]@('-metadata:s:s:0', "title=$(Get-StreamTitle $selected.MainSub)"))
    }
    if ($selected.SdhSub) {
        $subIndex = if ($selected.MainSub) { 1 } else { 0 }
        $ffArgs.AddRange([string[]]@("-metadata:s:s:$subIndex", "title=$(Get-StreamTitle $selected.SdhSub)"))
    }

    $ffArgs.AddRange([string[]]@('-progress', 'pipe:2', '-stats_period', '2'))
    $ffArgs.Add($Init.TempOutput)

    return ,$ffArgs
}

function Build-NvencFfmpegArgs {
    param(
        $Init,
        $NvencEnvironment
    )

    $selected = $Init.Selected
    $sourceProfile = $Init.SourceProfile
    $encodeColorProfile = $Init.EncodeColorProfile
    $nvencSettings = $Init.NvencSettings

    $ffArgs = New-Object System.Collections.Generic.List[string]
    $ffArgs.Add('-hide_banner')
    $ffArgs.Add('-y')

    if ($nvencSettings.DecodePath -eq 'cuda') {
        $ffArgs.AddRange([string[]]@('-hwaccel', 'cuda', '-hwaccel_output_format', 'cuda'))
    }

    $ffArgs.AddRange([string[]]@(
        '-i', $Init.InputPath,
        '-map', "0:$($selected.Video.index)",
        '-map', "0:$($selected.MainAudio.index)"
    ))

    if ($selected.FallbackAudio) { $ffArgs.AddRange([string[]]@('-map', "0:$($selected.FallbackAudio.index)")) }
    if ($selected.MainSub)       { $ffArgs.AddRange([string[]]@('-map', "0:$($selected.MainSub.index)")) }
    if ($selected.SdhSub)        { $ffArgs.AddRange([string[]]@('-map', "0:$($selected.SdhSub.index)")) }

    $ffArgs.AddRange([string[]]@(
        '-map_chapters', '0',
        '-map_metadata', '-1',
        '-max_muxing_queue_size', '4096',
        '-c:v', 'av1_nvenc'
    ))

    if ($NvencEnvironment.SupportsPreset -and -not [string]::IsNullOrWhiteSpace($nvencSettings.Preset)) {
        $ffArgs.AddRange([string[]]@('-preset', $nvencSettings.Preset))
    }

    if ($NvencEnvironment.SupportsTune -and -not [string]::IsNullOrWhiteSpace($nvencSettings.Tune)) {
        $ffArgs.AddRange([string[]]@('-tune', $nvencSettings.Tune))
    }

    if ($NvencEnvironment.SupportsRc) {
        $ffArgs.AddRange([string[]]@('-rc', 'vbr'))
    }

    if ($NvencEnvironment.SupportsCQ) {
        $ffArgs.AddRange([string[]]@('-cq', "$($nvencSettings.CQ)"))
    }

    if ($NvencEnvironment.SupportsLookahead) {
        $ffArgs.AddRange([string[]]@('-rc-lookahead', '32'))
    }

    if ($NvencEnvironment.SupportsSpatialAQ)  { $ffArgs.AddRange([string[]]@('-spatial-aq', '1')) }
    if ($NvencEnvironment.SupportsTemporalAQ) { $ffArgs.AddRange([string[]]@('-temporal-aq', '1')) }
    if ($NvencEnvironment.SupportsAQStrength) { $ffArgs.AddRange([string[]]@('-aq-strength', '8')) }
    if ($NvencEnvironment.SupportsBRefMode)   { $ffArgs.AddRange([string[]]@('-b_ref_mode', 'middle')) }
    if ($NvencEnvironment.SupportsMultipass)  { $ffArgs.AddRange([string[]]@('-multipass', 'fullres')) }

    if ($NvencEnvironment.SupportsSplitEncode -and -not $NvencAllowSplitFrame) {
        $ffArgs.AddRange([string[]]@('-split_encode_mode', 'disabled'))
    }

    $ffArgs.AddRange([string[]]@('-pix_fmt', $nvencSettings.PixFmt))
    if ($NvencEnvironment.SupportsHighBitDepth -and $nvencSettings.BitDepth -ge 10) {
        $ffArgs.AddRange([string[]]@('-highbitdepth', '1'))
    }

    $primaries = if (-not [string]::IsNullOrWhiteSpace($sourceProfile.SourcePrimaries)) { $sourceProfile.SourcePrimaries } elseif ($sourceProfile.HasHDR) { 'bt2020' } else { 'bt709' }
    $transfer  = if (-not [string]::IsNullOrWhiteSpace($sourceProfile.SourceTransfer))  { $sourceProfile.SourceTransfer  } elseif ($sourceProfile.HasHDR) { 'smpte2084' } else { 'bt709' }
    $matrix    = if (-not [string]::IsNullOrWhiteSpace($sourceProfile.SourceMatrix))    { $sourceProfile.SourceMatrix    } elseif ($sourceProfile.HasHDR) { 'bt2020nc' } else { 'bt709' }

    $ffArgs.AddRange([string[]]@(
        '-color_primaries', $primaries,
        '-color_trc',       $transfer,
        '-colorspace',      $matrix
    ))

    $ffArgs.AddRange([string[]]@('-c:a', 'copy'))
    if ($selected.MainSub -or $selected.SdhSub) { $ffArgs.AddRange([string[]]@('-c:s', 'copy')) }

    $ffArgs.AddRange([string[]]@(
        '-disposition:v:0', 'default',
        '-disposition:a:0', 'default'
    ))

    if ($selected.FallbackAudio) { $ffArgs.AddRange([string[]]@('-disposition:a:1', '0')) }
    if ($selected.MainSub)       { $ffArgs.AddRange([string[]]@('-disposition:s:0', 'default')) }
    if ($selected.SdhSub) {
        $subIndex = if ($selected.MainSub) { 1 } else { 0 }
        $ffArgs.AddRange([string[]]@("-disposition:s:$subIndex", '0'))
    }

    $baseTitle  = [System.IO.Path]::GetFileNameWithoutExtension($Init.FinalOutput)
    $videoTitle = "AV1 NVENC $($encodeColorProfile.DynamicRangeLabel) $($encodeColorProfile.BitDepth)-bit"
    $ffArgs.AddRange([string[]]@(
        '-metadata',       "title=$baseTitle",
        '-metadata:s:v:0', "title=$videoTitle",
        '-metadata:s:a:0', "title=$(Get-StreamTitle $selected.MainAudio)"
    ))

    if ($selected.FallbackAudio) {
        $ffArgs.AddRange([string[]]@('-metadata:s:a:1', "title=$(Get-StreamTitle $selected.FallbackAudio)"))
    }
    if ($selected.MainSub) {
        $ffArgs.AddRange([string[]]@('-metadata:s:s:0', "title=$(Get-StreamTitle $selected.MainSub)"))
    }
    if ($selected.SdhSub) {
        $subIndex = if ($selected.MainSub) { 1 } else { 0 }
        $ffArgs.AddRange([string[]]@("-metadata:s:s:$subIndex", "title=$(Get-StreamTitle $selected.SdhSub)"))
    }

    $ffArgs.AddRange([string[]]@('-progress', 'pipe:2', '-stats_period', '2'))
    $ffArgs.Add($Init.TempOutput)

    return ,$ffArgs
}

function Start-LaneWorker {
    param(
        $Init,
        $NvencEnvironment,
        [int]$SlotNumber
    )

    if (Test-Path -LiteralPath $Init.TempOutput) {
        Remove-Item -LiteralPath $Init.TempOutput -Force -ErrorAction SilentlyContinue
    }

    $ffArgs = if ($Init.ResolvedEncodeLane -eq 'Nvidia') {
        Build-NvencFfmpegArgs -Init $Init -NvencEnvironment $NvencEnvironment
    } else {
        Build-SoftwareFfmpegArgs -Init $Init
    }

    $tracked = Start-TrackedFfmpegProcess -Arguments $ffArgs -PriorityName $Init.WorkerProcessPriority
    $Init.WorkerProcessPriority = $tracked.WorkerProcessPriority
    Write-SessionEncodeStart -Init $Init
    return [pscustomobject][ordered]@{
        SlotNumber              = $SlotNumber
        WorkingJobPath          = $null
        Init                    = $Init
        TrackedProcess          = $tracked
        Stopwatch               = [System.Diagnostics.Stopwatch]::StartNew()
        NvencWorkerCountAtStart = if ($Init.ResolvedEncodeLane -eq 'Nvidia' -and $NvencEnvironment) { $NvencEnvironment.MaxParallel } else { '' }
        WorkerProcessPriority   = $tracked.WorkerProcessPriority
        WorkerState             = 'Running'
        ShutdownRequestedAt     = $null
        ManualStopRequested     = $false
        PendingResumeRequested  = $false
        HeldInputPath           = ''
        HeldEncodeMode          = ''
        HeldRestartReason       = ''
    }
}

function Write-LaneProgressUI {
    param(
        [object[]]$Workers,
        $Summary,
        $NvencEnvironment,
        [int]$UICursorRow = -1
    )

    $conW  = [Math]::Max(70, $Host.UI.RawUI.WindowSize.Width - 4)
    $inner = $conW - 4

    $ESC      = [char]27
    $reset    = "${ESC}[0m"
    $cBorder  = "${ESC}[38;5;240m"
    $cTitle   = "${ESC}[1;97m"
    $cFile    = "${ESC}[1;96m"
    $cHdr     = "${ESC}[1;93m"
    $cSdr     = "${ESC}[38;5;117m"
    $cMeta    = "${ESC}[38;5;250m"
    $cBarDone = "${ESC}[38;5;76m"
    $cBarTodo = "${ESC}[38;5;238m"
    $cPct     = "${ESC}[1;92m"
    $cQueue   = "${ESC}[38;5;245m"

    $TL = [char]0x2554
    $TR = [char]0x2557
    $BL = [char]0x255A
    $BR = [char]0x255D
    $HL = [char]0x2550
    $VL = [char]0x2551
    $LM = [char]0x2560
    $RM = [char]0x2563

    function Row ([string]$content, [string]$color = "") {
        $visible = Remove-AnsiDisplayFormatting $content
        $safe = if ($visible.Length -gt $inner) {
            Limit-String -Value $visible -MaxWidth $inner
        } else {
            $content
        }
        $pad  = " " * [Math]::Max(0, $inner - (Remove-AnsiDisplayFormatting $safe).Length)
        "${cBorder}${VL} ${reset}${color}${safe}${reset}${pad} ${cBorder}${VL}${reset}"
    }

    function DivRow ([string]$label) {
        $mid   = " $label "
        $left  = [int][Math]::Floor(($conW - 2 - $mid.Length) / 2)
        $right = $conW - 2 - $left - $mid.Length
        "${cBorder}${LM}$([string]$HL * $left)${cTitle}${mid}${reset}${cBorder}$([string]$HL * $right)${RM}${reset}"
    }

    $titleLabel = " Encoder Lanes "
    $tLeft      = [int][Math]::Floor(($conW - 2 - $titleLabel.Length) / 2)
    $tRight     = $conW - 2 - $tLeft - $titleLabel.Length
    $topBorder  = "${cBorder}${TL}$([string]$HL * $tLeft)${cTitle}${titleLabel}${reset}${cBorder}$([string]$HL * $tRight)${TR}${reset}"
    $botBorder  = "${cBorder}${BL}$([string]$HL * ($conW - 2))${BR}${reset}"

    $lines = [System.Collections.Generic.List[string]]::new()
    $lines.Add($topBorder)
    $lines.Add((Row "Encoder preference: $($Summary.EncoderPreference)  |  CPU active: $($Summary.CpuActive)/1  |  Nvidia active: $($Summary.NvidiaActive)/$($Summary.NvidiaCapacity)" $cMeta))
    $lines.Add((Row (Get-QueueControlStateText) $cMeta))
    if ($NvencEnvironment) {
        $lines.Add((Row "GPU: $($NvencEnvironment.GpuName)  |  NVENC engines: $($NvencEnvironment.NvencEngineCount)  |  Nvidia capacity: $($NvencEnvironment.MaxParallel) ($($NvencEnvironment.CapacitySource))" $cMeta))
    } else {
        $lines.Add((Row 'GPU: unavailable  |  Nvidia lane disabled for this session' $cMeta))
    }
    $lines.Add((Row "Pending: $($Summary.Pending)  |  Active: $($Summary.Active)  |  Completed: $($Summary.Completed)  |  Skipped: $($Summary.Skipped)  |  Failed: $($Summary.Failed)" $cQueue))

    foreach ($worker in @($Workers | Sort-Object SlotNumber)) {
        $workerState = Get-WorkerStateLabel -Worker $worker
        $shared = if ($worker.TrackedProcess) { $worker.TrackedProcess.Shared } else { [pscustomobject]@{ OutTimeSec = 0.0; OutSizeBytes = 0.0; SpeedX = 0.0 } }
        $workerPriority = Get-OptionalProperty -InputObject $worker -PropertyName 'WorkerProcessPriority' -Default $worker.Init.WorkerProcessPriority
        $estimate = if ($worker.TrackedProcess) { Update-LiveEstimateState -State $shared -SourceDurationSec $worker.Init.SourceDurationSec -SourceSizeBytes $worker.Init.SourceItem.Length } else { $null }
        $pct = if ($worker.Init.SourceDurationSec -gt 0) {
            [Math]::Min(100.0, ($shared.OutTimeSec / $worker.Init.SourceDurationSec) * 100.0)
        } else { 0.0 }
        $eta = if ($shared.SpeedX -gt 0.001 -and $worker.Init.SourceDurationSec -gt 0) {
            Format-Duration -Seconds (($worker.Init.SourceDurationSec - $shared.OutTimeSec) / $shared.SpeedX)
        } else { '--' }
        $sizeStr = if ($shared.OutSizeBytes -gt 0) { "{0:F2} GiB" -f ($shared.OutSizeBytes / 1GB) } else { "---" }
        $speedStr = if ($shared.SpeedX -gt 0.001) { "{0:F2}x" -f $shared.SpeedX } else { '---' }
        $color = if ($worker.Init.SourceProfile.Profile -eq 'HDR') { $cHdr } else { $cSdr }
        $barInner = [Math]::Max(20, $inner - 16)
        $filled = [int][Math]::Round($barInner * $pct / 100.0)
        $empty = $barInner - $filled
        $pctLabel = ("{0,5:F1}%" -f $pct)
        $bar = "[${cBarDone}$([string][char]0x2588 * $filled)${reset}${cBarTodo}$([string][char]0x2591 * $empty)${reset}] ${cPct}$pctLabel${reset}"
        $laneLabel = if ($worker.Init.ResolvedEncodeLane -eq 'Nvidia') {
            "Lane Nvidia  |  Mode NVENC  |  $($worker.Init.SourceProfile.Profile)  |  CQ $($worker.Init.NvencSettings.CQ)  |  Preset $($worker.Init.NvencSettings.Preset)"
        } else {
            "Lane CPU  |  Mode SVT-AV1  |  $($worker.Init.SourceProfile.Profile)  |  CRF $($worker.Init.PreflightWorkflow.FinalResolvedCRF)  |  Preset $($worker.Init.PreflightWorkflow.FinalResolvedPreset)  |  FilmGrain $($worker.Init.EffectiveFilmGrain)"
        }
        if ($worker.Init.ResolvedEncodeLane -eq 'Nvidia') {
            $laneLabel += "  |  Tune $($worker.Init.NvencSettings.TuneDisplay)  |  Decode $($worker.Init.NvencSettings.DecodePath)"
        }
        $laneLabel += "  |  State $workerState  |  Priority $workerPriority"
        $laneLabel = Add-RainbowHdrHighlights -Text $laneLabel -BaseColor $color
        $colorLine = Add-RainbowHdrHighlights -Text ("Color  |  Source {0}  ->  Output {1}" -f $worker.Init.SourceProfile.SourceColorSummary, $worker.Init.EncodeColorProfile.Summary) -BaseColor $color

        $lines.Add((DivRow "Worker $($worker.SlotNumber)"))
        $lines.Add((Row "$($worker.Init.DisplayInputName)  ->  $($worker.Init.DisplayOutputName)" $cFile))
        $lines.Add((Row $laneLabel $color))
        $lines.Add((Row $colorLine $color))
        $lines.Add((Row "Reason: $($worker.Init.LaneSelectionReason)" $cMeta))
        if ($workerState -eq 'Held') {
            $lines.Add((Row 'Held: manual stop. Press worker number then [r] to restart from scratch.' $cMeta))
        } elseif ($workerState -eq 'Paused') {
            $lines.Add((Row "Paused  |  Elapsed $(Format-Duration -Seconds $worker.Stopwatch.Elapsed.TotalSeconds)  |  Encoded $sizeStr" $cMeta))
        } else {
            $lines.Add((Row "Elapsed $(Format-Duration -Seconds $worker.Stopwatch.Elapsed.TotalSeconds)  |  Encoded $sizeStr  |  Speed $speedStr  |  ETA $eta" $cMeta))
        }
        if ($EnableLiveSizeEstimate -and $worker.TrackedProcess) {
            $lines.Add((Row (Get-LiveEstimateSummaryText -Estimate $estimate) $cMeta))
        }
        $lines.Add((Row $bar))
    }

    if ($Workers.Count -eq 0) {
        $lines.Add((DivRow 'Idle'))
        $lines.Add((Row 'No active encode workers.'))
    }

    $commandPrompt = Get-ConsoleCommandPrompt
    if (-not [string]::IsNullOrWhiteSpace($commandPrompt)) {
        $lines.Add((DivRow 'Command'))
        $lines.Add((Row $commandPrompt $cMeta))
    }

    $statusMessage = Get-ConsoleStatusMessage
    if (-not [string]::IsNullOrWhiteSpace($statusMessage)) {
        $lines.Add((Row $statusMessage $cMeta))
    }

    if ($script:ShowHelpOverlay) {
        $lines.Add((DivRow 'Help'))
        foreach ($helpLine in (Get-ConsoleHelpLines)) {
            $lines.Add((Row $helpLine $cMeta))
        }
    }

    $lines.Add($botBorder)

    $lineCount = $lines.Count
    $sb = [System.Text.StringBuilder]::new()
    if ($UICursorRow -ge 0) {
        $null = $sb.Append("${ESC}[${UICursorRow}A")
        $null = $sb.Append("`r")
    }
    foreach ($l in $lines) {
        $null = $sb.Append("`r")
        $null = $sb.Append($l)
        $null = $sb.Append("${ESC}[K")
        $null = $sb.Append("`r`n")
    }

    $staleLineCount = [Math]::Max(0, $UICursorRow - $lineCount)
    for ($i = 0; $i -lt $staleLineCount; $i++) {
        $null = $sb.Append("`r")
        $null = $sb.Append("${ESC}[K")
        $null = $sb.Append("`r`n")
    }
    if ($staleLineCount -gt 0) {
        $null = $sb.Append("${ESC}[${staleLineCount}A")
        $null = $sb.Append("`r")
    }

    [Console]::Write($sb.ToString())
    return $lineCount
}

function Complete-LaneWorker {
    param(
        $Worker,
        $NvencEnvironment
    )

    $init = $Worker.Init
    $tracked = $Worker.TrackedProcess
    $ffExit = $tracked.Process.ExitCode
    Stop-TrackedFfmpegProcess -TrackedProcess $tracked
    $liveEstimate = Update-LiveEstimateState -State $tracked.Shared -SourceDurationSec $init.SourceDurationSec -SourceSizeBytes $init.SourceItem.Length
    $isNvenc = ($init.ResolvedEncodeLane -eq 'Nvidia')

    $notesList = [System.Collections.Generic.List[string]]::new()
    if ($init.AutoSettings.BitrateReason) { $notesList.Add($init.AutoSettings.BitrateReason) }
    if ($init.LaneSelectionReason) { $notesList.Add($init.LaneSelectionReason) }
    if ($init.CpuOnlyReason) { $notesList.Add($init.CpuOnlyReason) }
    if ($init.PreflightEstimate.Ran) { $notesList.Add($init.PreflightEstimate.Reason) }
    if ($init.FilmGrainDisabledReason) { $notesList.Add($init.FilmGrainDisabledReason) }
    if ($isNvenc -and $init.NvencSettings.Reason) { $notesList.Add($init.NvencSettings.Reason) }
    if ($isNvenc -and $init.NvencSettings.TuneReason) { $notesList.Add($init.NvencSettings.TuneReason) }
    if ($tracked.WorkerPriorityReason) { $notesList.Add($tracked.WorkerPriorityReason) }

    if ($ffExit -ne 0) {
        if ($tracked.Shared.LogLines.Count -gt 0) {
            $notesList.Add(($tracked.Shared.LogLines | Select-Object -Last 6) -join ' || ')
        }

        Write-LogRow @{
            Timestamp         = (Get-Date).ToString("s")
            Status            = "FAILED"
            InputPath         = $init.InputPath
            OutputPath        = ""
            SourceSizeGiB     = $init.SourceSizeGiB
            OutputSizeGiB     = ""
            ReductionPercent  = ""
            SourceDurationSec = [Math]::Round($init.SourceDurationSec, 3)
            OutputDurationSec = ""
            ElapsedSec        = [Math]::Round($Worker.Stopwatch.Elapsed.TotalSeconds, 2)
            Profile           = $init.SourceProfile.Profile
            HasHDR            = $init.SourceProfile.HasHDR
            HasDV             = $init.SourceProfile.HasDV
            SelectedAudio     = $init.SelectedAudioSummary
            SelectedSubtitles = $init.SelectedSubtitleSummary
            EstimatedFinalSizeGiB = if ($liveEstimate.Ready) { [Math]::Round($liveEstimate.EstimatedFinalSizeGiB, 3) } elseif ($init.PreflightEstimate.Ran) { [Math]::Round($init.PreflightEstimate.EstimatedFinalSizeGiB, 3) } else { "" }
            EstimatedSavingsPercent = if ($liveEstimate.Ready) { [Math]::Round($liveEstimate.EstimatedSavingsPercent, 2) } elseif ($init.PreflightEstimate.Ran) { [Math]::Round($init.PreflightEstimate.EstimatedSavingsPercent, 2) } else { "" }
            EstimatedOutputGiBPerHour = if ($liveEstimate.Ready) { [Math]::Round($liveEstimate.EstimatedOutputGiBPerHour, 3) } elseif ($init.PreflightEstimate.Ran) { [Math]::Round($init.PreflightEstimate.EstimatedOutputGiBPerHour, 3) } else { "" }
            InitialResolvedCRF = $init.PreflightWorkflow.InitialResolvedCRF
            InitialResolvedPreset = $init.PreflightWorkflow.InitialResolvedPreset
            InitialResolvedFilmGrain = $init.PreflightWorkflow.InitialResolvedFilmGrain
            PreflightPassCount = $init.PreflightWorkflow.PreflightPassCount
            Preflight1EstimatedFinalGiB = if ($init.PreflightWorkflow.Preflight1 -and $init.PreflightWorkflow.Preflight1.Ran) { [Math]::Round($init.PreflightWorkflow.Preflight1.EstimatedFinalSizeGiB, 3) } else { "" }
            Preflight1EstimatedSavingsPercent = if ($init.PreflightWorkflow.Preflight1 -and $init.PreflightWorkflow.Preflight1.Ran) { [Math]::Round($init.PreflightWorkflow.Preflight1.EstimatedSavingsPercent, 2) } else { "" }
            Preflight1EstimatedGiBPerHour = if ($init.PreflightWorkflow.Preflight1 -and $init.PreflightWorkflow.Preflight1.Ran) { [Math]::Round($init.PreflightWorkflow.Preflight1.EstimatedOutputGiBPerHour, 3) } else { "" }
            Preflight2EstimatedFinalGiB = if ($init.PreflightWorkflow.Preflight2 -and $init.PreflightWorkflow.Preflight2.Ran) { [Math]::Round($init.PreflightWorkflow.Preflight2.EstimatedFinalSizeGiB, 3) } else { "" }
            Preflight2EstimatedSavingsPercent = if ($init.PreflightWorkflow.Preflight2 -and $init.PreflightWorkflow.Preflight2.Ran) { [Math]::Round($init.PreflightWorkflow.Preflight2.EstimatedSavingsPercent, 2) } else { "" }
            Preflight2EstimatedGiBPerHour = if ($init.PreflightWorkflow.Preflight2 -and $init.PreflightWorkflow.Preflight2.Ran) { [Math]::Round($init.PreflightWorkflow.Preflight2.EstimatedOutputGiBPerHour, 3) } else { "" }
            FinalResolvedCRF = $init.PreflightWorkflow.FinalResolvedCRF
            FinalResolvedPreset = $init.PreflightWorkflow.FinalResolvedPreset
            FinalResolvedFilmGrain = $init.PreflightWorkflow.FinalResolvedFilmGrain
            PreflightAutoTuneReason = $init.PreflightWorkflow.PreflightAutoTuneReason
            WasPreflightRetuned = "$($init.PreflightWorkflow.WasPreflightRetuned)"
            WasSkippedByPreflight = "$($init.PreflightWorkflow.WasSkippedByPreflight)"
            CRF               = $CRF
            Preset            = $Preset
            FilmGrain         = $FilmGrain
            AutoCRFOffset     = $AutoCRFOffset
            EncoderPreference = $init.EncoderPreference
            ResolvedEncodeLane = $init.ResolvedEncodeLane
            LaneSelectionReason = $init.LaneSelectionReason
            LaneSuitability   = $init.LaneSuitability
            CpuOnlyReason     = $init.CpuOnlyReason
            NvidiaFallbackAllowed = "$($init.NvidiaFallbackAllowed)"
            HeldForCpuLane    = "$($init.HeldForCpuLane)"
            WorkerProcessPriority = Get-OptionalProperty -InputObject $Worker -PropertyName 'WorkerProcessPriority' -Default $init.WorkerProcessPriority
            ScriptProcessPriority = $script:ResolvedScriptProcessPriority
            EncodeMode        = $init.EncodeMode
            ResolvedCRF       = $init.PreflightWorkflow.FinalResolvedCRF
            ResolvedPreset    = $init.PreflightWorkflow.FinalResolvedPreset
            ResolvedFilmGrain = $init.EffectiveFilmGrain
            ResolvedCQ        = if ($isNvenc) { $init.NvencSettings.CQ } else { "" }
            ResolvedNvencPreset = if ($isNvenc) { $init.NvencSettings.Preset } else { "" }
            ResolvedNvencTune = if ($isNvenc) { $init.NvencSettings.Tune } else { "" }
            ResolvedDecodePath = if ($isNvenc) { $init.NvencSettings.DecodePath } else { "" }
            AutoReason        = if (-not [string]::IsNullOrWhiteSpace($init.PreflightWorkflow.PreflightAutoTuneReason)) { $init.PreflightWorkflow.PreflightAutoTuneReason } else { $init.AutoSettings.Reason }
            BPP               = [Math]::Round($init.AutoSettings.BPP, 6)
            EffectiveVideoBitrate = $init.AutoSettings.VideoBitrate
            VideoBitratePerHourGiB = [Math]::Round($init.AutoSettings.VideoBitratePerHourGiB, 3)
            ResolutionTier    = $init.AutoSettings.ResolutionTier
            CodecClass        = $init.AutoSettings.CodecClass
            GrainClass        = $init.AutoSettings.GrainClass
            GrainScore        = $init.AutoSettings.GrainScore
            WasAutoSkipped    = "False"
            NvencWorkerCountAtStart = if ($isNvenc) { $Worker.NvencWorkerCountAtStart } else { "" }
            NvencEngineCountDetected = if ($isNvenc -and $NvencEnvironment) { $NvencEnvironment.NvencEngineCount } else { "" }
            NvencCapacitySource = if ($isNvenc -and $NvencEnvironment) { $NvencEnvironment.CapacitySource } else { "" }
            DetectedGpuName   = if ($isNvenc -and $NvencEnvironment) { $NvencEnvironment.GpuName } else { "" }
            FilmGrainDisabledReason = $init.FilmGrainDisabledReason
            FfmpegPath        = $FfmpegPath
            FfprobePath       = $FfprobePath
            Notes             = ($notesList -join ' | ')
        }
        return 'FAILED'
    }

    if (-not (Test-Path -LiteralPath $init.TempOutput)) {
        throw "Temporary output was not created: $($init.TempOutput)"
    }

    $outProbe       = Invoke-FfprobeJson -InputPath $init.TempOutput
    $outputDuration = [double](Get-StreamProp (Get-StreamProp $outProbe 'format' ([PSCustomObject]@{})) 'duration' 0)
    if ($init.SourceDurationSec -gt 0) {
        $allowedDelta = [Math]::Max(10.0, $init.SourceDurationSec * 0.02)
        if ($outputDuration -lt ($init.SourceDurationSec - $allowedDelta)) {
            throw ("Output duration check failed. Source={0:F3}s  Output={1:F3}s  AllowedDelta={2:F3}s" -f $init.SourceDurationSec, $outputDuration, $allowedDelta)
        }
    }

    $outItem       = Get-Item -LiteralPath $init.TempOutput
    $outputSizeGiB = [Math]::Round(($outItem.Length / 1GB), 3)
    $reduction     = if ($init.SourceItem.Length -gt 0) {
        [Math]::Round((1 - ($outItem.Length / [double]$init.SourceItem.Length)) * 100, 2)
    } else { 0 }

    $outputPathForLog = $init.FinalOutput
    if ($ReplaceOriginal) {
        try {
            if ($KeepBackupOriginal) {
                $backupPath = Move-ToBackup -OriginalPath $init.InputPath
                Write-Host "Moved original to backup: $backupPath" -ForegroundColor Yellow
            } else {
                Remove-Item -LiteralPath $init.InputPath -Force
            }
            Move-Item -LiteralPath $init.TempOutput -Destination $init.FinalOutput -Force
        } catch {
            $tempStillExists = Test-Path -LiteralPath $init.TempOutput
            $recovery = if ($tempStillExists) {
                "Encoded temp file still exists and can be recovered: $($init.TempOutput)"
            } else {
                "Encoded temp file is also missing. Check disk for partial writes."
            }
            throw "Post-encode file management failed: $_`n$recovery"
        }
    } else {
        Move-Item -LiteralPath $init.TempOutput -Destination $init.FinalOutput -Force
    }

    Write-LogRow @{
        Timestamp         = (Get-Date).ToString("s")
        Status            = "SUCCESS"
        InputPath         = $init.InputPath
        OutputPath        = $outputPathForLog
        SourceSizeGiB     = $init.SourceSizeGiB
        OutputSizeGiB     = $outputSizeGiB
        ReductionPercent  = $reduction
        SourceDurationSec = [Math]::Round($init.SourceDurationSec, 3)
        OutputDurationSec = [Math]::Round($outputDuration, 3)
        ElapsedSec        = [Math]::Round($Worker.Stopwatch.Elapsed.TotalSeconds, 2)
        Profile           = $init.SourceProfile.Profile
        HasHDR            = $init.SourceProfile.HasHDR
        HasDV             = $init.SourceProfile.HasDV
        SelectedAudio     = $init.SelectedAudioSummary
        SelectedSubtitles = $init.SelectedSubtitleSummary
        EstimatedFinalSizeGiB = $outputSizeGiB
        EstimatedSavingsPercent = $reduction
        EstimatedOutputGiBPerHour = if ($init.SourceDurationSec -gt 0) { [Math]::Round($outputSizeGiB / ($init.SourceDurationSec / 3600.0), 3) } else { "" }
        InitialResolvedCRF = $init.PreflightWorkflow.InitialResolvedCRF
        InitialResolvedPreset = $init.PreflightWorkflow.InitialResolvedPreset
        InitialResolvedFilmGrain = $init.PreflightWorkflow.InitialResolvedFilmGrain
        PreflightPassCount = $init.PreflightWorkflow.PreflightPassCount
        Preflight1EstimatedFinalGiB = if ($init.PreflightWorkflow.Preflight1 -and $init.PreflightWorkflow.Preflight1.Ran) { [Math]::Round($init.PreflightWorkflow.Preflight1.EstimatedFinalSizeGiB, 3) } else { "" }
        Preflight1EstimatedSavingsPercent = if ($init.PreflightWorkflow.Preflight1 -and $init.PreflightWorkflow.Preflight1.Ran) { [Math]::Round($init.PreflightWorkflow.Preflight1.EstimatedSavingsPercent, 2) } else { "" }
        Preflight1EstimatedGiBPerHour = if ($init.PreflightWorkflow.Preflight1 -and $init.PreflightWorkflow.Preflight1.Ran) { [Math]::Round($init.PreflightWorkflow.Preflight1.EstimatedOutputGiBPerHour, 3) } else { "" }
        Preflight2EstimatedFinalGiB = if ($init.PreflightWorkflow.Preflight2 -and $init.PreflightWorkflow.Preflight2.Ran) { [Math]::Round($init.PreflightWorkflow.Preflight2.EstimatedFinalSizeGiB, 3) } else { "" }
        Preflight2EstimatedSavingsPercent = if ($init.PreflightWorkflow.Preflight2 -and $init.PreflightWorkflow.Preflight2.Ran) { [Math]::Round($init.PreflightWorkflow.Preflight2.EstimatedSavingsPercent, 2) } else { "" }
        Preflight2EstimatedGiBPerHour = if ($init.PreflightWorkflow.Preflight2 -and $init.PreflightWorkflow.Preflight2.Ran) { [Math]::Round($init.PreflightWorkflow.Preflight2.EstimatedOutputGiBPerHour, 3) } else { "" }
        FinalResolvedCRF = $init.PreflightWorkflow.FinalResolvedCRF
        FinalResolvedPreset = $init.PreflightWorkflow.FinalResolvedPreset
        FinalResolvedFilmGrain = $init.PreflightWorkflow.FinalResolvedFilmGrain
        PreflightAutoTuneReason = $init.PreflightWorkflow.PreflightAutoTuneReason
        WasPreflightRetuned = "$($init.PreflightWorkflow.WasPreflightRetuned)"
        WasSkippedByPreflight = "$($init.PreflightWorkflow.WasSkippedByPreflight)"
        CRF               = $CRF
        Preset            = $Preset
        FilmGrain         = $FilmGrain
        AutoCRFOffset     = $AutoCRFOffset
        EncoderPreference = $init.EncoderPreference
        ResolvedEncodeLane = $init.ResolvedEncodeLane
        LaneSelectionReason = $init.LaneSelectionReason
        LaneSuitability   = $init.LaneSuitability
        CpuOnlyReason     = $init.CpuOnlyReason
        NvidiaFallbackAllowed = "$($init.NvidiaFallbackAllowed)"
        HeldForCpuLane    = "$($init.HeldForCpuLane)"
        WorkerProcessPriority = Get-OptionalProperty -InputObject $Worker -PropertyName 'WorkerProcessPriority' -Default $init.WorkerProcessPriority
        ScriptProcessPriority = $script:ResolvedScriptProcessPriority
        EncodeMode        = $init.EncodeMode
        ResolvedCRF       = $init.PreflightWorkflow.FinalResolvedCRF
        ResolvedPreset    = $init.PreflightWorkflow.FinalResolvedPreset
        ResolvedFilmGrain = $init.EffectiveFilmGrain
        ResolvedCQ        = if ($isNvenc) { $init.NvencSettings.CQ } else { "" }
        ResolvedNvencPreset = if ($isNvenc) { $init.NvencSettings.Preset } else { "" }
        ResolvedNvencTune = if ($isNvenc) { $init.NvencSettings.Tune } else { "" }
        ResolvedDecodePath = if ($isNvenc) { $init.NvencSettings.DecodePath } else { "" }
        AutoReason        = if (-not [string]::IsNullOrWhiteSpace($init.PreflightWorkflow.PreflightAutoTuneReason)) { $init.PreflightWorkflow.PreflightAutoTuneReason } else { $init.AutoSettings.Reason }
        BPP               = [Math]::Round($init.AutoSettings.BPP, 6)
        EffectiveVideoBitrate = $init.AutoSettings.VideoBitrate
        VideoBitratePerHourGiB = [Math]::Round($init.AutoSettings.VideoBitratePerHourGiB, 3)
        ResolutionTier    = $init.AutoSettings.ResolutionTier
        CodecClass        = $init.AutoSettings.CodecClass
        GrainClass        = $init.AutoSettings.GrainClass
        GrainScore        = $init.AutoSettings.GrainScore
        WasAutoSkipped    = "False"
        NvencWorkerCountAtStart = if ($isNvenc) { $Worker.NvencWorkerCountAtStart } else { "" }
        NvencEngineCountDetected = if ($isNvenc -and $NvencEnvironment) { $NvencEnvironment.NvencEngineCount } else { "" }
        NvencCapacitySource = if ($isNvenc -and $NvencEnvironment) { $NvencEnvironment.CapacitySource } else { "" }
        DetectedGpuName   = if ($isNvenc -and $NvencEnvironment) { $NvencEnvironment.GpuName } else { "" }
        FilmGrainDisabledReason = $init.FilmGrainDisabledReason
        FfmpegPath        = $FfmpegPath
        FfprobePath       = $FfprobePath
        Notes             = ($notesList -join ' | ')
    }

    return 'SUCCESS'
}

function Write-NvencProgressUI {
    param(
        [object[]]$Workers,
        $Summary,
        $NvencEnvironment,
        [int]$UICursorRow = -1
    )

    $conW  = [Math]::Max(70, $Host.UI.RawUI.WindowSize.Width - 4)
    $inner = $conW - 4

    $ESC      = [char]27
    $reset    = "${ESC}[0m"
    $cBorder  = "${ESC}[38;5;240m"
    $cTitle   = "${ESC}[1;97m"
    $cFile    = "${ESC}[1;96m"
    $cHdr     = "${ESC}[1;93m"
    $cSdr     = "${ESC}[38;5;117m"
    $cMeta    = "${ESC}[38;5;250m"
    $cBarDone = "${ESC}[38;5;76m"
    $cBarTodo = "${ESC}[38;5;238m"
    $cPct     = "${ESC}[1;92m"
    $cQueue   = "${ESC}[38;5;245m"

    $TL = [char]0x2554
    $TR = [char]0x2557
    $BL = [char]0x255A
    $BR = [char]0x255D
    $HL = [char]0x2550
    $VL = [char]0x2551
    $LM = [char]0x2560
    $RM = [char]0x2563

    function Row ([string]$content, [string]$color = "") {
        $visible = Remove-AnsiDisplayFormatting $content
        $safe = if ($visible.Length -gt $inner) {
            Limit-String -Value $visible -MaxWidth $inner
        } else {
            $content
        }
        $pad  = " " * [Math]::Max(0, $inner - (Remove-AnsiDisplayFormatting $safe).Length)
        "${cBorder}${VL} ${reset}${color}${safe}${reset}${pad} ${cBorder}${VL}${reset}"
    }

    function DivRow ([string]$label) {
        $mid   = " $label "
        $left  = [int][Math]::Floor(($conW - 2 - $mid.Length) / 2)
        $right = $conW - 2 - $left - $mid.Length
        "${cBorder}${LM}$([string]$HL * $left)${cTitle}${mid}${reset}${cBorder}$([string]$HL * $right)${RM}${reset}"
    }

    $titleLabel = " NVENC Queue "
    $tLeft      = [int][Math]::Floor(($conW - 2 - $titleLabel.Length) / 2)
    $tRight     = $conW - 2 - $tLeft - $titleLabel.Length
    $topBorder  = "${cBorder}${TL}$([string]$HL * $tLeft)${cTitle}${titleLabel}${reset}${cBorder}$([string]$HL * $tRight)${TR}${reset}"
    $botBorder  = "${cBorder}${BL}$([string]$HL * ($conW - 2))${BR}${reset}"

    $lines = [System.Collections.Generic.List[string]]::new()
    $lines.Add($topBorder)
    $lines.Add((Row "Encoder preference: $EncoderPreference  |  GPU: $($NvencEnvironment.GpuName)  |  NVENC engines: $($NvencEnvironment.NvencEngineCount)  |  Parallel capacity: $($NvencEnvironment.MaxParallel) ($($NvencEnvironment.CapacitySource))" $cMeta))
    $lines.Add((Row (Get-QueueControlStateText) $cMeta))
    $lines.Add((Row "Pending: $($Summary.Pending)  |  Active: $($Summary.Active)  |  Completed: $($Summary.Completed)  |  Skipped: $($Summary.Skipped)  |  Failed: $($Summary.Failed)" $cQueue))

    foreach ($worker in @($Workers | Sort-Object SlotNumber)) {
        $workerState = Get-WorkerStateLabel -Worker $worker
        $shared = if ($worker.TrackedProcess) { $worker.TrackedProcess.Shared } else { [pscustomobject]@{ OutTimeSec = 0.0; OutSizeBytes = 0.0; SpeedX = 0.0 } }
        $workerPriority = Get-OptionalProperty -InputObject $worker -PropertyName 'WorkerProcessPriority' -Default $worker.Init.WorkerProcessPriority
        $estimate = if ($worker.TrackedProcess) { Update-LiveEstimateState -State $shared -SourceDurationSec $worker.Init.SourceDurationSec -SourceSizeBytes $worker.Init.SourceItem.Length } else { $null }
        $pct = if ($worker.Init.SourceDurationSec -gt 0) {
            [Math]::Min(100.0, ($shared.OutTimeSec / $worker.Init.SourceDurationSec) * 100.0)
        } else { 0.0 }
        $eta = if ($shared.SpeedX -gt 0.001 -and $worker.Init.SourceDurationSec -gt 0) {
            Format-Duration -Seconds (($worker.Init.SourceDurationSec - $shared.OutTimeSec) / $shared.SpeedX)
        } else { '--' }
        $sizeStr = if ($shared.OutSizeBytes -gt 0) { "{0:F2} GiB" -f ($shared.OutSizeBytes / 1GB) } else { "---" }
        $speedStr = if ($shared.SpeedX -gt 0.001) { "{0:F2}x" -f $shared.SpeedX } else { '---' }
        $color = if ($worker.Init.SourceProfile.Profile -eq 'HDR') { $cHdr } else { $cSdr }
        $barInner = [Math]::Max(20, $inner - 16)
        $filled = [int][Math]::Round($barInner * $pct / 100.0)
        $empty = $barInner - $filled
        $pctLabel = ("{0,5:F1}%" -f $pct)
        $bar = "[${cBarDone}$([string][char]0x2588 * $filled)${reset}${cBarTodo}$([string][char]0x2591 * $empty)${reset}] ${cPct}$pctLabel${reset}"

        $lines.Add((DivRow "Worker $($worker.SlotNumber)"))
        $lines.Add((Row "$($worker.Init.DisplayInputName)  ->  $($worker.Init.DisplayOutputName)" $cFile))
        $nvencModeLine = Add-RainbowHdrHighlights -Text "Mode NVENC  |  $($worker.Init.SourceProfile.Profile)  |  CQ $($worker.Init.NvencSettings.CQ)  |  Preset $($worker.Init.NvencSettings.Preset)  |  Tune $($worker.Init.NvencSettings.TuneDisplay)  |  Decode $($worker.Init.NvencSettings.DecodePath)  |  State $workerState  |  Priority $workerPriority" -BaseColor $color
        $nvencColorLine = Add-RainbowHdrHighlights -Text ("Color  |  Source {0}  ->  Output {1}" -f $worker.Init.SourceProfile.SourceColorSummary, $worker.Init.EncodeColorProfile.Summary) -BaseColor $color
        $lines.Add((Row $nvencModeLine $color))
        $lines.Add((Row $nvencColorLine $color))
        $lines.Add((Row "Resolved lane: $($worker.Init.ResolvedEncodeLane)  |  Reason: $($worker.Init.LaneSelectionReason)" $cMeta))
        if ($workerState -eq 'Held') {
            $lines.Add((Row 'Held: manual stop. Press worker number then [r] to restart from scratch.' $cMeta))
        } elseif ($workerState -eq 'Paused') {
            $lines.Add((Row "Paused  |  Elapsed $(Format-Duration -Seconds $worker.Stopwatch.Elapsed.TotalSeconds)  |  Encoded $sizeStr" $cMeta))
        } else {
            $lines.Add((Row "Elapsed $(Format-Duration -Seconds $worker.Stopwatch.Elapsed.TotalSeconds)  |  Encoded $sizeStr  |  Speed $speedStr  |  ETA $eta" $cMeta))
        }
        if ($EnableLiveSizeEstimate -and $worker.TrackedProcess) {
            $lines.Add((Row (Get-LiveEstimateSummaryText -Estimate $estimate) $cMeta))
        }
        $lines.Add((Row $bar))
    }

    if ($Workers.Count -eq 0) {
        $lines.Add((DivRow 'Idle'))
        $lines.Add((Row 'No active NVENC workers.'))
    }

    $commandPrompt = Get-ConsoleCommandPrompt
    if (-not [string]::IsNullOrWhiteSpace($commandPrompt)) {
        $lines.Add((DivRow 'Command'))
        $lines.Add((Row $commandPrompt $cMeta))
    }

    $statusMessage = Get-ConsoleStatusMessage
    if (-not [string]::IsNullOrWhiteSpace($statusMessage)) {
        $lines.Add((Row $statusMessage $cMeta))
    }

    if ($script:ShowHelpOverlay) {
        $lines.Add((DivRow 'Help'))
        foreach ($helpLine in (Get-ConsoleHelpLines)) {
            $lines.Add((Row $helpLine $cMeta))
        }
    }

    $lines.Add($botBorder)

    $lineCount = $lines.Count
    $sb = [System.Text.StringBuilder]::new()
    if ($UICursorRow -ge 0) {
        $null = $sb.Append("${ESC}[${UICursorRow}A")
        $null = $sb.Append("`r")
    }
    foreach ($l in $lines) {
        $null = $sb.Append("`r")
        $null = $sb.Append($l)
        $null = $sb.Append("${ESC}[K")
        $null = $sb.Append("`r`n")
    }

    $staleLineCount = [Math]::Max(0, $UICursorRow - $lineCount)
    for ($i = 0; $i -lt $staleLineCount; $i++) {
        $null = $sb.Append("`r")
        $null = $sb.Append("${ESC}[K")
        $null = $sb.Append("`r`n")
    }
    if ($staleLineCount -gt 0) {
        $null = $sb.Append("${ESC}[${staleLineCount}A")
        $null = $sb.Append("`r")
    }

    [Console]::Write($sb.ToString())
    return $lineCount
}

function Complete-NvencWorker {
    param(
        $Worker,
        $NvencEnvironment
    )
    return (Complete-LaneWorker -Worker $Worker -NvencEnvironment $NvencEnvironment)
}

function Invoke-NvencQueueProcessing {
    param($NvencEnvironment)

    $summary = [ordered]@{
        Completed = 0
        Skipped   = 0
        Failed    = 0
        Pending   = 0
        Active    = 0
    }

    $activeWorkers = New-Object System.Collections.Generic.List[object]
    $uiLineCount = -1
    $shutdownBannerShown = $false

    while ($true) {
        if (Test-QueueShutdownRequested -Workers $activeWorkers.ToArray() -NvencEnvironment $NvencEnvironment) {
            if (-not $shutdownBannerShown) {
                if ($uiLineCount -ge 0) { Write-Host ""; $uiLineCount = -1 }
                Write-Host "Shutdown: stopping new queue launches and draining active workers..." -ForegroundColor Yellow
                $shutdownBannerShown = $true
            }
            $workersSnapshot = [object[]]$activeWorkers.ToArray()
            foreach ($worker in $workersSnapshot) {
                Request-WorkerShutdown -Worker $worker
            }
        }

        $pendingJobs = @(Get-ChildItem -LiteralPath $QueuePendingDir -Filter *.json -File -ErrorAction SilentlyContinue | Sort-Object CreationTimeUtc)

        while (-not $script:QueueShutdownRequested -and $activeWorkers.Count -lt $NvencEnvironment.MaxParallel -and $pendingJobs.Count -gt 0) {
            $nextJob = $pendingJobs[0]
            $pendingJobs = @($pendingJobs | Select-Object -Skip 1)
            $workingJobPath = Join-Path $QueueWorkingDir $nextJob.Name

            try {
                Move-Item -LiteralPath $nextJob.FullName -Destination $workingJobPath -Force

                $job = Get-Content -LiteralPath $workingJobPath -Raw | ConvertFrom-Json
                $init = Get-EncodeInitialization -InputPath $job.InputPath -EncodeMode 'nvenc' -NvencEnvironment $NvencEnvironment

                if ($init.EarlyExit) {
                    $row = $init.Row
                    $row.NvencWorkerCountAtStart = $NvencEnvironment.MaxParallel
                    Write-LogRow $row
                    if ($init.EarlyExit -like 'AUTO_SKIPPED*' -or $init.EarlyExit -eq 'SKIPPED_DV' -or $init.EarlyExit -eq 'PRECHECK_SKIPPED_UNFAVORABLE') {
                        $summary.Skipped++
                    } else {
                        $summary.Failed++
                    }
                    Remove-Item -LiteralPath $workingJobPath -Force -ErrorAction SilentlyContinue
                    continue
                }

                if (Test-Path -LiteralPath $init.TempOutput) {
                    Remove-Item -LiteralPath $init.TempOutput -Force -ErrorAction SilentlyContinue
                }

                $ffArgs = Build-NvencFfmpegArgs -Init $init -NvencEnvironment $NvencEnvironment
                $tracked = Start-TrackedFfmpegProcess -Arguments $ffArgs -PriorityName $init.WorkerProcessPriority
                $init.WorkerProcessPriority = $tracked.WorkerProcessPriority
                $slotNumber = 1
                while (@($activeWorkers | Where-Object { $_.SlotNumber -eq $slotNumber }).Count -gt 0) {
                    $slotNumber++
                }

                $activeWorkers.Add([pscustomobject][ordered]@{
                    SlotNumber               = $slotNumber
                    WorkingJobPath           = $workingJobPath
                    Init                     = $init
                    TrackedProcess           = $tracked
                    Stopwatch                = [System.Diagnostics.Stopwatch]::StartNew()
                    NvencWorkerCountAtStart  = $NvencEnvironment.MaxParallel
                    WorkerProcessPriority    = $tracked.WorkerProcessPriority
                    ShutdownRequestedAt      = $null
                }) | Out-Null
            } catch {
                if ($_.Exception.Message -eq $script:QueueShutdownSentinel) {
                    $script:QueueShutdownRequested = $true
                    Requeue-WorkingJob -WorkingJobPath $workingJobPath
                    break
                }

                throw
            }
        }

        $summary.Pending = @(Get-ChildItem -LiteralPath $QueuePendingDir -Filter *.json -File -ErrorAction SilentlyContinue).Count
        $summary.Active  = $activeWorkers.Count
        if (-not $script:QueueShutdownRequested) {
            $uiLineCount = Write-NvencProgressUI -Workers $activeWorkers.ToArray() -Summary $summary -NvencEnvironment $NvencEnvironment -UICursorRow $uiLineCount
        }

        for ($i = $activeWorkers.Count - 1; $i -ge 0; $i--) {
            $worker = $activeWorkers[$i]
            if (Test-TrackedWorkerProcessExited -Worker $worker) {
                try {
                    if ($script:QueueShutdownRequested) {
                        Stop-TrackedFfmpegProcess -TrackedProcess $worker.TrackedProcess
                        if (Test-Path -LiteralPath $worker.Init.TempOutput) {
                            Remove-Item -LiteralPath $worker.Init.TempOutput -Force -ErrorAction SilentlyContinue
                        }
                        Requeue-WorkingJob -WorkingJobPath $worker.WorkingJobPath
                        Write-Host ("Shutdown: requeued {0}" -f $worker.Init.DisplayInputName) -ForegroundColor DarkYellow
                    } else {
                        $result = Complete-NvencWorker -Worker $worker -NvencEnvironment $NvencEnvironment
                        switch ($result) {
                            'SUCCESS' { $summary.Completed++ }
                            'FAILED'  { $summary.Failed++ }
                        }
                    }
                } catch {
                    Write-Host ""
                    Write-Host "FAILED: $($_.Exception.Message)" -ForegroundColor Red
                    $summary.Failed++
                } finally {
                    if (Test-Path -LiteralPath $worker.WorkingJobPath) {
                        Remove-Item -LiteralPath $worker.WorkingJobPath -Force -ErrorAction SilentlyContinue
                    }
                    $activeWorkers.RemoveAt($i)
                }
            }
        }

        if ($script:QueueShutdownRequested -and $activeWorkers.Count -eq 0) { break }
        if ($activeWorkers.Count -eq 0 -and $summary.Pending -eq 0) { break }
        Start-Sleep -Milliseconds 200
    }

    if ($uiLineCount -ge 0) { Write-Host "" }
}

function Invoke-AutoEncoderLaneQueueProcessing {
    param($NvencEnvironment = $null)

    $nvidiaCapacity = if ($NvencEnvironment) { [Math]::Max(1, $NvencEnvironment.MaxParallel) } else { 0 }
    $summary = [pscustomobject][ordered]@{
        EncoderPreference = 'Auto'
        Completed         = 0
        Skipped           = 0
        Failed            = 0
        Pending           = 0
        Active            = 0
        CpuActive         = 0
        NvidiaActive      = 0
        NvidiaCapacity    = $nvidiaCapacity
    }

    $activeWorkers = New-Object System.Collections.Generic.List[object]
    $uiLineCount = -1
    $shutdownBannerShown = $false

    while ($true) {
        if (Test-QueueShutdownRequested -Workers $activeWorkers.ToArray() -NvencEnvironment $NvencEnvironment) {
            if (-not $shutdownBannerShown) {
                if ($uiLineCount -ge 0) { Write-Host ""; $uiLineCount = -1 }
                Write-Host "Shutdown: stopping new queue launches and draining active workers..." -ForegroundColor Yellow
                $shutdownBannerShown = $true
            }
            $workersSnapshot = [object[]]$activeWorkers.ToArray()
            foreach ($worker in $workersSnapshot) {
                Request-WorkerShutdown -Worker $worker
            }
        }

        $pendingJobs = @(Get-ChildItem -LiteralPath $QueuePendingDir -Filter *.json -File -ErrorAction SilentlyContinue | Sort-Object CreationTimeUtc)

        while (-not $script:QueueShutdownRequested -and $pendingJobs.Count -gt 0) {
            $cpuAvailable = (@($activeWorkers | Where-Object { $_.Init.ResolvedEncodeLane -eq 'CPU' }).Count -lt 1)
            $nvidiaAvailable = (@($activeWorkers | Where-Object { $_.Init.ResolvedEncodeLane -eq 'Nvidia' }).Count -lt $nvidiaCapacity)
            if (-not $cpuAvailable -and -not $nvidiaAvailable) { break }

            $scheduledAny = $false
            foreach ($nextJob in @($pendingJobs)) {
                $job = $null
                $workingJobPath = $null
                try {
                    $job = Get-Content -LiteralPath $nextJob.FullName -Raw | ConvertFrom-Json
                    $resolution = Resolve-EncoderLane `
                        -InputPath $job.InputPath `
                        -EncoderPreferenceValue 'Auto' `
                        -CpuLaneAvailable $cpuAvailable `
                        -NvidiaLaneAvailable $nvidiaAvailable `
                        -NvencEnvironment $NvencEnvironment

                    if (-not $resolution.Ready) {
                        if ($resolution.HeldForCpuLane -and $job.InputPath) {
                            if ($script:HeldForCpuAnnouncements.Add($job.InputPath)) {
                                Write-Host $resolution.Reason -ForegroundColor DarkYellow
                                Write-SessionTextLogMessage -Level Info -Message $resolution.Reason
                            }
                        }
                        continue
                    }

                    $workingJobPath = Join-Path $QueueWorkingDir $nextJob.Name
                    Move-Item -LiteralPath $nextJob.FullName -Destination $workingJobPath -Force

                    if ($resolution.Init.EarlyExit) {
                        $row = $resolution.Init.Row
                        $row.NvencWorkerCountAtStart = if ($resolution.Init.ResolvedEncodeLane -eq 'Nvidia' -and $NvencEnvironment) { $NvencEnvironment.MaxParallel } else { "" }
                        Write-LogRow $row
                        if ($resolution.Init.EarlyExit -like 'AUTO_SKIPPED*' -or $resolution.Init.EarlyExit -eq 'SKIPPED_DV' -or $resolution.Init.EarlyExit -eq 'PRECHECK_SKIPPED_UNFAVORABLE') {
                            $summary.Skipped++
                        } else {
                            $summary.Failed++
                        }
                        Remove-Item -LiteralPath $workingJobPath -Force -ErrorAction SilentlyContinue
                    } else {
                        $slotNumber = 1
                        while (@($activeWorkers | Where-Object { $_.SlotNumber -eq $slotNumber }).Count -gt 0) { $slotNumber++ }
                        if ($job.InputPath) { $null = $script:HeldForCpuAnnouncements.Remove($job.InputPath) }
                        $worker = Start-LaneWorker -Init $resolution.Init -NvencEnvironment $NvencEnvironment -SlotNumber $slotNumber
                        $worker.WorkingJobPath = $workingJobPath
                        $activeWorkers.Add($worker) | Out-Null
                    }
                } catch {
                    if ($_.Exception.Message -eq $script:QueueShutdownSentinel) {
                        $script:QueueShutdownRequested = $true
                        break
                    }

                    Write-Host ""
                    Write-Host "FAILED: $($_.Exception.Message)" -ForegroundColor Red
                    Write-LogRow @{
                        Timestamp         = (Get-Date).ToString("s")
                        Status            = "FAILED"
                        InputPath         = if ($job) { $job.InputPath } else { $nextJob.Name }
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
                        FilmGrain         = $FilmGrain
                        AutoCRFOffset     = $AutoCRFOffset
                        EncoderPreference = 'Auto'
                        ResolvedEncodeLane = ""
                        LaneSelectionReason = ""
                        WorkerProcessPriority = ""
                        ScriptProcessPriority = $script:ResolvedScriptProcessPriority
                        EncodeMode        = ""
                        ResolvedCRF       = ""
                        ResolvedPreset    = ""
                        ResolvedFilmGrain = ""
                        ResolvedCQ        = ""
                        ResolvedNvencPreset = ""
                        ResolvedNvencTune = ""
                        ResolvedDecodePath = ""
                        AutoReason        = ""
                        BPP               = ""
                        EffectiveVideoBitrate = ""
                        VideoBitratePerHourGiB = ""
                        ResolutionTier    = ""
                        CodecClass        = ""
                        GrainClass        = ""
                        GrainScore        = ""
                        WasAutoSkipped    = "False"
                        NvencWorkerCountAtStart = ""
                        NvencEngineCountDetected = if ($NvencEnvironment) { $NvencEnvironment.NvencEngineCount } else { "" }
                        NvencCapacitySource = if ($NvencEnvironment) { $NvencEnvironment.CapacitySource } else { "" }
                        DetectedGpuName   = if ($NvencEnvironment) { $NvencEnvironment.GpuName } else { "" }
                        FilmGrainDisabledReason = ""
                        FfmpegPath        = $FfmpegPath
                        FfprobePath       = $FfprobePath
                        Notes             = $_.Exception.Message
                    }
                    $summary.Failed++
                    if ($workingJobPath -and (Test-Path -LiteralPath $workingJobPath)) {
                        Remove-Item -LiteralPath $workingJobPath -Force -ErrorAction SilentlyContinue
                    } elseif (Test-Path -LiteralPath $nextJob.FullName) {
                        Remove-Item -LiteralPath $nextJob.FullName -Force -ErrorAction SilentlyContinue
                    }
                }

                $pendingJobs = @($pendingJobs | Where-Object { $_.FullName -ne $nextJob.FullName })
                $scheduledAny = $true
                break
            }

            if (-not $scheduledAny) { break }
        }

        $summary.Pending = @(Get-ChildItem -LiteralPath $QueuePendingDir -Filter *.json -File -ErrorAction SilentlyContinue).Count
        $summary.CpuActive = @($activeWorkers | Where-Object { $_.Init.ResolvedEncodeLane -eq 'CPU' }).Count
        $summary.NvidiaActive = @($activeWorkers | Where-Object { $_.Init.ResolvedEncodeLane -eq 'Nvidia' }).Count
        $summary.Active = $activeWorkers.Count
        if (-not $script:QueueShutdownRequested) {
            $uiLineCount = Write-LaneProgressUI -Workers $activeWorkers.ToArray() -Summary $summary -NvencEnvironment $NvencEnvironment -UICursorRow $uiLineCount
        }

        for ($i = $activeWorkers.Count - 1; $i -ge 0; $i--) {
            $worker = $activeWorkers[$i]
            if (Test-TrackedWorkerProcessExited -Worker $worker) {
                try {
                    if ($script:QueueShutdownRequested) {
                        Stop-TrackedFfmpegProcess -TrackedProcess $worker.TrackedProcess
                        if (Test-Path -LiteralPath $worker.Init.TempOutput) {
                            Remove-Item -LiteralPath $worker.Init.TempOutput -Force -ErrorAction SilentlyContinue
                        }
                        Requeue-WorkingJob -WorkingJobPath $worker.WorkingJobPath
                        Write-Host ("Shutdown: requeued {0}" -f $worker.Init.DisplayInputName) -ForegroundColor DarkYellow
                    } else {
                        $result = Complete-LaneWorker -Worker $worker -NvencEnvironment $NvencEnvironment
                        switch ($result) {
                            'SUCCESS' { $summary.Completed++ }
                            'FAILED'  { $summary.Failed++ }
                        }
                    }
                } catch {
                    Write-Host ""
                    Write-Host "FAILED: $($_.Exception.Message)" -ForegroundColor Red
                    $summary.Failed++
                } finally {
                    if (Test-Path -LiteralPath $worker.WorkingJobPath) {
                        Remove-Item -LiteralPath $worker.WorkingJobPath -Force -ErrorAction SilentlyContinue
                    }
                    $activeWorkers.RemoveAt($i)
                }
            }
        }

        if ($script:QueueShutdownRequested -and $activeWorkers.Count -eq 0) { break }
        if ($activeWorkers.Count -eq 0 -and $summary.Pending -eq 0) { break }
        Start-Sleep -Milliseconds 200
    }

    if ($uiLineCount -ge 0) { Write-Host "" }
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
            EncoderPreference = $EncoderPreference
            ResolvedEncodeLane = 'CPU'
            LaneSelectionReason = 'forced CPU lane by encoder preference'
            EncodeMode        = 'software'
            ResolvedCRF       = ""
            ResolvedPreset    = ""
            ResolvedFilmGrain = ""
            ResolvedCQ        = ""
            ResolvedNvencPreset = ""
            ResolvedNvencTune = ""
            ResolvedDecodePath = ""
            AutoReason        = ""
            BPP               = ""
            EffectiveVideoBitrate = ""
            VideoBitratePerHourGiB = ""
            ResolutionTier    = $sourceResolutionTier
            CodecClass        = $sourceCodecClass
            GrainClass        = ""
            GrainScore        = ""
            WasAutoSkipped    = "False"
            NvencWorkerCountAtStart = ""
            NvencEngineCountDetected = ""
            NvencCapacitySource = ""
            DetectedGpuName   = ""
            FilmGrainDisabledReason = ""
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
    $preflightWorkflow = [pscustomobject][ordered]@{
        InitialResolvedCRF = $resolvedCRF
        InitialResolvedPreset = $resolvedPreset
        InitialResolvedFilmGrain = $resolvedFilmGrain
        FinalResolvedCRF = $resolvedCRF
        FinalResolvedPreset = $resolvedPreset
        FinalResolvedFilmGrain = $resolvedFilmGrain
        FinalNvencSettings = $null
        PreflightPassCount = 0
        Preflight1 = $null
        Preflight2 = $null
        FinalPreflight = [pscustomobject][ordered]@{ Ran = $false; ShouldSkip = $false; Reason = '' }
        PreflightAutoTuneReason = ''
        WasPreflightRetuned = $false
        WasSkippedByPreflight = $false
        SkipStatus = ''
    }

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
            EncoderPreference = $EncoderPreference
            ResolvedEncodeLane = 'CPU'
            LaneSelectionReason = $autoSettings.SkipReason
            EncodeMode        = 'software'
            ResolvedCRF       = $resolvedCRF
            ResolvedPreset    = $resolvedPreset
            ResolvedFilmGrain = $resolvedFilmGrain
            ResolvedCQ        = ""
            ResolvedNvencPreset = ""
            ResolvedNvencTune = ""
            ResolvedDecodePath = ""
            AutoReason        = $autoSettings.SkipReason
            BPP               = [Math]::Round($autoSettings.BPP, 6)
            EffectiveVideoBitrate = $autoSettings.VideoBitrate
            VideoBitratePerHourGiB = [Math]::Round($autoSettings.VideoBitratePerHourGiB, 3)
            ResolutionTier    = $autoSettings.ResolutionTier
            CodecClass        = $autoSettings.CodecClass
            GrainClass        = $autoSettings.GrainClass
            GrainScore        = $autoSettings.GrainScore
            WasAutoSkipped    = "True"
            NvencWorkerCountAtStart = ""
            NvencEngineCountDetected = ""
            NvencCapacitySource = ""
            DetectedGpuName   = ""
            FilmGrainDisabledReason = ""
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

    $preflightWorkflow = Invoke-PreflightAutoTuneWorkflow `
        -InputPath $InputPath `
        -Selected $selected `
        -SourceProfile $sourceProfile `
        -EncodeMode 'software' `
        -SourceDurationSec $sourceDuration `
        -SourceSizeBytes $sourceItem.Length `
        -AutoSettings $autoSettings `
        -InitialResolvedCRF $resolvedCRF `
        -InitialResolvedPreset $resolvedPreset `
        -InitialResolvedFilmGrain $resolvedFilmGrain

    $preflightEstimate = $preflightWorkflow.FinalPreflight
    $resolvedCRF = [int]$preflightWorkflow.FinalResolvedCRF
    $resolvedPreset = [int]$preflightWorkflow.FinalResolvedPreset
    $resolvedFilmGrain = [int]$preflightWorkflow.FinalResolvedFilmGrain
    $resolvedCRFLabel = [string]$resolvedCRF
    $resolvedPresetLabel = [string]$resolvedPreset

    if ($preflightEstimate.Ran) {
        Write-Host ("Preflight estimate: {0:F2} GiB (projected savings {1:F1}%)" -f $preflightEstimate.EstimatedFinalSizeGiB, $preflightEstimate.EstimatedSavingsPercent) -ForegroundColor DarkCyan
        if ($preflightWorkflow.WasSkippedByPreflight) {
            Write-Host "Decision: skipped (estimated output exceeds threshold)" -ForegroundColor Yellow
            Write-Host ""

            Write-LogRow @{
                Timestamp         = (Get-Date).ToString("s")
                Status            = "PRECHECK_SKIPPED_UNFAVORABLE"
                InputPath         = $InputPath
                OutputPath        = ""
                SourceSizeGiB     = $sourceSizeGiB
                OutputSizeGiB     = ""
                ReductionPercent  = ""
                SourceDurationSec = [Math]::Round($sourceDuration, 3)
                OutputDurationSec = ""
                ElapsedSec        = ""
                Profile           = $sourceProfile.Profile
                HasHDR            = $sourceProfile.HasHDR
                HasDV             = $sourceProfile.HasDV
                SelectedAudio     = $selectedAudioSummary
                SelectedSubtitles = $selectedSubtitleSummary
                EstimatedFinalSizeGiB = [Math]::Round($preflightEstimate.EstimatedFinalSizeGiB, 3)
                EstimatedSavingsPercent = [Math]::Round($preflightEstimate.EstimatedSavingsPercent, 2)
                EstimatedOutputGiBPerHour = [Math]::Round($preflightEstimate.EstimatedOutputGiBPerHour, 3)
                InitialResolvedCRF = $preflightWorkflow.InitialResolvedCRF
                InitialResolvedPreset = $preflightWorkflow.InitialResolvedPreset
                InitialResolvedFilmGrain = $preflightWorkflow.InitialResolvedFilmGrain
                PreflightPassCount = $preflightWorkflow.PreflightPassCount
                Preflight1EstimatedFinalGiB = if ($preflightWorkflow.Preflight1 -and $preflightWorkflow.Preflight1.Ran) { [Math]::Round($preflightWorkflow.Preflight1.EstimatedFinalSizeGiB, 3) } else { "" }
                Preflight1EstimatedSavingsPercent = if ($preflightWorkflow.Preflight1 -and $preflightWorkflow.Preflight1.Ran) { [Math]::Round($preflightWorkflow.Preflight1.EstimatedSavingsPercent, 2) } else { "" }
                Preflight1EstimatedGiBPerHour = if ($preflightWorkflow.Preflight1 -and $preflightWorkflow.Preflight1.Ran) { [Math]::Round($preflightWorkflow.Preflight1.EstimatedOutputGiBPerHour, 3) } else { "" }
                Preflight2EstimatedFinalGiB = if ($preflightWorkflow.Preflight2 -and $preflightWorkflow.Preflight2.Ran) { [Math]::Round($preflightWorkflow.Preflight2.EstimatedFinalSizeGiB, 3) } else { "" }
                Preflight2EstimatedSavingsPercent = if ($preflightWorkflow.Preflight2 -and $preflightWorkflow.Preflight2.Ran) { [Math]::Round($preflightWorkflow.Preflight2.EstimatedSavingsPercent, 2) } else { "" }
                Preflight2EstimatedGiBPerHour = if ($preflightWorkflow.Preflight2 -and $preflightWorkflow.Preflight2.Ran) { [Math]::Round($preflightWorkflow.Preflight2.EstimatedOutputGiBPerHour, 3) } else { "" }
                FinalResolvedCRF = $preflightWorkflow.FinalResolvedCRF
                FinalResolvedPreset = $preflightWorkflow.FinalResolvedPreset
                FinalResolvedFilmGrain = $preflightWorkflow.FinalResolvedFilmGrain
                PreflightAutoTuneReason = $preflightWorkflow.PreflightAutoTuneReason
                WasPreflightRetuned = "$($preflightWorkflow.WasPreflightRetuned)"
                WasSkippedByPreflight = 'True'
                CRF               = $CRF
                Preset            = $Preset
                FilmGrain         = $FilmGrain
                AutoCRFOffset     = $AutoCRFOffset
                EncoderPreference = $EncoderPreference
                ResolvedEncodeLane = 'CPU'
                LaneSelectionReason = 'forced CPU lane by encoder preference'
                EncodeMode        = 'software'
                ResolvedCRF       = $preflightWorkflow.FinalResolvedCRF
                ResolvedPreset    = $preflightWorkflow.FinalResolvedPreset
                ResolvedFilmGrain = $preflightWorkflow.FinalResolvedFilmGrain
                ResolvedCQ        = ""
                ResolvedNvencPreset = ""
                ResolvedNvencTune = ""
                ResolvedDecodePath = ""
                AutoReason        = $preflightWorkflow.PreflightAutoTuneReason
                BPP               = [Math]::Round($autoSettings.BPP, 6)
                EffectiveVideoBitrate = $autoSettings.VideoBitrate
                VideoBitratePerHourGiB = [Math]::Round($autoSettings.VideoBitratePerHourGiB, 3)
                ResolutionTier    = $autoSettings.ResolutionTier
                CodecClass        = $autoSettings.CodecClass
                GrainClass        = $autoSettings.GrainClass
                GrainScore        = $autoSettings.GrainScore
                WasAutoSkipped    = "False"
                NvencWorkerCountAtStart = ""
                NvencEngineCountDetected = ""
                NvencCapacitySource = ""
                DetectedGpuName   = ""
                FilmGrainDisabledReason = ""
                FfmpegPath        = $FfmpegPath
                FfprobePath       = $FfprobePath
                Notes             = $preflightWorkflow.PreflightAutoTuneReason
            }
            return
        }

        if ($preflightEstimate.WarningTriggered) {
            Write-Host ("Warning: projected output is {0:F1}% of source size." -f $preflightEstimate.EstimatedPctOfSource) -ForegroundColor Yellow
        }
        Write-Host "Proceeding with full encode" -ForegroundColor DarkCyan
    } elseif ($EnablePreflightEstimate -and -not [string]::IsNullOrWhiteSpace($preflightEstimate.Reason)) {
        Write-Warning $preflightEstimate.Reason
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
        EstimatedFinalSizeGiB = if ($preflightEstimate.Ran) { [Math]::Round($preflightEstimate.EstimatedFinalSizeGiB, 3) } else { "" }
        EstimatedSavingsPercent = if ($preflightEstimate.Ran) { [Math]::Round($preflightEstimate.EstimatedSavingsPercent, 2) } else { "" }
        EstimatedOutputGiBPerHour = if ($preflightEstimate.Ran) { [Math]::Round($preflightEstimate.EstimatedOutputGiBPerHour, 3) } else { "" }
        InitialResolvedCRF = $preflightWorkflow.InitialResolvedCRF
        InitialResolvedPreset = $preflightWorkflow.InitialResolvedPreset
        InitialResolvedFilmGrain = $preflightWorkflow.InitialResolvedFilmGrain
        PreflightPassCount = $preflightWorkflow.PreflightPassCount
        Preflight1EstimatedFinalGiB = if ($preflightWorkflow.Preflight1 -and $preflightWorkflow.Preflight1.Ran) { [Math]::Round($preflightWorkflow.Preflight1.EstimatedFinalSizeGiB, 3) } else { "" }
        Preflight1EstimatedSavingsPercent = if ($preflightWorkflow.Preflight1 -and $preflightWorkflow.Preflight1.Ran) { [Math]::Round($preflightWorkflow.Preflight1.EstimatedSavingsPercent, 2) } else { "" }
        Preflight1EstimatedGiBPerHour = if ($preflightWorkflow.Preflight1 -and $preflightWorkflow.Preflight1.Ran) { [Math]::Round($preflightWorkflow.Preflight1.EstimatedOutputGiBPerHour, 3) } else { "" }
        Preflight2EstimatedFinalGiB = if ($preflightWorkflow.Preflight2 -and $preflightWorkflow.Preflight2.Ran) { [Math]::Round($preflightWorkflow.Preflight2.EstimatedFinalSizeGiB, 3) } else { "" }
        Preflight2EstimatedSavingsPercent = if ($preflightWorkflow.Preflight2 -and $preflightWorkflow.Preflight2.Ran) { [Math]::Round($preflightWorkflow.Preflight2.EstimatedSavingsPercent, 2) } else { "" }
        Preflight2EstimatedGiBPerHour = if ($preflightWorkflow.Preflight2 -and $preflightWorkflow.Preflight2.Ran) { [Math]::Round($preflightWorkflow.Preflight2.EstimatedOutputGiBPerHour, 3) } else { "" }
        FinalResolvedCRF = $preflightWorkflow.FinalResolvedCRF
        FinalResolvedPreset = $preflightWorkflow.FinalResolvedPreset
        FinalResolvedFilmGrain = $preflightWorkflow.FinalResolvedFilmGrain
        PreflightAutoTuneReason = $preflightWorkflow.PreflightAutoTuneReason
        WasPreflightRetuned = "$($preflightWorkflow.WasPreflightRetuned)"
        WasSkippedByPreflight = "$($preflightWorkflow.WasSkippedByPreflight)"
        CRF          = $CRF
        Preset       = $Preset
        FilmGrain    = $FilmGrain
        AutoCRFOffset = $AutoCRFOffset
        EncoderPreference = $EncoderPreference
        ResolvedEncodeLane = 'CPU'
        LaneSelectionReason = 'forced CPU lane by encoder preference'
        WorkerProcessPriority = $SoftwareEncodePriority
        ScriptProcessPriority = $script:ResolvedScriptProcessPriority
        EncodeMode   = 'software'
        ResolvedCRF  = $resolvedCRF
        ResolvedPreset = $resolvedPreset
        ResolvedFilmGrain = $resolvedFilmGrain
        ResolvedCQ   = ''
        ResolvedNvencPreset = ''
        ResolvedNvencTune = ''
        ResolvedDecodePath = ''
        AutoReason   = if (-not [string]::IsNullOrWhiteSpace($preflightWorkflow.PreflightAutoTuneReason)) { $preflightWorkflow.PreflightAutoTuneReason } else { $autoSettings.Reason }
        BPP          = [Math]::Round($autoSettings.BPP, 6)
        EffectiveVideoBitrate = $autoSettings.VideoBitrate
        VideoBitratePerHourGiB = [Math]::Round($autoSettings.VideoBitratePerHourGiB, 3)
        ResolutionTier = $autoSettings.ResolutionTier
        CodecClass   = $autoSettings.CodecClass
        GrainClass   = $autoSettings.GrainClass
        GrainScore   = $autoSettings.GrainScore
        WasAutoSkipped = $false
        NvencWorkerCountAtStart = ''
        NvencEngineCountDetected = ''
        NvencCapacitySource = ''
        DetectedGpuName = ''
        FilmGrainDisabledReason = ''
    } | ConvertTo-Json -Depth 8

    Set-Content -LiteralPath $StatePath -Value $currentState -Encoding UTF8

    # ── Print encode header ───────────────────────────────────────────────────
    Write-Host ""
    Write-Host "------------------------------------------------------------" -ForegroundColor DarkGray
    Write-Host "Source   : $InputPath"                                          -ForegroundColor Green
    Write-Host "Encoding : $displayOutputName"                                  -ForegroundColor Green
    Write-Host "Encoder Preference: $EncoderPreference"                         -ForegroundColor Green
    Write-Host "Resolved Lane: CPU (forced CPU lane by encoder preference)"     -ForegroundColor Green
    Write-Host ("Profile : {0}" -f (Add-RainbowHdrHighlights -Text $sourceProfile.Profile)) -ForegroundColor Green
    Write-Host ("Source Color: {0}" -f (Add-RainbowHdrHighlights -Text $sourceProfile.SourceColorSummary)) -ForegroundColor Green
    Write-Host ("Encode Color: {0}" -f (Add-RainbowHdrHighlights -Text $encodeColorProfile.Summary)) -ForegroundColor Green
    if (-not [string]::IsNullOrWhiteSpace($encodeColorProfile.Note)) {
        Write-Host "Color Note : $($encodeColorProfile.Note)"                   -ForegroundColor Yellow
    }
    if ($preflightEstimate.Ran) {
        Write-Host ("Preflight  : {0:F2} GiB estimate (projected savings {1:F1}%)" -f $preflightEstimate.EstimatedFinalSizeGiB, $preflightEstimate.EstimatedSavingsPercent) -ForegroundColor Green
    }
    if ($preflightWorkflow.WasPreflightRetuned -and -not [string]::IsNullOrWhiteSpace($preflightWorkflow.PreflightAutoTuneReason)) {
        Write-Host "Preflight Tune: $($preflightWorkflow.PreflightAutoTuneReason)" -ForegroundColor Green
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
    Write-SessionEncodeStart -Init ([pscustomobject]@{
        DisplayInputName      = $displayInputName
        DisplayOutputName     = $displayOutputName
        ResolvedEncodeLane    = 'CPU'
        SourceProfile         = $sourceProfile
        EncodeColorProfile    = $encodeColorProfile
        PreflightWorkflow     = $preflightWorkflow
        EffectiveFilmGrain    = $resolvedFilmGrain
        WorkerProcessPriority = $SoftwareEncodePriority
        AutoSettings          = $autoSettings
        NvencSettings         = $null
        LaneSelectionReason   = 'forced CPU lane by encoder preference'
    })

    # ── Launch ffmpeg with redirected stderr ──────────────────────────────────
    $psi                       = [System.Diagnostics.ProcessStartInfo]::new()
    $psi.FileName              = $FfmpegPath
    $psi.RedirectStandardError = $true
    $psi.RedirectStandardInput = $true
    $psi.UseShellExecute       = $false
    $psi.CreateNoWindow        = $false

    foreach ($a in $ffArgs) { $psi.ArgumentList.Add($a) }

    $proc           = [System.Diagnostics.Process]::new()
    $proc.StartInfo = $psi

    # Shared state between the async stderr callback and the main UI loop.
    # [hashtable]::Synchronized wraps every read/write in a monitor lock so
    # the threadpool callback and the main thread cannot race on these values.
    $shared = [hashtable]::Synchronized(@{
        OutTimeSec                  = 0.0
        OutSizeBytes                = 0.0
        SpeedX                      = 0.0
        LogLines                    = [System.Collections.Generic.List[string]]::new()
        SmoothedEstimatedFinalBytes = 0.0
        LastRawEstimatedFinalBytes  = 0.0
        EstimatedSavingsPercent     = 0.0
        EstimatedOutputGiBPerHour   = 0.0
        EstimateReady               = $false
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
    $workerPriorityResolution = Set-TrackedProcessPriority -Process $proc -PriorityName $SoftwareEncodePriority
    if ($workerPriorityResolution.Warning) {
        Write-Warning $workerPriorityResolution.Warning
    }
    $stderrAsync = $stderrPs.BeginInvoke()

    # ── Live UI loop ──────────────────────────────────────────────────────────
    $uiFileName     = $displayOutputName
    $uiLineCount    = -1   # -1 signals first paint; no cursor-up on first call
    $shutdownRequested = $false
    while (-not $proc.HasExited) {
        if (Test-QueueShutdownRequested) {
            $shutdownRequested = $true
            Write-Host "Shutdown: requesting active software encode to stop gracefully..." -ForegroundColor Yellow
            Request-FfmpegProcessQuit -Process $proc
            break
        }

        $uiLineCount = Write-ProgressUI `
            -FileName          $uiFileName `
            -Profile           $sourceProfile.Profile `
            -EncodeColorLabel  $encodeColorLabel `
            -CRFLabel          $resolvedCRFLabel `
            -PresetLabel       $resolvedPresetLabel `
            -SourceDurationSec $sourceDuration `
            -SourceSizeBytes   $sourceItem.Length `
            -ElapsedSec        $stopwatch.Elapsed.TotalSeconds `
            -OutTimeSec        $shared.OutTimeSec `
            -OutSizeBytes      $shared.OutSizeBytes `
            -SpeedX            $shared.SpeedX `
            -EstimateState     $shared `
            -UICursorRow       $uiLineCount

        Start-Sleep -Milliseconds 200
    }

    if ($shutdownRequested) {
        $deadline = (Get-Date).AddSeconds(20)
        while (-not $proc.HasExited -and (Get-Date) -lt $deadline) {
            Start-Sleep -Milliseconds 200
        }
        if (-not $proc.HasExited) {
            Write-Host "Shutdown: software encode did not exit in time; terminating ffmpeg." -ForegroundColor Yellow
            try { $proc.Kill() } catch {}
        }
    }

    $proc.WaitForExit()
    $ffExit = $proc.ExitCode
    $proc.Dispose()

    # Wait for the stderr reader to finish draining, then tear down its runspace.
    $null = $stderrPs.EndInvoke($stderrAsync)
    $stderrPs.Dispose()
    $stderrRunspace.Close()
    $stderrRunspace.Dispose()

    if ($shutdownRequested) {
        if (Test-Path -LiteralPath $tempOutput) {
            Remove-Item -LiteralPath $tempOutput -Force -ErrorAction SilentlyContinue
        }
        Write-Host ("Shutdown: requeued {0}" -f ([System.IO.Path]::GetFileName($InputPath))) -ForegroundColor DarkYellow
        throw $script:QueueShutdownSentinel
    }

    # Final paint: snap to 100% on success, leave at actual position on failure.
    $null = Write-ProgressUI `
        -FileName          $uiFileName `
        -Profile           $sourceProfile.Profile `
        -EncodeColorLabel  $encodeColorLabel `
        -CRFLabel          $resolvedCRFLabel `
        -PresetLabel       $resolvedPresetLabel `
        -SourceDurationSec $sourceDuration `
        -SourceSizeBytes   $sourceItem.Length `
        -ElapsedSec        $stopwatch.Elapsed.TotalSeconds `
        -OutTimeSec        $(if ($ffExit -eq 0) { $sourceDuration } else { $shared.OutTimeSec }) `
        -OutSizeBytes      $shared.OutSizeBytes `
        -SpeedX            $shared.SpeedX `
        -EstimateState     $shared `
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
    $finalOutputGiBPerHour = if ($sourceDuration -gt 0) { [Math]::Round($outputSizeGiB / ($sourceDuration / 3600.0), 3) } else { "" }

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
        EstimatedFinalSizeGiB = $outputSizeGiB
        EstimatedSavingsPercent = $reduction
        EstimatedOutputGiBPerHour = $finalOutputGiBPerHour
        InitialResolvedCRF = $preflightWorkflow.InitialResolvedCRF
        InitialResolvedPreset = $preflightWorkflow.InitialResolvedPreset
        InitialResolvedFilmGrain = $preflightWorkflow.InitialResolvedFilmGrain
        PreflightPassCount = $preflightWorkflow.PreflightPassCount
        Preflight1EstimatedFinalGiB = if ($preflightWorkflow.Preflight1 -and $preflightWorkflow.Preflight1.Ran) { [Math]::Round($preflightWorkflow.Preflight1.EstimatedFinalSizeGiB, 3) } else { "" }
        Preflight1EstimatedSavingsPercent = if ($preflightWorkflow.Preflight1 -and $preflightWorkflow.Preflight1.Ran) { [Math]::Round($preflightWorkflow.Preflight1.EstimatedSavingsPercent, 2) } else { "" }
        Preflight1EstimatedGiBPerHour = if ($preflightWorkflow.Preflight1 -and $preflightWorkflow.Preflight1.Ran) { [Math]::Round($preflightWorkflow.Preflight1.EstimatedOutputGiBPerHour, 3) } else { "" }
        Preflight2EstimatedFinalGiB = if ($preflightWorkflow.Preflight2 -and $preflightWorkflow.Preflight2.Ran) { [Math]::Round($preflightWorkflow.Preflight2.EstimatedFinalSizeGiB, 3) } else { "" }
        Preflight2EstimatedSavingsPercent = if ($preflightWorkflow.Preflight2 -and $preflightWorkflow.Preflight2.Ran) { [Math]::Round($preflightWorkflow.Preflight2.EstimatedSavingsPercent, 2) } else { "" }
        Preflight2EstimatedGiBPerHour = if ($preflightWorkflow.Preflight2 -and $preflightWorkflow.Preflight2.Ran) { [Math]::Round($preflightWorkflow.Preflight2.EstimatedOutputGiBPerHour, 3) } else { "" }
        FinalResolvedCRF = $preflightWorkflow.FinalResolvedCRF
        FinalResolvedPreset = $preflightWorkflow.FinalResolvedPreset
        FinalResolvedFilmGrain = $preflightWorkflow.FinalResolvedFilmGrain
        PreflightAutoTuneReason = $preflightWorkflow.PreflightAutoTuneReason
        WasPreflightRetuned = "$($preflightWorkflow.WasPreflightRetuned)"
        WasSkippedByPreflight = "$($preflightWorkflow.WasSkippedByPreflight)"
        CRF               = $CRF
        Preset            = $Preset
        FilmGrain         = $FilmGrain
        AutoCRFOffset     = $AutoCRFOffset
        EncoderPreference = $EncoderPreference
        ResolvedEncodeLane = 'CPU'
        LaneSelectionReason = 'forced CPU lane by encoder preference'
        EncodeMode        = 'software'
        ResolvedCRF       = $preflightWorkflow.FinalResolvedCRF
        ResolvedPreset    = $preflightWorkflow.FinalResolvedPreset
        ResolvedFilmGrain = $resolvedFilmGrain
        ResolvedCQ        = ""
        ResolvedNvencPreset = ""
        ResolvedNvencTune = ""
        ResolvedDecodePath = ""
        AutoReason        = if (-not [string]::IsNullOrWhiteSpace($preflightWorkflow.PreflightAutoTuneReason)) { $preflightWorkflow.PreflightAutoTuneReason } else { $autoSettings.Reason }
        BPP               = [Math]::Round($autoSettings.BPP, 6)
        EffectiveVideoBitrate = $autoSettings.VideoBitrate
        VideoBitratePerHourGiB = [Math]::Round($autoSettings.VideoBitratePerHourGiB, 3)
        ResolutionTier    = $autoSettings.ResolutionTier
        CodecClass        = $autoSettings.CodecClass
        GrainClass        = $autoSettings.GrainClass
        GrainScore        = $autoSettings.GrainScore
        WasAutoSkipped    = "False"
        NvencWorkerCountAtStart = ""
        NvencEngineCountDetected = ""
        NvencCapacitySource = ""
        DetectedGpuName   = ""
        FilmGrainDisabledReason = ""
        FfmpegPath        = $FfmpegPath
        FfprobePath       = $FfprobePath
        Notes             = if ($preflightEstimate.Ran) { $autoSettings.BitrateReason + ' | ' + $preflightWorkflow.PreflightAutoTuneReason } else { $autoSettings.BitrateReason }
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
                EstimatedFinalSizeGiB = Get-OptionalProperty $interrupted 'EstimatedFinalSizeGiB' ''
                EstimatedSavingsPercent = Get-OptionalProperty $interrupted 'EstimatedSavingsPercent' ''
                EstimatedOutputGiBPerHour = Get-OptionalProperty $interrupted 'EstimatedOutputGiBPerHour' ''
                InitialResolvedCRF = Get-OptionalProperty $interrupted 'InitialResolvedCRF' ''
                InitialResolvedPreset = Get-OptionalProperty $interrupted 'InitialResolvedPreset' ''
                InitialResolvedFilmGrain = Get-OptionalProperty $interrupted 'InitialResolvedFilmGrain' ''
                PreflightPassCount = Get-OptionalProperty $interrupted 'PreflightPassCount' ''
                Preflight1EstimatedFinalGiB = Get-OptionalProperty $interrupted 'Preflight1EstimatedFinalGiB' ''
                Preflight1EstimatedSavingsPercent = Get-OptionalProperty $interrupted 'Preflight1EstimatedSavingsPercent' ''
                Preflight1EstimatedGiBPerHour = Get-OptionalProperty $interrupted 'Preflight1EstimatedGiBPerHour' ''
                Preflight2EstimatedFinalGiB = Get-OptionalProperty $interrupted 'Preflight2EstimatedFinalGiB' ''
                Preflight2EstimatedSavingsPercent = Get-OptionalProperty $interrupted 'Preflight2EstimatedSavingsPercent' ''
                Preflight2EstimatedGiBPerHour = Get-OptionalProperty $interrupted 'Preflight2EstimatedGiBPerHour' ''
                FinalResolvedCRF = Get-OptionalProperty $interrupted 'FinalResolvedCRF' ''
                FinalResolvedPreset = Get-OptionalProperty $interrupted 'FinalResolvedPreset' ''
                FinalResolvedFilmGrain = Get-OptionalProperty $interrupted 'FinalResolvedFilmGrain' ''
                PreflightAutoTuneReason = Get-OptionalProperty $interrupted 'PreflightAutoTuneReason' ''
                WasPreflightRetuned = Get-OptionalProperty $interrupted 'WasPreflightRetuned' 'False'
                WasSkippedByPreflight = Get-OptionalProperty $interrupted 'WasSkippedByPreflight' 'False'
                CRF               = $interrupted.CRF
                Preset            = $interrupted.Preset
                FilmGrain         = $interrupted.FilmGrain
                AutoCRFOffset     = Get-OptionalProperty $interrupted 'AutoCRFOffset' ''
                EncoderPreference = Get-OptionalProperty $interrupted 'EncoderPreference' $EncoderPreference
                ResolvedEncodeLane = Get-OptionalProperty $interrupted 'ResolvedEncodeLane' 'CPU'
                LaneSelectionReason = Get-OptionalProperty $interrupted 'LaneSelectionReason' ''
                WorkerProcessPriority = Get-OptionalProperty $interrupted 'WorkerProcessPriority' ''
                ScriptProcessPriority = Get-OptionalProperty $interrupted 'ScriptProcessPriority' $script:ResolvedScriptProcessPriority
                EncodeMode        = Get-OptionalProperty $interrupted 'EncodeMode' 'software'
                ResolvedCRF       = Get-OptionalProperty $interrupted 'ResolvedCRF' ''
                ResolvedPreset    = Get-OptionalProperty $interrupted 'ResolvedPreset' ''
                ResolvedFilmGrain = Get-OptionalProperty $interrupted 'ResolvedFilmGrain' ''
                ResolvedCQ        = Get-OptionalProperty $interrupted 'ResolvedCQ' ''
                ResolvedNvencPreset = Get-OptionalProperty $interrupted 'ResolvedNvencPreset' ''
                ResolvedNvencTune = Get-OptionalProperty $interrupted 'ResolvedNvencTune' ''
                ResolvedDecodePath = Get-OptionalProperty $interrupted 'ResolvedDecodePath' ''
                AutoReason        = Get-OptionalProperty $interrupted 'AutoReason' ''
                BPP               = Get-OptionalProperty $interrupted 'BPP' ''
                EffectiveVideoBitrate = Get-OptionalProperty $interrupted 'EffectiveVideoBitrate' ''
                VideoBitratePerHourGiB = Get-OptionalProperty $interrupted 'VideoBitratePerHourGiB' ''
                ResolutionTier    = Get-OptionalProperty $interrupted 'ResolutionTier' ''
                CodecClass        = Get-OptionalProperty $interrupted 'CodecClass' ''
                GrainClass        = Get-OptionalProperty $interrupted 'GrainClass' ''
                GrainScore        = Get-OptionalProperty $interrupted 'GrainScore' ''
                WasAutoSkipped    = Get-OptionalProperty $interrupted 'WasAutoSkipped' 'False'
                NvencWorkerCountAtStart = Get-OptionalProperty $interrupted 'NvencWorkerCountAtStart' ''
                NvencEngineCountDetected = Get-OptionalProperty $interrupted 'NvencEngineCountDetected' ''
                NvencCapacitySource = Get-OptionalProperty $interrupted 'NvencCapacitySource' ''
                DetectedGpuName   = Get-OptionalProperty $interrupted 'DetectedGpuName' ''
                FilmGrainDisabledReason = Get-OptionalProperty $interrupted 'FilmGrainDisabledReason' ''
                FfmpegPath        = $FfmpegPath
                FfprobePath       = $FfprobePath
                Notes             = "Process was interrupted. Temp output may exist at: $tempPath"
            }
        } catch {
            Write-Warning "Could not parse interrupted state file: $_"
        }

        Remove-Item -LiteralPath $StatePath -Force -ErrorAction SilentlyContinue
    }

    $staleWorkingJobs = @(Get-ChildItem -LiteralPath $QueueWorkingDir -Filter *.json -File -ErrorAction SilentlyContinue | Sort-Object CreationTimeUtc)
    foreach ($staleJob in $staleWorkingJobs) {
        $requeuePath = Join-Path $QueuePendingDir $staleJob.Name
        try {
            if (Test-Path -LiteralPath $requeuePath) {
                Remove-Item -LiteralPath $staleJob.FullName -Force -ErrorAction SilentlyContinue
                continue
            }

            Move-Item -LiteralPath $staleJob.FullName -Destination $requeuePath -Force
            Write-Warning "Recovered stale working queue item back to pending: $($staleJob.Name)"
        } catch {
            Write-Warning "Could not recover stale working queue item $($staleJob.FullName): $($_.Exception.Message)"
        }
    }

    switch ($EncoderPreference) {
        'Nvidia' {
            Invoke-NvencQueueProcessing -NvencEnvironment $script:NvencEnvironment
            return
        }
        'Auto' {
            Invoke-AutoEncoderLaneQueueProcessing -NvencEnvironment $script:NvencEnvironment
            return
        }
    }

    while ($true) {
        if (Test-QueueShutdownRequested) { break }

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
            if ($message -eq $script:QueueShutdownSentinel) {
                Requeue-WorkingJob -WorkingJobPath $workingJobPath
                break
            }

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
                EstimatedFinalSizeGiB = if ($state) { Get-OptionalProperty $state 'EstimatedFinalSizeGiB' '' } else { "" }
                EstimatedSavingsPercent = if ($state) { Get-OptionalProperty $state 'EstimatedSavingsPercent' '' } else { "" }
                EstimatedOutputGiBPerHour = if ($state) { Get-OptionalProperty $state 'EstimatedOutputGiBPerHour' '' } else { "" }
                InitialResolvedCRF = if ($state) { Get-OptionalProperty $state 'InitialResolvedCRF' '' } else { "" }
                InitialResolvedPreset = if ($state) { Get-OptionalProperty $state 'InitialResolvedPreset' '' } else { "" }
                InitialResolvedFilmGrain = if ($state) { Get-OptionalProperty $state 'InitialResolvedFilmGrain' '' } else { "" }
                PreflightPassCount = if ($state) { Get-OptionalProperty $state 'PreflightPassCount' '' } else { "" }
                Preflight1EstimatedFinalGiB = if ($state) { Get-OptionalProperty $state 'Preflight1EstimatedFinalGiB' '' } else { "" }
                Preflight1EstimatedSavingsPercent = if ($state) { Get-OptionalProperty $state 'Preflight1EstimatedSavingsPercent' '' } else { "" }
                Preflight1EstimatedGiBPerHour = if ($state) { Get-OptionalProperty $state 'Preflight1EstimatedGiBPerHour' '' } else { "" }
                Preflight2EstimatedFinalGiB = if ($state) { Get-OptionalProperty $state 'Preflight2EstimatedFinalGiB' '' } else { "" }
                Preflight2EstimatedSavingsPercent = if ($state) { Get-OptionalProperty $state 'Preflight2EstimatedSavingsPercent' '' } else { "" }
                Preflight2EstimatedGiBPerHour = if ($state) { Get-OptionalProperty $state 'Preflight2EstimatedGiBPerHour' '' } else { "" }
                FinalResolvedCRF = if ($state) { Get-OptionalProperty $state 'FinalResolvedCRF' '' } else { "" }
                FinalResolvedPreset = if ($state) { Get-OptionalProperty $state 'FinalResolvedPreset' '' } else { "" }
                FinalResolvedFilmGrain = if ($state) { Get-OptionalProperty $state 'FinalResolvedFilmGrain' '' } else { "" }
                PreflightAutoTuneReason = if ($state) { Get-OptionalProperty $state 'PreflightAutoTuneReason' '' } else { "" }
                WasPreflightRetuned = if ($state) { Get-OptionalProperty $state 'WasPreflightRetuned' 'False' } else { "False" }
                WasSkippedByPreflight = if ($state) { Get-OptionalProperty $state 'WasSkippedByPreflight' 'False' } else { "False" }
                CRF               = if ($state) { Get-OptionalProperty $state 'CRF' $CRF } else { $CRF }
                Preset            = if ($state) { Get-OptionalProperty $state 'Preset' $Preset } else { $Preset }
                FilmGrain         = if ($state) { Get-OptionalProperty $state 'FilmGrain' $FilmGrain } else { $FilmGrain }
                AutoCRFOffset     = if ($state) { Get-OptionalProperty $state 'AutoCRFOffset' $AutoCRFOffset } else { $AutoCRFOffset }
                EncoderPreference = if ($state) { Get-OptionalProperty $state 'EncoderPreference' $EncoderPreference } else { $EncoderPreference }
                ResolvedEncodeLane = if ($state) { Get-OptionalProperty $state 'ResolvedEncodeLane' 'CPU' } else { 'CPU' }
                LaneSelectionReason = if ($state) { Get-OptionalProperty $state 'LaneSelectionReason' '' } else { 'forced CPU lane by encoder preference' }
                WorkerProcessPriority = if ($state) { Get-OptionalProperty $state 'WorkerProcessPriority' '' } else { '' }
                ScriptProcessPriority = if ($state) { Get-OptionalProperty $state 'ScriptProcessPriority' $script:ResolvedScriptProcessPriority } else { $script:ResolvedScriptProcessPriority }
                EncodeMode        = if ($state) { Get-OptionalProperty $state 'EncodeMode' 'software' } else { 'software' }
                ResolvedCRF       = if ($state) { Get-OptionalProperty $state 'ResolvedCRF' '' } else { "" }
                ResolvedPreset    = if ($state) { Get-OptionalProperty $state 'ResolvedPreset' '' } else { "" }
                ResolvedFilmGrain = if ($state) { Get-OptionalProperty $state 'ResolvedFilmGrain' '' } else { "" }
                ResolvedCQ        = if ($state) { Get-OptionalProperty $state 'ResolvedCQ' '' } else { "" }
                ResolvedNvencPreset = if ($state) { Get-OptionalProperty $state 'ResolvedNvencPreset' '' } else { "" }
                ResolvedNvencTune = if ($state) { Get-OptionalProperty $state 'ResolvedNvencTune' '' } else { "" }
                ResolvedDecodePath = if ($state) { Get-OptionalProperty $state 'ResolvedDecodePath' '' } else { "" }
                AutoReason        = if ($state) { Get-OptionalProperty $state 'AutoReason' '' } else { "" }
                BPP               = if ($state) { Get-OptionalProperty $state 'BPP' '' } else { "" }
                EffectiveVideoBitrate = if ($state) { Get-OptionalProperty $state 'EffectiveVideoBitrate' '' } else { "" }
                VideoBitratePerHourGiB = if ($state) { Get-OptionalProperty $state 'VideoBitratePerHourGiB' '' } else { "" }
                ResolutionTier    = if ($state) { Get-OptionalProperty $state 'ResolutionTier' '' } else { "" }
                CodecClass        = if ($state) { Get-OptionalProperty $state 'CodecClass' '' } else { "" }
                GrainClass        = if ($state) { Get-OptionalProperty $state 'GrainClass' '' } else { "" }
                GrainScore        = if ($state) { Get-OptionalProperty $state 'GrainScore' '' } else { "" }
                WasAutoSkipped    = if ($state) { Get-OptionalProperty $state 'WasAutoSkipped' 'False' } else { "False" }
                NvencWorkerCountAtStart = if ($state) { Get-OptionalProperty $state 'NvencWorkerCountAtStart' '' } else { "" }
                NvencEngineCountDetected = if ($state) { Get-OptionalProperty $state 'NvencEngineCountDetected' '' } else { "" }
                NvencCapacitySource = if ($state) { Get-OptionalProperty $state 'NvencCapacitySource' '' } else { "" }
                DetectedGpuName   = if ($state) { Get-OptionalProperty $state 'DetectedGpuName' '' } else { "" }
                FilmGrainDisabledReason = if ($state) { Get-OptionalProperty $state 'FilmGrainDisabledReason' '' } else { "" }
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
$EncoderPreference = Resolve-EncoderPreferenceConfigValue -Name 'EncoderPreference' -Value $EncoderPreference
$SoftwareEncodePriority = Resolve-ProcessPriorityConfigValue -Name 'SoftwareEncodePriority' -Value $SoftwareEncodePriority
$HardwareEncodePriority = Resolve-ProcessPriorityConfigValue -Name 'HardwareEncodePriority' -Value $HardwareEncodePriority
$ScriptProcessPriority = Resolve-ProcessPriorityConfigValue -Name 'ScriptProcessPriority' -Value $ScriptProcessPriority
$ApplyProcessPriority = Resolve-BooleanConfigValue -Name 'ApplyProcessPriority' -Value $ApplyProcessPriority
$NvencMaxParallel = Resolve-ConfigValue -Name 'NvencMaxParallel' -Value $NvencMaxParallel -Minimum 1 -Maximum 16
$NvencCQ = Resolve-ConfigValue -Name 'NvencCQ' -Value $NvencCQ -Minimum 0 -Maximum 63
$NvencPreset = Resolve-NvencPresetConfigValue -Name 'NvencPreset' -Value $NvencPreset
$NvencDecode = Resolve-NvencDecodeConfigValue -Name 'NvencDecode' -Value $NvencDecode
$NvencTune = Resolve-NvencTuneConfigValue -Name 'NvencTune' -Value $NvencTune
$EnablePreflightEstimate = Resolve-BooleanConfigValue -Name 'EnablePreflightEstimate' -Value $EnablePreflightEstimate
$PreflightSampleCount = Resolve-ConfigValue -Name 'PreflightSampleCount' -Value $PreflightSampleCount -Minimum 1 -Maximum 12
$PreflightSampleDurationSec = Resolve-ConfigValue -Name 'PreflightSampleDurationSec' -Value $PreflightSampleDurationSec -Minimum 5 -Maximum 300
$PreflightWarnIfEstimatedPctOfSource = Resolve-ConfigValue -Name 'PreflightWarnIfEstimatedPctOfSource' -Value $PreflightWarnIfEstimatedPctOfSource -Minimum 1 -Maximum 500
$PreflightAbortIfEstimatedPctOfSource = Resolve-ConfigValue -Name 'PreflightAbortIfEstimatedPctOfSource' -Value $PreflightAbortIfEstimatedPctOfSource -Minimum 1 -Maximum 500
$EnablePreflightAutoTune = Resolve-BooleanConfigValue -Name 'EnablePreflightAutoTune' -Value $EnablePreflightAutoTune
$EnableSecondPreflightPass = Resolve-BooleanConfigValue -Name 'EnableSecondPreflightPass' -Value $EnableSecondPreflightPass
$PreflightAutoTuneQuality = Resolve-PreflightAutoTuneQualityConfigValue -Name 'PreflightAutoTuneQuality' -Value $PreflightAutoTuneQuality
$PreflightAutoTuneCustomTargetGiBPerHour = Resolve-NullableDoubleRangeConfigValue -Name 'PreflightAutoTuneCustomTargetGiBPerHour' -Value $PreflightAutoTuneCustomTargetGiBPerHour -Minimum 0.1 -Maximum 100.0
$PreflightAutoTuneCustomUpperGiBPerHour = Resolve-NullableDoubleRangeConfigValue -Name 'PreflightAutoTuneCustomUpperGiBPerHour' -Value $PreflightAutoTuneCustomUpperGiBPerHour -Minimum 0.1 -Maximum 100.0
$PreflightAutoTuneCustomLowerGiBPerHour = Resolve-NullableDoubleRangeConfigValue -Name 'PreflightAutoTuneCustomLowerGiBPerHour' -Value $PreflightAutoTuneCustomLowerGiBPerHour -Minimum 0.1 -Maximum 100.0
$PreflightTinyOutputPctThreshold = Resolve-DoubleRangeConfigValue -Name 'PreflightTinyOutputPctThreshold' -Value $PreflightTinyOutputPctThreshold -Minimum 1.0 -Maximum 100.0
$PreflightTinyOutputAbsoluteGiBThreshold = Resolve-DoubleRangeConfigValue -Name 'PreflightTinyOutputAbsoluteGiBThreshold' -Value $PreflightTinyOutputAbsoluteGiBThreshold -Minimum 0.1 -Maximum 100.0
$EnableLiveSizeEstimate = Resolve-BooleanConfigValue -Name 'EnableLiveSizeEstimate' -Value $EnableLiveSizeEstimate
$LiveEstimateStartPercent = Resolve-ConfigValue -Name 'LiveEstimateStartPercent' -Value $LiveEstimateStartPercent -Minimum 1 -Maximum 99
$LiveEstimateSmoothingFactor = Resolve-DoubleRangeConfigValue -Name 'LiveEstimateSmoothingFactor' -Value $LiveEstimateSmoothingFactor -Minimum 0.01 -Maximum 1.0

if ($PreflightAbortIfEstimatedPctOfSource -lt $PreflightWarnIfEstimatedPctOfSource) {
    throw "PreflightAbortIfEstimatedPctOfSource must be greater than or equal to PreflightWarnIfEstimatedPctOfSource."
}
if ($null -ne $PreflightAutoTuneCustomTargetGiBPerHour -and $null -ne $PreflightAutoTuneCustomUpperGiBPerHour -and $PreflightAutoTuneCustomUpperGiBPerHour -lt $PreflightAutoTuneCustomTargetGiBPerHour) {
    throw "PreflightAutoTuneCustomUpperGiBPerHour must be greater than or equal to PreflightAutoTuneCustomTargetGiBPerHour."
}
if ($null -ne $PreflightAutoTuneCustomTargetGiBPerHour -and $null -ne $PreflightAutoTuneCustomLowerGiBPerHour -and $PreflightAutoTuneCustomLowerGiBPerHour -gt $PreflightAutoTuneCustomTargetGiBPerHour) {
    throw "PreflightAutoTuneCustomLowerGiBPerHour must be less than or equal to PreflightAutoTuneCustomTargetGiBPerHour."
}
if ($null -ne $PreflightAutoTuneCustomUpperGiBPerHour -and $null -ne $PreflightAutoTuneCustomLowerGiBPerHour -and $PreflightAutoTuneCustomUpperGiBPerHour -lt $PreflightAutoTuneCustomLowerGiBPerHour) {
    throw "PreflightAutoTuneCustomUpperGiBPerHour must be greater than or equal to PreflightAutoTuneCustomLowerGiBPerHour."
}

$script:FfmpegBuildInfo = Test-RequiredFfmpegBuild -ExecutablePath $FfmpegPath

$scriptPriorityResolution = Set-TrackedProcessPriority -Process (Get-Process -Id $PID) -PriorityName $ScriptProcessPriority
$script:ResolvedScriptProcessPriority = $scriptPriorityResolution.AppliedPriority
if ($scriptPriorityResolution.Warning) {
    Write-Warning $scriptPriorityResolution.Warning
}

Update-LogSchemaIfNeeded

$script:NvencEnvironment = $null
if ($EncoderPreference -eq 'Nvidia') {
    $script:NvencEnvironment = Get-NvencEnvironment
    $startupTuneResolution = Resolve-NvencTune -ConfiguredNvencTune $NvencTune -NvencEnvironment $script:NvencEnvironment
    if ($startupTuneResolution.Warning) {
        Write-Warning $startupTuneResolution.Warning
    }
} elseif ($EncoderPreference -eq 'Auto') {
    $script:NvencEnvironment = Try-Get-NvencEnvironment
    if ($script:NvencEnvironment) {
        $startupTuneResolution = Resolve-NvencTune -ConfiguredNvencTune $NvencTune -NvencEnvironment $script:NvencEnvironment
        if ($startupTuneResolution.Warning) {
            Write-Warning $startupTuneResolution.Warning
        }
    }
}

Initialize-ConsoleShutdownHandling
Initialize-TestHooks

$createdNew = $false
$mutex      = [System.Threading.Mutex]::new($false, $MutexName, [ref]$createdNew)

$hasLock = $false
try {
    $hasLock = $mutex.WaitOne(0)
    if ($hasLock) {
        Recover-StaleQueueArtifactsForEnqueue
    }

    if ($InputPaths -and $InputPaths.Count -gt 0) {
        Add-QueueInputs -Paths $InputPaths
    }

    if (-not $hasLock) {
        if ($InputPaths -and $InputPaths.Count -gt 0) {
            Write-Host "Another encode worker is already running. Files were added to queue." -ForegroundColor Yellow
        } else {
            Write-Host "Another encode worker is already running." -ForegroundColor Yellow
        }
        return
    }

    $pendingCount = @(Get-ChildItem -LiteralPath $QueuePendingDir -Filter *.json -File -ErrorAction SilentlyContinue).Count
    $workingCount = @(Get-ChildItem -LiteralPath $QueueWorkingDir -Filter *.json -File -ErrorAction SilentlyContinue).Count
    $hasInterruptedState = Test-Path -LiteralPath $StatePath
    if ((-not $InputPaths -or $InputPaths.Count -eq 0) -and $pendingCount -eq 0 -and $workingCount -eq 0 -and -not $hasInterruptedState) {
        Show-NoWorkToResumeMessage
        return
    }

    Start-SessionTextLog
    Write-SessionTextLogMessage -Level Info -Message ("Queue start: pending={0} working={1}" -f $pendingCount, $workingCount)
    if ($script:TestAutoShutdownSeconds -gt 0) {
        Write-SessionTextLogMessage -Level Warn -Message ("Test hook | auto shutdown after {0}s" -f $script:TestAutoShutdownSeconds)
    }

    try {
        Invoke-QueueProcessing
    } catch {
        $message = $_.Exception.Message
        $position = $_.InvocationInfo.PositionMessage
        $stack = $_.ScriptStackTrace

        Write-SessionTextLogMessage -Level Err -Message ("Unhandled queue error | {0}" -f $message)
        if (-not [string]::IsNullOrWhiteSpace($position)) {
            Write-SessionTextLogMessage -Level Err -Message ("Position | {0}" -f (($position -replace '\r?\n', ' | ').Trim()))
        }
        if (-not [string]::IsNullOrWhiteSpace($stack)) {
            Write-SessionTextLogMessage -Level Err -Message ("Stack | {0}" -f (($stack -replace '\r?\n', ' | ').Trim()))
        }
        throw
    }
}
finally {
    Restore-ConsoleShutdownHandling
    if ($hasLock) { $mutex.ReleaseMutex() | Out-Null }
    $mutex.Dispose()
}
