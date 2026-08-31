#requires -Version 7.0
# =============================================================================
# Media2AV1Queue.ps1
#
# AV1 encoding for video libraries: queue, quality targeting, HDR handling, and
# the tools that go with them -- all in one file.
#
#   Drop files on Media2AV1Queue.bat        encode them
#   Run it with nothing dropped             a menu appears
#
# THIS FILE IS GENERATED. Edit the sources in src\ and run Build-Single.ps1;
# editing here works but the next build overwrites it.
#
# Settings live in the "User-configurable settings" section a little further
# down, exactly where they always did.
# =============================================================================

[CmdletBinding()]
param(
    # Position = 0 matters more than it looks. Without it, "pwsh -File
    # Media2AV1Queue.ps1 <file>" binds nothing at all on some hosts:
    # ValueFromRemainingArguments only collects arguments the binder has
    # already decided are positional, and a parameter with no position is not
    # a candidate. The failure is silent -- dropped files simply vanish and the
    # script behaves as though it were started with no arguments.
    [Parameter(Position = 0, ValueFromRemainingArguments = $true)]
    [string[]]$InputPaths,

    # Per-drop CRF bias, supplied by Media2AV1Queue-Interactive.ps1.
    #
    # This parameter was missing, while the interactive wrapper has always
    # splatted it for its Aggressive / Balanced / Quality tiers. Because the
    # param block declared only $InputPaths, PowerShell rejected the call with
    # "A parameter cannot be found that matches parameter name
    # 'AutoCRFOffsetOverride'" -- so tiers 2, 3 and 4 failed outright and only
    # tier 1 (Auto, which passes no override) ever worked.
    #
    # The value is attached to each queued job rather than held in a global, so
    # a drop queued as Quality still encodes as Quality even if the worker picks
    # it up much later alongside jobs from a different drop.
    [string]$AutoCRFOffsetOverride = '',

    # Explicit output-rate target in GiB/hr for this drop, supplied by
    # Media2AV1Queue-Interactive.ps1. Beats both the resolution ladder and the
    # source-rate cap. Empty means "decide normally".
    [string]$TargetGiBPerHourOverride = '',

    # Encoder lane for this drop, supplied by Media2AV1Queue-Interactive.ps1.
    # Auto | CPU | Nvidia, or empty to defer to the $EncoderPreference setting.
    #
    # Attached to each queued job rather than held in a global, for the same
    # reason as the CRF override: a drop queued as CPU must still encode on the
    # CPU lane when a worker picks it up later, possibly alongside jobs from a
    # different drop that asked for something else.
    [string]$EncoderPreferenceOverride = '',

    # Which face of the script to show. Empty means "decide": files dropped on
    # it encode straight away, and a bare double-click opens the menu.
    [ValidateSet('', 'Auto', 'Interactive', 'Tools', 'Doctor', 'Candidates', 'Loudness', 'Quality', 'LibraryScan', 'Bench', 'Verify')]
    [string]$Mode = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ErrorView = 'NormalView'



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
$CRF = Auto            # Software quality target. 0-63, recommend 10-28, or Auto. Lower = better quality / larger files.
$Preset = Auto         # Software speed/compression setting. 0-13, recommend 3-6, or Auto. Lower = slower / smaller files.
$AutoCRFOffset = Auto  # Auto mode CRF adjustment. Integer or Auto, recommend -2 to +2. Positive = smaller files, negative = higher quality.
$FilmGrain = Auto      # AV1 film grain synthesis strength. 0-50, recommend 0-16, or Auto. Higher = better grain retention / smaller files on grainy sources.

# ------------------------------------------------------------------------
# Encoder lanes
# ------------------------------------------------------------------------
$EncoderPreference = 'Auto'       # Preferred encoder path. Auto | CPU | Nvidia. Auto chooses per file.
$CpuMaxParallel = 1              # Simultaneous CPU (SVT-AV1) encodes. 1-4. Read the CPU parallelism note below first.
$SoftwarePinCores = 0            # Cores per CPU encode, via svtav1-params pin. 0 = no pinning. Only meaningful when $CpuMaxParallel > 1.
$SoftwareEncodePriority = 'BelowNormal' # Priority for CPU software encodes. Idle | BelowNormal | Normal | AboveNormal.
$HardwareEncodePriority = 'Normal'      # Priority for GPU/NVENC encodes. Idle | BelowNormal | Normal | AboveNormal.
$ScriptProcessPriority  = 'Normal'      # Priority for this PowerShell controller process. Idle | BelowNormal | Normal | AboveNormal.
$ApplyProcessPriority   = $true         # Apply the priority settings above. $true recommended.

# ------------------------------------------------------------------------
# NVENC
# ------------------------------------------------------------------------
$NvencMaxParallel = Auto          # Max simultaneous NVENC workers. 1-8 or Auto. Auto uses GPU model/engine lookup.
$NvencCQ = Auto                   # NVENC quality target. 0-51, recommend 18-28, or Auto. Lower = better quality / larger files.
$NvencPreset = Auto               # NVENC speed/compression setting. p1-p7 or Auto. Higher preset = slower / better compression.
$NvencDecode = Auto               # Decode path for NVENC jobs. Auto | cpu | cuda. Auto prefers the safest path.
$NvencTune = 'auto'               # NVENC tuning bias. auto | hq | ll | ull | $null. hq = quality, ll/ull = latency-focused.
$NvencAllowSplitFrame = $false    # Allow split-frame NVENC if supported. $false recommended; $true is advanced/testing only.

# ------------------------------------------------------------------------
# Preflight estimation & auto-tuning
# ------------------------------------------------------------------------
$EnablePreflightEstimate = $true              # Run short sample encodes before the full job to estimate final size.
$PreflightSampleCount = 4                     # Number of preflight samples. 1-12, recommend 3-6. More = slower but more accurate.
$PreflightSampleDurationSec = 20              # Seconds per preflight sample. 5-120, recommend 15-30. Longer = slower but more accurate.
$PreflightWarnIfEstimatedPctOfSource = 90     # Warn if projected output reaches this % of source size. 1-1000, recommend 90-100.
$PreflightAbortIfEstimatedPctOfSource = 95    # Skip encode if projected output reaches this % of source size. 1-1000, recommend 95-110.
$EnablePreflightAutoTune = $true              # Let preflight adjust Auto quality settings before the full encode starts.
$EnableSecondPreflightPass = $true            # Run a second preflight after major auto-tuning changes to validate the new settings. 
$PreflightAutoTuneQuality = 'High'            # Auto-tune quality profile. Low | Medium | High. High = more quality-preserving targets, larger file.

# Tiny-output safety check
# Helps catch cases where Auto mode may compress too aggressively.
$PreflightTinyOutputPctThreshold = 35         # Flag outputs smaller than this % of source size as suspicious. 1-100, recommend 25-50.
$PreflightTinyOutputAbsoluteGiBThreshold = 1.0 # Also flag projected outputs below this size in GiB. 0.1-100.0, recommend 0.5-2.0.

# Live size estimate
$EnableLiveSizeEstimate = $true               # Show estimated final size and savings while encoding.
$LiveEstimateStartPercent = 3                 # Start showing the estimate after this % of progress. 1-100, recommend 3-15.
$LiveEstimateSmoothingFactor = 0.30           # Smooth live estimate fluctuations. 0.01-1.00, recommend 0.20-0.40. Lower = steadier, higher = more responsive.

# Advanced preflight overrides
# Leave these at $null unless you specifically want manual GiB/hr control.
$PreflightMaxFractionOfSourceRate = 0.65      # Cap the target at this fraction of the SOURCE's own GiB/hr. 0.1-1.0, or $null to disable. See the note below.
$PreflightAutoTuneCustomTargetGiBPerHour = $null # Override target output rate in GiB/hr. Decimal or $null. Recommend 1.0-20.0 depending on source.
$PreflightAutoTuneCustomUpperGiBPerHour = $null  # Override upper tuning threshold in GiB/hr. Decimal or $null. Recommend target + 1 to +4.
$PreflightAutoTuneCustomLowerGiBPerHour = $null  # Override lower tuning threshold in GiB/hr. Decimal or $null. Recommend target - 1 to -4.

# ------------------------------------------------------------------------
# Perceptual quality targeting                          (requires FFmpeg 7.1+)
# ------------------------------------------------------------------------
# This is what turns "make the file smaller" into "make the file smaller
# without losing anything you can see". Read the quality notes below before
# changing the thresholds.
$EnableQualityTargeting = $true    # Measure sample encodes and search for the highest CRF that still looks the same. $true recommended.
$QualityMetric = Auto              # Metric to use. Auto | VMAF | XPSNR | Off. Auto = VMAF for SDR, XPSNR for HDR.
$QualityMode = Auto                # Auto | Absolute | Anchor. Auto = Absolute for VMAF, Anchor for XPSNR.
$QualitySampleCount = 2            # Sample positions measured per CRF probe. 1-6, recommend 2-3. More = slower but steadier.
$QualitySampleDurationSec = 15     # Seconds per quality sample. 5-60, recommend 10-20.
$QualityMaxSearchPasses = 3        # CRF probes after the first. 1-6, recommend 2-4. Each pass is one more round of sample encodes.
$QualityMaxCrfStep = 6             # Largest CRF jump the search may take in one pass. 1-16.
$QualityMaxCrfAboveAuto = 12       # Never raise CRF more than this above what Auto picked. 0-30.
$QualityMaxCrfBelowAuto = 8        # Never lower CRF more than this below what Auto picked. 0-30.
$QualityCrfCeiling = Auto          # Absolute CRF cap, or Auto to use Auto CRF + $QualityMaxCrfAboveAuto.
$QualityFloorUnreachableAction = 'AutoCrf' # When no CRF in range meets the quality target. AutoCrf | Skip. See the quality notes.
$QualityReportSecondMetric = $true # Also report the other metric at the chosen CRF, for the log. Costs one extra measurement per file.

# VMAF (SDR): absolute, human-calibrated scale where 100 is the reference.
$QualityVmafTarget = 95.0          # Minimum VMAF for the output. 88-98, recommend 93-96. Higher = better quality / larger files.
$QualityVmafAnchorDrop = 0.5       # VMAF points allowed below the anchor, when $QualityMode = Anchor. 0.2-3.0.
$QualityVmafConvergenceBand = 0.5  # Stop searching once VMAF is within this much above target. 0.1-2.0.
$QualityVmafThreads = 8            # Threads for the VMAF calculation. 1-16.

# XPSNR (HDR, and the fallback when libvmaf is missing): anchored, because the
# absolute dB number means almost nothing across different content.
$QualityAnchorCRF = 22             # REFERENCE quality level, not a ceiling. The search finds the cheapest CRF that looks this good.
$QualityXpsnrAnchorDropDb = 0.25   # dB allowed below the anchor. 0.1-1.0, recommend 0.2-0.4. Larger = smaller files.
$QualityXpsnrConvergenceBand = 0.15 # Stop searching once within this many dB above threshold. 0.05-0.50.
$QualityXpsnrAbsoluteTarget = 42.0 # Only used if $QualityMode = Absolute. See the warning in the quality notes.
$QualityXpsnrAggregation = 'Weighted' # How to combine the y/u/v scores. Weighted | Min | Luma.

# ------------------------------------------------------------------------
# SVT-AV1 compression efficiency
# ------------------------------------------------------------------------
# Every one of these is verified against the encoder before it is used, so an
# unsupported setting is reported rather than silently discarded.
$SoftwareTune = Auto               # SVT-AV1 tune. 0 = VQ, 1 = PSNR, 2 = SSIM, or Auto (2). Library default is 1.
$SoftwareKeyintSeconds = 10        # Keyframe interval in seconds. 0 = leave at the library default (161 frames). 5-15 recommended.
$SoftwareVarianceBoost = Auto      # Protect flat and dark areas at higher CRF. Auto | $true | $false. Auto = on.
$SoftwareVarianceBoostStrength = 2 # Variance boost strength. 1-4, recommend 2.
$SoftwareFilmGrainDenoise = $false # Let film-grain synthesis replace real grain instead of adding to it. See the film grain note.
$SoftwareSceneChangeDetection = $false # svtav1-params scd=1. Off by default; measure it before enabling.
$SoftwareEnableOverlays = $false   # svtav1-params enable-overlays=1. Off by default; measure it before enabling.
$SoftwareQpScaleCompressStrength = $null # svtav1-params qp-scale-compress-strength, 0-3, or $null to leave at the library default.

# ------------------------------------------------------------------------
# HDR handling                                            (requires FFmpeg 9+)
# ------------------------------------------------------------------------
$PreserveHdrStaticMetadata = $true   # Carry mastering-display colour volume + MaxCLL/MaxFALL into the output. $true strongly recommended.
$PreserveHDR10Plus = 'Auto'          # HDR10+ dynamic metadata. Auto | $true | $false. Auto preserves it when the required tools are present.
$PreserveHLG = $true                 # Keep HLG sources tagged HLG. $false reverts to the old behaviour of mislabelling them as PQ.
$HdrToolsDir = $null                 # Folder containing hdr10plus_tool / dovi_tool. $null = look next to this script, then on PATH.
$ClampMaxCllToMasteringPeak = $false # Fix sources that declare MaxCLL brighter than their own mastering peak. See the MaxCLL note.
$ClampMaxCllMinPeakNits = 400        # Only clamp when the mastering peak is at least this bright. 100-4000.
$ClampMaxCllMinOvershoot = 1.5       # Only clamp when MaxCLL exceeds the peak by at least this factor. 1.1-10.0.

# ------------------------------------------------------------------------
# Source handling
# ------------------------------------------------------------------------
$DolbyVisionMode = 'HDR10'        # Dolby Vision sources. Skip | HDR10 | Passthrough. HDR10 converts the base layer; Skip is the old behaviour.
$SkipDolbyVisionSources = $false  # Legacy switch, kept for compatibility. $true forces $DolbyVisionMode to 'Skip'.
$KeepBackupOriginal = $false      # Keep a backup copy of the original after a successful encode.
$ReplaceOriginal = $true          # Replace the source file with the finished AV1 output after success.

# Legacy override: if the old switch is explicitly on, honour it so existing
# deployments do not change behaviour on upgrade without the user asking.
if ($SkipDolbyVisionSources) { $DolbyVisionMode = 'Skip' }

# ------------------------------------------------------------------------
# Stream selection
# ------------------------------------------------------------------------
$KeepEnglishSDH = $false          # Keep an English SDH subtitle track in addition to the main subtitle.
$KeepEnglishFallbackAudio = $true # Keep a secondary lossy English audio track when the main track is lossless.

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
#
# Preflight target notes:
# - The resolution/HDR ladder below sets a target output rate, but a ladder
#   cannot know how well the SOURCE was already encoded. A 2160p AMZN WEB-DL can
#   sit at 6.4 GiB/hr while the UHD SDR "High" ladder aims for 10 GiB/hr -- and
#   a target above the source's own rate can only INFLATE the file. Auto then
#   picks a very low CRF to reach that target, preflight projects an output
#   larger than the source, and the job is refused. Nothing is wrong with the
#   encoder; the target was impossible to satisfy usefully.
# - $PreflightMaxFractionOfSourceRate caps the target at a fraction of the
#   measured source rate, so the target is always a saving. 0.65 aims for
#   roughly a third off. Lower it for smaller files, raise it toward 1.0 to
#   preserve more, or set $null to restore the old ladder-only behaviour.
# - For one-off control, the interactive drop menu can set an explicit target
#   for a single drop, which overrides both the ladder and this cap.
#
# CPU parallelism notes:
# - A single SVT-AV1 instance does not scale perfectly to very high core counts:
#   synchronisation and serial phases leave capacity unused. Two concurrent
#   encodes can therefore beat one wide encode on TOTAL throughput, which is what
#   matters when converting a whole library.
# - It is hardware- and content-dependent, so measure rather than guess.
#   Media2AV1Queue-Bench.ps1 reports both, establishes a noise floor from repeat
#   runs, and only calls a winner when the margin exceeds it.
# - Raising $CpuMaxParallel costs memory (each 4K SVT-AV1 instance wants several
#   GB) and raises per-file latency: each file takes proportionally longer even
#   though more of them finish per hour.
# - $SoftwarePinCores maps to svtav1-params pin=N, restricting an encode to N
#   cores so concurrent encodes contend less. Leave at 0 for a single encode --
#   pinning a lone encode only starves it.
#
# Quality notes:
# - The goal of this script is a smaller file with no visible quality loss. The
#   preflight machinery above only ever measured SIZE, which cannot answer the
#   second half of that. Quality targeting closes the gap: it encodes short
#   samples at several CRFs, MEASURES each one against the source, and picks the
#   highest CRF that is still visually transparent. Higher CRF means a smaller
#   file, so "highest that still looks the same" is exactly the objective.
# - This moves CRF in both directions. The Auto ladder is driven by resolution,
#   codec and bits-per-pixel, none of which knows how hard the picture actually
#   is to encode. Easy content is routinely given far more bitrate than it needs;
#   difficult content is occasionally given too little.
# - Two metrics, each used only where it is valid:
#     VMAF  (SDR) Netflix's perceptual model. Its scale is calibrated against
#           human scores, so an absolute target such as 95 means something. Its
#           models are trained on SDR, so it is not used on HDR.
#     XPSNR (HDR) Fraunhofer HHI's perceptually weighted PSNR, developed on
#           UHD/HDR material. Stock FFmpeg 7.1+, no model files needed.
# - XPSNR is used against an ANCHOR, not against a fixed dB number, and the
#   reason is worth knowing before changing $QualityXpsnrAbsoluteTarget. Measured
#   while building this, on one build, same encoder settings:
#       flat, low-detail source   CRF 18 -> 64.5 dB   CRF 50 -> 49.9 dB
#       heavily grained source    CRF 18 -> 35.1 dB   CRF 46 -> 32.0 dB
#   The widely quoted "above 42 dB is visually lossless" would refuse to
#   compress the grained source at any CRF at all, and would wave the flat
#   source through at CRF 50. The absolute number carries almost no information
#   across content. $QualityAnchorCRF instead names a reference quality level:
#   the search asks "how good would CRF 22 have looked on THIS content?" and
#   then finds the cheapest CRF that still looks that good.
# - $QualityAnchorCRF is the setting to change if output is consistently bigger
#   or smaller than wanted. Raise it for smaller files, lower it for higher
#   quality. It costs one extra sample encode per file.
# - An ABSOLUTE VMAF target cannot be met on grain-heavy sources, and that is
#   a property of the metric, not of the file. Measured while building this, on
#   a heavily grained 1080p source: CRF 14 -> VMAF 75.6, CRF 22 -> 75.0,
#   CRF 30 -> 73.8, while the file shrank 6.8x. VMAF moved 1.8 points across 16
#   CRF steps because the score is dominated by grain the encoder cannot
#   reproduce exactly at any bitrate. A clean source over the same range scored
#   98.4 at CRF 22. So a fixed "VMAF >= 95" gate refuses every film-grain
#   Blu-ray remux outright -- exactly the files most worth converting.
#   $QualityFloorUnreachableAction decides what happens then:
#     AutoCrf  (default) keep the CRF the Auto ladder chose. That is the answer
#              the script would have given without quality targeting at all, it
#              is never more expensive than Auto asked for, and preflight
#              auto-tuning still runs from there. The file gets converted.
#     Skip     leave the file alone (the behaviour before this option existed).
# - For a library that is mostly grainy film transfers, the better fix is to
#   stop using an absolute scale at all: set $QualityMode = 'Anchor'. Every CRF
#   is then compared against a reference encode of the SAME content at
#   $QualityAnchorCRF, so the grain penalty lands on both sides of the
#   comparison and cancels. It costs one extra sample encode per file.
# - The defaults are starting points, not settled constants. Run
#   Media2AV1Queue-Quality.ps1 against a few real files from your own library:
#   it prints the size-versus-quality curve so the thresholds can be set from
#   your own content and your own eyes rather than from someone's defaults.
#
# Film grain note:
# - Film-grain synthesis is emitted as film-grain=N:film-grain-denoise=0. With
#   denoise off the encoder adds synthetic grain on top of the grain it already
#   coded, which preserves the look but saves almost nothing: measured here,
#   film-grain 0 -> 16 at fixed CRF changed the file by under 1%.
# - The size win comes from $SoftwareFilmGrainDenoise = $true, where the encoder
#   denoises first (cheap to code) and re-synthesises grain afterwards. That is
#   a real trade: it replaces the original grain rather than reproducing it, so
#   reference-based metrics score it lower even when it looks fine. Measure it
#   on grainy content of your own before turning it on library-wide.
#
# MaxCLL note:
# - Some masters declare a MaxCLL brighter than their own mastering-display
#   peak, which is self-contradictory: the content cannot be brighter than the
#   display it was graded on. A library census here found 15 such files, and
#   they split into genuinely different cases -- placeholders near 10,000 nits,
#   overshoots of two to six times, mild overshoots under twice, and at least
#   one file where the PEAK is the wrong field rather than MaxCLL.
# - $ClampMaxCllToMasteringPeak is therefore off by default and deliberately
#   conservative when on: it clamps only when the peak is credible
#   ($ClampMaxCllMinPeakNits or brighter) AND the overshoot is large
#   ($ClampMaxCllMinOvershoot or more). A file with a 200-nit peak and a
#   574-nit MaxCLL is left untouched, because there the peak is what looks
#   wrong. Every clamp is written to the log.
#
# HDR notes:
# - $PreserveHdrStaticMetadata is the single most important setting for HDR
#   output quality. Without it the encode carries PQ/BT.2020 signalling but no
#   mastering-display colour volume and no MaxCLL/MaxFALL, so the display has
#   to fall back to generic tone-mapping assumptions. That is the usual cause
#   of an AV1 re-encode looking flatter than its source on an HDR10+ TV.
# - HDR10+ (SMPTE ST 2094-40) cannot be carried into AV1 by stock FFmpeg and
#   mainline SVT-AV1. Preserving it needs either:
#     a) an SVT-AV1 built from svt-av1-hdr / SVT-AV1-PSY with enable-hdr10plus,
#        which accepts hdr10plus-json during the encode, or
#     b) an hdr10plus_tool build with AV1 support, used to re-inject the
#        metadata OBUs after encoding.
#   The script probes for both and reports which route it took. With neither,
#   output is still correct static HDR10 -- it just has no dynamic metadata.
# - Dolby Vision Profile 5 is deliberately never converted. Its base layer has
#   no HDR10-compatible representation, so re-tagging it as HDR10 produces
#   visibly wrong colour. Profiles 7 and 8 convert cleanly.

# =============================================================================
# End of user-configurable settings
# =============================================================================


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

# --- Per-session decision caches ---------------------------------------------
# The Auto lane scheduler asks "what would this file do on this lane?" once per
# pending job, per lane, per scheduling pass. Every one of those questions used
# to re-probe the file and re-run the sample encodes behind it, so a queue whose
# jobs were all waiting on one busy lane could spend hours re-deciding and never
# encode anything -- the same VMAF numbers printed over and over in the console
# while both workers sat idle. The answers depend only on the file's bytes and
# the settings, so they are memoised for the life of the session. Keys carry
# size and last-write time, so a file that changes on disk is never answered
# from a stale entry.
$script:FfprobeCache         = @{}
$script:QualitySearchCache   = @{}
$script:PreflightSampleCache = @{}
$script:GrainPreScanCache    = @{}
$script:LaneHoldSignatures   = @{}
$script:AnnouncedOnce        = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
$script:DecisionCacheHits    = 0
$script:DecisionCacheMisses  = 0
$script:QueueStalled         = $false
$script:SessionOutcomes      = @{}
$script:SessionSourceGiB     = 0.0
$script:SessionOutputGiB     = 0.0

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
    # --- HDR / dynamic metadata -------------------------------------------
    # These make it possible to answer "did this file actually keep its HDR
    # metadata?" from the log alone, without re-probing the output.
    'SourceHdrFormat',
    'HdrTargetFormat',
    'HdrStaticMetadata',
    'HdrMaxCLL',
    'HdrMaxFALL',
    'HdrHDR10PlusSource',
    'HdrHDR10PlusOutput',
    'DolbyVisionProfile',
    'DolbyVisionStrategy',
    'HdrPlanSummary',
    'MaxCllClamped',
    # --- Measured perceptual quality --------------------------------------
    # These are the columns that make "did this stay transparent?" answerable
    # from the log alone, per file, months later.
    'QualityMetric',
    'QualityMode',
    'QualityThreshold',
    'QualityMeasured',
    'QualityAnchorCRF',
    'QualityAnchorMetric',
    'QualityTransparencyMet',
    'QualityProbeCount',
    'QualityCrfDelta',
    'QualitySecondMetric',
    'QualitySecondMetricValue',
    'SvtEfficiencyParams',
    'FfmpegPath',
    'FfprobePath',
    'Notes'
)

# Named mutex used to enforce a single queue-manager instance.
# The "Global\" prefix makes it machine-wide so it works across all console
# sessions and UAC boundaries. Each script directory keeps its own .queue
# folder, so two separate copies of this script queue independently but will
# never drive the same queue at the same time.
# Clamped rather than trusted: an accidental large value would spawn enough
# concurrent 4K encodes to exhaust memory.
$CpuMaxParallel   = [Math]::Max(1, [Math]::Min(4, [int]$CpuMaxParallel))
$SoftwarePinCores = [Math]::Max(0, [Math]::Min([Environment]::ProcessorCount, [int]$SoftwarePinCores))

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

    # Every terminal outcome in every queue loop goes through here, which makes
    # it the one honest place to total up what a session actually did. Without
    # this the run ends by simply erasing its progress box, leaving no answer to
    # "so what happened?" short of opening the CSV.
    $status = [string]$ordered['Status']
    if (-not [string]::IsNullOrWhiteSpace($status)) {
        if (-not $script:SessionOutcomes.ContainsKey($status)) { $script:SessionOutcomes[$status] = 0 }
        $script:SessionOutcomes[$status]++
    }
    if ($status -eq 'SUCCESS') {
        $script:SessionSourceGiB += (Convert-ToInvariantDouble $ordered['SourceSizeGiB'] 0.0)
        $script:SessionOutputGiB += (Convert-ToInvariantDouble $ordered['OutputSizeGiB'] 0.0)
    }

    if ($script:SessionLogPath) {
        Write-SessionTextLogEntry -Row $ordered
    }
}

# =============================================================================
# FUNCTION: Write-SessionSummary
#
# The last thing a run prints. The live box is erased when the queue drains, so
# before this existed a finished session left nothing on screen that said how
# many files were converted, how many were left alone and why, or how much
# space was actually saved.
# =============================================================================
function Write-SessionSummary {
    $total = 0
    foreach ($count in $script:SessionOutcomes.Values) { $total += $count }
    if ($total -eq 0) { return }

    $succeeded = if ($script:SessionOutcomes.ContainsKey('SUCCESS')) { $script:SessionOutcomes['SUCCESS'] } else { 0 }
    $failed    = if ($script:SessionOutcomes.ContainsKey('FAILED'))  { $script:SessionOutcomes['FAILED'] }  else { 0 }
    $skipped   = 0
    $skipDetail = New-Object System.Collections.Generic.List[string]
    foreach ($entry in $script:SessionOutcomes.GetEnumerator()) {
        if ($entry.Key -eq 'SUCCESS' -or $entry.Key -eq 'FAILED') { continue }
        $skipped += $entry.Value
        # Each arm breaks. A -Wildcard switch runs EVERY matching arm otherwise,
        # so PRECHECK_SKIPPED_QUALITY_FLOOR matched both its own arm and the
        # PRECHECK_SKIPPED* arm and produced an array where a label belonged.
        $label = switch -Wildcard ($entry.Key) {
            'PRECHECK_SKIPPED_QUALITY_FLOOR' { 'no CRF stayed transparent'; break }
            'PRECHECK_SKIPPED*'              { 'projected output too large'; break }
            'AUTO_SKIPPED*'                  { 'already efficient'; break }
            'SKIPPED_DV'                     { 'Dolby Vision profile not convertible'; break }
            'INTERRUPTED'                    { 'interrupted'; break }
            default                          { $entry.Key }
        }
        $skipDetail.Add(("{0} {1}" -f $entry.Value, $label))
    }

    Write-Host ""
    Write-Host "Session summary" -ForegroundColor Cyan
    Write-Host ("  Converted : {0}" -f $succeeded) -ForegroundColor $(if ($succeeded -gt 0) { 'Green' } else { 'Gray' })
    if ($succeeded -gt 0 -and $script:SessionSourceGiB -gt 0) {
        $savedGiB = $script:SessionSourceGiB - $script:SessionOutputGiB
        $savedPct = 100.0 * ($savedGiB / $script:SessionSourceGiB)
        Write-Host ("  Saved     : {0:F2} GiB of {1:F2} GiB ({2:F1}%)" -f $savedGiB, $script:SessionSourceGiB, $savedPct) -ForegroundColor Green
    }
    if ($skipped -gt 0) {
        Write-Host ("  Left alone: {0}  ({1})" -f $skipped, ($skipDetail -join ', ')) -ForegroundColor Yellow
    }
    if ($failed -gt 0) {
        Write-Host ("  Failed    : {0}" -f $failed) -ForegroundColor Red
    }

    $reused = $script:DecisionCacheHits
    if ($reused -gt 0) {
        Write-Host ("  Reused {0} cached decision(s) instead of re-probing or re-measuring." -f $reused) -ForegroundColor DarkGray
    }
    if ($script:SessionLogPath) {
        Write-Host ("  Full log  : {0}" -f $script:SessionLogPath) -ForegroundColor DarkGray
    }
    Write-SessionTextLogMessage -Level Info -Message (
        "Session summary | converted={0} | left alone={1} | failed={2} | cache reuse={3}" -f $succeeded, $skipped, $failed, $reused)
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

function Resolve-QualityFloorActionConfigValue {
    param(
        [string]$Name,
        $Value
    )

    $text = ([string]$Value).Trim()
    # Accept the switch this setting replaced, so a settings block copied
    # forward from an older build keeps working instead of throwing on startup.
    switch ($text.ToLowerInvariant()) {
        'autocrf'  { return 'AutoCrf' }
        'continue' { return 'AutoCrf' }
        'anchor'   { return 'AutoCrf' }
        'skip'     { return 'Skip' }
        'true'     { return 'Skip' }
        'false'    { return 'AutoCrf' }
        default    { throw "$Name must be 'AutoCrf' or 'Skip'. Current value: $Value" }
    }
}

function Test-RequiredFfmpegBuild {
    param([string]$ExecutablePath)

    $versionText = (& $ExecutablePath -hide_banner -version | Out-String)
    if ([string]::IsNullOrWhiteSpace($versionText)) {
        throw "Unable to inspect FFmpeg version information. This script requires a full FFmpeg 9.0 or newer build."
    }

    $versionLine = (($versionText -split "\r?\n")[0]).Trim()

    # 6.x / 7.x lack too much of what this script relies on to be worth
    # supporting, and 7.x additionally predates the Dolby Vision work.
    if ($versionText -match '(?im)^ffmpeg version [^\r\n]*\b(?:n?6(?:\.\d+)?|n?7(?:\.\d+)?)\b') {
        throw "This script requires a full FFmpeg 9.0 or newer build. FFmpeg 6.x / 7.x builds are unsupported. Detected: $versionLine"
    }

    if ($versionText -match '(?i)\b(?:essentials|basic|minimal|lite)(?:[_ -]?build)?\b') {
        throw "This script requires a full FFmpeg build. Stripped/basic FFmpeg builds are unsupported. Detected: $versionLine"
    }

    # Parse the major version so the HDR feature gate can be reported precisely
    # rather than pattern-matched against one expected release string.
    #
    # Git snapshot builds carry no version number at all -- gyan.dev, for
    # instance, reports "ffmpeg version 2026-08-17-git-426841da9d-full_build".
    # Those are typically *ahead* of the numbered releases, so treating an
    # unparseable version as suspect would be exactly backwards. A dated git
    # build is recognised and dated instead, and the capability probes below
    # decide what it can actually do.
    $majorVersion = 0
    $isGitBuild   = $false
    $buildDate    = $null

    if ($versionText -match '(?im)^ffmpeg version \D*(\d+)\.\d') {
        $majorVersion = [int]$Matches[1]
    } elseif ($versionText -match '(?im)^ffmpeg version\s+(\d{4})-(\d{2})-(\d{2})\S*git') {
        $isGitBuild = $true
        $buildDate  = [datetime]::new([int]$Matches[1], [int]$Matches[2], [int]$Matches[3])
        # The 9.0 branch was cut from master on 2026-06-26, so any git build
        # from that date onward contains the 9.0 feature set.
        if ($buildDate -ge [datetime]::new(2026, 6, 26)) { $majorVersion = 9 }
    } elseif ($versionText -match '(?im)^ffmpeg version \S*git') {
        $isGitBuild = $true
    }

    # 8.x still runs, but without -mastering_display / -content_light the static
    # HDR10 payload can only reach the output through svtav1-params, which means
    # the NVENC lane cannot carry it at all. Say so plainly instead of letting
    # HDR jobs quietly lose their colour volume.
    if ($majorVersion -gt 0 -and $majorVersion -lt 9) {
        Write-Warning "FFmpeg $majorVersion.x detected: $versionLine"
        Write-Warning "  This script is built for FFmpeg 9.0+. On 8.x the following are unavailable:"
        Write-Warning "    - -mastering_display / -content_light  (static HDR10 metadata on the NVENC lane)"
        Write-Warning "    - dovi_split                           (Dolby Vision Profile 7 base-layer extraction)"
        Write-Warning "  HDR encodes will be steered to the CPU lane where possible, and Dolby Vision"
        Write-Warning "  sources may be skipped. Upgrading to FFmpeg 9.0.1 or newer is recommended."
    } elseif ($isGitBuild -and $majorVersion -ge 9) {
        Write-Host ("FFmpeg git build dated {0:yyyy-MM-dd} (post-9.0 branch point): {1}" -f $buildDate, $versionLine) -ForegroundColor DarkGray
    } elseif ($majorVersion -eq 0) {
        Write-Warning "Could not determine the FFmpeg version from: $versionLine. Relying on capability probing."
    }

    return [ordered]@{
        VersionLine  = $versionLine
        VersionText  = $versionText
        MajorVersion = $majorVersion
        IsGitBuild   = $isGitBuild
        BuildDate    = $buildDate
    }
}

# -----------------------------------------------------------------------------
# Splits one CSV line into its fields, honouring quotes and doubled quotes.
# Used to read a log header without handing it to Import-Csv, which throws on
# a repeated column name instead of reporting one.
# -----------------------------------------------------------------------------
function Split-CsvLine {
    param([string]$Line)

    $fields = New-Object System.Collections.Generic.List[string]
    if ($null -eq $Line) { return ,([string[]]$fields.ToArray()) }

    $current = New-Object System.Text.StringBuilder
    $inQuotes = $false
    for ($i = 0; $i -lt $Line.Length; $i++) {
        $ch = $Line[$i]
        if ($inQuotes) {
            if ($ch -eq '"') {
                if (($i + 1) -lt $Line.Length -and $Line[$i + 1] -eq '"') {
                    $null = $current.Append('"'); $i++
                } else {
                    $inQuotes = $false
                }
            } else {
                $null = $current.Append($ch)
            }
        } elseif ($ch -eq '"') {
            $inQuotes = $true
        } elseif ($ch -eq ',') {
            $fields.Add($current.ToString()); $null = $current.Clear()
        } else {
            $null = $current.Append($ch)
        }
    }
    $fields.Add($current.ToString())
    return ,([string[]]$fields.ToArray())
}

function Update-LogSchemaIfNeeded {
    if (-not (Test-Path -LiteralPath $LogPath)) { return }

    $expectedHeader = $LogColumns -join ","
    $currentHeader  = Get-Content -LiteralPath $LogPath -TotalCount 1 -ErrorAction SilentlyContinue
    if ($currentHeader -eq $expectedHeader) { return }

    # Archive rather than append. The old code warned that "appending with the
    # new schema may misalign old rows" and then did exactly that, which
    # quietly corrupts every later row in a file the user may be relying on for
    # history. Moving the unreadable log aside loses nothing and keeps the new
    # one clean.
    $archiveDamagedLog = {
        param([string]$Why)

        $archive = Join-Path ([System.IO.Path]::GetDirectoryName($LogPath)) (
            "{0}.{1}.csv" -f [System.IO.Path]::GetFileNameWithoutExtension($LogPath), (Get-Date).ToString('yyyyMMdd_HHmmss'))
        try {
            Move-Item -LiteralPath $LogPath -Destination $archive -Force
            Write-Warning ("The existing encode log could not be migrated ({0}). It has been kept as {1} and a fresh log will be started, so no rows are lost or misaligned." -f $Why, [System.IO.Path]::GetFileName($archive))
        } catch {
            Write-Warning ("The existing encode log could not be migrated ({0}) or archived: {1}" -f $Why, $_.Exception.Message)
        }
    }

    # Import-Csv throws "The member <name> is already present" on a header that
    # repeats a column, which is what a header written twice or a header line
    # merged with a data row produces. Checking here means that case is
    # diagnosed and archived instead of surfacing as an opaque error.
    $headerFields = @(Split-CsvLine -Line ([string]$currentHeader))
    $duplicateNames = @($headerFields |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
        Group-Object -CaseSensitive:$false |
        Where-Object { $_.Count -gt 1 } |
        ForEach-Object { $_.Name })
    if ($duplicateNames.Count -gt 0) {
        & $archiveDamagedLog ("its header repeats the column name(s): " + (($duplicateNames | Select-Object -First 5) -join ', '))
        return
    }

    $existingRows = @()
    try {
        $existingRows = @(Import-Csv -LiteralPath $LogPath)
    } catch {
        & $archiveDamagedLog ("it could not be parsed: " + $_.Exception.Message)
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

# ffmpeg prints its two kinds of options with different indentation, and
# conflating them silently breaks capability detection:
#
#   AVOptions (from -h encoder=X) are INDENTED:
#       "  -preset            <int>   E..V....... Encoding preset"
#   CLI options (from -h full) start at COLUMN 0:
#       "-map_metadata outfile[,metadata]:infile[,metadata]  set metadata"
#
# Test-TextContainsOption requires leading whitespace, so it finds the former
# and can NEVER find the latter. -mastering_display and -content_light are CLI
# options, which is why a perfectly capable FFmpeg 9 build reported them as
# missing. This variant tolerates either indentation.
function Test-TextContainsCliOption {
    param(
        [string]$Text,
        [string]$OptionName
    )

    return ($Text -match "(?m)^\s*-$([Regex]::Escape($OptionName))\b")
}

# The definitive capability check: hand the option to ffmpeg and see whether it
# is rejected. Help-text parsing is a heuristic that depends on formatting that
# changes between releases; this depends only on ffmpeg's own parser.
#
# Encodes a single 64x64 frame to the null muxer, so it costs milliseconds and
# writes nothing. Only argument-parsing errors are treated as "unsupported" --
# an option that parses but then fails for an unrelated reason (no GPU, say) is
# reported through $Accepted separately so callers can tell the two apart.
function Test-FfmpegOptionSupported {
    param(
        [string[]]$OptionArguments,
        [string]$Encoder = 'libsvtav1',
        [string[]]$ExtraEncoderArguments = @()
    )

    $probeArgs = New-Object System.Collections.Generic.List[string]
    $probeArgs.AddRange([string[]]@(
        '-hide_banner', '-nostdin',
        '-f', 'lavfi', '-i', 'color=c=black:s=64x64:r=1:d=1',
        '-c:v', $Encoder, '-frames:v', '1'
    ))
    if ($ExtraEncoderArguments.Count -gt 0) { $probeArgs.AddRange([string[]]$ExtraEncoderArguments) }
    $probeArgs.AddRange([string[]]$OptionArguments)
    $probeArgs.AddRange([string[]]@('-f', 'null', '-'))

    $stdErrText = ''
    $exitCode = -1
    try {
        $psi = [System.Diagnostics.ProcessStartInfo]::new()
        $psi.FileName = $FfmpegPath
        foreach ($a in $probeArgs) { $psi.ArgumentList.Add($a) }
        $psi.RedirectStandardError  = $true
        $psi.RedirectStandardOutput = $true
        $psi.UseShellExecute = $false
        $proc = [System.Diagnostics.Process]::Start($psi)
        $stdErrText = $proc.StandardError.ReadToEnd()
        $null = $proc.StandardOutput.ReadToEnd()
        if (-not $proc.WaitForExit(30000)) {
            try { $proc.Kill($true) } catch { }
            return [ordered]@{ Supported = $false; Accepted = $false; ExitCode = -1; Detail = 'probe timed out' }
        }
        $exitCode = $proc.ExitCode
    } catch {
        return [ordered]@{ Supported = $false; Accepted = $false; ExitCode = -1; Detail = "probe could not run: $($_.Exception.Message)" }
    }

    # ffmpeg's own wording for an option it does not know.
    # Covers both "ffmpeg does not know this option" and "ffmpeg knows it but
    # not in this position/form" -- the latter is how a stream specifier that
    # the option does not accept actually reports itself.
    $rejected = $stdErrText -match '(?im)Unrecognized option|Option not found|Error splitting the argument list|No such option|Unable to find a suitable output format|cannot be applied to|Error parsing options'
    $lastLine = (($stdErrText -split "\r?\n" | Where-Object { $_.Trim() }) | Select-Object -Last 1)

    return [ordered]@{
        Supported = (-not $rejected)
        Accepted  = ((-not $rejected) -and $exitCode -eq 0)
        ExitCode  = $exitCode
        Detail    = if ($rejected) { 'option rejected by ffmpeg' } else { [string]$lastLine }
    }
}

# Determines whether av1_nvenc can actually encode with B-frames.
#
# This is a DIFFERENTIAL probe: it runs the same encode twice, once without
# B-frames and once with, and only concludes "unsupported" when the control
# succeeds and the B-frame run fails. That distinction matters because a naive
# single probe blames B-frames for any failure at all -- and the first version
# of this check did exactly that, on two counts:
#
#   * It used a 64x64 source. NVENC enforces a driver-side minimum frame size
#     that is larger than that for AV1, so the probe failed on dimensions and
#     reported "B-frames disabled" on a GPU that supports them perfectly well.
#   * It encoded a single frame. One frame cannot exercise a B-frame GOP, since
#     there is nothing for a B-frame to sit between.
#
# 1280x720 is comfortably above any NVENC minimum, and 16 frames is enough for
# a real pyramid. On an Ada-class GPU the whole probe is well under a second,
# and it runs once per session.
#
# ffmpeg reports an unsupported B-frame count as a *warning*
# ("Max B-frames %d exceed %d") and then fails the session, so the last line of
# stderr is the generic "Conversion failed!". The informative lines are
# extracted here rather than just the final one.
function Test-Av1NvencBFrameSupport {
    param([int]$BFrames = 2)

    function Invoke-NvencProbe {
        param([string[]]$ExtraArgs)

        $probeArgs = New-Object System.Collections.Generic.List[string]
        $probeArgs.AddRange([string[]]@(
            '-hide_banner', '-nostdin',
            '-f', 'lavfi', '-i', 'color=c=black:s=1280x720:r=24:d=1',
            '-c:v', 'av1_nvenc', '-preset', 'p1', '-frames:v', '16'
        ))
        if ($ExtraArgs.Count -gt 0) { $probeArgs.AddRange([string[]]$ExtraArgs) }
        $probeArgs.AddRange([string[]]@('-f', 'null', '-'))

        try {
            $psi = [System.Diagnostics.ProcessStartInfo]::new()
            $psi.FileName = $FfmpegPath
            foreach ($a in $probeArgs) { $psi.ArgumentList.Add($a) }
            $psi.RedirectStandardError  = $true
            $psi.RedirectStandardOutput = $true
            $psi.UseShellExecute = $false
            $proc = [System.Diagnostics.Process]::Start($psi)
            $errText = $proc.StandardError.ReadToEnd()
            $null = $proc.StandardOutput.ReadToEnd()
            if (-not $proc.WaitForExit(60000)) {
                try { $proc.Kill($true) } catch { }
                return [ordered]@{ Ok = $false; Diagnostic = 'probe timed out' }
            }

            # Keep the lines that actually say something, not the generic tail.
            $interesting = @($errText -split "\r?\n" | Where-Object {
                $_ -match '(?i)b-?frames|exceed|not supported|invalid|no capable|cannot|failed to|error'
            } | Where-Object { $_ -notmatch '(?i)^\s*Conversion failed' } | Select-Object -First 3)

            $diag = if ($interesting.Count -gt 0) { ($interesting -join ' || ').Trim() }
                    elseif ($proc.ExitCode -ne 0) { 'no diagnostic message; exit code ' + $proc.ExitCode }
                    else { '' }

            return [ordered]@{ Ok = ($proc.ExitCode -eq 0); Diagnostic = $diag }
        } catch {
            return [ordered]@{ Ok = $false; Diagnostic = "probe could not run: $($_.Exception.Message)" }
        }
    }

    $control = Invoke-NvencProbe -ExtraArgs @()

    if (-not $control.Ok) {
        # NVENC itself is not usable in this environment, so nothing can be
        # concluded about B-frames. Reported as inconclusive, and B-frames are
        # left off rather than guessed at.
        return [ordered]@{
            Supported    = $false
            Inconclusive = $true
            Detail       = "av1_nvenc could not complete even a plain test encode, so B-frame support is undetermined. $($control.Diagnostic)".Trim()
        }
    }

    $withB = Invoke-NvencProbe -ExtraArgs @('-bf', "$BFrames", '-b_ref_mode', 'middle')

    if ($withB.Ok) {
        return [ordered]@{
            Supported    = $true
            Inconclusive = $false
            Detail       = "av1_nvenc accepted bf=$BFrames with b_ref_mode=middle."
        }
    }

    return [ordered]@{
        Supported    = $false
        Inconclusive = $false
        Detail       = "av1_nvenc encodes fine without B-frames but rejected bf=$BFrames. $($withB.Diagnostic)".Trim()
    }
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

    # Guarded: Join-Path throws "Cannot bind argument to parameter 'Path'
    # because it is null" when $env:ProgramFiles is not set, which surfaced as
    # an unexplained warning at every startup on hosts where it is missing.
    # A missing environment variable means the fallback location cannot exist,
    # not that tool detection should fail.
    if (-not [string]::IsNullOrWhiteSpace(${env:ProgramFiles})) {
        $defaultPath = Join-Path ${env:ProgramFiles} 'NVIDIA Corporation\NVSMI\nvidia-smi.exe'
        if (Test-Path -LiteralPath $defaultPath) { return $defaultPath }
    }

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

    # Whether av1_nvenc can actually use B-frames has to be tested, not read.
    #
    # FFmpeg 9 added hierarchical B-frame support to the AV1 NVENC encoder. There
    # is no dedicated option for it -- it is driven by the existing generic -bf
    # (max_b_frames) together with -b_ref_mode. Earlier builds and older drivers
    # refuse B-frames for AV1, and the refusal shows up at encoder init, not at
    # argument parsing, so only a real one-frame encode distinguishes them.
    #
    # Runs once per session and is cached with the rest of the NVENC environment.
    $nvencBFrameProbe = Test-Av1NvencBFrameSupport -BFrames 2

    if ($nvencBFrameProbe.Inconclusive) {
        Write-Warning "av1_nvenc B-frame probe was inconclusive: $($nvencBFrameProbe.Detail)"
    } elseif (-not $nvencBFrameProbe.Supported) {
        Write-Warning "av1_nvenc will encode without B-frames: $($nvencBFrameProbe.Detail)"
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
        # -bf is a GENERIC AVCodecContext option (max_b_frames), not an encoder
        # private option, so it never appears in -h encoder=av1_nvenc output.
        # Probing the help text for it always returns false, which would leave
        # every NVENC encode at bf=0 -- exactly the compression the hierarchical
        # B-frame work is supposed to gain. It is resolved functionally instead,
        # by $nvencBFrameProbe below.
        SupportsBFrames       = $nvencBFrameProbe.Supported
        SupportsBFrameReason  = $nvencBFrameProbe.Detail
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

    # B-frame depth. AV1 NVENC on Ada and newer handles a pyramid reference
    # structure in dedicated silicon, so a deeper GOP is close to free in encode
    # time while measurably improving compression. Animation and low-motion
    # content benefit most, so the depth follows the same difficulty signal the
    # preset selection already computes.
    #
    # 4 is the practical ceiling for AV1 NVENC; going deeper stops paying for
    # itself and increases decode complexity on playback devices.
    $bFrames = if ($AutoSettings.GrainClass -in @('heavy', 'extreme')) {
        2   # Heavy grain: long reference chains smear grain detail.
    } elseif ($AutoSettings.ResolutionTier -eq 'UHD') {
        3
    } else {
        4
    }

    # Hierarchical structure is implied by bf >= 2 with b_ref_mode middle; there
    # is no separate flag to set. Recorded for the log only.
    $useHierarchicalB = $NvencEnvironment.SupportsBFrames -and $NvencEnvironment.SupportsBRefMode -and $bFrames -ge 2

    return [ordered]@{
        CQ               = $cq
        Preset           = $preset
        PresetReason     = $presetReason
        Tune             = $tune
        TuneReason       = $tuneReason
        TuneWarning      = $tuneResolution.Warning
        TuneDisplay      = if ([string]::IsNullOrWhiteSpace([string]$tune)) { 'disabled' } else { $tune }
        DecodePath       = $decodePath
        DecodeReason     = $decodeReason
        PixFmt           = $pixFmt
        BitDepth         = $bitDepth
        BFrames          = $bFrames
        UseHierarchicalB = $useHierarchicalB
        BFrameReason     = if ($useHierarchicalB) {
            "Hierarchical B-frames: bf=$bFrames with b_ref_mode=middle (FFmpeg 9 AV1 NVENC)."
        } elseif ($NvencEnvironment.SupportsBFrames) {
            "bf=$bFrames without a pyramid reference structure (b_ref_mode unavailable)."
        } else {
            "B-frames unavailable on this build/driver: $($NvencEnvironment.SupportsBFrameReason)"
        }
        Reason           = "Mapped software-style Auto CRF $softwareCrf to NVENC CQ $cq."
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

    $preflightTargets = Resolve-PreflightAutoTuneTargets -QualityProfile $PreflightAutoTuneQuality -ResolutionTier $AutoSettings.ResolutionTier -SourceProfile $SourceProfile `
        -SourceGiBPerHour ([double](Get-OptionalProperty -InputObject $AutoSettings -PropertyName 'VideoBitratePerHourGiB' -Default 0.0))

    # HDR10+ can only be carried inline by SVT-AV1, and the CPU lane is also
    # where the static colour volume is guaranteed to survive on any build. A
    # dynamic-metadata source is therefore steered to the CPU lane before any
    # throughput consideration applies.
    if ($SourceProfile.HasHDR10Plus) {
        return [pscustomobject][ordered]@{
            Lane   = 'CPU'
            Reason = 'HDR10+ dynamic metadata source; software lane required to preserve it'
        }
    }

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
    $preflightTargets = Resolve-PreflightAutoTuneTargets -QualityProfile $PreflightAutoTuneQuality -ResolutionTier $AutoSettings.ResolutionTier -SourceProfile $SourceProfile `
        -SourceGiBPerHour ([double](Get-OptionalProperty -InputObject $AutoSettings -PropertyName 'VideoBitratePerHourGiB' -Default 0.0))

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

    # This can be handed an EARLY-EXIT init -- @{ EarlyExit; Row } -- when the
    # alternate lane's preflight declined the job. That object has none of the
    # fields below, and under Set-StrictMode -Version Latest reading a missing
    # key throws, so the whole queue entry died with:
    #
    #   The property 'ResolvedEncodeLane' cannot be found on this object.
    #
    # which is what turned "both lanes declined this file" into FAILED. There is
    # nothing to judge for NVENC suitability when there is no resolved encode:
    # return neutral and let the caller's own EarlyExit branch decide.
    $earlyExit = [string](Get-OptionalProperty -InputObject $Init -PropertyName 'EarlyExit' -Default '')
    if (-not [string]::IsNullOrWhiteSpace($earlyExit)) {
        return [pscustomobject][ordered]@{
            Allowed = $true
            Reason  = "No resolved encode to assess (early exit: $earlyExit)."
        }
    }

    $initSourceProfile = Get-OptionalProperty -InputObject $Init -PropertyName 'SourceProfile' -Default $null
    $initAutoSettings  = Get-OptionalProperty -InputObject $Init -PropertyName 'AutoSettings'  -Default $null
    $initLane          = [string](Get-OptionalProperty -InputObject $Init -PropertyName 'ResolvedEncodeLane' -Default '')

    if ($initLane -ne 'Nvidia') {
        return [pscustomobject][ordered]@{
            Allowed = $true
            Reason  = ''
        }
    }

    $preflight = Get-OptionalProperty -InputObject $Init -PropertyName 'PreflightEstimate' -Default $null
    $isHdr = [bool](Get-OptionalProperty -InputObject $initSourceProfile -PropertyName 'HasHDR' -Default $false)
    $resolutionTier = [string](Get-OptionalProperty -InputObject $initAutoSettings -PropertyName 'ResolutionTier' -Default '')
    $codecClass = [string](Get-OptionalProperty -InputObject $initAutoSettings -PropertyName 'CodecClass' -Default '')
    $grainClass = [string](Get-OptionalProperty -InputObject $initAutoSettings -PropertyName 'GrainClass' -Default '')
    $preflightTargets = Resolve-PreflightAutoTuneTargets -QualityProfile $PreflightAutoTuneQuality -ResolutionTier $resolutionTier -SourceProfile $initSourceProfile `
        -SourceGiBPerHour ([double](Get-OptionalProperty -InputObject $initAutoSettings -PropertyName 'VideoBitratePerHourGiB' -Default 0.0))

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
# FUNCTION: Get-ContentCacheKey
#
# Identity for a file's CONTENT, not just its name: path + length + last-write
# time. Two calls a second apart return the same key; a call after the file was
# rewritten returns a different one. That is what makes it safe to memoise an
# expensive answer (a probe, a quality search, a size projection) against it.
#
# A file that cannot be stat'ed -- deleted, or a growing temp output that has
# gone away between calls -- gets a key with no size or timestamp, so it is
# never confused with a readable file of the same name.
# =============================================================================
function Get-ContentCacheKey {
    param([string]$Path, [string]$Scope = '')
    $normalised = if ([string]::IsNullOrWhiteSpace($Path)) { '' } else { $Path.ToLowerInvariant() }
    try {
        $item = Get-Item -LiteralPath $Path -ErrorAction Stop
        return ("{0}|{1}|{2}|{3}" -f $normalised, $item.Length, $item.LastWriteTimeUtc.Ticks, $Scope)
    } catch {
        return ("{0}|?|?|{1}" -f $normalised, $Scope)
    }
}

# =============================================================================
# FUNCTION: Write-HostOnce
#
# Console output that says the same thing about the same file only once.
#
# The lane scheduler can legitimately re-evaluate a file several times in a
# session (a lane frees up, a retry happens). Repeating "HDR: SDR passthrough"
# and an identical quality-search line every time buries the lines that DID
# change, which is what made a real run unreadable. The message is only
# suppressed when both the file and the exact text match a previous one.
# =============================================================================
function Write-HostOnce {
    param(
        [string]$Key,
        [string]$Message,
        [string]$ForegroundColor = 'Gray'
    )
    # Emits nothing to the pipeline. Callers sit inside functions that return
    # structured objects, and a stray $true would be appended to the result.
    if (-not $script:AnnouncedOnce.Add(("{0}||{1}" -f $Key, $Message))) { return }
    Write-Host $Message -ForegroundColor $ForegroundColor
}

# =============================================================================
# FUNCTION: Test-EncoderWorkPath
#
# True for a path this script produced as scratch rather than as a deliverable:
# an in-progress .encoding.tmp. output, a leftover .clean.tmp. experiment, or
# anything living under the .queue work tree (pending, working, preflight
# samples, backup_originals).
#
# This exists because of a real failure. A folder drop enumerated a leftover
# .encoding.tmp.mkv from an earlier aborted run and queued it as its own job.
# The queue then re-encoded that file at the same moment another worker was
# writing to it, which produced a stream of EBML parse errors from probing a
# file mid-write and ended in "Output duration check failed. Source=4039.488s
# Output=1412.511s". Nothing downstream could have caught it; the only correct
# place to refuse is where paths enter the queue.
# =============================================================================
function Test-EncoderWorkPath {
    param([string]$Path)

    if ([string]::IsNullOrWhiteSpace($Path)) { return $false }

    $leaf = try { [System.IO.Path]::GetFileName($Path) } catch { $Path }
    if ($leaf -match '(?i)\.(encoding|clean)\.tmp\.') { return $true }

    $workFolders = @('.queue', 'preflight', 'backup_originals')
    foreach ($segment in ($Path -split '[\\/]')) {
        if ($workFolders -contains $segment.ToLowerInvariant()) { return $true }
    }
    return $false
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
    param(
        [string[]]$Paths,
        [string]$AutoCRFOffsetOverrideValue = '',
        [string]$TargetGiBPerHourOverrideValue = '',
        [string]$EncoderPreferenceOverrideValue = ''
    )

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

        # A file this script is still writing, or one it left behind after an
        # aborted run, is not an input. Queueing one means encoding a partial
        # file -- or racing a live worker for the same bytes.
        if (Test-EncoderWorkPath -Path $full) {
            Write-Host "Skipping this script's own work file: $full" -ForegroundColor DarkYellow
            Write-SessionTextLogMessage -Level Warn -Message ("Queue add skipped | encoder work file | {0}" -f $full)
            continue
        }

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
            AutoCRFOffsetOverride = $AutoCRFOffsetOverrideValue
            TargetGiBPerHourOverride = $TargetGiBPerHourOverrideValue
            EncoderPreferenceOverride = $EncoderPreferenceOverrideValue
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
    param(
        [string]$InputPath,
        [switch]$NoCache
    )

    # Same file, same bytes, same answer. Lane resolution and encode
    # initialisation each used to probe independently, so a single scheduling
    # pass over a queue of N files ran 2N-3N probes of unchanged files.
    $cacheKey = Get-ContentCacheKey -Path $InputPath -Scope 'ffprobe'
    if (-not $NoCache -and $script:FfprobeCache.ContainsKey($cacheKey)) {
        $script:DecisionCacheHits++
        return $script:FfprobeCache[$cacheKey]
    }

    # Pass arguments as an explicit array so PowerShell handles quoting
    # for the path correctly. The --% stop-parsing token only works inline
    # on the same logical line as the command; it does not survive backtick
    # line continuations and would be passed to ffprobe as a literal string
    # argument, causing an 'Invalid argument' error on the path.
    #
    # -v fatal, not -v error. Many perfectly playable Matroska remuxes make
    # ffprobe print container gripes ("0x00 at pos N invalid as first byte of
    # an EBML number") that it then recovers from and parses correctly. At
    # -v error those go to the console on every probe of every file, which in a
    # real run buried the encoder's own output. Anything that actually stops
    # the probe still surfaces below, with ffprobe's own wording.
    $ffprobeArgs = @(
        '-v',            'fatal',
        '-print_format', 'json',
        '-show_format',
        '-show_streams',
        '-show_chapters',
        $InputPath
    )

    $json = & $FfprobePath @ffprobeArgs

    if (-not $json) {
        # Only now is the noise worth having: re-run loudly so the exception
        # carries ffprobe's own diagnosis instead of a bare "no output".
        $detail = ''
        try {
            $detail = ((& $FfprobePath '-v' 'error' '-print_format' 'json' '-show_format' $InputPath 2>&1) |
                        Select-Object -Last 3) -join ' | '
        } catch {}
        if ([string]::IsNullOrWhiteSpace($detail)) {
            throw "ffprobe returned no output for: $InputPath"
        }
        throw "ffprobe returned no output for: $InputPath ($detail)"
    }

    $parsed = ($json | ConvertFrom-Json -Depth 100)
    if (-not $NoCache) {
        $script:FfprobeCache[$cacheKey] = $parsed
        $script:DecisionCacheMisses++
    }
    return $parsed
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

# =============================================================================
# Perceptual quality measurement and quality-targeted CRF search
#
# Everything else in this script decides file size from bitrate heuristics.
# A heuristic can answer "how big will it be"; it can never answer "does it
# still look the same". The second question is the actual goal -- convert to
# AV1 and shrink the file WITHOUT losing perceptible quality -- and it cannot
# be optimised for without being measured.
#
# Two metrics are used, each only where it is valid:
#
#   VMAF   Netflix's perceptual model. Its scale is absolute and calibrated
#          against human scores (100 = reference), which is what makes a fixed
#          target meaningful. Its models are trained on SDR BT.1886 material,
#          so it is used for SDR sources only.
#   XPSNR  Fraunhofer HHI's perceptually weighted PSNR, developed and
#          validated on UHD/HDR. Present in stock FFmpeg 7.1+, needs no model
#          files, and is meaningful on PQ and HLG signals.
#
# Why XPSNR is used RELATIVE TO AN ANCHOR instead of against a fixed dB
# threshold. Measured on a current FFmpeg/SVT-AV1 build while building this:
#
#   flat, low-detail source     CRF 18 -> 64.5 dB      CRF 50 -> 49.9 dB
#   heavily grained source      CRF 18 -> 35.1 dB      CRF 46 -> 32.0 dB
#
# The commonly quoted "above 42 dB is visually lossless" would therefore
# refuse to compress the grained source at ANY CRF, and would wave through the
# flat source at CRF 50. The absolute number carries almost no information
# across content. Anchoring to a near-lossless encode of the SAME content
# removes that dependence. It has a second benefit: the anchor is encoded with
# the same film-grain synthesis settings as the candidate, so the pixel-
# fidelity penalty that grain synthesis inflicts on ANY reference metric is
# common to both sides and cancels out of the difference.
#
# Absolute VMAF has its own version of the same problem in the other
# direction: measured here, a lossless comparison of a grained source against
# itself scored 99.32, not 100, while an easy source hit exactly 100.000 well
# before the CRF ceiling. That is why the VMAF path also supports anchor mode,
# and why the shipped defaults are treated as starting points to be checked
# with Media2AV1Queue-Quality.ps1 on real library content rather than as
# settled constants.
# =============================================================================

# XPSNR prints "inf" when the two inputs are bit-identical. Arithmetic on the
# parsed value still has to work, so "inf" is mapped to a finite sentinel that
# is far above any real measurement and flagged separately.

# -----------------------------------------------------------------------------
# Determines whether libsvtav1 actually accepts an -svtav1-params key.
#
# This CANNOT be done by exit code, and it cannot be done from help text.
# Verified against SVT-AV1 4.2 in a current FFmpeg build:
#
#   * ffmpeg -h encoder=libsvtav1 lists "-svtav1-params <dictionary>" and
#     nothing about the keys inside it, because the keys are the library's,
#     not ffmpeg's. Help-text probing can therefore never find one. The old
#     HDR10+ check looked for "hdr10plus-json" in that text, which means it
#     was guaranteed to report "unsupported" even on a fork that supports it.
#   * An unknown key is reported as
#         [libsvtav1 @ ...] Error parsing option totally-bogus-key: 1.
#     and the encode then CONTINUES AND SUCCEEDS, exit code 0. So a probe that
#     trusts the exit code reports every key as supported, including keys that
#     were silently discarded. That failure mode is the dangerous one: it would
#     have the script announce "HDR10+ preserved via SVT-AV1" on a build that
#     threw the metadata away.
#
# The log line is the only reliable signal, and it is emitted below error
# level, so the probe has to capture stderr at warning verbosity.
# -----------------------------------------------------------------------------
function Test-SvtAv1ParamSupported {
    param([Parameter(Mandatory = $true)][string]$ParamPair)

    $key = ($ParamPair -split '=', 2)[0]
    if ([string]::IsNullOrWhiteSpace($key)) { return $false }
    if ($script:SvtParamSupportCache.ContainsKey($key)) { return [bool]$script:SvtParamSupportCache[$key] }

    $supported = $false
    try {
        $probeArgs = @(
            '-hide_banner', '-nostdin', '-nostats', '-loglevel', 'warning', '-y',
            '-f', 'lavfi', '-i', 'color=c=black:s=256x144:r=24:d=1',
            '-frames:v', '4', '-c:v', 'libsvtav1', '-preset', '12',
            '-svtav1-params', $ParamPair,
            '-f', 'null', '-'
        )

        $psi = [System.Diagnostics.ProcessStartInfo]::new()
        $psi.FileName = $FfmpegPath
        foreach ($a in $probeArgs) { $psi.ArgumentList.Add($a) }
        $psi.RedirectStandardError  = $true
        $psi.RedirectStandardOutput = $true
        $psi.UseShellExecute = $false
        $proc = [System.Diagnostics.Process]::Start($psi)
        $stdErrText = $proc.StandardError.ReadToEnd()
        $null = $proc.StandardOutput.ReadToEnd()
        if (-not $proc.WaitForExit(30000)) {
            try { $proc.Kill($true) } catch { }
            $script:SvtParamSupportCache[$key] = $false
            return $false
        }

        $pattern = '(?im)Error\s+parsing\s+option\s+' + [Regex]::Escape($key) + '\s*:'
        $supported = -not ($stdErrText -match $pattern)
    } catch {
        $supported = $false
    }

    $script:SvtParamSupportCache[$key] = [bool]$supported
    return [bool]$supported
}

# -----------------------------------------------------------------------------
# Parses the xpsnr filter's summary line.
#
# Verified output format:
#   [Parsed_xpsnr_2 @ 0x...] XPSNR  y: 37.2171  u: 31.9970  v: 32.5697  (minimum: 31.9970)
# and, for bit-identical inputs:
#   [Parsed_xpsnr_0 @ 0x...] XPSNR  y: inf  u: inf  v: inf  (minimum: inf)
#
# The last match in the text is the summary; per-frame lines, if stats are
# enabled, come earlier.
# -----------------------------------------------------------------------------
function ConvertFrom-XpsnrOutput {
    param([string]$Text)

    if ([string]::IsNullOrWhiteSpace($Text)) { return $null }

    $num = 'inf|-?[0-9]+(?:\.[0-9]+)?'
    $pattern = "(?im)XPSNR\s+y:\s*(?<y>$num)\s+u:\s*(?<u>$num)\s+v:\s*(?<v>$num)"
    $matches = [Regex]::Matches($Text, $pattern)
    if ($matches.Count -eq 0) { return $null }

    $m = $matches[$matches.Count - 1]
    $isLossless = $false
    $parsed = @{}
    foreach ($component in @('y', 'u', 'v')) {
        $raw = $m.Groups[$component].Value
        if ($raw -match '(?i)^inf$') {
            $parsed[$component] = $script:QualityLosslessDbSentinel
            $isLossless = $true
        } else {
            $value = 0.0
            if (-not [double]::TryParse($raw, [System.Globalization.NumberStyles]::Float,
                                        [System.Globalization.CultureInfo]::InvariantCulture, [ref]$value)) {
                return $null
            }
            $parsed[$component] = $value
        }
    }

    $y = [double]$parsed['y']
    $u = [double]$parsed['u']
    $v = [double]$parsed['v']

    # Weighted is the default because the filter authors' recommended
    # "minimum of the three" is dominated by chroma on 4:2:0 material, where
    # chroma is already subsampled and scores several dB lower than luma
    # regardless of encoder settings. That makes the minimum track chroma
    # subsampling rather than encode quality.
    $aggregate = switch ($QualityXpsnrAggregation) {
        'Min'  { [Math]::Min($y, [Math]::Min($u, $v)) }
        'Luma' { $y }
        default { ((4.0 * $y) + $u + $v) / 6.0 }
    }

    return [pscustomobject][ordered]@{
        Y          = $y
        U          = $u
        V          = $v
        Minimum    = [Math]::Min($y, [Math]::Min($u, $v))
        Weighted   = ((4.0 * $y) + $u + $v) / 6.0
        Value      = $aggregate
        IsLossless = $isLossless
    }
}

# -----------------------------------------------------------------------------
# Parses the libvmaf filter's summary line: "VMAF score: 95.735583".
# -----------------------------------------------------------------------------
function ConvertFrom-VmafOutput {
    param([string]$Text)

    if ([string]::IsNullOrWhiteSpace($Text)) { return $null }

    $matches = [Regex]::Matches($Text, '(?im)VMAF\s+score:\s*(?<v>-?[0-9]+(?:\.[0-9]+)?)')
    if ($matches.Count -eq 0) { return $null }

    $raw = $matches[$matches.Count - 1].Groups['v'].Value
    $value = 0.0
    if (-not [double]::TryParse($raw, [System.Globalization.NumberStyles]::Float,
                                [System.Globalization.CultureInfo]::InvariantCulture, [ref]$value)) {
        return $null
    }

    return [pscustomobject][ordered]@{
        Value      = $value
        IsLossless = ($value -ge 99.999)
    }
}

# -----------------------------------------------------------------------------
# Runs a two-input measurement filtergraph and returns its stderr.
# -----------------------------------------------------------------------------
function Invoke-QualityMeasurementFilter {
    param(
        [string]$DistortedPath,
        [string]$SourcePath,
        [double]$StartSec,
        [int]$DurationSec,
        [int]$SourceVideoStreamIndex,
        [string]$InputBitstreamFilter = '',
        [Parameter(Mandatory = $true)][string]$FilterSpec
    )

    $ffArgs = New-Object System.Collections.Generic.List[string]
    $ffArgs.AddRange([string[]]@('-hide_banner', '-nostdin', '-nostats', '-loglevel', 'info'))

    # Input 0: the encoded sample, exactly as it will exist in the output.
    $ffArgs.AddRange([string[]]@('-i', $DistortedPath))

    # Input 1: the same source segment, decoded the same way the sample encode
    # decoded it. Identical -ss / -t / -bsf:v flags mean identical frames --
    # -ss before -i is a keyframe-relative fast seek, so the two legs only line
    # up if the flags match exactly. They must also match the sample encode's
    # flags, which is why both come from one builder.
    $ffArgs.AddRange([string[]]@('-ss', ("{0:0.###}" -f $StartSec), '-t', "$DurationSec"))
    if (-not [string]::IsNullOrWhiteSpace($InputBitstreamFilter)) {
        $ffArgs.AddRange([string[]]@('-bsf:v', $InputBitstreamFilter))
    }
    $ffArgs.AddRange([string[]]@('-i', $SourcePath))

    # The reference is addressed by absolute stream index, not by [1:v]. A
    # container with cover art or a thumbnail stream has more than one video
    # stream, and [1:v] would silently pick the wrong one.
    $refLabel = "1:$SourceVideoStreamIndex"
    $graph = "[0:v]setpts=PTS-STARTPTS,format=yuv420p10le[qdist];" +
             "[$refLabel]setpts=PTS-STARTPTS,format=yuv420p10le[qref];" +
             "[qdist][qref]$FilterSpec"

    $ffArgs.AddRange([string[]]@('-lavfi', $graph, '-an', '-sn', '-dn', '-f', 'null', '-'))

    return (Invoke-FfmpegSync -Arguments $ffArgs.ToArray())
}

# -----------------------------------------------------------------------------
# Picks the VMAF model for the source resolution. The 4K model is trained for
# UHD viewing distances; using the 1080p model on UHD material overstates
# quality, and vice versa.
# -----------------------------------------------------------------------------
function Get-VmafModelName {
    param([string]$ResolutionTier)

    if ($ResolutionTier -eq 'UHD') { return 'vmaf_4k_v0.6.1' }
    return 'vmaf_v0.6.1'
}

# -----------------------------------------------------------------------------
# Probes what this ffmpeg build can actually measure.
#
# Both filters are checked functionally rather than by name, because a filter
# can be listed and still be unusable -- libvmaf in particular needs its model
# files, and a build can carry the filter without them.
# -----------------------------------------------------------------------------
function Get-QualityToolchainEnvironment {
    if ($null -ne $script:QualityToolchainCache) { return $script:QualityToolchainCache }

    $filterText = ''
    try { $filterText = (& $FfmpegPath -hide_banner -filters 2>&1 | Out-String) } catch { $filterText = '' }

    $xpsnrListed = ($filterText -match '(?im)^\s*\S+\s+xpsnr\s')
    $vmafListed  = ($filterText -match '(?im)^\s*\S+\s+libvmaf\s')

    $probe = {
        param([string]$Spec)
        try {
            $probeArgs = @(
                '-hide_banner', '-nostdin', '-nostats', '-loglevel', 'info', '-y',
                '-f', 'lavfi', '-i', 'testsrc2=size=256x144:rate=24:duration=1',
                '-f', 'lavfi', '-i', 'testsrc2=size=256x144:rate=24:duration=1',
                '-lavfi', ("[0:v]format=yuv420p10le[a];[1:v]format=yuv420p10le[b];[a][b]{0}" -f $Spec),
                '-frames:v', '4', '-f', 'null', '-'
            )
            $psi = [System.Diagnostics.ProcessStartInfo]::new()
            $psi.FileName = $FfmpegPath
            foreach ($a in $probeArgs) { $psi.ArgumentList.Add($a) }
            $psi.RedirectStandardError  = $true
            $psi.RedirectStandardOutput = $true
            $psi.UseShellExecute = $false
            $p = [System.Diagnostics.Process]::Start($psi)
            $err = $p.StandardError.ReadToEnd()
            $null = $p.StandardOutput.ReadToEnd()
            if (-not $p.WaitForExit(60000)) {
                try { $p.Kill($true) } catch { }
                return [pscustomobject]@{ Usable = $false; Detail = 'probe timed out' }
            }
            $ok = ($p.ExitCode -eq 0)
            $lastLine = (($err -split "\r?\n" | Where-Object { $_.Trim() }) | Select-Object -Last 1)
            return [pscustomobject]@{ Usable = $ok; Detail = [string]$lastLine; Stderr = $err }
        } catch {
            return [pscustomobject]@{ Usable = $false; Detail = $_.Exception.Message; Stderr = '' }
        }
    }

    $xpsnrUsable = $false
    $xpsnrDetail = 'xpsnr filter not present (requires FFmpeg 7.1 or newer)'
    if ($xpsnrListed) {
        $r = & $probe 'xpsnr'
        $xpsnrUsable = [bool]$r.Usable
        $xpsnrDetail = [string]$r.Detail
    }

    $vmafUsable = $false
    $vmafDetail = 'libvmaf filter not present (build lacks --enable-libvmaf)'
    $vmaf4kUsable = $false
    if ($vmafListed) {
        $r = & $probe 'libvmaf=n_threads=2'
        $vmafUsable = [bool]$r.Usable
        $vmafDetail = [string]$r.Detail
        if ($vmafUsable) {
            $r4k = & $probe "libvmaf=model='version=vmaf_4k_v0.6.1':n_threads=2"
            $vmaf4kUsable = [bool]$r4k.Usable
        }
    }

    # libsvtav1 gained a -dolbyvision AVOption that defaults to "auto". On a
    # source whose RPU survived into frame side data, auto can re-emit Dolby
    # Vision into the AV1 stream -- which is the opposite of what the HDR10
    # conversion path is trying to produce. Detecting the option lets the
    # encoder be told explicitly not to.
    $encoderHelpText = ''
    try { $encoderHelpText = (& $FfmpegPath -hide_banner -h encoder=libsvtav1 2>&1 | Out-String) } catch { $encoderHelpText = '' }
    $supportsDolbyVisionOption = Test-TextContainsOption -Text $encoderHelpText -OptionName 'dolbyvision'

    $script:QualityToolchainCache = [ordered]@{
        SupportsXpsnr              = [bool]$xpsnrUsable
        XpsnrDetail                = $xpsnrDetail
        SupportsVmaf               = [bool]$vmafUsable
        VmafDetail                 = $vmafDetail
        SupportsVmaf4kModel        = [bool]$vmaf4kUsable
        SupportsDolbyVisionOption  = [bool]$supportsDolbyVisionOption
    }

    return $script:QualityToolchainCache
}

# -----------------------------------------------------------------------------
# Decides which metric to use for this source, in which mode, and against what
# threshold.
#
# The split is deliberate and is about validity, not preference:
#   SDR -> VMAF absolute. VMAF's scale is human-calibrated, so a fixed target
#          means something, and its models fit SDR material.
#   HDR -> XPSNR anchored. VMAF's models are not trained on PQ or HLG, so an
#          absolute VMAF number on HDR is not interpretable. XPSNR is, and
#          anchoring removes its content dependence.
# -----------------------------------------------------------------------------
function Resolve-QualityMetricPlan {
    param(
        $SourceProfile,
        $AutoSettings,
        [string]$EncodeMode
    )

    $disabled = {
        param([string]$Why)
        [pscustomobject][ordered]@{
            Enabled = $false; Metric = 'None'; Mode = 'None'; FilterSpec = ''
            Threshold = 0.0; AbsoluteTarget = 0.0; AnchorDrop = 0.0
            AnchorCRF = 0; ConvergenceBand = 0.0; InitialSlope = 0.0
            VmafModel = ''; Reason = $Why
        }
    }

    if (-not $EnableQualityTargeting) { return & $disabled 'Quality targeting disabled by configuration.' }
    if ($QualityMetric -eq 'Off')     { return & $disabled 'Quality targeting disabled ($QualityMetric = Off).' }

    $tc = Get-QualityToolchainEnvironment
    $isHdr = [bool](Get-OptionalProperty -InputObject $SourceProfile -PropertyName 'HasHDR' -Default $false)
    $tier  = [string](Get-OptionalProperty -InputObject $AutoSettings -PropertyName 'ResolutionTier' -Default 'HD')

    $requested = if ([string]::IsNullOrWhiteSpace([string]$QualityMetric)) { 'Auto' } else { [string]$QualityMetric }
    $metric = ''
    $reasonParts = New-Object System.Collections.Generic.List[string]

    switch ($requested) {
        'VMAF' {
            if (-not $tc.SupportsVmaf) { return & $disabled "VMAF requested but this ffmpeg build cannot run libvmaf ($($tc.VmafDetail))." }
            $metric = 'VMAF'
            if ($isHdr) { $reasonParts.Add('VMAF forced on an HDR source; its models are SDR-trained, so treat the number as indicative only') }
        }
        'XPSNR' {
            if (-not $tc.SupportsXpsnr) { return & $disabled "XPSNR requested but this ffmpeg build cannot run the xpsnr filter ($($tc.XpsnrDetail))." }
            $metric = 'XPSNR'
        }
        default {
            if ($isHdr) {
                if ($tc.SupportsXpsnr)   { $metric = 'XPSNR'; $reasonParts.Add('HDR source: XPSNR (VMAF models are SDR-trained)') }
                elseif ($tc.SupportsVmaf) { $metric = 'VMAF'; $reasonParts.Add('HDR source and no xpsnr filter: falling back to VMAF, treat as indicative') }
            } else {
                if ($tc.SupportsVmaf)     { $metric = 'VMAF'; $reasonParts.Add('SDR source: VMAF absolute target') }
                elseif ($tc.SupportsXpsnr) { $metric = 'XPSNR'; $reasonParts.Add('SDR source and no libvmaf: XPSNR anchored') }
            }
            if ([string]::IsNullOrWhiteSpace($metric)) {
                return & $disabled "No usable quality metric in this ffmpeg build (xpsnr: $($tc.XpsnrDetail); libvmaf: $($tc.VmafDetail))."
            }
        }
    }

    $mode = if ([string]::IsNullOrWhiteSpace([string]$QualityMode) -or $QualityMode -eq 'Auto') {
        if ($metric -eq 'VMAF') { 'Absolute' } else { 'Anchor' }
    } else {
        [string]$QualityMode
    }

    $vmafModel = ''
    $filterSpec = ''
    $absoluteTarget = 0.0
    $anchorDrop = 0.0
    $band = 0.0
    $slope = 0.0

    if ($metric -eq 'VMAF') {
        $vmafModel = Get-VmafModelName -ResolutionTier $tier
        if ($vmafModel -eq 'vmaf_4k_v0.6.1' -and -not $tc.SupportsVmaf4kModel) {
            $vmafModel = 'vmaf_v0.6.1'
            $reasonParts.Add('4K VMAF model unavailable in this build; using the 1080p model, which reads optimistic on UHD')
        }
        $threads = [Math]::Max(1, [Math]::Min(16, [int]$QualityVmafThreads))
        $filterSpec = "libvmaf=model='version=$vmafModel':n_threads=$threads"
        $absoluteTarget = [double]$QualityVmafTarget
        $anchorDrop = [double]$QualityVmafAnchorDrop
        $band = [double]$QualityVmafConvergenceBand
        $slope = 0.55   # VMAF points per CRF step, measured mid-range; refined from real points as soon as two exist
    } else {
        $filterSpec = 'xpsnr'
        $absoluteTarget = [double]$QualityXpsnrAbsoluteTarget
        $anchorDrop = [double]$QualityXpsnrAnchorDropDb
        $band = [double]$QualityXpsnrConvergenceBand
        $slope = 0.25   # dB per CRF step; measured range across content was 0.11 (grain) to 0.46 (flat)
    }

    $threshold = if ($mode -eq 'Absolute') { $absoluteTarget } else { 0.0 }  # anchored threshold is set once the anchor is measured

    if ($mode -eq 'Absolute') {
        $reasonParts.Add(("absolute target {0:F2}" -f $absoluteTarget))
    } else {
        $reasonParts.Add(("anchored at CRF {0}, budget {1:F2} below it" -f $QualityAnchorCRF, $anchorDrop))
    }

    return [pscustomobject][ordered]@{
        Enabled         = $true
        Metric          = $metric
        Mode            = $mode
        FilterSpec      = $filterSpec
        Threshold       = $threshold
        AbsoluteTarget  = $absoluteTarget
        AnchorDrop      = $anchorDrop
        AnchorCRF       = [int]$QualityAnchorCRF
        ConvergenceBand = $band
        InitialSlope    = $slope
        VmafModel       = $vmafModel
        Reason          = ($reasonParts -join '; ')
    }
}

# -----------------------------------------------------------------------------
# Measures one already-encoded sample against its source segment.
# -----------------------------------------------------------------------------
function Measure-SampleQuality {
    param(
        [string]$SamplePath,
        [string]$SourcePath,
        [double]$StartSec,
        [int]$DurationSec,
        [int]$SourceVideoStreamIndex,
        [string]$InputBitstreamFilter = '',
        $MetricPlan
    )

    $result = Invoke-QualityMeasurementFilter `
        -DistortedPath $SamplePath `
        -SourcePath $SourcePath `
        -StartSec $StartSec `
        -DurationSec $DurationSec `
        -SourceVideoStreamIndex $SourceVideoStreamIndex `
        -InputBitstreamFilter $InputBitstreamFilter `
        -FilterSpec $MetricPlan.FilterSpec

    if ($result.ExitCode -ne 0) {
        return [pscustomobject][ordered]@{
            Measured = $false
            Value = 0.0
            Detail = ("measurement failed: " + (($result.LogLines | Select-Object -Last 2) -join ' || '))
        }
    }

    $parsed = if ($MetricPlan.Metric -eq 'VMAF') {
        ConvertFrom-VmafOutput -Text $result.Stderr
    } else {
        ConvertFrom-XpsnrOutput -Text $result.Stderr
    }

    if ($null -eq $parsed) {
        return [pscustomobject][ordered]@{
            Measured = $false
            Value = 0.0
            Detail = 'measurement produced no parseable score'
        }
    }

    $detail = if ($MetricPlan.Metric -eq 'VMAF') {
        ("VMAF {0:F3}" -f $parsed.Value)
    } else {
        ("XPSNR {0:F3} dB (y {1:F2} / u {2:F2} / v {3:F2})" -f $parsed.Value, $parsed.Y, $parsed.U, $parsed.V)
    }

    return [pscustomobject][ordered]@{
        Measured   = $true
        Value      = [double]$parsed.Value
        IsLossless = [bool]$parsed.IsLossless
        Detail     = $detail
        Raw        = $parsed
    }
}

# -----------------------------------------------------------------------------
# Encodes the designated sample positions at one CRF and measures each.
#
# Returns the median metric and the median bytes/sec, so a single unusual
# sample cannot drag the decision. Samples are deleted as soon as they have
# been measured.
# -----------------------------------------------------------------------------
function Invoke-QualityProbe {
    param(
        [string]$InputPath,
        $Selected,
        $SourceProfile,
        [string]$EncodeMode,
        [int]$CRF,
        [int]$ResolvedPreset,
        [int]$ResolvedFilmGrain,
        [double[]]$SamplePositions,
        [int]$SampleDurationSec,
        $MetricPlan,
        $NvencSettings = $null,
        $NvencEnvironment = $null,
        $HdrPlan = $null,
        [string]$Label = ''
    )

    $metricValues = [System.Collections.Generic.List[double]]::new()
    $rateValues   = [System.Collections.Generic.List[double]]::new()
    $failures     = [System.Collections.Generic.List[string]]::new()
    $anyLossless  = $false
    $bsf = if ($null -ne $HdrPlan) { [string](Get-OptionalProperty -InputObject $HdrPlan -PropertyName 'InputBitstreamFilter' -Default '') } else { '' }
    $videoIndex = [int]$Selected.Video.index

    for ($i = 0; $i -lt $SamplePositions.Count; $i++) {
        $startSec = [double]$SamplePositions[$i]
        $samplePath = Join-Path $PreflightDir ("q{0}_{1}_{2}.mkv" -f $CRF, [Guid]::NewGuid().ToString('N'), $i)

        try {
            $ffArgs = Build-PreflightSampleArgs `
                -InputPath $InputPath `
                -Selected $Selected `
                -SourceProfile $SourceProfile `
                -EncodeMode $EncodeMode `
                -StartSec $startSec `
                -DurationSec $SampleDurationSec `
                -ResolvedCRF $CRF `
                -ResolvedPreset $ResolvedPreset `
                -ResolvedFilmGrain $ResolvedFilmGrain `
                -NvencSettings $NvencSettings `
                -NvencEnvironment $NvencEnvironment `
                -HdrPlan $HdrPlan `
                -OutputPath $samplePath

            $encodeResult = Invoke-FfmpegSync -Arguments $ffArgs
            if ($encodeResult.ExitCode -ne 0) {
                $failures.Add(("sample {0} encode failed: {1}" -f ($i + 1), (($encodeResult.LogLines | Select-Object -Last 2) -join ' || ')))
                continue
            }
            if (-not (Test-Path -LiteralPath $samplePath)) {
                $failures.Add(("sample {0} produced no file" -f ($i + 1)))
                continue
            }

            $item = Get-Item -LiteralPath $samplePath
            if ($item.Length -le 0) {
                $failures.Add(("sample {0} produced an empty file" -f ($i + 1)))
                continue
            }

            $measurement = Measure-SampleQuality `
                -SamplePath $samplePath `
                -SourcePath $InputPath `
                -StartSec $startSec `
                -DurationSec $SampleDurationSec `
                -SourceVideoStreamIndex $videoIndex `
                -InputBitstreamFilter $bsf `
                -MetricPlan $MetricPlan

            if (-not $measurement.Measured) {
                $failures.Add(("sample {0}: {1}" -f ($i + 1), $measurement.Detail))
                continue
            }

            $metricValues.Add([double]$measurement.Value)
            $rateValues.Add($item.Length / [double]$SampleDurationSec)
            if ($measurement.IsLossless) { $anyLossless = $true }
        } finally {
            if (Test-Path -LiteralPath $samplePath) {
                Remove-Item -LiteralPath $samplePath -Force -ErrorAction SilentlyContinue
            }
        }
    }

    if ($metricValues.Count -eq 0) {
        return [pscustomobject][ordered]@{
            Ran = $false; CRF = $CRF; Metric = 0.0; BytesPerSec = 0.0; SampleCount = 0
            IsLossless = $false
            Reason = if ($failures.Count -gt 0) { ($failures | Select-Object -First 2) -join ' | ' } else { 'no samples measured' }
        }
    }

    return [pscustomobject][ordered]@{
        Ran         = $true
        CRF         = $CRF
        Metric      = (Get-MedianValue -Values $metricValues)
        BytesPerSec = (Get-MedianValue -Values $rateValues)
        SampleCount = $metricValues.Count
        IsLossless  = $anyLossless
        Reason      = if ($failures.Count -gt 0) { ("partial: " + (($failures | Select-Object -First 1) -join '')) } else { '' }
    }
}

# -----------------------------------------------------------------------------
# Median of a numeric collection. Used everywhere a mean would let one outlier
# sample decide a CRF.
# -----------------------------------------------------------------------------
function Get-MedianValue {
    param($Values)

    $sorted = @($Values | Sort-Object)
    if ($sorted.Count -eq 0) { return 0.0 }
    if ($sorted.Count % 2 -eq 1) { return [double]$sorted[[int][Math]::Floor($sorted.Count / 2)] }
    return ([double]$sorted[($sorted.Count / 2) - 1] + [double]$sorted[$sorted.Count / 2]) / 2.0
}

# -----------------------------------------------------------------------------
# Chooses the sample positions used for quality measurement.
#
# The same positions are used for every probe in a search, including the
# anchor. Comparing an anchor measured on one scene against a candidate
# measured on another would compare content, not settings.
# -----------------------------------------------------------------------------
function Get-QualitySamplePositions {
    param(
        [double]$SourceDurationSec,
        [int]$SampleDurationSec,
        [int]$RequestedCount
    )

    $positions = New-Object System.Collections.Generic.List[double]
    $maxBySpan = [int][Math]::Floor(($SourceDurationSec * 0.80) / [Math]::Max(1, $SampleDurationSec))
    $count = [Math]::Min([Math]::Max(1, $RequestedCount), [Math]::Max(0, $maxBySpan))

    for ($i = 0; $i -lt $count; $i++) {
        $fraction = 0.10 + (0.80 * (($i + 0.5) / $count))
        $centerSec = $SourceDurationSec * $fraction
        $startSec = [Math]::Max(0.0, [Math]::Min($SourceDurationSec - $SampleDurationSec, $centerSec - ($SampleDurationSec / 2.0)))
        $positions.Add($startSec)
    }

    # Returned through the comma operator as a single typed object. A bare
    # "return @()" unrolls to nothing, so the caller gets $null instead of an
    # empty array, and reading .Count on it throws under StrictMode -- which is
    # exactly what a too-short source would hit.
    $result = [double[]]$positions.ToArray()
    return ,$result
}

# -----------------------------------------------------------------------------
# Finds the highest CRF that still meets the transparency criterion.
#
# The search is a secant method over (CRF, metric) points rather than a
# bisection, because the metric-versus-CRF slope varies about fourfold across
# content -- roughly 0.11 dB per CRF on heavy grain against 0.46 dB per CRF on
# flat material -- so a fixed step size either crawls or overshoots. Every
# probe is a real measurement, so the slope is re-estimated from the two
# nearest measured points and converges in two or three passes.
#
# Direction matters: the goal is the HIGHEST CRF that is still transparent, so
# a metric comfortably above the threshold is not success, it is wasted
# bitrate, and CRF is raised until the headroom is used up.
# -----------------------------------------------------------------------------
function Invoke-QualityTargetedCrfSearch {
    param(
        [string]$InputPath,
        $Selected,
        $SourceProfile,
        [string]$EncodeMode,
        [double]$SourceDurationSec,
        [int]$StartCRF,
        [int]$ResolvedPreset,
        [int]$ResolvedFilmGrain,
        $MetricPlan,
        $NvencSettings = $null,
        $NvencEnvironment = $null,
        $HdrPlan = $null
    )

    $notRun = {
        param([string]$Why)
        [pscustomobject][ordered]@{
            Ran = $false; ChosenCRF = $StartCRF; StartCRF = $StartCRF
            Metric = 0.0; MetricName = 'None'; Mode = 'None'; Threshold = 0.0
            AnchorCRF = 0; AnchorMetric = 0.0; TransparencyMet = $true
            ProbeCount = 0; Passes = @(); Reason = $Why
            BytesPerSecAtChosen = 0.0
        }
    }

    if (-not $MetricPlan.Enabled) { return & $notRun $MetricPlan.Reason }

    # A quality search is the most expensive question this script asks: several
    # sample encodes plus a metric run each, per CRF probe. The answer depends
    # only on the file's bytes and the encode settings being measured, so the
    # same question is answered once per session however many times the
    # scheduler asks it.
    $searchCacheKey = Get-ContentCacheKey -Path $InputPath -Scope (
        "qsearch|{0}|{1}|{2}|{3}|{4}|{5}|{6:F3}|{7:F3}|{8}|{9}" -f `
            $EncodeMode, $StartCRF, $ResolvedPreset, $ResolvedFilmGrain,
            $MetricPlan.Metric, $MetricPlan.Mode, [double]$MetricPlan.AbsoluteTarget,
            [double]$MetricPlan.AnchorDrop, $MetricPlan.AnchorCRF,
            $(if ($NvencSettings) { "$($NvencSettings.CQ)/$($NvencSettings.Preset)/$($NvencSettings.Tune)" } else { 'sw' })
    )
    if ($script:QualitySearchCache.ContainsKey($searchCacheKey)) {
        $script:DecisionCacheHits++
        return $script:QualitySearchCache[$searchCacheKey]
    }

    $sampleDuration = [Math]::Max(4, [Math]::Min(120, [int]$QualitySampleDurationSec))
    $positions = Get-QualitySamplePositions -SourceDurationSec $SourceDurationSec -SampleDurationSec $sampleDuration -RequestedCount ([int]$QualitySampleCount)
    if ($positions.Count -lt 1) {
        return & $notRun 'Source is too short for the configured quality sample spacing.'
    }

    $ceiling = if ($QualityCrfCeiling -eq 'Auto') {
        [Math]::Min(63, $StartCRF + [int]$QualityMaxCrfAboveAuto)
    } else {
        [Math]::Max(0, [Math]::Min(63, [int]$QualityCrfCeiling))
    }
    $floor = [Math]::Max(0, $StartCRF - [int]$QualityMaxCrfBelowAuto)
    if ($ceiling -lt $StartCRF) { $ceiling = $StartCRF }

    $probes = @{}
    $passLog = New-Object System.Collections.Generic.List[string]
    $probeCount = 0

    $runProbe = {
        param([int]$Crf, [string]$Label)
        if ($probes.ContainsKey($Crf)) { return $probes[$Crf] }
        $p = Invoke-QualityProbe `
            -InputPath $InputPath -Selected $Selected -SourceProfile $SourceProfile `
            -EncodeMode $EncodeMode -CRF $Crf -ResolvedPreset $ResolvedPreset `
            -ResolvedFilmGrain $ResolvedFilmGrain -SamplePositions $positions `
            -SampleDurationSec $sampleDuration -MetricPlan $MetricPlan `
            -NvencSettings $NvencSettings -NvencEnvironment $NvencEnvironment `
            -HdrPlan $HdrPlan -Label $Label
        $probes[$Crf] = $p
        return $p
    }

    # ---- Threshold -------------------------------------------------------
    $anchorCrf = 0
    $anchorMetric = 0.0
    $threshold = 0.0

    if ($MetricPlan.Mode -eq 'Anchor') {
        # The anchor is a REFERENCE QUALITY LEVEL, not a near-lossless ceiling.
        # It answers "how good would CRF N have looked on this content?", and
        # the search then finds the cheapest CRF that still looks that good.
        # It is deliberately NOT constrained to sit below the Auto CRF: on easy
        # content the answer is a much higher CRF than Auto would have picked,
        # and on hard content it is a lower one. That two-way movement is the
        # whole point -- the Auto ladder is driven by resolution, codec and BPP,
        # none of which knows how hard the picture actually is to encode.
        $anchorCrf = [Math]::Max(0, [Math]::Min(63, [int]$MetricPlan.AnchorCRF))

        Write-Host ("Quality anchor: encoding reference samples at CRF {0}" -f $anchorCrf) -ForegroundColor DarkCyan
        $anchor = & $runProbe $anchorCrf 'anchor'
        $probeCount++
        if (-not $anchor.Ran) {
            return & $notRun ("Quality anchor probe failed: {0}" -f $anchor.Reason)
        }
        $anchorMetric = [double]$anchor.Metric
        $threshold = $anchorMetric - [double]$MetricPlan.AnchorDrop
        $passLog.Add(("anchor CRF {0}: {1} {2:F3}" -f $anchorCrf, $MetricPlan.Metric, $anchorMetric))
    } else {
        $threshold = [double]$MetricPlan.AbsoluteTarget
        $passLog.Add(("absolute threshold: {0} {1:F2}" -f $MetricPlan.Metric, $threshold))
    }

    # ---- First real point ------------------------------------------------
    $current = & $runProbe $StartCRF 'start'
    $probeCount++
    if (-not $current.Ran) {
        return & $notRun ("Quality probe at CRF {0} failed: {1}" -f $StartCRF, $current.Reason)
    }
    $passLog.Add(("CRF {0}: {1} {2:F3}" -f $StartCRF, $MetricPlan.Metric, $current.Metric))

    # ---- Slope -----------------------------------------------------------
    # An anchor gives a second real point for free, so the very first step is
    # already content-aware instead of using the generic default.
    $slope = [double]$MetricPlan.InitialSlope
    $slopeMin = if ($MetricPlan.Metric -eq 'VMAF') { 0.10 } else { 0.04 }
    $slopeMax = if ($MetricPlan.Metric -eq 'VMAF') { 3.00 } else { 1.20 }
    if ($MetricPlan.Mode -eq 'Anchor' -and [Math]::Abs($StartCRF - $anchorCrf) -ge 2) {
        $measured = ($anchorMetric - [double]$current.Metric) / [double]($StartCRF - $anchorCrf)
        if ($measured -gt 0) { $slope = [Math]::Max($slopeMin, [Math]::Min($slopeMax, $measured)) }
    }

    $band = [double]$MetricPlan.ConvergenceBand
    $maxPasses = [Math]::Max(0, [Math]::Min(6, [int]$QualityMaxSearchPasses))
    $maxStep = [Math]::Max(1, [Math]::Min(16, [int]$QualityMaxCrfStep))

    for ($pass = 1; $pass -le $maxPasses; $pass++) {
        $err = [double]$current.Metric - $threshold

        if ($err -ge 0 -and $err -le $band) {
            $passLog.Add(("converged at CRF {0} (headroom {1:F3} within band {2:F2})" -f $current.CRF, $err, $band))
            break
        }

        $step = [int][Math]::Round($err / $slope)
        if ($step -eq 0) { $step = if ($err -gt 0) { 1 } else { -1 } }

        # The per-pass step cap stops the search overshooting a crossing it is
        # hunting for. When the projected crossing is outside the permitted CRF
        # range there is no crossing to overshoot -- the bound IS the answer --
        # so walking towards it 6 CRF at a time only buys extra sample encodes
        # of a CRF nobody will use. On a grain-heavy source the projection can
        # be 20 CRF below the floor; that used to cost two wasted probes on
        # every single file.
        $projected = $current.CRF + $step
        if ($projected -le $floor) {
            $next = $floor
        } elseif ($projected -ge $ceiling) {
            $next = $ceiling
        } else {
            if ($step -gt $maxStep)  { $step = $maxStep }
            if ($step -lt -$maxStep) { $step = -$maxStep }
            $next = $current.CRF + $step
            if ($next -gt $ceiling) { $next = $ceiling }
            if ($next -lt $floor)   { $next = $floor }
        }

        if ($next -eq $current.CRF) {
            $boundName = if ($step -gt 0) { 'ceiling' } else { 'floor' }
            $passLog.Add(("stopped at CRF {0}: bounded by the CRF {1}" -f $current.CRF, $boundName))
            break
        }
        if ($probes.ContainsKey($next)) {
            $passLog.Add(("stopped at CRF {0}: CRF {1} already measured" -f $current.CRF, $next))
            break
        }

        $previous = $current
        $probe = & $runProbe $next ("pass$pass")
        $probeCount++
        if (-not $probe.Ran) {
            $passLog.Add(("CRF {0} probe failed: {1}" -f $next, $probe.Reason))
            break
        }
        $passLog.Add(("CRF {0}: {1} {2:F3}" -f $next, $MetricPlan.Metric, $probe.Metric))

        if ($probe.CRF -ne $previous.CRF) {
            $secant = ([double]$previous.Metric - [double]$probe.Metric) / [double]($probe.CRF - $previous.CRF)
            if ($secant -gt 0) { $slope = [Math]::Max($slopeMin, [Math]::Min($slopeMax, $secant)) }
        }

        $current = $probe
    }

    # ---- Pick the winner -------------------------------------------------
    # The highest measured CRF that meets the threshold. The anchor is eligible
    # like any other measured point, but only if it clears the CRF floor -- an
    # anchor set well below the Auto CRF would otherwise be chosen whenever
    # nothing else passed, inflating the file in the name of quality nobody
    # asked for.
    $candidates = @($probes.Values | Where-Object { $_.Ran -and $_.CRF -ge $floor })
    $passing = @($candidates | Where-Object { [double]$_.Metric -ge $threshold } | Sort-Object -Property CRF -Descending)

    if ($candidates.Count -eq 0) {
        # Possible when every probe that ran sits below the CRF floor, which
        # only happens if the anchor was the sole successful probe. Nothing was
        # learned about the operating range, so report no result rather than
        # indexing into an empty set.
        return & $notRun 'No quality probe inside the permitted CRF range produced a measurement.'
    }

    # ---- When nothing measured met the threshold --------------------------
    # An absolute VMAF target is a statement about the metric's SCALE, and that
    # scale collapses on grain-heavy sources: the score is dominated by grain no
    # bitrate reproduces exactly, so it barely moves with CRF and never reaches
    # the target. Measured while building this, on a heavily grained 1080p
    # source: CRF 14 -> 75.6, CRF 22 -> 75.0, CRF 30 -> 73.8, while the file
    # shrank 6.8x. A clean source scored 98.4 at CRF 22. So "no CRF was
    # transparent" on such a file is the metric failing to discriminate, not
    # evidence that the file cannot be converted.
    #
    # The old answer was the LOWEST CRF probed -- the most expensive point
    # measured -- and then, by default, refusing the file anyway. Neither half
    # was right. Nothing measured says the lowest CRF is needed; it is simply
    # where the search ran out of room. The honest fallback is the CRF the Auto
    # ladder chose before any of this ran, which is the answer the script would
    # have given without quality targeting at all. Preflight auto-tuning still
    # runs from there, so size targeting is unaffected.
    $startProbe = @($candidates | Where-Object { $_.CRF -eq $StartCRF })

    $transparencyMet = ($passing.Count -gt 0)
    $chosen = if ($transparencyMet) {
        $passing[0]
    } elseif ($startProbe.Count -gt 0) {
        $startProbe[0]
    } else {
        # Only reachable if the starting CRF itself never produced a
        # measurement, in which case the lowest measured point is all there is.
        @($candidates | Sort-Object -Property CRF)[0]
    }

    $reasonParts = New-Object System.Collections.Generic.List[string]
    $reasonParts.Add(("{0} {1} / {2}" -f $MetricPlan.Metric, $MetricPlan.Mode, $MetricPlan.Reason))
    $reasonParts.Add(("threshold {0:F3}" -f $threshold))
    foreach ($line in $passLog) { $reasonParts.Add($line) }
    if ($transparencyMet) {
        $reasonParts.Add(("chose CRF {0} ({1} {2:F3}, {3:F3} above threshold)" -f $chosen.CRF, $MetricPlan.Metric, $chosen.Metric, ([double]$chosen.Metric - $threshold)))
    } else {
        $reasonParts.Add(("threshold {0:F3} not met at any measured CRF down to {1}; the metric could not discriminate on this content, so the Auto CRF {2} stands ({3} {4:F3})" -f `
            $threshold, $floor, $chosen.CRF, $MetricPlan.Metric, $chosen.Metric))
    }
    if ($chosen.CRF -ne $StartCRF) {
        $reasonParts.Add(("CRF {0} -> {1} by quality search" -f $StartCRF, $chosen.CRF))
    }

    $searchResult = [pscustomobject][ordered]@{
        Ran                 = $true
        ChosenCRF           = [int]$chosen.CRF
        StartCRF            = $StartCRF
        Metric              = [double]$chosen.Metric
        MetricName          = [string]$MetricPlan.Metric
        Mode                = [string]$MetricPlan.Mode
        Threshold           = $threshold
        AnchorCRF           = $anchorCrf
        AnchorMetric        = $anchorMetric
        TransparencyMet     = $transparencyMet
        ProbeCount          = $probeCount
        BytesPerSecAtChosen = [double]$chosen.BytesPerSec
        Passes              = @($passLog)
        Reason              = ($reasonParts -join ' | ')
    }

    $script:QualitySearchCache[$searchCacheKey] = $searchResult
    $script:DecisionCacheMisses++
    return $searchResult
}

# -----------------------------------------------------------------------------
# Compression-efficiency parameters for SVT-AV1.
#
# Every pair is functionally probed before it is emitted, because -svtav1-params
# silently discards keys the library does not know: an unsupported key is logged
# below error level and the encode still succeeds. Emitting a key blind means
# believing a setting is active when it was thrown away.
#
# Only settings with broad agreement are on by default. The rest are exposed
# and off, with a note to measure them on real content using
# Media2AV1Queue-Quality.ps1 rather than to trust a forum post -- including
# this one.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Tells libsvtav1 not to code a Dolby Vision RPU when the target is HDR10.
#
# libsvtav1 exposes -dolbyvision and it defaults to "auto". On a source whose
# RPU reached frame side data, auto can re-emit Dolby Vision into the AV1
# stream -- the exact opposite of what the HDR10 conversion path is for, and
# invisible until a player prefers the DV layer over the HDR10 base. The option
# is probed rather than assumed, because it is recent.
# -----------------------------------------------------------------------------
function Add-SvtAv1DolbyVisionSuppression {
    param(
        [System.Collections.Generic.List[string]]$ArgumentList,
        $HdrPlan
    )

    $qtc = Get-QualityToolchainEnvironment
    if (-not $qtc.SupportsDolbyVisionOption) { return }

    $target = [string](Get-OptionalProperty -InputObject $HdrPlan -PropertyName 'TargetDynamicRange' -Default '')
    if ($target -match '(?i)dolby|dovi') { return }

    $ArgumentList.AddRange([string[]]@('-dolbyvision', '0'))
}

function Get-SvtAv1EfficiencyParamPairs {
    param(
        $AutoSettings = $null,
        $SourceProfile = $null,
        [double]$FrameRate = 0.0
    )

    $pairs = New-Object System.Collections.Generic.List[string]

    # --tune: 0 = VQ, 1 = PSNR, 2 = SSIM. The library default is PSNR.
    #
    # PSNR is deliberately NOT the default here even though the quality search
    # can measure XPSNR, which is a PSNR-family metric. Tuning the encoder for
    # the same thing the acceptance test measures optimises the score rather
    # than the picture. SSIM is the setting with the broadest agreement for
    # visual fidelity on live-action material.
    $tuneValue = if ($SoftwareTune -eq 'Auto') { 2 } else { [int]$SoftwareTune }
    if ($tuneValue -ge 0 -and $tuneValue -le 3) {
        if (Test-SvtAv1ParamSupported -ParamPair "tune=$tuneValue") { $pairs.Add("tune=$tuneValue") }
    }

    # --keyint: the library default is 161 frames regardless of frame rate,
    # which is roughly 5.4s at 30fps. Longer GOPs code fewer keyframes and cost
    # less; 10s is standard VOD practice and still seeks acceptably.
    if ([double]$SoftwareKeyintSeconds -gt 0) {
        $fps = if ($FrameRate -gt 0.1) { $FrameRate } else { 24.0 }
        $keyint = [int][Math]::Round($fps * [double]$SoftwareKeyintSeconds)
        if ($keyint -gt 0) {
            if (Test-SvtAv1ParamSupported -ParamPair "keyint=$keyint") { $pairs.Add("keyint=$keyint") }
        }
    }

    # --enable-variance-boost: raises quality in low-variance regions, which is
    # exactly where a higher CRF shows first as banding and blocking in flat
    # skies and dark scenes. Since the point of the quality search is to push
    # CRF as high as transparency allows, this guards the failure mode the
    # search is walking towards.
    $varianceOn = if ($SoftwareVarianceBoost -eq 'Auto') { $true } else { [bool]$SoftwareVarianceBoost }
    if ($varianceOn) {
        if (Test-SvtAv1ParamSupported -ParamPair 'enable-variance-boost=1') {
            $pairs.Add('enable-variance-boost=1')
            $strength = [Math]::Max(1, [Math]::Min(4, [int]$SoftwareVarianceBoostStrength))
            if (Test-SvtAv1ParamSupported -ParamPair "variance-boost-strength=$strength") {
                $pairs.Add("variance-boost-strength=$strength")
            }
        }
    }

    # Off by default. Each of these is plausible and none is settled, so they
    # are switches rather than assumptions.
    if ($SoftwareSceneChangeDetection) {
        if (Test-SvtAv1ParamSupported -ParamPair 'scd=1') { $pairs.Add('scd=1') }
    }
    if ($SoftwareEnableOverlays) {
        if (Test-SvtAv1ParamSupported -ParamPair 'enable-overlays=1') { $pairs.Add('enable-overlays=1') }
    }
    if ($null -ne $SoftwareQpScaleCompressStrength) {
        $qpsc = [Math]::Max(0, [Math]::Min(3, [int]$SoftwareQpScaleCompressStrength))
        if (Test-SvtAv1ParamSupported -ParamPair "qp-scale-compress-strength=$qpsc") {
            $pairs.Add("qp-scale-compress-strength=$qpsc")
        }
    }

    # Comma-returned for the same reason as Get-QualitySamplePositions: an
    # empty array must arrive as an empty array, not as $null.
    $result = [string[]]$pairs.ToArray()
    return ,$result
}

# -----------------------------------------------------------------------------
# Builds the ffmpeg argument list for ONE preflight / quality sample encode.
#
# Extracted into its own function because two callers need byte-identical
# encodes: the size-projection preflight and the quality search. If they built
# their arguments separately they would drift, and the earlier version of this
# script already demonstrated what that costs -- the HDR static-metadata probe
# tested one option form while the argument builder emitted another, so a
# capability the build had was reported as missing.
#
# For the quality search the requirement is stronger than "similar": the
# measurement compares the sample against the same source segment decoded with
# the same seek and the same input bitstream filter. Any difference between the
# encode leg and the measurement leg shows up as quality loss that the encoder
# never caused.
# -----------------------------------------------------------------------------
function Build-PreflightSampleArgs {
    param(
        [string]$InputPath,
        $Selected,
        $SourceProfile,
        [string]$EncodeMode,
        [double]$StartSec,
        [int]$DurationSec,
        [int]$ResolvedCRF,
        [int]$ResolvedPreset,
        [int]$ResolvedFilmGrain,
        $NvencSettings = $null,
        $NvencEnvironment = $null,
        $HdrPlan = $null,
        [string]$OutputPath
    )

    $ffArgs = New-Object System.Collections.Generic.List[string]
    $ffArgs.AddRange([string[]]@('-hide_banner', '-nostdin', '-y',
                                 '-ss', ("{0:0.###}" -f $StartSec), '-t', "$DurationSec"))

    if ($EncodeMode -eq 'nvenc' -and $NvencSettings -and $NvencSettings.DecodePath -eq 'cuda') {
        $ffArgs.AddRange([string[]]@('-hwaccel', 'cuda', '-hwaccel_output_format', 'cuda'))
    }

    # The Dolby Vision base-layer filter changes what the encoder sees, so a
    # sample taken without it measures a stream the real encode never touches.
    if ($null -ne $HdrPlan) {
        $bsf = [string](Get-OptionalProperty -InputObject $HdrPlan -PropertyName 'InputBitstreamFilter' -Default '')
        if (-not [string]::IsNullOrWhiteSpace($bsf)) {
            $ffArgs.AddRange([string[]]@('-bsf:v', $bsf))
        }
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
        if ($NvencEnvironment.SupportsBFrames -and $NvencSettings.BFrames -gt 0) {
            $ffArgs.AddRange([string[]]@('-bf', "$($NvencSettings.BFrames)"))
        }
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

        # One -svtav1-params occurrence only. A second occurrence replaces the
        # first outright rather than merging, so splitting these across several
        # options silently discards everything but the last group.
        $svtParams = New-Object System.Collections.Generic.List[string]
        if ($ResolvedFilmGrain -gt 0) {
            $svtParams.Add("film-grain=$ResolvedFilmGrain")
            $denoise = if ($SoftwareFilmGrainDenoise) { 1 } else { 0 }
            $svtParams.Add("film-grain-denoise=$denoise")
        }
        if ($SoftwarePinCores -gt 0 -and $CpuMaxParallel -gt 1) {
            $svtParams.Add("pin=$SoftwarePinCores")
        }
        foreach ($pair in (Get-SvtAv1EfficiencyParamPairs -SourceProfile $SourceProfile -FrameRate ([double](Get-OptionalProperty -InputObject $SourceProfile -PropertyName 'FrameRate' -Default 0.0)))) {
            $svtParams.Add($pair)
        }
        foreach ($pair in (Get-HdrSvtAv1ParamPairs -HdrPlan $HdrPlan)) { $svtParams.Add($pair) }
        if ($svtParams.Count -gt 0) {
            $ffArgs.AddRange([string[]]@('-svtav1-params', ($svtParams -join ':')))
        }
    }

    if ($EncodeMode -ne 'nvenc') {
        Add-SvtAv1DolbyVisionSuppression -ArgumentList $ffArgs -HdrPlan $HdrPlan
    }
    Add-HdrOutputArguments -ArgumentList $ffArgs -HdrPlan $HdrPlan -SourceProfile $SourceProfile
    $ffArgs.Add($OutputPath)

    $result = [string[]]$ffArgs.ToArray()
    return ,$result
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
        $NvencEnvironment = $null,
        $HdrPlan = $null
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

    # Four sample encodes at the same settings on the same bytes give the same
    # projection every time. Memoised so re-evaluating a job the scheduler
    # could not place is a dictionary lookup rather than another round of
    # encodes. $PassNumber and $SettingsLabel are deliberately NOT part of the
    # key: they only affect what is printed, and the second pass always runs at
    # settings that differ somewhere else in the key.
    $preflightCacheKey = Get-ContentCacheKey -Path $InputPath -Scope (
        "preflight|{0}|{1}|{2}|{3}|{4}" -f `
            $EncodeMode, $ResolvedCRF, $ResolvedPreset, $ResolvedFilmGrain,
            $(if ($NvencSettings) { "$($NvencSettings.CQ)/$($NvencSettings.Preset)/$($NvencSettings.Tune)/$($NvencSettings.DecodePath)" } else { 'sw' })
    )
    if ($script:PreflightSampleCache.ContainsKey($preflightCacheKey)) {
        $script:DecisionCacheHits++
        return $script:PreflightSampleCache[$preflightCacheKey]
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
            # Built by the shared sample-argument builder, so the projection
            # measures exactly what the quality search and the real encode
            # produce. Two separate builders drift, and a drift between the
            # probe and the emitter is what previously made a supported HDR
            # option look unsupported.
            $ffArgs = Build-PreflightSampleArgs `
                -InputPath $InputPath `
                -Selected $Selected `
                -SourceProfile $SourceProfile `
                -EncodeMode $EncodeMode `
                -StartSec $startSec `
                -DurationSec $PreflightSampleDurationSec `
                -ResolvedCRF $ResolvedCRF `
                -ResolvedPreset $ResolvedPreset `
                -ResolvedFilmGrain $ResolvedFilmGrain `
                -NvencSettings $NvencSettings `
                -NvencEnvironment $NvencEnvironment `
                -HdrPlan $HdrPlan `
                -OutputPath $sampleOutput

            $sampleResult = Invoke-FfmpegSync -Arguments $ffArgs
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

    $estimate = [pscustomobject][ordered]@{
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

    $script:PreflightSampleCache[$preflightCacheKey] = $estimate
    $script:DecisionCacheMisses++
    return $estimate
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
        $SourceProfile,
        [double]$SourceGiBPerHour = 0.0,
        [string]$TargetOverrideGiBPerHour = ''
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

    $ladderTarget = $target
    $capNote = ''

    if ($null -ne $PreflightAutoTuneCustomTargetGiBPerHour) { $target = [double]$PreflightAutoTuneCustomTargetGiBPerHour }
    if ($null -ne $PreflightAutoTuneCustomUpperGiBPerHour)  { $upper  = [double]$PreflightAutoTuneCustomUpperGiBPerHour }
    if ($null -ne $PreflightAutoTuneCustomLowerGiBPerHour)  { $lower  = [double]$PreflightAutoTuneCustomLowerGiBPerHour }

    # ---- Cap the target against the source's own rate ---------------------
    # The ladder is resolution- and HDR-based; it has no idea how efficiently
    # the source was encoded. Targeting a rate at or above the source's own
    # rate cannot save space -- it inflates the file, Auto picks an extremely
    # low CRF chasing the target, and preflight then refuses the job for being
    # oversized. Capping to a fraction of the measured source rate keeps the
    # target achievable AND a genuine saving.
    if ($SourceGiBPerHour -gt 0 -and $null -ne $PreflightMaxFractionOfSourceRate) {
        $fraction = [double]$PreflightMaxFractionOfSourceRate
        if ($fraction -gt 0) {
            $cap = $SourceGiBPerHour * $fraction
            if ($cap -lt $target) {
                $spread = ($upper - $lower)
                if ($spread -le 0) { $spread = [Math]::Max(0.5, $target * 0.3) }
                $target = [Math]::Round($cap, 2)
                $lower  = [Math]::Round([Math]::Max(0.1, $target - ($spread / 2.0)), 2)
                $upper  = [Math]::Round($target + ($spread / 2.0), 2)
                $capNote = ("capped to {0:P0} of the {1:F2} GiB/hr source rate; ladder wanted {2:F1}" -f $fraction, $SourceGiBPerHour, $ladderTarget)
            }
        }
    }

    # ---- Explicit per-drop target beats everything ------------------------
    if (-not [string]::IsNullOrWhiteSpace($TargetOverrideGiBPerHour)) {
        $parsed = 0.0
        if ([double]::TryParse($TargetOverrideGiBPerHour, [System.Globalization.NumberStyles]::Float,
                               [System.Globalization.CultureInfo]::InvariantCulture, [ref]$parsed) -and $parsed -gt 0) {
            $spread = [Math]::Max(0.5, $parsed * 0.25)
            $target = [Math]::Round($parsed, 2)
            $lower  = [Math]::Round([Math]::Max(0.1, $target - $spread), 2)
            $upper  = [Math]::Round($target + $spread, 2)
            $capNote = 'explicit target requested for this drop'
        }
    }

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
        TargetReason = if ($capNote) {
            "{0} {1}, range {2}-{3}; {4}" -f $sourceLabel, $profileLabel, $lower, $upper, $capNote
        } else {
            "{0} {1}, range {2}-{3}" -f $sourceLabel, $profileLabel, $lower, $upper
        }
        SourceGiBPerHour = $SourceGiBPerHour
        LadderTargetGiBPerHour = $ladderTarget
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
        $NvencEnvironment = $null,
        $HdrPlan = $null,
        [string]$TargetGiBPerHourOverrideValue = ''
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
        QualitySearch = $null
        QualitySecondMetricName = ''
        QualitySecondMetricValue = 0.0
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
    $preflightTargets = Resolve-PreflightAutoTuneTargets -QualityProfile $PreflightAutoTuneQuality -ResolutionTier $AutoSettings.ResolutionTier -SourceProfile $SourceProfile `
        -SourceGiBPerHour ([double](Get-OptionalProperty -InputObject $AutoSettings -PropertyName 'VideoBitratePerHourGiB' -Default 0.0)) `
        -TargetOverrideGiBPerHour $TargetGiBPerHourOverrideValue
    $reasons = [System.Collections.Generic.List[string]]::new()
    $reasons.Add("Initial Auto: CRF $InitialResolvedCRF / Preset $InitialResolvedPreset / FilmGrain $InitialResolvedFilmGrain ($($AutoSettings.ResolutionTier) / $($SourceProfile.Profile) / $($AutoSettings.CodecLabel) / $($AutoSettings.BPPTier) BPP)")
    $reasons.Add(("Preflight target: {0} GiB/hr ({1})" -f $preflightTargets.TargetGiBPerHour, $preflightTargets.TargetReason))

    $currentCrf = $InitialResolvedCRF
    $currentPreset = $InitialResolvedPreset
    $currentFilmGrain = $InitialResolvedFilmGrain

    $currentNvencSettings = $workflow.FinalNvencSettings

    # ---- Quality-targeted CRF search ------------------------------------
    # Runs BEFORE the size projection, because it decides which CRF the
    # projection should be measuring. Only when CRF is on Auto: a CRF the user
    # pinned by hand is an instruction, not a starting guess.
    if ($allowCrfTune) {
        $metricPlan = Resolve-QualityMetricPlan -SourceProfile $SourceProfile -AutoSettings $AutoSettings -EncodeMode $EncodeMode
        if ($metricPlan.Enabled) {
            Write-HostOnce -Key $InputPath -Message ("Quality target: {0} ({1})" -f $metricPlan.Metric, $metricPlan.Reason) -ForegroundColor DarkCyan
            Write-SessionTextLogMessage -Level Info -Message ("Quality target | {0} {1} | {2}" -f $metricPlan.Metric, $metricPlan.Mode, $metricPlan.Reason)

            $search = Invoke-QualityTargetedCrfSearch `
                -InputPath $InputPath `
                -Selected $Selected `
                -SourceProfile $SourceProfile `
                -EncodeMode $EncodeMode `
                -SourceDurationSec $SourceDurationSec `
                -StartCRF $currentCrf `
                -ResolvedPreset $currentPreset `
                -ResolvedFilmGrain $currentFilmGrain `
                -MetricPlan $metricPlan `
                -NvencSettings $currentNvencSettings `
                -NvencEnvironment $NvencEnvironment `
                -HdrPlan $HdrPlan

            $workflow.QualitySearch = $search

            if ($search.Ran) {
                $reasons.Add(("Quality search: " + $search.Reason))
                Write-HostOnce -Key $InputPath -ForegroundColor DarkCyan -Message (
                    "Quality search: CRF {0} -> {1} ({2} {3:F3}, threshold {4:F3})" -f `
                    $search.StartCRF, $search.ChosenCRF, $search.MetricName, $search.Metric, $search.Threshold)
                Write-SessionTextLogMessage -Level Info -Message ("Quality search | CRF {0} -> {1} | {2} {3:F3} vs threshold {4:F3} | transparent={5} | {6} probes" -f `
                    $search.StartCRF, $search.ChosenCRF, $search.MetricName, $search.Metric, $search.Threshold, $search.TransparencyMet, $search.ProbeCount)

                # Nothing in the permitted CRF range met the threshold. On an
                # absolute scale that usually means the metric could not
                # discriminate on this content rather than that the file is
                # unconvertible, so refusing outright is opt-in.
                if (-not $search.TransparencyMet -and $QualityFloorUnreachableAction -eq 'Skip') {
                    $workflow.WasSkippedByPreflight = $true
                    $workflow.SkipStatus = 'PRECHECK_SKIPPED_QUALITY_FLOOR'
                    $reasons.Add(('Decision: skipped (no CRF down to {0} reached the quality threshold; source left untouched)' -f ($currentCrf - [int]$QualityMaxCrfBelowAuto)))
                    $workflow.PreflightAutoTuneReason = ($reasons -join ' | ')
                    $workflow | Add-Member -NotePropertyName PreflightTargets -NotePropertyValue $preflightTargets -Force
                    Write-HostOnce -Key $InputPath -Message 'Quality floor unreachable: leaving this file as it is.' -ForegroundColor Yellow
                    return $workflow
                }

                if (-not $search.TransparencyMet) {
                    $reasons.Add(('Quality note: no measured CRF met the threshold on this content; keeping the Auto CRF {0} rather than refusing the file.' -f $search.ChosenCRF))
                    Write-HostOnce -Key $InputPath -ForegroundColor Yellow -Message (
                        'Quality: no permitted CRF reached the {0} target on this content (usual on grain-heavy sources). Keeping the Auto CRF {1}.' -f $search.MetricName, $search.ChosenCRF)
                    Write-SessionTextLogMessage -Level Warn -Message ("Quality floor | target not reached | keeping Auto CRF {0}" -f $search.ChosenCRF)
                }

                if ($search.ChosenCRF -ne $currentCrf) {
                    $workflow.WasPreflightRetuned = $true
                    $currentCrf = [int]$search.ChosenCRF
                    $workflow.FinalResolvedCRF = $currentCrf
                    if ($EncodeMode -eq 'nvenc') {
                        $qualityNvencAuto = [ordered]@{}
                        foreach ($prop in $AutoSettings.Keys) { $qualityNvencAuto[$prop] = $AutoSettings[$prop] }
                        $qualityNvencAuto.CRF = $currentCrf
                        $workflow.FinalNvencSettings = Convert-SoftwareQualityToNvencSettings `
                            -AutoSettings $qualityNvencAuto `
                            -SourceProfile $SourceProfile `
                            -ConfiguredNvencPreset $NvencPreset `
                            -ConfiguredNvencCQ $NvencCQ `
                            -ConfiguredNvencTune $NvencTune `
                            -ConfiguredNvencDecode $NvencDecode `
                            -NvencEnvironment $NvencEnvironment
                        $currentNvencSettings = $workflow.FinalNvencSettings
                    }
                }
            } else {
                $reasons.Add(("Quality search not run: " + $search.Reason))
                Write-SessionTextLogMessage -Level Warn -Message ("Quality search not run | " + $search.Reason)
            }
        } else {
            $reasons.Add(("Quality targeting off: " + $metricPlan.Reason))
        }
    } else {
        $reasons.Add('Quality targeting skipped: CRF is pinned by configuration, not Auto.')
    }

    Write-HostOnce -Key $InputPath -Message ("Preflight target profile: {0}" -f $preflightTargets.QualityProfile) -ForegroundColor DarkCyan
    Write-HostOnce -Key $InputPath -Message ("Resolved target: {0} GiB/hr ({1})" -f $preflightTargets.TargetGiBPerHour, $preflightTargets.TargetReason) -ForegroundColor DarkCyan
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
        -NvencEnvironment $NvencEnvironment `
        -HdrPlan $HdrPlan

    if ($workflow.Preflight1.Ran) {
        $workflow.PreflightPassCount = 1
        $reasons.Add(("Preflight 1: projected {0:F2} GiB/hr, {1:F1}% of source" -f $workflow.Preflight1.EstimatedOutputGiBPerHour, $workflow.Preflight1.EstimatedPctOfSource))
    } else {
        $workflow.PreflightAutoTuneReason = $workflow.Preflight1.Reason
        $workflow.FinalPreflight = $workflow.Preflight1
        return $workflow
    }

    # The size-based nudge must not undo the quality search. Its oversize rule
    # raises CRF by 2 to hit a GiB/hr target -- which, applied on top of a CRF
    # that was measured to sit right at the transparency threshold, spends the
    # quality budget the search just finished allocating. When a search
    # succeeded, CRF is left where the measurement put it; the oversize REFUSAL
    # further down still applies, so a file that cannot be shrunk is still
    # skipped rather than encoded larger than its source.
    $qualitySearchDecidedCrf = ($null -ne $workflow.QualitySearch -and $workflow.QualitySearch.Ran)
    $allowSizeCrfTune = ($allowCrfTune -and -not $qualitySearchDecidedCrf)
    if ($qualitySearchDecidedCrf) {
        $reasons.Add('Size-based CRF nudge suppressed: CRF was set by measured quality, not by the GiB/hr ladder.')
    }

    if ($EnablePreflightAutoTune -and ($allowSizeCrfTune -or $allowPresetTune -or $allowFilmGrainTune)) {
        $adjustment = Get-PreflightAutoTuneAdjustment `
            -PreflightResult $workflow.Preflight1 `
            -PreflightTargets $preflightTargets `
            -AutoSettings $AutoSettings `
            -EncodeMode $EncodeMode `
            -CurrentCRF $currentCrf `
            -CurrentPreset $currentPreset `
            -CurrentFilmGrain $currentFilmGrain `
            -AllowCrfTune $allowSizeCrfTune `
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
                    -NvencEnvironment $NvencEnvironment `
                    -HdrPlan $HdrPlan

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

    # ---- Corroborating second metric ------------------------------------
    # One number is a decision; two are a sanity check. The metric that did NOT
    # drive the decision is measured once at the chosen CRF, so the log can be
    # read later to see whether VMAF and XPSNR agree about a given file. Where
    # they disagree sharply, that file is worth looking at with actual eyes.
    if ($QualityReportSecondMetric -and $null -ne $workflow.QualitySearch -and $workflow.QualitySearch.Ran -and -not $workflow.WasSkippedByPreflight) {
        $secondMetricName = if ($workflow.QualitySearch.MetricName -eq 'VMAF') { 'XPSNR' } else { 'VMAF' }
        $qtc = Get-QualityToolchainEnvironment
        $secondAvailable = if ($secondMetricName -eq 'VMAF') { $qtc.SupportsVmaf } else { $qtc.SupportsXpsnr }
        if ($secondAvailable) {
            $secondPlan = [pscustomobject][ordered]@{
                Enabled = $true
                Metric  = $secondMetricName
                FilterSpec = if ($secondMetricName -eq 'VMAF') {
                    ("libvmaf=model='version={0}':n_threads={1}" -f (Get-VmafModelName -ResolutionTier ([string](Get-OptionalProperty -InputObject $AutoSettings -PropertyName 'ResolutionTier' -Default 'HD'))), ([Math]::Max(1, [Math]::Min(16, [int]$QualityVmafThreads))))
                } else { 'xpsnr' }
            }
            $secondProbe = Invoke-QualityProbe `
                -InputPath $InputPath -Selected $Selected -SourceProfile $SourceProfile `
                -EncodeMode $EncodeMode -CRF ([int]$workflow.FinalResolvedCRF) `
                -ResolvedPreset $currentPreset -ResolvedFilmGrain $currentFilmGrain `
                -SamplePositions (Get-QualitySamplePositions -SourceDurationSec $SourceDurationSec -SampleDurationSec ([Math]::Max(4, [Math]::Min(120, [int]$QualitySampleDurationSec))) -RequestedCount 1) `
                -SampleDurationSec ([Math]::Max(4, [Math]::Min(120, [int]$QualitySampleDurationSec))) `
                -MetricPlan $secondPlan -NvencSettings $currentNvencSettings `
                -NvencEnvironment $NvencEnvironment -HdrPlan $HdrPlan -Label 'second-metric'
            if ($secondProbe.Ran) {
                $workflow.QualitySecondMetricName = $secondMetricName
                $workflow.QualitySecondMetricValue = [double]$secondProbe.Metric
                $reasons.Add(("Corroborating {0} at CRF {1}: {2:F3}" -f $secondMetricName, $workflow.FinalResolvedCRF, $secondProbe.Metric))
            }
        }
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
        # These notes describe only the label. What actually happens to the
        # metadata is decided by Resolve-HdrEncodePlan and reported through
        # Get-HdrPlanSummary, which is the authoritative account.
        $note = if ($SourceProfile.SourceHdrFormat -eq 'HDR10+') {
            'Source has HDR10+ dynamic metadata; see the HDR plan for whether it was preserved.'
        } elseif ($SourceProfile.SourceHdrFormat -eq 'HLG') {
            if ($PreserveHLG) { 'HLG source retains its HLG transfer function.' }
            else { 'HLG source is being tagged as PQ/HDR10 because $PreserveHLG is disabled.' }
        } elseif ($SourceProfile.Profile -eq 'DV') {
            'Dolby Vision source; the base layer is converted per the HDR plan. AV1 does not carry the DV RPU here.'
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

# =============================================================================
# HDR / DYNAMIC METADATA MODULE                            (requires FFmpeg 9+)
# =============================================================================
# Everything in this section exists to answer one question correctly:
#
#   "What does the display actually receive?"
#
# Before this module, the script tagged HDR output with primaries / transfer /
# matrix and nothing else. That is only the *signalling*. The static HDR10
# payload -- mastering display colour volume (ST 2086) plus MaxCLL / MaxFALL --
# was silently dropped on every single encode, and any HDR10+ (ST 2094-40)
# dynamic metadata went with it. A display receiving PQ/BT.2020 with no
# mastering-display block has to fall back to its own generic tone-mapping
# assumptions, which is the usual cause of "the AV1 version looks flatter /
# duller than the source" on an HDR10+ TV.
#
# FFmpeg 9 is what makes fixing this clean: it added per-stream
# `-mastering_display` and `-content_light` output options, so the payload can
# be re-stated on the output stream explicitly instead of hoping it survives
# the filter graph. It also added the `dovi_split` bitstream filter and a
# complete Dolby Vision Profile 7 pipeline, which is what lets DV sources be
# converted to a correct HDR10 base layer rather than skipped outright.
#
# Design rules followed here:
#   1. Never assume a capability. Probe the local ffmpeg / tool binaries once,
#      cache the answer, and degrade to the next-best path. Same pattern the
#      script already uses for NVENC via Get-NvencEnvironment.
#   2. Never silently lose metadata. If something cannot be carried, say so in
#      the log row and in the console note.
#   3. Never guess at colour volume. If the source has no mastering display
#      block, do not invent one -- absent is better than wrong.
# =============================================================================

# -----------------------------------------------------------------------------
# Chromaticity is carried in units of 0.00002 (1/50000) and luminance in units
# of 0.0001 (1/10000) cd/m^2, per the -mastering_display option's documented
# format string:  G(%u,%u)B(%u,%u)R(%u,%u)WP(%u,%u)L(%u,%u)
#
# ffprobe reports these fields as rational strings ("34000/50000"). Rather than
# assume the denominator matches the target unit -- it usually does, but muxers
# are free to reduce the fraction -- each value is evaluated and re-quantised.
# -----------------------------------------------------------------------------
$script:HdrChromaUnit    = 0.00002
$script:HdrLuminanceUnit = 0.0001

# Declared up front, not lazily on first use. This script runs under
# Set-StrictMode -Version Latest, where *reading* a script-scope variable that
# has never been assigned throws rather than returning $null -- so the usual
# "if ($null -eq $script:Cache) { $script:Cache = @{} }" idiom fails on the
# very first call it is meant to guard.
$script:HdrToolchainCache      = $null
$script:HdrStaticMetadataCache = @{}
$script:QualityToolchainCache  = $null
$script:SvtParamSupportCache   = @{}
$script:QualityLosslessDbSentinel = 140.0

# Lets a companion script that loads these functions by AST (the calibration
# tool does) tell them where to look for hdr10plus_tool / dovi_tool, since
# $PSScriptRoot is empty in a function that was not loaded from a file.
$script:HdrToolSearchRoot      = $PSScriptRoot

function Convert-RationalToDouble {
    param($Value)

    if ($null -eq $Value) { return $null }

    $text = ([string]$Value).Trim()
    if ([string]::IsNullOrWhiteSpace($text)) { return $null }

    # "num/den" form, as emitted by ffprobe for mastering display fields.
    if ($text -match '^\s*(-?\d+)\s*/\s*(-?\d+)\s*$') {
        $numerator   = [double]$Matches[1]
        $denominator = [double]$Matches[2]
        if ($denominator -eq 0) { return $null }
        return ($numerator / $denominator)
    }

    # Plain decimal form, as emitted for content light level fields.
    $parsed = 0.0
    if ([double]::TryParse($text, [System.Globalization.NumberStyles]::Float,
                           [System.Globalization.CultureInfo]::InvariantCulture, [ref]$parsed)) {
        return $parsed
    }

    return $null
}

# Convert-ToInvariantInt64 types its -Default as [int64], so passing $null to it
# coerces to 0. That distinction matters here: Dolby Vision profile 0 and "no
# profile reported" are different states, and conflating them would send an
# unidentified source down the Profile-0 path. This returns a real $null.
function Convert-ToNullableInt {
    param($Value)

    if ($null -eq $Value) { return $null }
    $text = ([string]$Value).Trim()
    if ([string]::IsNullOrWhiteSpace($text)) { return $null }

    $parsed = 0L
    if ([int64]::TryParse($text, [System.Globalization.NumberStyles]::Any,
                          [System.Globalization.CultureInfo]::InvariantCulture, [ref]$parsed)) {
        return [int]$parsed
    }
    return $null
}

function Convert-ToHdrUnits {
    param(
        $Value,
        [double]$Unit
    )

    $asDouble = Convert-RationalToDouble -Value $Value
    if ($null -eq $asDouble) { return $null }
    if ($Unit -le 0) { return $null }

    $units = [Math]::Round($asDouble / $Unit, [System.MidpointRounding]::AwayFromZero)
    if ($units -lt 0) { return $null }

    return [uint32]$units
}

# -----------------------------------------------------------------------------
# Locates an optional external HDR tool. Mirrors the ffmpeg / ffprobe discovery
# convention already used at the top of the script: prefer a copy sitting next
# to the script (portable deployment), then fall back to PATH. $HdrToolsDir, if
# set, is checked ahead of both.
# -----------------------------------------------------------------------------
function Get-HdrToolPath {
    param([string]$ToolName)

    $candidateDirs = New-Object System.Collections.Generic.List[string]
    if (-not [string]::IsNullOrWhiteSpace($HdrToolsDir)) { $candidateDirs.Add($HdrToolsDir) }
    # Guarded rather than added unconditionally: $PSScriptRoot is empty in any
    # context where this function was not loaded from a file on disk, and
    # Join-Path throws on an empty -Path rather than returning nothing.
    if (-not [string]::IsNullOrWhiteSpace($PSScriptRoot)) { $candidateDirs.Add($PSScriptRoot) }
    if (-not [string]::IsNullOrWhiteSpace($script:HdrToolSearchRoot)) { $candidateDirs.Add($script:HdrToolSearchRoot) }

    foreach ($dir in $candidateDirs) {
        if ([string]::IsNullOrWhiteSpace($dir)) { continue }
        foreach ($ext in @('.exe', '')) {
            $candidate = Join-Path $dir ($ToolName + $ext)
            if (Test-Path -LiteralPath $candidate -PathType Leaf) {
                return (Get-Item -LiteralPath $candidate).FullName
            }
        }
    }

    $onPath = Get-Command $ToolName -ErrorAction SilentlyContinue
    if ($onPath) { return $onPath.Source }

    return $null
}

# -----------------------------------------------------------------------------
# One-time capability probe of the local toolchain. Cached for the life of the
# process because each probe spawns a subprocess and the answers cannot change
# while the queue is running.
#
# What gets probed and why:
#   StaticMetadataOptions - whether this ffmpeg exposes -mastering_display and
#       -content_light as output options. Added in FFmpeg 9; absent in 8.x. If
#       missing, the CPU lane can still carry the payload through
#       -svtav1-params, but the NVENC lane cannot, so HDR jobs get steered to
#       the CPU lane rather than silently losing colour volume.
#   DoviSplitBsf / DoviRpuBsf - the Dolby Vision bitstream filters. dovi_split
#       (FFmpeg 9) separates a Profile 7 dual-layer stream; dovi_rpu can strip
#       the RPU from a single-layer stream.
#   Hdr10PlusTool - quietvoid/hdr10plus_tool for extracting ST 2094-40 to JSON.
#       The mainline tool reads HEVC only; AV1 injection needs a build with AV1
#       support, which is probed separately because that determines whether the
#       dynamic metadata can actually be put back.
#   SvtHdr10PlusJson - whether the linked SVT-AV1 accepts hdr10plus-json, i.e.
#       whether it is an svt-av1-hdr / SVT-AV1-PSY build compiled with
#       enable-hdr10plus. When present, HDR10+ can be muxed in during the
#       encode and no post-encode injection pass is needed.
# -----------------------------------------------------------------------------
function Get-HdrToolchainEnvironment {
    if ($null -ne $script:HdrToolchainCache) { return $script:HdrToolchainCache }

    $ffmpegHelpText = ''
    try   { $ffmpegHelpText = (& $FfmpegPath -hide_banner -h full 2>&1 | Out-String) }
    catch { $ffmpegHelpText = '' }

    $bsfListText = ''
    try   { $bsfListText = (& $FfmpegPath -hide_banner -bsfs 2>&1 | Out-String) }
    catch { $bsfListText = '' }

    $encoderHelpText = ''
    try   { $encoderHelpText = (& $FfmpegPath -hide_banner -h encoder=libsvtav1 2>&1 | Out-String) }
    catch { $encoderHelpText = '' }

    # Detected with the CLI-option matcher (these are CLI options, not
    # AVOptions), then confirmed functionally. The help text is the cheap
    # check; ffmpeg's parser is the authority, and it settles any disagreement.
    $supportsMasteringDisplay = Test-TextContainsCliOption -Text $ffmpegHelpText -OptionName 'mastering_display'
    $supportsContentLight     = Test-TextContainsCliOption -Text $ffmpegHelpText -OptionName 'content_light'

    # The help text says the option exists; it does not say what form ffmpeg
    # accepts. Those are different questions, and getting the second one wrong
    # is silent until an encode fails.
    #
    # Emitting "-mastering_display:v:0" -- a stream specifier the documented
    # syntax presents as optional -- is rejected by real FFmpeg 9 builds with:
    #
    #   Option mastering_display:v:0 ... cannot be applied to output url ...
    #   you are trying to apply an input option to an output file or vice versa
    #
    # while the bare form is accepted. So the accepted FORM is probed too, and
    # the style that works is recorded and used when building arguments. Probing
    # the exact string the builder emits is what keeps the two from drifting.
    # Selecting a style requires exit code 0, which introduces a hazard: if the
    # probe encode fails for a reason unrelated to the option, BOTH styles look
    # unsupported and static HDR10 metadata would be dropped silently. So a
    # control encode runs first, exactly as in Test-Av1NvencBFrameSupport. If
    # the control cannot even complete, the probe is inconclusive and the help
    # text decides -- with the bare form, which is the documented default.
    $probeExtra = @('-preset', '12', '-crf', '60')
    $controlProbe = Test-FfmpegOptionSupported -OptionArguments @() -ExtraEncoderArguments $probeExtra
    $probeUsable = $controlProbe.Accepted

    $sampleMastering = 'G(13250,34500)B(7500,3000)R(34000,16000)WP(15635,16450)L(10000000,1)'

    $masteringStyle = 'none'
    if ($probeUsable) {
        foreach ($style in @('bare','stream_spec')) {
            $optName = if ($style -eq 'bare') { '-mastering_display' } else { '-mastering_display:v:0' }
            $probe = Test-FfmpegOptionSupported -OptionArguments @($optName, $sampleMastering) -ExtraEncoderArguments $probeExtra
            if ($probe.Accepted) { $masteringStyle = $style; break }
        }
    } elseif ($supportsMasteringDisplay) {
        $masteringStyle = 'bare'
    }

    $contentLightStyle = 'none'
    if ($probeUsable) {
        foreach ($style in @('bare','stream_spec')) {
            $optName = if ($style -eq 'bare') { '-content_light' } else { '-content_light:v:0' }
            $probe = Test-FfmpegOptionSupported -OptionArguments @($optName, '1000,400') -ExtraEncoderArguments $probeExtra
            if ($probe.Accepted) { $contentLightStyle = $style; break }
        }
    } elseif ($supportsContentLight) {
        $contentLightStyle = 'bare'
    }

    if (-not $probeUsable) {
        Write-Warning "HDR static-metadata form probe was inconclusive (a plain test encode did not complete: $($controlProbe.Detail)). Falling back to the documented bare option form."
    }

    # A help-text match with no working form is not support.
    $supportsMasteringDisplay = ($masteringStyle -ne 'none')
    $supportsContentLight     = ($contentLightStyle -ne 'none')

    $doviSplitBsf = $bsfListText -match '(?im)\bdovi_split\b'
    $doviRpuBsf   = $bsfListText -match '(?im)\bdovi_rpu\b'

    # SVT-AV1 forks that support HDR10+ advertise the token in the svtav1-params
    # help text. Absence is not proof it is unsupported on every build, but it
    # is the only signal available without running a throwaway encode, and
    # treating absence as unsupported fails safe.
    $svtHdr10PlusJson = ($encoderHelpText -match '(?im)hdr10plus[-_]json')

    $hdr10PlusToolPath = Get-HdrToolPath -ToolName 'hdr10plus_tool'
    $hdr10PlusAv1      = $false
    $hdr10PlusVersion  = ''
    if ($hdr10PlusToolPath) {
        try {
            $hdr10PlusVersion = ((& $hdr10PlusToolPath --version 2>&1 | Out-String)).Trim()
            $injectHelp = (& $hdr10PlusToolPath inject --help 2>&1 | Out-String)
            # Forks with AV1 support mention av1 / ivf in the inject help text.
            $hdr10PlusAv1 = ($injectHelp -match '(?im)\bav1\b|\bivf\b')
        } catch {
            $hdr10PlusAv1 = $false
        }
    }

    $doviToolPath = Get-HdrToolPath -ToolName 'dovi_tool'

    $script:HdrToolchainCache = [ordered]@{
        SupportsMasteringDisplayOption = [bool]$supportsMasteringDisplay
        SupportsContentLightOption     = [bool]$supportsContentLight
        MasteringDisplayArgStyle       = $masteringStyle      # bare | stream_spec | none
        ContentLightArgStyle           = $contentLightStyle
        SupportsStaticMetadataOptions  = ([bool]$supportsMasteringDisplay -and [bool]$supportsContentLight)
        SupportsDoviSplitBsf           = [bool]$doviSplitBsf
        SupportsDoviRpuBsf             = [bool]$doviRpuBsf
        SupportsSvtHdr10PlusJson       = [bool]$svtHdr10PlusJson
        Hdr10PlusToolPath              = $hdr10PlusToolPath
        Hdr10PlusToolVersion           = $hdr10PlusVersion
        Hdr10PlusToolSupportsAv1       = [bool]$hdr10PlusAv1
        DoviToolPath                   = $doviToolPath
    }

    return $script:HdrToolchainCache
}

# -----------------------------------------------------------------------------
# Reads the static HDR10 payload (mastering display colour volume + content
# light level) for a video stream.
#
# This has to look in two places. When the payload lives in the container --
# Matroska colour elements, MP4 boxes -- ffprobe reports it in the stream's
# side_data_list. When it lives in the elementary stream as HEVC SEI, which is
# the common case for Blu-ray remuxes, the stream-level list is null and the
# payload only appears at frame level. The original HDR10+ detection in
# Get-SourceProfile checked stream level only, which is why it never fired on
# real sources.
#
# A single-frame probe is cheap (-read_intervals "%+#1" decodes one frame) and
# is only run when the stream-level lookup comes up empty.
# -----------------------------------------------------------------------------
function Get-HdrStaticMetadata {
    param(
        [string]$InputPath,
        $VideoStream
    )

    # This runs a frame-level ffprobe, which costs a subprocess and a decode.
    # Several callers want the same answer for the same file during one job, so
    # the result is memoised per (path, stream index). The cache is per-process
    # and the queue re-probes on the next run, so a file edited between runs is
    # never served a stale answer.
    $cacheKey = "{0}|{1}" -f $InputPath, [int](Get-StreamProp $VideoStream 'index' 0)
    if ($script:HdrStaticMetadataCache.ContainsKey($cacheKey)) {
        return $script:HdrStaticMetadataCache[$cacheKey]
    }

    $result = [ordered]@{
        HasMasteringDisplay = $false
        HasContentLight     = $false
        # Two mastering-display renderings are kept, because ffmpeg and SVT-AV1
        # disagree about units and the difference is silent:
        #
        #   ffmpeg  -mastering_display : INTEGER units. Chromaticity in steps of
        #       0.00002, luminance in steps of 0.0001 cd/m^2.
        #       -> G(13250,34500)B(7500,3000)R(34000,16000)WP(15635,16450)L(10000000,1)
        #
        #   SVT-AV1 mastering-display : NORMALISED FLOATS. Chromaticity as actual
        #       CIE xy coordinates in 0.0-1.0, luminance in nits.
        #       -> G(0.265,0.690)B(0.150,0.060)R(0.680,0.320)WP(0.3127,0.3290)L(1000.0,0.0001)
        #
        # Feeding the integer form to SVT-AV1 does not error. It emits
        # "Invalid mastering display info will be clipped to 0.0 to 1.0", pins
        # every primary to 1.0, and writes a negative max_luminance -- metadata
        # actively worse than none at all. Hence both forms, built once, here.
        MasteringDisplay      = $null  # ffmpeg integer-unit form
        MasteringDisplayFloat = $null  # SVT-AV1 normalised-float form
        MaxCLL              = $null
        MaxFALL             = $null
        MaxCLLOriginal      = $null
        MaxCLLClampNote     = ''
        MasteringMaxNits    = $null
        ContentLight        = $null    # "maxcll,maxfall" -- same for both
        HasHDR10Plus        = $false
        Source              = 'none'   # stream | frame | none
        Diagnostics         = ''
    }

    function Read-SideDataCollection {
        param($SideDataList, [string]$Origin)

        $found = [ordered]@{
            Mastering        = $null
            MasteringFloat   = $null
            MasteringMaxNits = $null
            ContentLight     = $null
            HDR10Plus        = $false
        }

        foreach ($sd in @($SideDataList)) {
            if ($null -eq $sd) { continue }
            $type = [string](Get-StreamProp $sd 'side_data_type' '')

            if ($type -match '(?i)mastering\s*display') {
                $green_x = Convert-ToHdrUnits (Get-StreamProp $sd 'green_x' $null) $script:HdrChromaUnit
                $green_y = Convert-ToHdrUnits (Get-StreamProp $sd 'green_y' $null) $script:HdrChromaUnit
                $blue_x  = Convert-ToHdrUnits (Get-StreamProp $sd 'blue_x'  $null) $script:HdrChromaUnit
                $blue_y  = Convert-ToHdrUnits (Get-StreamProp $sd 'blue_y'  $null) $script:HdrChromaUnit
                $red_x   = Convert-ToHdrUnits (Get-StreamProp $sd 'red_x'   $null) $script:HdrChromaUnit
                $red_y   = Convert-ToHdrUnits (Get-StreamProp $sd 'red_y'   $null) $script:HdrChromaUnit
                $wp_x    = Convert-ToHdrUnits (Get-StreamProp $sd 'white_point_x' $null) $script:HdrChromaUnit
                $wp_y    = Convert-ToHdrUnits (Get-StreamProp $sd 'white_point_y' $null) $script:HdrChromaUnit
                $lumMax  = Convert-ToHdrUnits (Get-StreamProp $sd 'max_luminance' $null) $script:HdrLuminanceUnit
                $lumMin  = Convert-ToHdrUnits (Get-StreamProp $sd 'min_luminance' $null) $script:HdrLuminanceUnit

                # All ten components are required. A partial block is worse than
                # no block: the format string would render as G(13250,34500)B(,)
                # and ffmpeg would reject the option outright, failing the whole
                # encode. An incomplete set is therefore discarded, not padded.
                #
                # The missing count is accumulated in an explicit loop rather
                # than with `-not ($components | Where-Object { $null -eq $_ })`.
                # That idiom is wrong here: when exactly one component is null
                # the pipeline yields a bare $null instead of a one-element
                # collection, `-not $null` is $true, and the partial block would
                # be accepted -- the single-element collapse behaviour.
                $components = @($green_x, $green_y, $blue_x, $blue_y, $red_x, $red_y, $wp_x, $wp_y, $lumMax, $lumMin)
                $missingCount = 0
                foreach ($component in $components) {
                    if ($null -eq $component) { $missingCount++ }
                }

                if ($missingCount -eq 0) {
                    $found.Mastering = ('G({0},{1})B({2},{3})R({4},{5})WP({6},{7})L({8},{9})' -f `
                        $green_x, $green_y, $blue_x, $blue_y, $red_x, $red_y, $wp_x, $wp_y, $lumMax, $lumMin)

                    # Normalised-float form for SVT-AV1. Formatted against
                    # InvariantCulture explicitly: on a machine with a comma
                    # decimal separator the default would emit "0,265", which
                    # collides with the comma that separates the x,y pair and
                    # silently corrupts the parameter.
                    $inv = [System.Globalization.CultureInfo]::InvariantCulture
                    $c = { param($units) (([double]$units * $script:HdrChromaUnit)).ToString('0.#####', $inv) }
                    $l = { param($units) (([double]$units * $script:HdrLuminanceUnit)).ToString('0.####', $inv) }

                    $found.MasteringFloat = ('G({0},{1})B({2},{3})R({4},{5})WP({6},{7})L({8},{9})' -f `
                        (& $c $green_x), (& $c $green_y), (& $c $blue_x), (& $c $blue_y),
                        (& $c $red_x),   (& $c $red_y),   (& $c $wp_x),   (& $c $wp_y),
                        (& $l $lumMax),  (& $l $lumMin))

                    # Kept in nits so the MaxCLL sanity check has something to
                    # compare against without re-parsing the formatted string.
                    $found.MasteringMaxNits = [double]$lumMax * $script:HdrLuminanceUnit
                }
                continue
            }

            if ($type -match '(?i)content\s*light\s*level') {
                $maxContent = Convert-RationalToDouble (Get-StreamProp $sd 'max_content' $null)
                $maxAverage = Convert-RationalToDouble (Get-StreamProp $sd 'max_average' $null)
                if ($null -ne $maxContent -and $null -ne $maxAverage) {
                    $found.ContentLight = [ordered]@{
                        MaxCLL  = [int][Math]::Round($maxContent)
                        MaxFALL = [int][Math]::Round($maxAverage)
                    }
                }
                continue
            }

            # ST 2094-40. ffprobe has labelled this differently across versions,
            # so match generously rather than on one exact spelling.
            if ($type -match '(?i)HDR10\+|SMPTE\s*2094-40|Dynamic\s*HDR(\+|10\+)?') {
                $found.HDR10Plus = $true
                continue
            }
        }

        return $found
    }

    # --- Pass 1: stream level (container-carried metadata) --------------------
    $streamSideData = Get-StreamSideDataList -Stream $VideoStream
    $fromStream = Read-SideDataCollection -SideDataList $streamSideData -Origin 'stream'

    if ($fromStream.Mastering -or $fromStream.ContentLight -or $fromStream.HDR10Plus) {
        $result.Source = 'stream'
    }

    $mastering       = $fromStream.Mastering
    $masteringFloat  = $fromStream.MasteringFloat
    $masteringMaxNits = $fromStream.MasteringMaxNits
    $contentLight    = $fromStream.ContentLight
    $hdr10Plus       = $fromStream.HDR10Plus

    # --- Pass 2: frame level (SEI-carried metadata) --------------------------
    # Run whenever anything is still missing. One frame is enough for the static
    # payload; HDR10+ presence is also detectable from the first frame because a
    # conformant stream carries a metadata block on every displayed frame.
    if (-not $mastering -or -not $contentLight -or -not $hdr10Plus) {
        try {
            $streamIndex = [int](Get-StreamProp $VideoStream 'index' 0)
            $frameProbeArgs = @(
                '-v',            'error',
                '-print_format', 'json',
                '-show_frames',
                '-read_intervals', '%+#1',
                '-select_streams', "$streamIndex",
                $InputPath
            )
            $frameJson = & $FfprobePath @frameProbeArgs
            if ($frameJson) {
                $frameProbe = ($frameJson | ConvertFrom-Json -Depth 100)
                $frames = @(Get-OptionalProperty -InputObject $frameProbe -PropertyName 'frames' -Default @())
                foreach ($frame in $frames) {
                    $frameSideData = Get-OptionalProperty -InputObject $frame -PropertyName 'side_data_list' -Default $null
                    if ($null -eq $frameSideData) { continue }
                    $fromFrame = Read-SideDataCollection -SideDataList $frameSideData -Origin 'frame'
                    if (-not $mastering    -and $fromFrame.Mastering)    {
                        $mastering        = $fromFrame.Mastering
                        $masteringFloat   = $fromFrame.MasteringFloat
                        $masteringMaxNits = $fromFrame.MasteringMaxNits
                        $result.Source    = 'frame'
                    }
                    if (-not $contentLight -and $fromFrame.ContentLight) { $contentLight = $fromFrame.ContentLight; $result.Source = 'frame' }
                    if (-not $hdr10Plus    -and $fromFrame.HDR10Plus)    { $hdr10Plus = $true;                      $result.Source = 'frame' }
                }
            }
        } catch {
            $result.Diagnostics = "Frame-level HDR metadata probe failed: $($_.Exception.Message)"
        }
    }

    if ($mastering) {
        $result.HasMasteringDisplay   = $true
        $result.MasteringDisplay      = $mastering
        $result.MasteringDisplayFloat = $masteringFloat
    }
    if ($mastering -and $null -ne $masteringMaxNits) {
        $result.MasteringMaxNits = [double]$masteringMaxNits
    }

    if ($contentLight) {
        $result.HasContentLight = $true
        $result.MaxCLL          = $contentLight.MaxCLL
        $result.MaxFALL         = $contentLight.MaxFALL

        # ---- MaxCLL sanity check -----------------------------------------
        # Some masters declare a MaxCLL brighter than their own mastering
        # display peak, which cannot be true: the content was graded on that
        # display, so it cannot exceed it. A census of this library found 15
        # such files, and they were not one problem but four -- placeholders
        # near 10,000 nits, overshoots of two to six times, mild overshoots
        # under twice, and at least one file whose PEAK is the wrong field
        # rather than its MaxCLL (a 200-nit peak against a 574-nit MaxCLL,
        # where clamping MaxCLL down to 200 would be the damaging edit).
        #
        # So the clamp is off by default, and when on it fires only where the
        # peak is credible AND the overshoot is large. Both conditions have to
        # hold, which is what keeps the 200-nit case out of scope.
        if ($ClampMaxCllToMasteringPeak -and
            $null -ne $result.MasteringMaxNits -and
            [double]$result.MasteringMaxNits -ge [double]$ClampMaxCllMinPeakNits -and
            [double]$result.MaxCLL -gt ([double]$result.MasteringMaxNits * [double]$ClampMaxCllMinOvershoot)) {

            $clampedTo = [int][Math]::Round([double]$result.MasteringMaxNits)
            $result.MaxCLLOriginal  = $result.MaxCLL
            $result.MaxCLLClampNote = ('MaxCLL {0} -> {1} (mastering peak {2:F0} nits, overshoot {3:F1}x)' -f `
                $result.MaxCLL, $clampedTo, [double]$result.MasteringMaxNits,
                ([double]$result.MaxCLL / [double]$result.MasteringMaxNits))
            $result.MaxCLL = $clampedTo
        }

        $result.ContentLight    = ('{0},{1}' -f $result.MaxCLL, $result.MaxFALL)
    }
    $result.HasHDR10Plus = [bool]$hdr10Plus

    $script:HdrStaticMetadataCache[$cacheKey] = $result
    return $result
}

# -----------------------------------------------------------------------------
# Reads the Dolby Vision configuration record and decides what can honestly be
# done with the source.
#
# The profile number is what matters, and it is the reason FFmpeg 8 output
# "wasn't great":
#
#   Profile 7  - dual layer, from UHD Blu-ray. The base layer is HDR10. Split
#                it out with dovi_split and you get a correct HDR10 encode.
#   Profile 8  - single layer with a cross-compatibility ID. ID 1 means the base
#                layer is already HDR10-compatible, ID 4 means HLG, ID 2 means
#                SDR. Strip the RPU and the base layer stands on its own.
#   Profile 5  - single layer, no backward-compatible base. The picture is in a
#                DV-native IPT-PQ-c2 representation. Re-encoding it as if it
#                were HDR10 is exactly what produces the washed-out, magenta /
#                green-cast result. This cannot be fixed by tagging; it needs a
#                real DV-to-HDR10 conversion. The script refuses rather than
#                producing a bad file.
# -----------------------------------------------------------------------------
function Resolve-DolbyVisionSourcePlan {
    param(
        $Probe,
        $VideoStream,
        $HdrToolchain
    )

    $plan = [ordered]@{
        IsDolbyVision      = $false
        Profile            = $null
        Level              = $null
        BlSignalCompatId   = $null
        RpuPresent         = $false
        ElPresent          = $false
        BlPresent          = $false
        Strategy           = 'none'   # none | split_bl | strip_rpu | passthrough | unsupported
        InputBitstreamFilter  = $null
        TargetDynamicRange = $null    # HDR10 | HLG | SDR
        CanConvert         = $false
        Reason             = ''
        Label              = ''
    }

    $dovi = $null
    foreach ($sd in @(Get-StreamSideDataList -Stream $VideoStream)) {
        if ($null -eq $sd) { continue }
        $type = [string](Get-StreamProp $sd 'side_data_type' '')
        if ($type -match '(?i)DOVI|Dolby\s*Vision') { $dovi = $sd; break }
    }

    $codecName = [string](Get-StreamProp $VideoStream 'codec_name'       '')
    $codecTag  = [string](Get-StreamProp $VideoStream 'codec_tag_string' '')
    $taggedDv  = ($codecName -match 'dvhe|dvav' -or $codecTag -match 'dvhe|dvav|dvh1|dav1')

    if ($null -eq $dovi -and -not $taggedDv) { return $plan }

    $plan.IsDolbyVision = $true

    if ($null -ne $dovi) {
        $plan.Profile          = Convert-ToNullableInt (Get-StreamProp $dovi 'dv_profile' $null)
        $plan.Level            = Convert-ToNullableInt (Get-StreamProp $dovi 'dv_level'   $null)
        $plan.BlSignalCompatId = Convert-ToNullableInt (Get-StreamProp $dovi 'dv_bl_signal_compatibility_id' $null)
        $plan.RpuPresent       = [bool](Convert-ToInvariantInt64 (Get-StreamProp $dovi 'rpu_present_flag' 0) 0)
        $plan.ElPresent        = [bool](Convert-ToInvariantInt64 (Get-StreamProp $dovi 'el_present_flag'  0) 0)
        $plan.BlPresent        = [bool](Convert-ToInvariantInt64 (Get-StreamProp $dovi 'bl_present_flag'  0) 0)
    }

    $profileLabel = if ($null -ne $plan.Profile) { "Profile $($plan.Profile)" } else { 'Profile unknown' }
    $compatLabel  = switch ($plan.BlSignalCompatId) {
        1       { 'HDR10-compatible base layer' }
        2       { 'SDR-compatible base layer' }
        4       { 'HLG-compatible base layer' }
        0       { 'no backward-compatible base layer' }
        default { 'unknown base-layer compatibility' }
    }
    $plan.Label = "Dolby Vision $profileLabel ($compatLabel)"

    switch ([int]($plan.Profile ?? -1)) {
        7 {
            # Dual layer. The base layer is HDR10; dovi_split isolates it.
            if ($HdrToolchain.SupportsDoviSplitBsf) {
                $plan.Strategy             = 'split_bl'
                $plan.InputBitstreamFilter = 'dovi_split=bl'
                $plan.TargetDynamicRange   = 'HDR10'
                $plan.CanConvert           = $true
                $plan.Reason               = 'Profile 7 dual-layer source: base layer extracted with dovi_split and encoded as HDR10.'
            } elseif ($HdrToolchain.SupportsDoviRpuBsf) {
                $plan.Strategy             = 'strip_rpu'
                $plan.InputBitstreamFilter = 'dovi_rpu=strip=1'
                $plan.TargetDynamicRange   = 'HDR10'
                $plan.CanConvert           = $true
                $plan.Reason               = 'Profile 7 source: dovi_split unavailable, falling back to stripping the RPU from the base layer.'
            } else {
                $plan.Strategy = 'unsupported'
                $plan.Reason   = 'Profile 7 source requires the dovi_split or dovi_rpu bitstream filter (FFmpeg 9+). Neither is available in this build.'
            }
        }
        8 {
            $target = switch ($plan.BlSignalCompatId) {
                1       { 'HDR10' }
                4       { 'HLG' }
                2       { 'SDR' }
                default { $null }
            }
            if ($null -ne $target -and $HdrToolchain.SupportsDoviRpuBsf) {
                $plan.Strategy             = 'strip_rpu'
                $plan.InputBitstreamFilter = 'dovi_rpu=strip=1'
                $plan.TargetDynamicRange   = $target
                $plan.CanConvert           = $true
                $plan.Reason               = "Profile 8 source with a $target-compatible base layer: RPU stripped and encoded as $target."
            } elseif ($null -ne $target) {
                # The base layer is already conformant; without the bsf the RPU
                # simply is not carried into AV1, which is the desired result
                # anyway. This is safe, unlike the Profile 5 case.
                $plan.Strategy           = 'strip_rpu'
                $plan.TargetDynamicRange = $target
                $plan.CanConvert         = $true
                $plan.Reason             = "Profile 8 source with a $target-compatible base layer: RPU is not carried into the AV1 output, base layer encoded as $target."
            } else {
                $plan.Strategy = 'unsupported'
                $plan.Reason   = 'Profile 8 source reports no usable base-layer compatibility ID; refusing to guess at the transfer function.'
            }
        }
        10 {
            # Dolby Vision carried in AV1 rather than HEVC. The cross-compat IDs
            # mean the same things as Profile 8's, and dovi_rpu operates on AV1 as
            # well as HEVC, so 10.1 / 10.2 / 10.4 convert the same way.
            #
            # Such a source is already AV1, so re-encoding it to AV1 usually gains
            # nothing and the Auto "already efficient" check normally skips it
            # first. Handled properly here so that if one does reach this point the
            # outcome is correct rather than an unnecessary refusal.
            $p10Target = switch ($plan.BlSignalCompatId) {
                1       { 'HDR10' }
                4       { 'HLG' }
                2       { 'SDR' }
                default { $null }
            }
            if ($null -ne $p10Target) {
                $plan.Strategy             = 'strip_rpu'
                $plan.InputBitstreamFilter = if ($HdrToolchain.SupportsDoviRpuBsf) { 'dovi_rpu=strip=1' } else { $null }
                $plan.TargetDynamicRange   = $p10Target
                $plan.CanConvert           = $true
                $plan.Reason               = "Profile 10 source (Dolby Vision in AV1) with a $p10Target-compatible base layer: RPU stripped and encoded as $p10Target. Note the source is already AV1."
            } else {
                $plan.Strategy = 'unsupported'
                $plan.Reason   = 'Profile 10 source (Dolby Vision in AV1) reports no backward-compatible base layer; refusing to guess at the transfer function.'
            }
        }
        5 {
            # No honest conversion available without real DV tone mapping.
            $plan.Strategy = 'unsupported'
            $plan.Reason   = 'Profile 5 source has no backward-compatible base layer. Re-encoding it as HDR10 produces incorrect colour, so it is skipped rather than converted.'
        }
        default {
            if ($plan.BlSignalCompatId -eq 1 -and $HdrToolchain.SupportsDoviRpuBsf) {
                $plan.Strategy             = 'strip_rpu'
                $plan.InputBitstreamFilter = 'dovi_rpu=strip=1'
                $plan.TargetDynamicRange   = 'HDR10'
                $plan.CanConvert           = $true
                $plan.Reason               = 'Dolby Vision source reports an HDR10-compatible base layer: RPU stripped and encoded as HDR10.'
            } else {
                $plan.Strategy = 'unsupported'
                $plan.Reason   = "Unrecognised Dolby Vision configuration ($profileLabel, $compatLabel); refusing to guess."
            }
        }
    }

    return $plan
}

# -----------------------------------------------------------------------------
# Decides, for one source, exactly what HDR handling the encode will use. This
# is resolved once in Get-EncodeInitialization and then consumed by every arg
# builder, so the preflight samples and the real encode cannot drift apart --
# which matters, because a preflight that omits HDR metadata reports a
# different bitrate than the encode it is supposed to be predicting.
# -----------------------------------------------------------------------------
function Resolve-HdrEncodePlan {
    param(
        [string]$InputPath,
        $Probe,
        $VideoStream,
        $SourceProfile,
        [string]$EncodeMode
    )

    $toolchain = Get-HdrToolchainEnvironment

    $plan = [ordered]@{
        Toolchain            = $toolchain
        IsHdr                = [bool]$SourceProfile.HasHDR
        TargetDynamicRange   = 'SDR'
        Primaries            = $null
        Transfer             = $null
        Matrix               = $null
        StaticMetadata       = $null
        CarryStaticMetadata  = $false
        StaticMetadataRoute  = 'none'     # ffmpeg_options | svtav1_params | none
        PreserveHDR10Plus    = $false
        Hdr10PlusRoute       = 'none'     # svt_inline | post_inject | none
        Hdr10PlusJsonPath    = $null
        DolbyVision          = $null
        InputBitstreamFilter = $null
        RequiresCpuLane      = $false
        RequiresCpuLaneReason = ''
        Skip                 = $false
        SkipReason           = ''
        Notes                = New-Object System.Collections.Generic.List[string]
    }

    # ---- Dolby Vision ------------------------------------------------------
    $dvPlan = Resolve-DolbyVisionSourcePlan -Probe $Probe -VideoStream $VideoStream -HdrToolchain $toolchain
    $plan.DolbyVision = $dvPlan

    if ($dvPlan.IsDolbyVision) {
        switch ($DolbyVisionMode) {
            'Skip' {
                $plan.Skip       = $true
                $plan.SkipReason = "$($dvPlan.Label): skipped because DolbyVisionMode is set to 'Skip'."
                return $plan
            }
            'HDR10' {
                if (-not $dvPlan.CanConvert) {
                    $plan.Skip       = $true
                    $plan.SkipReason = $dvPlan.Reason
                    return $plan
                }
                $plan.InputBitstreamFilter = $dvPlan.InputBitstreamFilter
                $plan.Notes.Add($dvPlan.Reason)
            }
            'Passthrough' {
                # AV1 can carry a DV RPU, but only Profile 10, and nothing in
                # this pipeline produces a conformant P10 stream. Rather than
                # emit a file that claims DV and is not, fall back to the
                # base-layer conversion and say so.
                if (-not $dvPlan.CanConvert) {
                    $plan.Skip       = $true
                    $plan.SkipReason = $dvPlan.Reason
                    return $plan
                }
                $plan.InputBitstreamFilter = $dvPlan.InputBitstreamFilter
                $plan.Notes.Add("Dolby Vision passthrough into AV1 is not produced by this pipeline; $($dvPlan.Reason)")
            }
        }
    }

    # ---- Transfer / primaries / matrix -------------------------------------
    # HLG is the case the old code got wrong: it tagged every HDR source as PQ,
    # so an HLG broadcast capture came out claiming smpte2084 and displayed
    # far too dark. HLG is now preserved as HLG.
    $sourceTransfer = [string]$SourceProfile.SourceTransfer
    $isHlgSource = ($sourceTransfer -match 'arib-std-b67') -or ($dvPlan.TargetDynamicRange -eq 'HLG')

    if ($dvPlan.IsDolbyVision -and $dvPlan.TargetDynamicRange -eq 'SDR') {
        $plan.TargetDynamicRange = 'SDR'
        $plan.Primaries = 'bt709'
        $plan.Transfer  = 'bt709'
        $plan.Matrix    = 'bt709'
    } elseif ($isHlgSource -and $PreserveHLG) {
        $plan.TargetDynamicRange = 'HLG'
        $plan.Primaries = 'bt2020'
        $plan.Transfer  = 'arib-std-b67'
        $plan.Matrix    = 'bt2020nc'
        $plan.IsHdr     = $true
    } elseif ($SourceProfile.HasHDR -or $dvPlan.TargetDynamicRange -eq 'HDR10') {
        $plan.TargetDynamicRange = 'HDR10'
        $plan.Primaries = 'bt2020'
        $plan.Transfer  = 'smpte2084'
        $plan.Matrix    = 'bt2020nc'
        $plan.IsHdr     = $true
    } else {
        $plan.TargetDynamicRange = 'SDR'
        $plan.Primaries = if (-not [string]::IsNullOrWhiteSpace($SourceProfile.SourcePrimaries)) { $SourceProfile.SourcePrimaries } else { 'bt709' }
        $plan.Transfer  = if (-not [string]::IsNullOrWhiteSpace($SourceProfile.SourceTransfer))  { $SourceProfile.SourceTransfer  } else { 'bt709' }
        $plan.Matrix    = if (-not [string]::IsNullOrWhiteSpace($SourceProfile.SourceMatrix))    { $SourceProfile.SourceMatrix    } else { 'bt709' }
    }

    if (-not $plan.IsHdr) { return $plan }

    # ---- Static HDR10 payload ---------------------------------------------
    $static = Get-HdrStaticMetadata -InputPath $InputPath -VideoStream $VideoStream
    $plan.StaticMetadata = $static

    if ($PreserveHdrStaticMetadata -and ($static.HasMasteringDisplay -or $static.HasContentLight)) {
        if ($toolchain.SupportsStaticMetadataOptions) {
            $plan.CarryStaticMetadata = $true
            $plan.StaticMetadataRoute = 'ffmpeg_options'
        } elseif ($EncodeMode -ne 'nvenc') {
            $plan.CarryStaticMetadata = $true
            $plan.StaticMetadataRoute = 'svtav1_params'
            $plan.Notes.Add('FFmpeg build lacks -mastering_display / -content_light; carrying static HDR10 metadata through svtav1-params instead.')
        } else {
            $plan.RequiresCpuLane       = $true
            $plan.RequiresCpuLaneReason = 'NVENC cannot carry static HDR10 metadata on this FFmpeg build (-mastering_display unavailable).'
            $plan.Notes.Add($plan.RequiresCpuLaneReason)
        }
    } elseif ($PreserveHdrStaticMetadata) {
        $plan.Notes.Add('Source carries no mastering-display or content-light metadata; none is invented for the output.')
    }

    # Surfaced as a note, not silently: a metadata value the script changed on
    # the user's behalf is exactly the kind of edit that should be visible in
    # the log and on screen rather than discovered later.
    if ($null -ne $plan.StaticMetadata) {
        $clampNote = [string](Get-OptionalProperty -InputObject $plan.StaticMetadata -PropertyName 'MaxCLLClampNote' -Default '')
        if (-not [string]::IsNullOrWhiteSpace($clampNote)) {
            $plan.Notes.Add($clampNote)
        }
    }

    # ---- HDR10+ dynamic metadata ------------------------------------------
    $wantHdr10Plus = switch ("$PreserveHDR10Plus") {
        'Auto'  { $static.HasHDR10Plus }
        'True'  { $static.HasHDR10Plus }
        default { $false }
    }

    if ($wantHdr10Plus) {
        # The inline route runs inside SVT-AV1, so it does not exist on the NVENC
        # lane. Rather than set a flag nobody enforces and then silently drop the
        # metadata, fall through to post-encode injection when this job is
        # already committed to NVENC, and only give up if that is unavailable too.
        $inlineAvailable = $toolchain.SupportsSvtHdr10PlusJson -and $toolchain.Hdr10PlusToolPath -and ($EncodeMode -ne 'nvenc')

        if ($EncodeMode -eq 'nvenc' -and $toolchain.SupportsSvtHdr10PlusJson) {
            $plan.RequiresCpuLane       = $true
            $plan.RequiresCpuLaneReason = 'HDR10+ can be carried inline only by the SVT-AV1 (CPU) lane; the Nvidia lane needs post-encode injection.'
        }

        if ($inlineAvailable) {
            $plan.PreserveHDR10Plus = $true
            $plan.Hdr10PlusRoute    = 'svt_inline'
        } elseif ($toolchain.Hdr10PlusToolPath -and $toolchain.Hdr10PlusToolSupportsAv1) {
            $plan.PreserveHDR10Plus = $true
            $plan.Hdr10PlusRoute    = 'post_inject'
            $plan.Notes.Add('HDR10+ metadata will be re-injected into the finished AV1 stream after encoding.')
        } else {
            $missing = if (-not $toolchain.Hdr10PlusToolPath) {
                'hdr10plus_tool was not found next to the script or on PATH'
            } else {
                'the available hdr10plus_tool build does not support AV1, and the linked SVT-AV1 does not accept hdr10plus-json'
            }
            $plan.Notes.Add("Source has HDR10+ dynamic metadata but it cannot be preserved: $missing. Output will be correctly-tagged static HDR10.")
        }
    }

    return $plan
}

# -----------------------------------------------------------------------------
# Emits the HDR-related output arguments. Shared by the software lane, the
# NVENC lane, and the preflight sampler so all three agree.
#
# Note on -svtav1-params: ffmpeg treats it as a single AVOption, so passing it
# twice means the second occurrence wins and the first is silently discarded.
# Film grain and HDR metadata therefore have to be merged into one string,
# which is why this returns the params rather than appending them directly.
# -----------------------------------------------------------------------------
function Get-HdrSvtAv1ParamPairs {
    param($HdrPlan)

    $pairs = New-Object System.Collections.Generic.List[string]

    if ($null -eq $HdrPlan) { return ,$pairs }
    if (-not $HdrPlan.IsHdr) { return ,$pairs }
    if ($HdrPlan.StaticMetadataRoute -ne 'svtav1_params') { return ,$pairs }

    $static = $HdrPlan.StaticMetadata
    if ($null -eq $static) { return ,$pairs }

    # SVT-AV1 takes the normalised-float form, NOT ffmpeg's integer units.
    # Read through Get-OptionalProperty rather than direct property access: under
    # Set-StrictMode -Version Latest a missing key on an ordered dictionary
    # throws, and this can be handed a plan built by an older code path.
    $masteringFloat = Get-OptionalProperty -InputObject $static -PropertyName 'MasteringDisplayFloat' -Default $null
    $hasMastering   = Get-OptionalProperty -InputObject $static -PropertyName 'HasMasteringDisplay' -Default $false
    $hasContent     = Get-OptionalProperty -InputObject $static -PropertyName 'HasContentLight' -Default $false
    $contentLight   = Get-OptionalProperty -InputObject $static -PropertyName 'ContentLight' -Default $null

    if ($hasMastering -and -not [string]::IsNullOrWhiteSpace($masteringFloat)) {
        $pairs.Add("mastering-display=$masteringFloat")
    }
    if ($hasContent -and -not [string]::IsNullOrWhiteSpace($contentLight)) {
        $pairs.Add("content-light=$contentLight")
    }

    return ,$pairs
}

function Add-HdrOutputArguments {
    param(
        [System.Collections.Generic.List[string]]$ArgumentList,
        $HdrPlan,
        $SourceProfile
    )

    # Fall back to the pre-existing behaviour if no plan was resolved, so the
    # arg builders stay callable in isolation.
    if ($null -eq $HdrPlan) {
        if ($SourceProfile -and $SourceProfile.HasHDR) {
            $ArgumentList.AddRange([string[]]@('-color_primaries', 'bt2020', '-color_trc', 'smpte2084', '-colorspace', 'bt2020nc'))
        }
        return
    }

    $ArgumentList.AddRange([string[]]@(
        '-color_primaries', $HdrPlan.Primaries,
        '-color_trc',       $HdrPlan.Transfer,
        '-colorspace',      $HdrPlan.Matrix
    ))

    if (-not $HdrPlan.IsHdr) { return }

    if ($HdrPlan.CarryStaticMetadata -and $HdrPlan.StaticMetadataRoute -eq 'ffmpeg_options') {
        $static = $HdrPlan.StaticMetadata
        $tc = $HdrPlan.Toolchain

        # Option name comes from the probed style, not from an assumption. See
        # Get-HdrToolchainEnvironment: ":v:0" is rejected by real FFmpeg 9
        # builds even though the documented syntax shows the specifier as
        # optional. Defaults to the bare form, which is the one that works.
        $mdStyle = Get-OptionalProperty -InputObject $tc -PropertyName 'MasteringDisplayArgStyle' -Default 'bare'
        $clStyle = Get-OptionalProperty -InputObject $tc -PropertyName 'ContentLightArgStyle'     -Default 'bare'

        if ($static.HasMasteringDisplay -and $mdStyle -ne 'none') {
            $opt = if ($mdStyle -eq 'stream_spec') { '-mastering_display:v:0' } else { '-mastering_display' }
            $ArgumentList.AddRange([string[]]@($opt, $static.MasteringDisplay))
        }
        if ($static.HasContentLight -and $clStyle -ne 'none') {
            $opt = if ($clStyle -eq 'stream_spec') { '-content_light:v:0' } else { '-content_light' }
            $ArgumentList.AddRange([string[]]@($opt, $static.ContentLight))
        }
    }
}

# -----------------------------------------------------------------------------
# Extracts ST 2094-40 metadata from the source into a JSON sidecar.
#
# hdr10plus_tool reads an HEVC elementary stream, so the video track is
# demuxed to Annex B and piped in. Failure is non-fatal: the encode continues
# without dynamic metadata and the reason is recorded.
# -----------------------------------------------------------------------------
function Export-HDR10PlusMetadata {
    param(
        [string]$InputPath,
        $VideoStream,
        $HdrPlan
    )

    $toolchain = $HdrPlan.Toolchain
    if (-not $toolchain.Hdr10PlusToolPath) {
        return [ordered]@{ Success = $false; Path = $null; Reason = 'hdr10plus_tool not available.' }
    }

    $codecName = [string](Get-StreamProp $VideoStream 'codec_name' '')
    if ($codecName -notmatch '(?i)hevc|h265') {
        return [ordered]@{ Success = $false; Path = $null; Reason = "HDR10+ extraction requires an HEVC source; this source is '$codecName'." }
    }

    $jsonPath = Join-Path $PreflightDir ("hdr10plus_{0}.json" -f [Guid]::NewGuid().ToString('N'))
    $streamIndex = [int](Get-StreamProp $VideoStream 'index' 0)

    try {
        # ffmpeg writes the Annex B elementary stream to stdout; hdr10plus_tool
        # reads it from stdin ("-") and writes the JSON sidecar.
        $ffmpegArgs = @(
            '-hide_banner', '-loglevel', 'error',
            '-i', $InputPath,
            '-map', "0:$streamIndex",
            '-c:v', 'copy',
            '-bsf:v', 'hevc_mp4toannexb',
            '-f', 'hevc', 'pipe:1'
        )

        & $FfmpegPath @ffmpegArgs 2>$null |
            & $toolchain.Hdr10PlusToolPath extract -o $jsonPath - 2>$null

        if ((Test-Path -LiteralPath $jsonPath) -and ((Get-Item -LiteralPath $jsonPath).Length -gt 0)) {
            return [ordered]@{ Success = $true; Path = $jsonPath; Reason = 'HDR10+ metadata extracted to JSON sidecar.' }
        }

        if (Test-Path -LiteralPath $jsonPath) {
            Remove-Item -LiteralPath $jsonPath -Force -ErrorAction SilentlyContinue
        }
        return [ordered]@{ Success = $false; Path = $null; Reason = 'hdr10plus_tool produced no metadata for this source.' }
    } catch {
        if (Test-Path -LiteralPath $jsonPath) {
            Remove-Item -LiteralPath $jsonPath -Force -ErrorAction SilentlyContinue
        }
        return [ordered]@{ Success = $false; Path = $null; Reason = "HDR10+ extraction failed: $($_.Exception.Message)" }
    }
}

# -----------------------------------------------------------------------------
# Re-injects HDR10+ metadata into a finished AV1 file.
#
# Used only on the post_inject route, i.e. when the encoder could not carry the
# metadata itself. The video track is demuxed to a raw AV1 stream, the metadata
# OBUs are inserted, and the result is remuxed against the original output so
# audio, subtitles, and chapters are preserved untouched.
#
# The original file is only replaced once every step has succeeded.
# -----------------------------------------------------------------------------
function Import-HDR10PlusMetadata {
    param(
        [string]$EncodedPath,
        [string]$JsonPath,
        $HdrPlan
    )

    $toolchain = $HdrPlan.Toolchain
    if (-not $toolchain.Hdr10PlusToolPath -or -not $toolchain.Hdr10PlusToolSupportsAv1) {
        return [ordered]@{ Success = $false; Reason = 'No AV1-capable hdr10plus_tool available for injection.' }
    }
    if (-not (Test-Path -LiteralPath $JsonPath)) {
        return [ordered]@{ Success = $false; Reason = 'HDR10+ JSON sidecar is missing.' }
    }

    $workId      = [Guid]::NewGuid().ToString('N')
    $rawAv1      = Join-Path $PreflightDir ("hdr10plus_bl_{0}.obu"  -f $workId)
    $injectedAv1 = Join-Path $PreflightDir ("hdr10plus_inj_{0}.obu" -f $workId)
    $remuxed     = Join-Path $PreflightDir ("hdr10plus_mux_{0}.mkv" -f $workId)

    try {
        # 1. Demux the AV1 video track to a raw OBU stream.
        $extractArgs = @(
            '-hide_banner', '-loglevel', 'error', '-y',
            '-i', $EncodedPath,
            '-map', '0:v:0',
            '-c:v', 'copy',
            '-f', 'obu', $rawAv1
        )
        & $FfmpegPath @extractArgs
        if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $rawAv1)) {
            return [ordered]@{ Success = $false; Reason = 'Could not demux the AV1 stream for HDR10+ injection.' }
        }

        # 2. Insert the metadata OBUs.
        & $toolchain.Hdr10PlusToolPath inject -i $rawAv1 -j $JsonPath -o $injectedAv1 2>$null
        if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $injectedAv1)) {
            return [ordered]@{ Success = $false; Reason = 'hdr10plus_tool injection failed.' }
        }

        # 3. Remux: injected video + every non-video stream from the encode.
        $remuxArgs = @(
            '-hide_banner', '-loglevel', 'error', '-y',
            '-i', $injectedAv1,
            '-i', $EncodedPath,
            '-map', '0:v:0',
            '-map', '1:a?',
            '-map', '1:s?',
            '-map_chapters', '1',
            '-c', 'copy',
            $remuxed
        )
        & $FfmpegPath @remuxArgs
        if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $remuxed)) {
            return [ordered]@{ Success = $false; Reason = 'Remux after HDR10+ injection failed.' }
        }

        # 4. Sanity check before replacing anything: the remux should be within a
        #    few percent of the encode. A wildly different size means something
        #    went wrong and the original is left alone.
        $originalSize = (Get-Item -LiteralPath $EncodedPath).Length
        $remuxedSize  = (Get-Item -LiteralPath $remuxed).Length
        if ($originalSize -gt 0) {
            $ratio = $remuxedSize / [double]$originalSize
            if ($ratio -lt 0.90 -or $ratio -gt 1.10) {
                return [ordered]@{
                    Success = $false
                    Reason  = ("HDR10+ injection produced an unexpected output size ({0:P1} of the encode); keeping the un-injected file." -f $ratio)
                }
            }
        }

        Move-Item -LiteralPath $remuxed -Destination $EncodedPath -Force
        return [ordered]@{ Success = $true; Reason = 'HDR10+ dynamic metadata injected into the AV1 output.' }
    } catch {
        return [ordered]@{ Success = $false; Reason = "HDR10+ injection failed: $($_.Exception.Message)" }
    } finally {
        foreach ($tmp in @($rawAv1, $injectedAv1, $remuxed)) {
            if ($tmp -and (Test-Path -LiteralPath $tmp)) {
                Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
            }
        }
    }
}

# -----------------------------------------------------------------------------
# Human-readable one-line summary of what the HDR plan will actually do, for
# the console and the session log.
# -----------------------------------------------------------------------------
function Get-HdrPlanSummary {
    param($HdrPlan)

    if ($null -eq $HdrPlan) { return 'HDR plan unavailable.' }
    if (-not $HdrPlan.IsHdr) { return "SDR passthrough ($($HdrPlan.Primaries)/$($HdrPlan.Transfer))" }

    $parts = New-Object System.Collections.Generic.List[string]
    $parts.Add($HdrPlan.TargetDynamicRange)

    if ($HdrPlan.CarryStaticMetadata) {
        $static = $HdrPlan.StaticMetadata
        $bits = New-Object System.Collections.Generic.List[string]
        if ($static.HasMasteringDisplay) { $bits.Add('mastering display') }
        if ($static.HasContentLight)     { $bits.Add("MaxCLL $($static.MaxCLL) / MaxFALL $($static.MaxFALL)") }
        $parts.Add(($bits -join ' + '))
    } else {
        $parts.Add('no static colour volume')
    }

    if ($HdrPlan.PreserveHDR10Plus) {
        $parts.Add("HDR10+ preserved ($($HdrPlan.Hdr10PlusRoute -replace '_','-'))")
    } elseif ($HdrPlan.StaticMetadata -and $HdrPlan.StaticMetadata.HasHDR10Plus) {
        $parts.Add('HDR10+ present but not preserved')
    }

    if ($HdrPlan.DolbyVision -and $HdrPlan.DolbyVision.IsDolbyVision) {
        $parts.Add("from $($HdrPlan.DolbyVision.Label)")
    }

    return ($parts -join ' | ')
}

# -----------------------------------------------------------------------------
# Flattens the HDR plan into the log columns. Kept as one helper so every row
# that reports HDR reports it identically, and so adding a column later means
# touching one place instead of every Write-LogRow call site.
# -----------------------------------------------------------------------------
function Get-HdrLogFields {
    param($Init)

    $empty = [ordered]@{
        SourceHdrFormat    = ''
        HdrTargetFormat    = ''
        HdrStaticMetadata  = ''
        HdrMaxCLL          = ''
        HdrMaxFALL         = ''
        HdrHDR10PlusSource = ''
        HdrHDR10PlusOutput = ''
        DolbyVisionProfile = ''
        DolbyVisionStrategy = ''
        HdrPlanSummary     = ''
        MaxCllClamped      = ''
    }

    if ($null -eq $Init) { return $empty }

    $plan = Get-OptionalProperty -InputObject $Init -PropertyName 'HdrPlan' -Default $null
    if ($null -eq $plan) { return $empty }

    $sourceProfile = Get-OptionalProperty -InputObject $Init -PropertyName 'SourceProfile' -Default $null
    $static = $plan.StaticMetadata
    $dv     = $plan.DolbyVision

    return [ordered]@{
        SourceHdrFormat    = if ($sourceProfile) { $sourceProfile.SourceHdrFormat } else { '' }
        HdrTargetFormat    = $plan.TargetDynamicRange
        HdrStaticMetadata  = if ($plan.CarryStaticMetadata -and $static -and $static.HasMasteringDisplay) { $static.MasteringDisplay } else { '' }
        HdrMaxCLL          = if ($plan.CarryStaticMetadata -and $static -and $static.HasContentLight) { $static.MaxCLL } else { '' }
        HdrMaxFALL         = if ($plan.CarryStaticMetadata -and $static -and $static.HasContentLight) { $static.MaxFALL } else { '' }
        HdrHDR10PlusSource = if ($static) { "$($static.HasHDR10Plus)" } else { '' }
        HdrHDR10PlusOutput = if ($plan.PreserveHDR10Plus) { $plan.Hdr10PlusRoute } else { 'none' }
        DolbyVisionProfile = if ($dv -and $dv.IsDolbyVision -and $null -ne $dv.Profile) { $dv.Profile } else { '' }
        DolbyVisionStrategy = if ($dv -and $dv.IsDolbyVision) { $dv.Strategy } else { '' }
        HdrPlanSummary     = Get-HdrPlanSummary -HdrPlan $plan
        MaxCllClamped      = if ($static) { [string](Get-OptionalProperty -InputObject $static -PropertyName 'MaxCLLClampNote' -Default '') } else { '' }
    }
}

function Add-HdrLogFields {
    param(
        [hashtable]$Row,
        $Init
    )

    foreach ($kv in (Get-HdrLogFields -Init $Init).GetEnumerator()) {
        $Row[$kv.Key] = $kv.Value
    }
    return $Row
}

# -----------------------------------------------------------------------------
# Log fields for the measured-quality columns.
#
# Written with the same "every shape carries every field" discipline as the HDR
# fields: the workflow object is built at three different places in this script,
# and under StrictMode a reader that finds a property on one shape and not
# another is a hard error rather than a blank cell.
# -----------------------------------------------------------------------------
function Get-QualityLogFields {
    param($Init)

    $empty = [ordered]@{
        QualityMetric            = ''
        QualityMode              = ''
        QualityThreshold         = ''
        QualityMeasured          = ''
        QualityAnchorCRF         = ''
        QualityAnchorMetric      = ''
        QualityTransparencyMet   = ''
        QualityProbeCount        = ''
        QualityCrfDelta          = ''
        QualitySecondMetric      = ''
        QualitySecondMetricValue = ''
        SvtEfficiencyParams      = ''
    }

    if ($null -eq $Init) { return $empty }

    $workflow = Get-OptionalProperty -InputObject $Init -PropertyName 'PreflightWorkflow' -Default $null
    if ($null -eq $workflow) { return $empty }

    # Recomputed rather than carried on $Init: the support probe is cached, so
    # this is free, and it records what was actually emitted rather than what
    # some earlier stage believed would be.
    $efficiency = ''
    if ([string](Get-OptionalProperty -InputObject $Init -PropertyName 'ResolvedEncodeLane' -Default '') -ne 'Nvidia') {
        $sp = Get-OptionalProperty -InputObject $Init -PropertyName 'SourceProfile' -Default $null
        $efficiency = ((Get-SvtAv1EfficiencyParamPairs -SourceProfile $sp -FrameRate ([double](Get-OptionalProperty -InputObject $sp -PropertyName 'FrameRate' -Default 0.0))) -join ':')
    }
    $empty.SvtEfficiencyParams = $efficiency

    $search = Get-OptionalProperty -InputObject $workflow -PropertyName 'QualitySearch' -Default $null
    if ($null -eq $search -or -not $search.Ran) { return $empty }

    $secondName  = [string](Get-OptionalProperty -InputObject $workflow -PropertyName 'QualitySecondMetricName' -Default '')
    $secondValue = [double](Get-OptionalProperty -InputObject $workflow -PropertyName 'QualitySecondMetricValue' -Default 0.0)

    return [ordered]@{
        QualityMetric            = [string]$search.MetricName
        QualityMode              = [string]$search.Mode
        QualityThreshold         = ("{0:F3}" -f [double]$search.Threshold)
        QualityMeasured          = ("{0:F3}" -f [double]$search.Metric)
        # 'Absolute->Anchor' is a real anchored decision too -- the absolute
        # target was unreachable and the search re-decided against a reference
        # encode. Recording the anchor only for Mode -eq 'Anchor' left the
        # column blank for exactly the decisions most worth auditing.
        QualityAnchorCRF         = if ($search.Mode -eq 'Anchor') { "$($search.AnchorCRF)" } else { '' }
        QualityAnchorMetric      = if ($search.Mode -eq 'Anchor') { ("{0:F3}" -f [double]$search.AnchorMetric) } else { '' }
        QualityTransparencyMet   = "$($search.TransparencyMet)"
        QualityProbeCount        = "$($search.ProbeCount)"
        QualityCrfDelta          = "$([int]$search.ChosenCRF - [int]$search.StartCRF)"
        QualitySecondMetric      = $secondName
        QualitySecondMetricValue = if ([string]::IsNullOrWhiteSpace($secondName)) { '' } else { ("{0:F3}" -f $secondValue) }
        SvtEfficiencyParams      = $efficiency
    }
}

function Add-QualityLogFields {
    param(
        [hashtable]$Row,
        $Init
    )

    foreach ($kv in (Get-QualityLogFields -Init $Init).GetEnumerator()) {
        $Row[$kv.Key] = $kv.Value
    }
    return $Row
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
    param($Probe, $VideoStream, [string]$InputPath = '')

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

    # HDR10+ detection used to scan the *stream-level* side_data_list only. For
    # the sources that matter here -- HEVC Blu-ray remuxes, where the metadata
    # lives in SEI rather than in container elements -- that list is empty, so
    # the check never fired and every HDR10+ source was silently treated as
    # plain HDR10. Get-HdrStaticMetadata checks stream level and then frame
    # level, and caches the result so this costs one probe per file.
    if (-not [string]::IsNullOrWhiteSpace($InputPath)) {
        $staticProbe = Get-HdrStaticMetadata -InputPath $InputPath -VideoStream $VideoStream
        $hasHDR10Plus = $staticProbe.HasHDR10Plus
    } else {
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
        # Carried here so the keyframe-interval calculation works from the real
        # frame rate. Without it the seconds-to-frames conversion silently fell
        # back to 24 fps, which is right for film and wrong for 25 and 30 fps
        # material -- a 10-second setting becoming 8 seconds without saying so.
        FrameRate           = [double](Get-FrameRate -Stream $VideoStream)
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

    # Six 2-second decode-and-SSIM passes. Get-AutoEncodeSettings is called
    # once by the lane resolver and again by encode initialisation for each
    # lane it evaluates, so an uncached pre-scan meant up to eighteen of these
    # per file per scheduling pass -- on a file whose grain had not changed.
    $grainCacheKey = Get-ContentCacheKey -Path $InputPath -Scope 'grainprescan'
    if ($script:GrainPreScanCache.ContainsKey($grainCacheKey)) {
        $script:DecisionCacheHits++
        return $script:GrainPreScanCache[$grainCacheKey]
    }

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
            '-'
        )

        # $matches is a PowerShell automatic variable; writing to it here works
        # but shadows the one -match populates, which is the sort of thing that
        # bites two functions later.
        $scanOutput = & $FfmpegPath @scanArgs 2>&1 | Out-String
        $ssimMatches = [System.Text.RegularExpressions.Regex]::Matches($scanOutput, 'All:([0-9]+\.[0-9]+)')
        if ($ssimMatches.Count -gt 0) {
            $lastValue = Convert-ToInvariantDouble $ssimMatches[$ssimMatches.Count - 1].Groups[1].Value 0.0
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

    $preScan = [ordered]@{
        Success    = $true
        GrainScore = $grainScore
        GrainClass = $grainClass
        Reason     = "Grain pre-scan: 6 x 2s samples, average SSIM after mild denoise = $([Math]::Round($avgSsim, 5))."
    }

    $script:GrainPreScanCache[$grainCacheKey] = $preScan
    $script:DecisionCacheMisses++
    return $preScan
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
        [bool]$HeldForCpuLane = $false,
        [string]$AutoCRFOffsetOverrideValue = '',
        [string]$TargetGiBPerHourOverrideValue = ''
    )

    # A per-job override beats the global setting. Empty means "no override",
    # which is distinct from "0" -- 0 is a real, explicitly-pinned value that
    # the Balanced tier uses to mean "no bias, and do not defer to the global".
    $effectiveAutoCRFOffset = if (-not [string]::IsNullOrWhiteSpace($AutoCRFOffsetOverrideValue)) {
        $AutoCRFOffsetOverrideValue
    } else {
        $AutoCRFOffset
    }

    $sourceItem = Get-Item -LiteralPath $InputPath
    $sourceSizeGiB = [Math]::Round(($sourceItem.Length / 1GB), 3)

    $outputDir = Split-Path -Path $InputPath -Parent
    $null = Test-SufficientDiskSpace -TargetDirectory $outputDir -SourceSizeBytes $sourceItem.Length

    $probe         = Invoke-FfprobeJson -InputPath $InputPath
    $selected      = Select-Streams     -Probe $probe
    $sourceProfile = Get-SourceProfile  -Probe $probe -VideoStream $selected.Video -InputPath $InputPath
    $encodeColorProfile = Get-EncodeColorProfile -SourceProfile $sourceProfile
    $sourceFormat  = Get-OptionalProperty -InputObject $probe -PropertyName 'format' -Default ([PSCustomObject]@{})
    $sourceDuration = Convert-ToInvariantDouble (Get-OptionalProperty $sourceFormat 'duration' 0) 0.0
    $sourceResolutionTier = Get-ResolutionTier -Width ([int](Get-StreamProp $selected.Video 'width' 0))
    $sourceCodecClass = Get-CodecClass -Stream $selected.Video
    $selectedAudioSummary = Format-StreamSummary -Streams @($selected.MainAudio, $selected.FallbackAudio)
    $selectedSubtitleSummary = Format-StreamSummary -Streams @($selected.MainSub, $selected.SdhSub)
    $copiedStreamsEstimate = Get-CopiedStreamsSizeEstimate -Streams @($selected.MainAudio, $selected.FallbackAudio, $selected.MainSub, $selected.SdhSub) -DurationSec $sourceDuration

    # Resolve the HDR plan once, here, and hand it to every downstream consumer
    # (preflight sampler, both argument builders, the log row). Resolving it in
    # one place is what keeps the preflight samples and the real encode from
    # drifting apart.
    $hdrPlan = Resolve-HdrEncodePlan `
        -InputPath $InputPath `
        -Probe $probe `
        -VideoStream $selected.Video `
        -SourceProfile $sourceProfile `
        -EncodeMode $EncodeMode

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

    # Dolby Vision sources are no longer skipped wholesale. Resolve-HdrEncodePlan
    # decides per source whether a correct conversion exists: Profile 7 and 8
    # convert cleanly to HDR10 (or HLG), while Profile 5 has no HDR10-compatible
    # base layer and is still refused, because re-tagging it produces visibly
    # wrong colour rather than an imperfect-but-watchable file.
    if ($hdrPlan.Skip) {
        Write-SessionTextLogMessage -Level Warn -Message ("HDR plan skip | {0} | {1}" -f $displayInputName, $hdrPlan.SkipReason)
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
                SourceHdrFormat   = $sourceProfile.SourceHdrFormat
                HdrTargetFormat   = ""
                HdrStaticMetadata = ""
                HdrMaxCLL         = ""
                HdrMaxFALL        = ""
                HdrHDR10PlusSource = ""
                HdrHDR10PlusOutput = ""
                DolbyVisionProfile = if ($hdrPlan.DolbyVision) { $hdrPlan.DolbyVision.Profile } else { "" }
                DolbyVisionStrategy = if ($hdrPlan.DolbyVision) { $hdrPlan.DolbyVision.Strategy } else { "" }
                HdrPlanSummary    = Get-HdrPlanSummary -HdrPlan $hdrPlan
                FfmpegPath        = $FfmpegPath
                FfprobePath       = $FfprobePath
                Notes             = $hdrPlan.SkipReason
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
        -ConfiguredAutoCRFOffset $effectiveAutoCRFOffset

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
        QualitySearch = $null
        QualitySecondMetricName = ''
        QualitySecondMetricValue = 0.0
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

    # HDR10+ metadata is extracted before preflight for two reasons: the inline
    # SVT-AV1 route needs the JSON sidecar to exist when the encoder starts, and
    # extracting it now means a source whose metadata turns out to be unreadable
    # is reported before hours of encoding rather than after.
    if ($hdrPlan.PreserveHDR10Plus) {
        $hdr10PlusExport = Export-HDR10PlusMetadata -InputPath $InputPath -VideoStream $selected.Video -HdrPlan $hdrPlan
        if ($hdr10PlusExport.Success) {
            $hdrPlan.Hdr10PlusJsonPath = $hdr10PlusExport.Path
            Write-SessionTextLogMessage -Level Info -Message ("HDR10+ extracted | {0} | {1}" -f $displayInputName, $hdr10PlusExport.Reason)
        } else {
            # Extraction failed: fall back to static HDR10 rather than starting
            # an encode that claims dynamic metadata it does not have.
            $hdrPlan.PreserveHDR10Plus = $false
            $hdrPlan.Hdr10PlusRoute    = 'none'
            $hdrPlan.Notes.Add("HDR10+ not preserved: $($hdr10PlusExport.Reason)")
            Write-Warning "HDR10+ extraction failed for $displayInputName -- continuing as static HDR10. $($hdr10PlusExport.Reason)"
            Write-SessionTextLogMessage -Level Warn -Message ("HDR10+ extraction failed | {0} | {1}" -f $displayInputName, $hdr10PlusExport.Reason)
        }
    }

    $hdrPlanSummary = Get-HdrPlanSummary -HdrPlan $hdrPlan
    Write-HostOnce -Key $InputPath -Message ("HDR: {0}" -f $hdrPlanSummary) -ForegroundColor DarkCyan
    Write-SessionTextLogMessage -Level Info -Message ("HDR plan | {0} | {1}" -f $displayInputName, $hdrPlanSummary)
    foreach ($hdrNote in $hdrPlan.Notes) {
        Write-SessionTextLogMessage -Level Info -Message ("HDR note | {0}" -f $hdrNote)
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
        -NvencEnvironment $NvencEnvironment `
        -HdrPlan $hdrPlan `
        -TargetGiBPerHourOverrideValue $TargetGiBPerHourOverrideValue

    $preflightEstimate = $preflightWorkflow.FinalPreflight
    $effectiveFilmGrain = [int]$preflightWorkflow.FinalResolvedFilmGrain
    if ($EncodeMode -eq 'nvenc' -and $preflightWorkflow.FinalNvencSettings) {
        $nvencSettings = $preflightWorkflow.FinalNvencSettings
    }

    # The skip decision is checked BEFORE the "did the size estimate run?"
    # branch, and deliberately so. The quality-targeted search can refuse a
    # file before any size projection happens -- if no CRF in range is visually
    # transparent there is nothing worth projecting. Leaving this check nested
    # inside "if the estimate ran" would silently drop that refusal and encode
    # the file anyway at reduced quality, which is the single outcome the
    # feature exists to prevent.
    if ($preflightWorkflow.WasSkippedByPreflight) {
        $skipStatus = if ([string]::IsNullOrWhiteSpace($preflightWorkflow.SkipStatus)) { 'PRECHECK_SKIPPED_UNFAVORABLE' } else { [string]$preflightWorkflow.SkipStatus }
        $skipMessage = if ($skipStatus -eq 'PRECHECK_SKIPPED_QUALITY_FLOOR') {
            'no CRF in the permitted range stayed visually transparent; source left untouched'
        } else {
            'estimated output exceeds threshold'
        }
        Write-HostOnce -Key $InputPath -Message ("Decision: skipped ({0})" -f $skipMessage) -ForegroundColor Yellow
        Write-SessionTextLogMessage -Level Warn -Message ("Preflight decision | skipped | {0} | {1}" -f $displayInputName, $skipMessage)

        $skipHdrFields = Get-HdrLogFields -Init ([ordered]@{ HdrPlan = $hdrPlan; SourceProfile = $sourceProfile })
        $skipQualityFields = Get-QualityLogFields -Init ([ordered]@{
            PreflightWorkflow  = $preflightWorkflow
            SourceProfile      = $sourceProfile
            ResolvedEncodeLane = $resolvedEncodeLane
        })

        $skipRow = @{
                    Timestamp         = (Get-Date).ToString("s")
                    Status            = $skipStatus
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

        # A skip row that does not say what the metadata and the measurement
        # were is a skip nobody can audit later.
        foreach ($kv in $skipHdrFields.GetEnumerator())     { $skipRow[$kv.Key] = $kv.Value }
        foreach ($kv in $skipQualityFields.GetEnumerator()) { $skipRow[$kv.Key] = $kv.Value }

        return [ordered]@{
            EarlyExit = $skipStatus
            Row       = $skipRow
        }
    }

    if ($preflightEstimate.Ran) {
        Write-HostOnce -Key $InputPath -Message ("Preflight estimate: {0:F2} GiB (projected savings {1:F1}%)" -f $preflightEstimate.EstimatedFinalSizeGiB, $preflightEstimate.EstimatedSavingsPercent) -ForegroundColor DarkCyan
        Write-SessionTextLogMessage -Level Info -Message ("Preflight estimate | {0} | {1:F2} GiB | savings {2:F1}%" -f $displayInputName, $preflightEstimate.EstimatedFinalSizeGiB, $preflightEstimate.EstimatedSavingsPercent)

        if ($preflightEstimate.WarningTriggered) {
            Write-Host ("Warning: projected output is {0:F1}% of source size." -f $preflightEstimate.EstimatedPctOfSource) -ForegroundColor Yellow
            Write-SessionTextLogMessage -Level Warn -Message ("Preflight warning | {0} | projected output is {1:F1}% of source size" -f $displayInputName, $preflightEstimate.EstimatedPctOfSource)
        }
        Write-HostOnce -Key $InputPath -Message "Proceeding with full encode" -ForegroundColor DarkCyan
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
        HdrPlan                 = $hdrPlan
        HdrPlanSummary          = $hdrPlanSummary
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

# -----------------------------------------------------------------------------
# The lane preference for one queued job.
#
# A job carries an override only when the drop explicitly asked for one. With no
# override the configured $EncoderPreference applies, so an ordinary drag-drop
# behaves exactly as it did before per-drop lane selection existed.
#
# Read through Get-OptionalProperty because jobs queued by an older version of
# this script have no EncoderPreferenceOverride property at all, and under
# StrictMode reading a missing property is a hard error rather than $null.
# -----------------------------------------------------------------------------
function Resolve-JobEncoderPreference {
    param($Job)

    $override = [string](Get-OptionalProperty -InputObject $Job -PropertyName 'EncoderPreferenceOverride' -Default '')
    if ([string]::IsNullOrWhiteSpace($override)) { return $EncoderPreference }

    switch ($override.Trim().ToLowerInvariant()) {
        'cpu'    { return 'CPU' }
        'nvidia' { return 'Nvidia' }
        'auto'   { return 'Auto' }
        default  { return $EncoderPreference }
    }
}

# -----------------------------------------------------------------------------
# Whether anything in the pending queue asks for a lane other than the one the
# configured $EncoderPreference would run.
#
# This matters because $EncoderPreference does not just bias a decision -- it
# selects which queue-processing loop runs for the whole session. Only the Auto
# lane scheduler reads a per-job preference; the CPU and Nvidia loops each drive
# one lane and cannot honour a job that asked for the other. So if any queued
# job disagrees with the global setting, the session has to run the scheduler
# that can actually act on it, or the setting is accepted and then ignored.
# -----------------------------------------------------------------------------
function Test-QueueHasLanePreferenceConflict {
    $pending = @(Get-ChildItem -LiteralPath $QueuePendingDir -Filter *.json -File -ErrorAction SilentlyContinue)
    foreach ($jobFile in $pending) {
        try {
            $job = Get-Content -LiteralPath $jobFile.FullName -Raw | ConvertFrom-Json
        } catch {
            continue
        }
        $preference = Resolve-JobEncoderPreference -Job $job
        if ($preference -ne $EncoderPreference) { return $true }
    }
    return $false
}

function Resolve-EncoderLane {
    param(
        [string]$InputPath,
        [string]$EncoderPreferenceValue,
        [bool]$CpuLaneAvailable = $true,
        [bool]$NvidiaLaneAvailable = $false,
        $NvencEnvironment = $null,
        [string]$AutoCRFOffsetOverrideValue = '',
        [string]$TargetGiBPerHourOverrideValue = ''
    )

    $effectiveAutoCRFOffset = if (-not [string]::IsNullOrWhiteSpace($AutoCRFOffsetOverrideValue)) {
        $AutoCRFOffsetOverrideValue
    } else {
        $AutoCRFOffset
    }

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
            Init   = (Get-EncodeInitialization -InputPath $InputPath -EncodeMode 'software' -EncoderPreferenceValue $EncoderPreferenceValue -LaneSelectionReason 'forced CPU lane by encoder preference' -AutoCRFOffsetOverrideValue $AutoCRFOffsetOverrideValue -TargetGiBPerHourOverrideValue $TargetGiBPerHourOverrideValue)
        }
    }

    if ($EncoderPreferenceValue -eq 'Nvidia') {
        if (-not $NvencEnvironment) {
            # Reachable now that a single drop can ask for the Nvidia lane on a
            # machine where the configured preference never probed for one.
            # Falling back with a logged reason is right; throwing here would
            # take down the whole queue over one job's preference.
            return [pscustomobject][ordered]@{
                Ready  = $true
                Reason = 'Nvidia lane was requested but no usable AV1 NVENC environment was found; using the CPU lane.'
                Init   = (Get-EncodeInitialization -InputPath $InputPath -EncodeMode 'software' -EncoderPreferenceValue 'CPU' -LaneSelectionReason 'requested Nvidia lane unavailable; fell back to CPU lane' -AutoCRFOffsetOverrideValue $AutoCRFOffsetOverrideValue -TargetGiBPerHourOverrideValue $TargetGiBPerHourOverrideValue)
            }
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
            Init   = (Get-EncodeInitialization -InputPath $InputPath -EncodeMode 'nvenc' -NvencEnvironment $NvencEnvironment -EncoderPreferenceValue $EncoderPreferenceValue -LaneSelectionReason 'forced Nvidia lane by encoder preference' -AutoCRFOffsetOverrideValue $AutoCRFOffsetOverrideValue -TargetGiBPerHourOverrideValue $TargetGiBPerHourOverrideValue)
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
            Init   = (Get-EncodeInitialization -InputPath $InputPath -EncodeMode 'software' -EncoderPreferenceValue $EncoderPreferenceValue -LaneSelectionReason 'Nvidia lane unavailable; using CPU lane' -AutoCRFOffsetOverrideValue $AutoCRFOffsetOverrideValue -TargetGiBPerHourOverrideValue $TargetGiBPerHourOverrideValue)
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
        -ConfiguredAutoCRFOffset $effectiveAutoCRFOffset
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
            -NvidiaFallbackAllowed $laneSuitability.NvidiaFallbackAllowed `
            -AutoCRFOffsetOverrideValue $AutoCRFOffsetOverrideValue `
        -TargetGiBPerHourOverrideValue $TargetGiBPerHourOverrideValue

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

        if ($alternateInit.EarlyExit -like 'PRECHECK_SKIPPED*') {
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
        -NvidiaFallbackAllowed $laneSuitability.NvidiaFallbackAllowed `
        -AutoCRFOffsetOverrideValue $AutoCRFOffsetOverrideValue `
        -TargetGiBPerHourOverrideValue $TargetGiBPerHourOverrideValue

    if ($firstInit.EarlyExit -notlike 'PRECHECK_SKIPPED*') {
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

    # NVENC cannot rescue a file the CPU encoder could not keep transparent.
    # At matched quality settings SVT-AV1 is the stronger encoder, so if the CPU
    # lane could not stay above the quality threshold anywhere in the permitted
    # CRF range, the Nvidia lane cannot either -- and evaluating it means a
    # second full quality search and preflight, several minutes, to arrive at a
    # verdict already known.
    if ($preferredLane -eq 'CPU' -and $firstInit.EarlyExit -eq 'PRECHECK_SKIPPED_QUALITY_FLOOR') {
        $firstInit.Row.LaneSelectionReason = "$($hint.Reason); CPU lane could not hold the quality threshold, and the Nvidia lane cannot beat it at matched quality"
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
        -NvidiaFallbackAllowed $laneSuitability.NvidiaFallbackAllowed `
        -AutoCRFOffsetOverrideValue $AutoCRFOffsetOverrideValue `
        -TargetGiBPerHourOverrideValue $TargetGiBPerHourOverrideValue

    # Reaching here means the PREFERRED lane already ran and refused the file.
    # When the alternate lane is Nvidia, the preferred lane was CPU -- so the
    # CPU verdict is in hand and is final. Returning "not ready, waiting for the
    # CPU lane" would hold a job for a lane that has already answered: the
    # scheduler leaves it Pending, comes back, re-runs the whole quality search
    # and preflight, and reaches the same verdict again, forever. That is
    # exactly what a real run did -- "Pending: 7 | Active: 0" with both workers
    # idle and the same VMAF numbers scrolling past for hours. A verdict that
    # cannot change has to be consumed, not re-asked.
    $consumeFirstSkip = {
        param([string]$Why)
        $firstInit.Row.LaneSelectionReason = $Why
        [pscustomobject][ordered]@{
            Ready                 = $true
            Reason                = $Why
            Init                  = $firstInit
            HeldForCpuLane        = $false
            LaneSuitability       = $laneSuitability.Suitability
            CpuOnlyReason         = $laneSuitability.CpuOnlyReason
            NvidiaFallbackAllowed = $laneSuitability.NvidiaFallbackAllowed
        }
    }

    if ($alternateLane -eq 'Nvidia') {
        $nvencFallback = Test-NvencFallbackSuitable -LaneSuitability $laneSuitability -Init $alternateInit
        if (-not $nvencFallback.Allowed) {
            return & $consumeFirstSkip ("$($hint.Reason); CPU preflight was unfavorable and the Nvidia lane is not suitable for this source ($($nvencFallback.Reason))")
        }
    }

    if ($alternateInit.EarlyExit -like 'PRECHECK_SKIPPED*') {
        if ($alternateLane -eq 'Nvidia') {
            return & $consumeFirstSkip ("$alternateReason; Nvidia preflight was also unfavorable; both lanes were unfavorable")
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

    # Split-Path -Qualifier THROWS when the path has no drive qualifier, and the
    # existing "UNC or unmapped drive -- skip check" guard below never got the
    # chance to run. A UNC target such as \\server\share\Media therefore failed
    # the disk-space check with "does not have a qualifier specified" instead of
    # skipping it. Same for any non-Windows path.
    #
    # A space check that cannot determine free space should decline to block the
    # encode, not abort it.
    $drive = $null
    try   { $drive = Split-Path -Qualifier $TargetDirectory -ErrorAction Stop }
    catch { return $true }   # no qualifier (UNC, or a non-Windows path) -- skip check.

    if ([string]::IsNullOrWhiteSpace($drive)) { return $true }

    $disk = Get-PSDrive -Name ($drive.TrimEnd(':')) -ErrorAction SilentlyContinue
    if (-not $disk) { return $true }   # unmapped drive -- skip check.
    if ($null -eq $disk.Free) { return $true }   # provider reports no free-space figure.

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

    $hdrPlan = Get-OptionalProperty -InputObject $Init -PropertyName 'HdrPlan' -Default $null

    $ffArgs = New-Object System.Collections.Generic.List[string]
    $ffArgs.AddRange([string[]]@('-hide_banner', '-y'))

    # Dolby Vision base-layer extraction has to happen on the *input* side, so
    # the decoder is handed a clean single-layer HDR10 stream. For a Profile 7
    # source this is dovi_split=bl; for Profile 8 it is dovi_rpu=strip=1.
    if ($null -ne $hdrPlan -and -not [string]::IsNullOrWhiteSpace($hdrPlan.InputBitstreamFilter)) {
        $ffArgs.AddRange([string[]]@('-bsf:v', $hdrPlan.InputBitstreamFilter))
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
        '-c:v', 'libsvtav1',
        '-preset', "$($Init.PreflightWorkflow.FinalResolvedPreset)",
        '-crf', "$($Init.PreflightWorkflow.FinalResolvedCRF)",
        '-pix_fmt', 'yuv420p10le'
    ))

    # -svtav1-params is a single AVOption: passing it twice means the second
    # occurrence wins and the first is silently discarded. Film grain, HDR
    # static metadata, and HDR10+ therefore all have to be merged into one
    # colon-separated string rather than appended as separate arguments.
    $svtParams = New-Object System.Collections.Generic.List[string]

    if ([int]$Init.EffectiveFilmGrain -gt 0) {
        $svtParams.Add("film-grain=$($Init.EffectiveFilmGrain)")
        # denoise=0 adds synthetic grain on top of the grain that was coded, so
        # it preserves the look and saves almost nothing -- measured here, film
        # grain 0 -> 16 at fixed CRF moved the file by under 1%. denoise=1 is
        # where the size win lives, at the cost of replacing the original grain
        # rather than reproducing it. Off by default; see the film grain note.
        $svtParams.Add(("film-grain-denoise={0}" -f $(if ($SoftwareFilmGrainDenoise) { 1 } else { 0 })))
    }

    # Core pinning so concurrent CPU encodes contend less. Only emitted when more
    # than one is actually running -- pinning a lone encode just starves it.
    if ($SoftwarePinCores -gt 0 -and $CpuMaxParallel -gt 1) {
        $svtParams.Add("pin=$SoftwarePinCores")
    }

    # Compression-efficiency settings, each verified against the encoder first.
    # These are the same pairs the preflight samples and the quality probes
    # used, so the CRF the search measured is the CRF this encode delivers.
    foreach ($pair in (Get-SvtAv1EfficiencyParamPairs -SourceProfile $sourceProfile -AutoSettings (Get-OptionalProperty -InputObject $Init -PropertyName 'AutoSettings' -Default $null) -FrameRate ([double](Get-OptionalProperty -InputObject $sourceProfile -PropertyName 'FrameRate' -Default 0.0)))) {
        $svtParams.Add($pair)
    }

    foreach ($pair in (Get-HdrSvtAv1ParamPairs -HdrPlan $hdrPlan)) { $svtParams.Add($pair) }

    # HDR10+ carried inline by an svt-av1-hdr / SVT-AV1-PSY build compiled with
    # enable-hdr10plus. This is the preferred route because it avoids a whole
    # extra demux/inject/remux pass over the finished file.
    if ($null -ne $hdrPlan -and $hdrPlan.PreserveHDR10Plus -and
        $hdrPlan.Hdr10PlusRoute -eq 'svt_inline' -and
        -not [string]::IsNullOrWhiteSpace($hdrPlan.Hdr10PlusJsonPath)) {
        $svtParams.Add("hdr10plus-json=$($hdrPlan.Hdr10PlusJsonPath)")
    }

    if ($svtParams.Count -gt 0) {
        $ffArgs.AddRange([string[]]@('-svtav1-params', ($svtParams -join ':')))
    }

    Add-SvtAv1DolbyVisionSuppression -ArgumentList $ffArgs -HdrPlan $hdrPlan
    Add-HdrOutputArguments -ArgumentList $ffArgs -HdrPlan $hdrPlan -SourceProfile $sourceProfile

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

    $hdrPlan = Get-OptionalProperty -InputObject $Init -PropertyName 'HdrPlan' -Default $null

    $ffArgs = New-Object System.Collections.Generic.List[string]
    $ffArgs.Add('-hide_banner')
    $ffArgs.Add('-y')

    if ($nvencSettings.DecodePath -eq 'cuda') {
        $ffArgs.AddRange([string[]]@('-hwaccel', 'cuda', '-hwaccel_output_format', 'cuda'))
    }

    # Same Dolby Vision base-layer handling as the software lane: applied on the
    # input so the decoder never sees the enhancement layer or the RPU.
    if ($null -ne $hdrPlan -and -not [string]::IsNullOrWhiteSpace($hdrPlan.InputBitstreamFilter)) {
        $ffArgs.AddRange([string[]]@('-bsf:v', $hdrPlan.InputBitstreamFilter))
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
    if ($NvencEnvironment.SupportsMultipass)  { $ffArgs.AddRange([string[]]@('-multipass', 'fullres')) }

    # FFmpeg 9 exposes AV1 hierarchical B-frames in NVENC. On Ada (RTX 40-series)
    # this is a straight compression win at the same CQ: a pyramid B reference
    # structure with more B-frames between anchors costs nothing in encode time
    # on dedicated silicon. Older builds only understood b_ref_mode, so the
    # depth options are probed rather than assumed.
    if ($NvencEnvironment.SupportsBRefMode) { $ffArgs.AddRange([string[]]@('-b_ref_mode', 'middle')) }
    # FFmpeg 9's AV1 NVENC hierarchical B-frames are driven by these two
    # existing options together -- there is no separate switch for the feature.
    # -b_ref_mode middle is what makes the structure a pyramid rather than a
    # flat run of disposable B-frames.
    if ($NvencEnvironment.SupportsBFrames -and $nvencSettings.BFrames -gt 0) {
        $ffArgs.AddRange([string[]]@('-bf', "$($nvencSettings.BFrames)"))
    }

    if ($NvencEnvironment.SupportsSplitEncode -and -not $NvencAllowSplitFrame) {
        $ffArgs.AddRange([string[]]@('-split_encode_mode', 'disabled'))
    }

    $ffArgs.AddRange([string[]]@('-pix_fmt', $nvencSettings.PixFmt))
    if ($NvencEnvironment.SupportsHighBitDepth -and $nvencSettings.BitDepth -ge 10) {
        $ffArgs.AddRange([string[]]@('-highbitdepth', '1'))
    }

    # Colour signalling and the static HDR10 payload now come from the resolved
    # HDR plan rather than being re-derived here. The old inline derivation
    # copied the source transfer verbatim, which meant an HLG source was tagged
    # HLG on this lane but PQ on the software lane -- the two lanes disagreed
    # about the same file. Add-HdrOutputArguments is the single source of truth.
    Add-HdrOutputArguments -ArgumentList $ffArgs -HdrPlan $hdrPlan -SourceProfile $sourceProfile

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
    $lines.Add((Row "Encoder preference: $($Summary.EncoderPreference)  |  CPU active: $($Summary.CpuActive)/$CpuMaxParallel  |  Nvidia active: $($Summary.NvidiaActive)/$($Summary.NvidiaCapacity)" $cMeta))
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

    $hdrPlan = Get-OptionalProperty -InputObject $init -PropertyName 'HdrPlan' -Default $null
    $hdrLogFields = Get-HdrLogFields -Init $init
    $qualityLogFields = Get-QualityLogFields -Init $init

    $notesList = [System.Collections.Generic.List[string]]::new()
    if ($init.AutoSettings.BitrateReason) { $notesList.Add($init.AutoSettings.BitrateReason) }
    if ($init.LaneSelectionReason) { $notesList.Add($init.LaneSelectionReason) }
    if ($init.CpuOnlyReason) { $notesList.Add($init.CpuOnlyReason) }
    if ($init.PreflightEstimate.Ran) { $notesList.Add($init.PreflightEstimate.Reason) }
    if ($init.FilmGrainDisabledReason) { $notesList.Add($init.FilmGrainDisabledReason) }
    if ($isNvenc -and $init.NvencSettings.Reason) { $notesList.Add($init.NvencSettings.Reason) }
    if ($isNvenc -and $init.NvencSettings.TuneReason) { $notesList.Add($init.NvencSettings.TuneReason) }
    if ($tracked.WorkerPriorityReason) { $notesList.Add($tracked.WorkerPriorityReason) }
    if ($null -ne $hdrPlan) {
        foreach ($hdrNote in $hdrPlan.Notes) { $notesList.Add($hdrNote) }
    }

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
            SourceHdrFormat   = $hdrLogFields.SourceHdrFormat
            HdrTargetFormat   = $hdrLogFields.HdrTargetFormat
            HdrStaticMetadata = $hdrLogFields.HdrStaticMetadata
            HdrMaxCLL         = $hdrLogFields.HdrMaxCLL
            HdrMaxFALL        = $hdrLogFields.HdrMaxFALL
            HdrHDR10PlusSource = $hdrLogFields.HdrHDR10PlusSource
            HdrHDR10PlusOutput = $hdrLogFields.HdrHDR10PlusOutput
            DolbyVisionProfile = $hdrLogFields.DolbyVisionProfile
            DolbyVisionStrategy = $hdrLogFields.DolbyVisionStrategy
            HdrPlanSummary    = $hdrLogFields.HdrPlanSummary
            MaxCllClamped            = $hdrLogFields.MaxCllClamped
            QualityMetric            = $qualityLogFields.QualityMetric
            QualityMode              = $qualityLogFields.QualityMode
            QualityThreshold         = $qualityLogFields.QualityThreshold
            QualityMeasured          = $qualityLogFields.QualityMeasured
            QualityAnchorCRF         = $qualityLogFields.QualityAnchorCRF
            QualityAnchorMetric      = $qualityLogFields.QualityAnchorMetric
            QualityTransparencyMet   = $qualityLogFields.QualityTransparencyMet
            QualityProbeCount        = $qualityLogFields.QualityProbeCount
            QualityCrfDelta          = $qualityLogFields.QualityCrfDelta
            QualitySecondMetric      = $qualityLogFields.QualitySecondMetric
            QualitySecondMetricValue = $qualityLogFields.QualitySecondMetricValue
            SvtEfficiencyParams      = $qualityLogFields.SvtEfficiencyParams
            FfmpegPath        = $FfmpegPath
            FfprobePath       = $FfprobePath
            Notes             = ($notesList -join ' | ')
        }
        return 'FAILED'
    }

    if (-not (Test-Path -LiteralPath $init.TempOutput)) {
        throw "Temporary output was not created: $($init.TempOutput)"
    }

    # HDR10+ re-injection, for the post_inject route only. This runs on the temp
    # output before any of the validation below, so a failed injection is caught
    # by the same duration and size checks as the encode itself, and before the
    # file is ever moved over the original.
    #
    # Deliberately non-fatal: a correctly-tagged static HDR10 file is a good
    # outcome, so if injection fails the encode is kept and the loss is recorded
    # rather than throwing away hours of work.
    if ($null -ne $hdrPlan -and $hdrPlan.PreserveHDR10Plus -and $hdrPlan.Hdr10PlusRoute -eq 'post_inject') {
        $injection = Import-HDR10PlusMetadata -EncodedPath $init.TempOutput -JsonPath $hdrPlan.Hdr10PlusJsonPath -HdrPlan $hdrPlan
        $notesList.Add($injection.Reason)
        if ($injection.Success) {
            Write-SessionTextLogMessage -Level Info -Message ("HDR10+ injected | {0}" -f $init.DisplayOutputName)
        } else {
            $hdrLogFields.HdrHDR10PlusOutput = 'failed'
            Write-Warning "HDR10+ injection failed for $($init.DisplayOutputName): $($injection.Reason)"
            Write-SessionTextLogMessage -Level Warn -Message ("HDR10+ injection failed | {0} | {1}" -f $init.DisplayOutputName, $injection.Reason)
        }
    }

    # The HDR10+ JSON sidecar has served its purpose either way.
    if ($null -ne $hdrPlan -and -not [string]::IsNullOrWhiteSpace($hdrPlan.Hdr10PlusJsonPath) -and
        (Test-Path -LiteralPath $hdrPlan.Hdr10PlusJsonPath)) {
        Remove-Item -LiteralPath $hdrPlan.Hdr10PlusJsonPath -Force -ErrorAction SilentlyContinue
    }

    $outProbe       = Invoke-FfprobeJson -InputPath $init.TempOutput -NoCache
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
        SourceHdrFormat   = $hdrLogFields.SourceHdrFormat
        HdrTargetFormat   = $hdrLogFields.HdrTargetFormat
        HdrStaticMetadata = $hdrLogFields.HdrStaticMetadata
        HdrMaxCLL         = $hdrLogFields.HdrMaxCLL
        HdrMaxFALL        = $hdrLogFields.HdrMaxFALL
        HdrHDR10PlusSource = $hdrLogFields.HdrHDR10PlusSource
        HdrHDR10PlusOutput = $hdrLogFields.HdrHDR10PlusOutput
        DolbyVisionProfile = $hdrLogFields.DolbyVisionProfile
        DolbyVisionStrategy = $hdrLogFields.DolbyVisionStrategy
        HdrPlanSummary    = $hdrLogFields.HdrPlanSummary
        MaxCllClamped            = $hdrLogFields.MaxCllClamped
        QualityMetric            = $qualityLogFields.QualityMetric
        QualityMode              = $qualityLogFields.QualityMode
        QualityThreshold         = $qualityLogFields.QualityThreshold
        QualityMeasured          = $qualityLogFields.QualityMeasured
        QualityAnchorCRF         = $qualityLogFields.QualityAnchorCRF
        QualityAnchorMetric      = $qualityLogFields.QualityAnchorMetric
        QualityTransparencyMet   = $qualityLogFields.QualityTransparencyMet
        QualityProbeCount        = $qualityLogFields.QualityProbeCount
        QualityCrfDelta          = $qualityLogFields.QualityCrfDelta
        QualitySecondMetric      = $qualityLogFields.QualitySecondMetric
        QualitySecondMetricValue = $qualityLogFields.QualitySecondMetricValue
        SvtEfficiencyParams      = $qualityLogFields.SvtEfficiencyParams
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
                $jobOffsetOverride = [string](Get-OptionalProperty -InputObject $job -PropertyName 'AutoCRFOffsetOverride' -Default '')
                $jobTargetOverride = [string](Get-OptionalProperty -InputObject $job -PropertyName 'TargetGiBPerHourOverride' -Default '')
                $init = Get-EncodeInitialization -InputPath $job.InputPath -EncodeMode 'nvenc' -NvencEnvironment $NvencEnvironment -AutoCRFOffsetOverrideValue $jobOffsetOverride -TargetGiBPerHourOverrideValue $jobTargetOverride

                if ($init.EarlyExit) {
                    $row = $init.Row
                    $row.NvencWorkerCountAtStart = $NvencEnvironment.MaxParallel
                    Write-LogRow $row
                    if ($init.EarlyExit -like 'AUTO_SKIPPED*' -or $init.EarlyExit -eq 'SKIPPED_DV' -or $init.EarlyExit -like 'PRECHECK_SKIPPED*') {
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

    $script:QueueStalled = $false
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
            $cpuAvailable = (@($activeWorkers | Where-Object { $_.Init.ResolvedEncodeLane -eq 'CPU' }).Count -lt $CpuMaxParallel)
            $nvidiaAvailable = (@($activeWorkers | Where-Object { $_.Init.ResolvedEncodeLane -eq 'Nvidia' }).Count -lt $nvidiaCapacity)
            if (-not $cpuAvailable -and -not $nvidiaAvailable) { break }

            # Lane availability is the ONLY thing that can turn a held job into
            # a runnable one. Re-asking an unchanged question costs a full probe,
            # quality search and preflight per job per pass, so a job that was
            # held under this exact availability is skipped outright until a
            # worker starts or finishes.
            $availabilitySignature = "cpu=$cpuAvailable|nvidia=$nvidiaAvailable"

            $scheduledAny = $false
            $heldThisPass = 0
            foreach ($nextJob in @($pendingJobs)) {
                if ($script:LaneHoldSignatures.ContainsKey($nextJob.FullName) -and
                    $script:LaneHoldSignatures[$nextJob.FullName] -eq $availabilitySignature) {
                    $heldThisPass++
                    continue
                }

                $job = $null
                $workingJobPath = $null
                try {
                    $job = Get-Content -LiteralPath $nextJob.FullName -Raw | ConvertFrom-Json
                    # Per-job lane preference. This used to be hard-coded to
                    # 'Auto' here, which meant a drop that asked for a specific
                    # lane was silently scheduled by the automatic logic anyway.
                    # A job with no override falls back to the configured
                    # $EncoderPreference, so plain drag-drops are unchanged.
                    $jobLanePreference = Resolve-JobEncoderPreference -Job $job

                    $resolution = Resolve-EncoderLane `
                        -AutoCRFOffsetOverrideValue ([string](Get-OptionalProperty -InputObject $job -PropertyName 'AutoCRFOffsetOverride' -Default '')) `
                        -TargetGiBPerHourOverrideValue ([string](Get-OptionalProperty -InputObject $job -PropertyName 'TargetGiBPerHourOverride' -Default '')) `
                        -InputPath $job.InputPath `
                        -EncoderPreferenceValue $jobLanePreference `
                        -CpuLaneAvailable $cpuAvailable `
                        -NvidiaLaneAvailable $nvidiaAvailable `
                        -NvencEnvironment $NvencEnvironment

                    if (-not $resolution.Ready) {
                        $script:LaneHoldSignatures[$nextJob.FullName] = $availabilitySignature
                        $heldThisPass++
                        if ($resolution.HeldForCpuLane -and $job.InputPath) {
                            if ($script:HeldForCpuAnnouncements.Add($job.InputPath)) {
                                Write-Host $resolution.Reason -ForegroundColor DarkYellow
                                Write-SessionTextLogMessage -Level Info -Message $resolution.Reason
                            }
                        }
                        continue
                    }

                    $null = $script:LaneHoldSignatures.Remove($nextJob.FullName)
                    $workingJobPath = Join-Path $QueueWorkingDir $nextJob.Name
                    Move-Item -LiteralPath $nextJob.FullName -Destination $workingJobPath -Force

                    if ($resolution.Init.EarlyExit) {
                        $row = $resolution.Init.Row
                        # The early-exit object is @{ EarlyExit; Row } and has no
                        # ResolvedEncodeLane of its own -- reading it threw
                        # "The property 'ResolvedEncodeLane' cannot be found on
                        # this object", turning every legitimate skip into a
                        # FAILED row. The lane is recorded on the Row, and it is
                        # read defensively because Set-StrictMode -Version Latest
                        # throws on a missing hashtable key too.
                        $rowLane = [string](Get-OptionalProperty -InputObject $row -PropertyName 'ResolvedEncodeLane' -Default '')
                        $row.NvencWorkerCountAtStart = if ($rowLane -eq 'Nvidia' -and $NvencEnvironment) { $NvencEnvironment.MaxParallel } else { "" }
                        Write-LogRow $row
                        if ($resolution.Init.EarlyExit -like 'AUTO_SKIPPED*' -or $resolution.Init.EarlyExit -eq 'SKIPPED_DV' -or $resolution.Init.EarlyExit -like 'PRECHECK_SKIPPED*') {
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

            if (-not $scheduledAny) {
                # Every hold in this function requires a busy lane, so "nothing
                # scheduled, nothing held-for-later, and no worker running" is
                # not a wait -- it is a stall, and spinning on it would burn the
                # machine re-deciding jobs that can never start. Say so and stop
                # instead; the jobs stay queued for the next run.
                if ($activeWorkers.Count -eq 0 -and $heldThisPass -gt 0) {
                    Write-Host ""
                    Write-Host ("Stopping: {0} queued job(s) are waiting for an encoder lane, but no encode is running to free one." -f $heldThisPass) -ForegroundColor Yellow
                    Write-Host "They stay queued and will be retried on the next run." -ForegroundColor Yellow
                    Write-SessionTextLogMessage -Level Warn -Message ("Queue stalled | {0} job(s) held with no active worker" -f $heldThisPass)
                    $script:QueueStalled = $true
                }
                break
            }
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
        if ($activeWorkers.Count -eq 0 -and $script:QueueStalled) { break }
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
    param(
        [string]$InputPath,
        [string]$AutoCRFOffsetOverrideValue = '',
        # This loop accepted the CRF override but not the size target, so the
        # interactive "Target GiB/hr" tier was stored on the job and then
        # dropped on the floor whenever $EncoderPreference was 'CPU'.
        [string]$TargetGiBPerHourOverrideValue = ''
    )

    $effectiveAutoCRFOffset = if (-not [string]::IsNullOrWhiteSpace($AutoCRFOffsetOverrideValue)) {
        $AutoCRFOffsetOverrideValue
    } else {
        $AutoCRFOffset
    }

    $stopwatch     = [System.Diagnostics.Stopwatch]::StartNew()
    $sourceItem    = Get-Item -LiteralPath $InputPath
    $sourceSizeGiB = [Math]::Round(($sourceItem.Length / 1GB), 3)

    $outputDir = Split-Path -Path $InputPath -Parent
    $null = Test-SufficientDiskSpace -TargetDirectory $outputDir -SourceSizeBytes $sourceItem.Length

    $probe         = Invoke-FfprobeJson -InputPath $InputPath
    $selected      = Select-Streams     -Probe $probe
    $sourceProfile = Get-SourceProfile  -Probe $probe -VideoStream $selected.Video -InputPath $InputPath
    $encodeColorProfile = Get-EncodeColorProfile -SourceProfile $sourceProfile
    $sourceFormat  = Get-OptionalProperty -InputObject $probe -PropertyName 'format' -Default ([PSCustomObject]@{})
    $sourceDuration = Convert-ToInvariantDouble (Get-OptionalProperty $sourceFormat 'duration' 0) 0.0
    $sourceResolutionTier = Get-ResolutionTier -Width ([int](Get-StreamProp $selected.Video 'width' 0))
    $sourceCodecClass = Get-CodecClass -Stream $selected.Video
    $selectedAudioSummary = Format-StreamSummary -Streams @($selected.MainAudio, $selected.FallbackAudio)
    $selectedSubtitleSummary = Format-StreamSummary -Streams @($selected.MainSub, $selected.SdhSub)

    # Same HDR resolution as the lane-based path, so this serial fallback path
    # produces byte-identical output decisions rather than quietly reverting to
    # the old tag-only behaviour.
    $hdrPlan = Resolve-HdrEncodePlan `
        -InputPath $InputPath `
        -Probe $probe `
        -VideoStream $selected.Video `
        -SourceProfile $sourceProfile `
        -EncodeMode 'software'

    if ($hdrPlan.Skip) {
        Write-Warning "Skipping source: $($hdrPlan.SkipReason)"
        Write-Warning "  $InputPath"

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
            SourceHdrFormat   = $sourceProfile.SourceHdrFormat
            HdrTargetFormat   = ""
            DolbyVisionProfile = if ($hdrPlan.DolbyVision) { $hdrPlan.DolbyVision.Profile } else { "" }
            DolbyVisionStrategy = if ($hdrPlan.DolbyVision) { $hdrPlan.DolbyVision.Strategy } else { "" }
            HdrPlanSummary    = Get-HdrPlanSummary -HdrPlan $hdrPlan
            FfmpegPath        = $FfmpegPath
            FfprobePath       = $FfprobePath
            Notes             = $hdrPlan.SkipReason
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
        -ConfiguredAutoCRFOffset $effectiveAutoCRFOffset

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
        QualitySearch = $null
        QualitySecondMetricName = ''
        QualitySecondMetricValue = 0.0
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
    # Pre-existing bug, unrelated to the FFmpeg 9 work: $displayInputName was
    # read further down (in the Write-SessionEncodeStart banner) but never
    # assigned anywhere in this function. Under Set-StrictMode -Version Latest
    # that is a hard error -- "the variable cannot be retrieved because it has
    # not been set" -- so this serial path threw as soon as it reached the
    # banner. Its sibling $displayOutputName was assigned here all along.
    $displayInputName  = [System.IO.Path]::GetFileName($InputPath)

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

    if ($hdrPlan.PreserveHDR10Plus) {
        $hdr10PlusExport = Export-HDR10PlusMetadata -InputPath $InputPath -VideoStream $selected.Video -HdrPlan $hdrPlan
        if ($hdr10PlusExport.Success) {
            $hdrPlan.Hdr10PlusJsonPath = $hdr10PlusExport.Path
        } else {
            $hdrPlan.PreserveHDR10Plus = $false
            $hdrPlan.Hdr10PlusRoute    = 'none'
            $hdrPlan.Notes.Add("HDR10+ not preserved: $($hdr10PlusExport.Reason)")
            Write-Warning "HDR10+ extraction failed -- continuing as static HDR10. $($hdr10PlusExport.Reason)"
        }
    }

    $hdrPlanSummary = Get-HdrPlanSummary -HdrPlan $hdrPlan
    Write-HostOnce -Key $InputPath -Message ("HDR: {0}" -f $hdrPlanSummary) -ForegroundColor DarkCyan
    Write-SessionTextLogMessage -Level Info -Message ("HDR plan | {0}" -f $hdrPlanSummary)

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
        -InitialResolvedFilmGrain $resolvedFilmGrain `
        -HdrPlan $hdrPlan `
        -TargetGiBPerHourOverrideValue $TargetGiBPerHourOverrideValue

    $preflightEstimate = $preflightWorkflow.FinalPreflight
    $resolvedCRF = [int]$preflightWorkflow.FinalResolvedCRF
    $resolvedPreset = [int]$preflightWorkflow.FinalResolvedPreset
    $resolvedFilmGrain = [int]$preflightWorkflow.FinalResolvedFilmGrain
    $resolvedCRFLabel = [string]$resolvedCRF
    $resolvedPresetLabel = [string]$resolvedPreset

    if ($preflightEstimate.Ran) {
        Write-HostOnce -Key $InputPath -Message ("Preflight estimate: {0:F2} GiB (projected savings {1:F1}%)" -f $preflightEstimate.EstimatedFinalSizeGiB, $preflightEstimate.EstimatedSavingsPercent) -ForegroundColor DarkCyan
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
        Write-HostOnce -Key $InputPath -Message "Proceeding with full encode" -ForegroundColor DarkCyan
    } elseif ($EnablePreflightEstimate -and -not [string]::IsNullOrWhiteSpace($preflightEstimate.Reason)) {
        Write-Warning $preflightEstimate.Reason
    }

    # ── Build ffmpeg argument list ────────────────────────────────────────────
    $ffArgs = New-Object System.Collections.Generic.List[string]
    $ffArgs.AddRange([string[]]@("-hide_banner", "-y"))

    # Dolby Vision base-layer extraction, applied on the input so the decoder is
    # handed a clean single-layer HDR10 stream (dovi_split=bl for Profile 7,
    # dovi_rpu=strip=1 for Profile 8).
    if (-not [string]::IsNullOrWhiteSpace($hdrPlan.InputBitstreamFilter)) {
        $ffArgs.AddRange([string[]]@('-bsf:v', $hdrPlan.InputBitstreamFilter))
    }

    $ffArgs.AddRange([string[]]@(
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

    # Encoder-specific options are chained with colons into a single
    # -svtav1-params. It must be a single occurrence: ffmpeg treats it as one
    # AVOption, so a second -svtav1-params silently discards the first.
    #
    # film-grain-denoise=0 tells the encoder to synthesise grain at decode time
    # WITHOUT pre-denoising the source -- preferred when the source grain is
    # already well-behaved and the underlying image texture should be left alone.
    $svtParams = New-Object System.Collections.Generic.List[string]
    if ($resolvedFilmGrain -gt 0) {
        $svtParams.Add("film-grain=$resolvedFilmGrain")
        $svtParams.Add(("film-grain-denoise={0}" -f $(if ($SoftwareFilmGrainDenoise) { 1 } else { 0 })))
    }
    # Core pinning so concurrent CPU encodes contend less. Only emitted when more
    # than one is actually running -- pinning a lone encode just starves it.
    if ($SoftwarePinCores -gt 0 -and $CpuMaxParallel -gt 1) {
        $svtParams.Add("pin=$SoftwarePinCores")
    }

    foreach ($pair in (Get-SvtAv1EfficiencyParamPairs -SourceProfile $sourceProfile -FrameRate ([double](Get-OptionalProperty -InputObject $sourceProfile -PropertyName 'FrameRate' -Default 0.0)))) {
        $svtParams.Add($pair)
    }

    foreach ($pair in (Get-HdrSvtAv1ParamPairs -HdrPlan $hdrPlan)) { $svtParams.Add($pair) }
    if ($hdrPlan.PreserveHDR10Plus -and $hdrPlan.Hdr10PlusRoute -eq 'svt_inline' -and
        -not [string]::IsNullOrWhiteSpace($hdrPlan.Hdr10PlusJsonPath)) {
        $svtParams.Add("hdr10plus-json=$($hdrPlan.Hdr10PlusJsonPath)")
    }
    if ($svtParams.Count -gt 0) {
        $ffArgs.AddRange([string[]]@('-svtav1-params', ($svtParams -join ':')))
    }

    # Colour signalling plus the static HDR10 payload (mastering display colour
    # volume and MaxCLL/MaxFALL). Previously this wrote only the three colour
    # tags and always claimed smpte2084, which mislabelled HLG sources as PQ and
    # dropped the colour volume entirely.
    Add-SvtAv1DolbyVisionSuppression -ArgumentList $ffArgs -HdrPlan $hdrPlan
    Add-HdrOutputArguments -ArgumentList $ffArgs -HdrPlan $hdrPlan -SourceProfile $sourceProfile

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
        SourceHdrFormat = $sourceProfile.SourceHdrFormat
        HdrTargetFormat = $hdrPlan.TargetDynamicRange
        HdrStaticMetadata = if ($hdrPlan.CarryStaticMetadata -and $hdrPlan.StaticMetadata -and $hdrPlan.StaticMetadata.HasMasteringDisplay) { $hdrPlan.StaticMetadata.MasteringDisplay } else { '' }
        HdrMaxCLL    = if ($hdrPlan.CarryStaticMetadata -and $hdrPlan.StaticMetadata -and $hdrPlan.StaticMetadata.HasContentLight) { $hdrPlan.StaticMetadata.MaxCLL } else { '' }
        HdrMaxFALL   = if ($hdrPlan.CarryStaticMetadata -and $hdrPlan.StaticMetadata -and $hdrPlan.StaticMetadata.HasContentLight) { $hdrPlan.StaticMetadata.MaxFALL } else { '' }
        HdrHDR10PlusSource = if ($hdrPlan.StaticMetadata) { "$($hdrPlan.StaticMetadata.HasHDR10Plus)" } else { '' }
        HdrHDR10PlusOutput = $hdr10PlusOutputState
        DolbyVisionProfile = if ($hdrPlan.DolbyVision -and $hdrPlan.DolbyVision.IsDolbyVision -and $null -ne $hdrPlan.DolbyVision.Profile) { $hdrPlan.DolbyVision.Profile } else { '' }
        DolbyVisionStrategy = if ($hdrPlan.DolbyVision -and $hdrPlan.DolbyVision.IsDolbyVision) { $hdrPlan.DolbyVision.Strategy } else { '' }
        HdrPlanSummary = $hdrPlanSummary
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

    # ── HDR10+ re-injection (post_inject route only) ──────────────────────────
    # Runs before the validation below so a bad injection is caught by the same
    # duration and size checks as the encode itself. Non-fatal by design: a
    # correctly-tagged static HDR10 file is still a good result.
    $hdr10PlusOutputState = if ($hdrPlan.PreserveHDR10Plus) { $hdrPlan.Hdr10PlusRoute } else { 'none' }
    if ($hdrPlan.PreserveHDR10Plus -and $hdrPlan.Hdr10PlusRoute -eq 'post_inject') {
        $injection = Import-HDR10PlusMetadata -EncodedPath $tempOutput -JsonPath $hdrPlan.Hdr10PlusJsonPath -HdrPlan $hdrPlan
        if ($injection.Success) {
            Write-SessionTextLogMessage -Level Info -Message ("HDR10+ injected | {0}" -f $tempOutput)
        } else {
            $hdr10PlusOutputState = 'failed'
            $hdrPlan.Notes.Add($injection.Reason)
            Write-Warning "HDR10+ injection failed: $($injection.Reason)"
            Write-SessionTextLogMessage -Level Warn -Message ("HDR10+ injection failed | {0}" -f $injection.Reason)
        }
    }

    if (-not [string]::IsNullOrWhiteSpace($hdrPlan.Hdr10PlusJsonPath) -and
        (Test-Path -LiteralPath $hdrPlan.Hdr10PlusJsonPath)) {
        Remove-Item -LiteralPath $hdrPlan.Hdr10PlusJsonPath -Force -ErrorAction SilentlyContinue
    }

    # ── Duration sanity check ─────────────────────────────────────────────────
    # Uses a flat 10-second floor or 2% of source duration, whichever is larger.
    # A flat percentage alone would reject valid short clips where muxer rounding
    # differences are proportionally significant.
    $outProbe       = Invoke-FfprobeJson -InputPath $tempOutput -NoCache
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

    switch ($script:QueueLoopPreference) {
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
            Invoke-EncodeJob -InputPath $job.InputPath `
                -AutoCRFOffsetOverrideValue ([string](Get-OptionalProperty -InputObject $job -PropertyName 'AutoCRFOffsetOverride' -Default '')) `
                -TargetGiBPerHourOverrideValue ([string](Get-OptionalProperty -InputObject $job -PropertyName 'TargetGiBPerHourOverride' -Default ''))
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

# Validated rather than trusted. A typo here would otherwise be stored on every
# job in the drop and then quietly ignored at scheduling time, which is exactly
# how the interactive quality tiers used to fail.
if (-not [string]::IsNullOrWhiteSpace($EncoderPreferenceOverride)) {
    $EncoderPreferenceOverride = switch ($EncoderPreferenceOverride.Trim().ToLowerInvariant()) {
        'cpu'    { 'CPU' }
        'nvidia' { 'Nvidia' }
        'auto'   { 'Auto' }
        default  { throw "EncoderPreferenceOverride must be Auto, CPU, or Nvidia (got '$EncoderPreferenceOverride')." }
    }
}
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
$QualityFloorUnreachableAction = Resolve-QualityFloorActionConfigValue -Name 'QualityFloorUnreachableAction' -Value $QualityFloorUnreachableAction
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



# =============================================================================
# Doctor
# Merged from Media2AV1Queue-Doctor.ps1. Body unchanged; see Build-Single.ps1.
# =============================================================================
function Invoke-DoctorTool {
param(
    [string]$TestFile = '',
    [string]$HdrToolsDir = $null
)



$script:issues   = New-Object System.Collections.Generic.List[string]
$script:warnings = New-Object System.Collections.Generic.List[string]

function Write-Head([string]$Text) {
    Write-Host ""
    Write-Host ("=" * 74) -ForegroundColor DarkCyan
    Write-Host " $Text" -ForegroundColor Cyan
    Write-Host ("=" * 74) -ForegroundColor DarkCyan
}

function Write-Item {
    param(
        [string]$Label,
        [string]$Value,
        [ValidateSet('ok','warn','bad','info')][string]$State = 'info',
        [string]$Note = ''
    )
    $mark, $colour = switch ($State) {
        'ok'   { '[ ok ]',   'Green' }
        'warn' { '[warn ]',  'Yellow' }
        'bad'  { '[FAIL ]',  'Red' }
        default { '[info ]', 'Gray' }
    }
    Write-Host ("  {0} {1,-38} {2}" -f $mark, $Label, $Value) -ForegroundColor $colour
    if ($Note) { Write-Host ("           {0}" -f $Note) -ForegroundColor DarkGray }
}

# ---------------------------------------------------------------------------
# Binary discovery: same order the main script uses -- next to the script
# first (portable deployment), then PATH.
# ---------------------------------------------------------------------------
function Find-Binary([string]$Name) {
    foreach ($dir in @($HdrToolsDir, $PSScriptRoot) | Where-Object { $_ }) {
        foreach ($ext in @('.exe','')) {
            $c = Join-Path $dir ($Name + $ext)
            if (Test-Path -LiteralPath $c -PathType Leaf) { return (Get-Item -LiteralPath $c).FullName }
        }
    }
    $cmd = Get-Command $Name -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    return $null
}

Write-Head "FFmpeg / FFprobe"

$ffmpeg  = Find-Binary 'ffmpeg'
$ffprobe = Find-Binary 'ffprobe'

if (-not $ffmpeg) {
    Write-Item 'ffmpeg' 'NOT FOUND' 'bad' 'Place ffmpeg.exe next to the script or add it to PATH.'
    $script:issues.Add('ffmpeg not found')
    Write-Host ""
    $script:LastToolExitCode = 1; return
}
Write-Item 'ffmpeg' $ffmpeg 'ok'
Write-Item 'ffprobe' ($(if ($ffprobe) { $ffprobe } else { 'NOT FOUND' })) $(if ($ffprobe) { 'ok' } else { 'bad' })
if (-not $ffprobe) { $script:issues.Add('ffprobe not found') }

$versionText = (& $ffmpeg -hide_banner -version | Out-String)
$versionLine = (($versionText -split "\r?\n")[0]).Trim()
$major = 0
$isGit = $false
$buildDate = $null

# Git snapshot builds carry no version number (e.g. gyan.dev's
# "ffmpeg version 2026-08-17-git-426841da9d-full_build"). They are normally
# AHEAD of the numbered releases, so an unparseable version must not be
# treated as suspect. The 9.0 branch was cut from master on 2026-06-26.
if ($versionText -match '(?im)^ffmpeg version \D*(\d+)\.\d') {
    $major = [int]$Matches[1]
} elseif ($versionText -match '(?im)^ffmpeg version\s+(\d{4})-(\d{2})-(\d{2})\S*git') {
    $isGit = $true
    $buildDate = [datetime]::new([int]$Matches[1], [int]$Matches[2], [int]$Matches[3])
    if ($buildDate -ge [datetime]::new(2026,6,26)) { $major = 9 }
} elseif ($versionText -match '(?im)^ffmpeg version \S*git') {
    $isGit = $true
}

Write-Item 'version' $versionLine 'info'

if ($isGit -and $major -ge 9) {
    Write-Item 'version gate' ("git build {0:yyyy-MM-dd}, post-9.0 branch point" -f $buildDate) 'ok' `
        'Git snapshots are ahead of the numbered releases. Capability probes below are authoritative.'
} elseif ($major -ge 9) {
    Write-Item 'version gate' "FFmpeg $major.x - fully supported" 'ok'
} elseif ($isGit) {
    Write-Item 'version gate' 'git build, date not determinable' 'warn' 'Relying on the capability probes below.'
} elseif ($major -eq 8) {
    Write-Item 'version gate' "FFmpeg 8.x - reduced HDR capability" 'warn' `
        'No -mastering_display / -content_light and no dovi_split. See below.'
    $script:warnings.Add('FFmpeg 8.x: upgrade to 9.0.1+ for full static HDR10 and Dolby Vision support')
} elseif ($major -gt 0) {
    Write-Item 'version gate' "FFmpeg $major.x - UNSUPPORTED" 'bad' 'The script requires 9.0+ (8.x runs with warnings).'
    $script:issues.Add("FFmpeg $major.x is unsupported")
} else {
    Write-Item 'version gate' 'could not parse version' 'warn'
}

if ($versionText -match '(?i)\b(?:essentials|basic|minimal|lite)(?:[_ -]?build)?\b') {
    Write-Item 'build type' 'stripped/basic build detected' 'bad' 'A full build is required.'
    $script:issues.Add('stripped ffmpeg build')
} else {
    Write-Item 'build type' 'full build' 'ok'
}

# ---------------------------------------------------------------------------
Write-Head "Static HDR10 metadata (mastering display + MaxCLL/MaxFALL)"

$helpFull = (& $ffmpeg -hide_banner -h full 2>&1 | Out-String)

# ffmpeg prints CLI options at column 0 and AVOptions indented. Matching with
# "^\s+" (required whitespace) finds AVOptions but can NEVER find a CLI option
# -- which is why a capable FFmpeg 9 build previously reported these missing.
# "^\s*" matches either, and a functional probe settles any disagreement.
function Test-OptionWorks {
    # Not named $Args: that shadows a PowerShell automatic variable.
    param([string[]]$ProbeOption)
    $probe = @('-hide_banner','-nostdin','-f','lavfi','-i','color=c=black:s=64x64:r=1:d=1',
               '-c:v','libsvtav1','-preset','12','-crf','60','-frames:v','1') + $ProbeOption + @('-f','null','-')
    try {
        $psi = [System.Diagnostics.ProcessStartInfo]::new()
        $psi.FileName = $ffmpeg
        foreach ($a in $probe) { $psi.ArgumentList.Add($a) }
        $psi.RedirectStandardError = $true; $psi.RedirectStandardOutput = $true; $psi.UseShellExecute = $false
        $pr = [System.Diagnostics.Process]::Start($psi)
        $err = $pr.StandardError.ReadToEnd(); $null = $pr.StandardOutput.ReadToEnd()
        if (-not $pr.WaitForExit(30000)) { try { $pr.Kill($true) } catch {}; return $false }
        return (-not ($err -match '(?im)Unrecognized option|Option not found|Error splitting the argument list'))
    } catch { return $false }
}

$hasMD = ($helpFull -match '(?m)^\s*-mastering_display\b')
$hasCL = ($helpFull -match '(?m)^\s*-content_light\b')
$mdProbe = Test-OptionWorks @('-mastering_display','G(13250,34500)B(7500,3000)R(34000,16000)WP(15635,16450)L(10000000,1)')
$clProbe = Test-OptionWorks @('-content_light','1000,400')
$hasMD = $hasMD -or $mdProbe
$hasCL = $hasCL -or $clProbe

Write-Item '-mastering_display' $(if ($hasMD) { 'available' } else { 'missing' }) $(if ($hasMD) { 'ok' } else { 'warn' }) `
    ("help-text match: {0}   functional probe: {1}" -f ($helpFull -match '(?m)^\s*-mastering_display\b'), $mdProbe)
Write-Item '-content_light'     $(if ($hasCL) { 'available' } else { 'missing' }) $(if ($hasCL) { 'ok' } else { 'warn' }) `
    ("help-text match: {0}   functional probe: {1}" -f ($helpFull -match '(?m)^\s*-content_light\b'), $clProbe)

if ($hasMD -and $hasCL) {
    Write-Item 'route' 'ffmpeg CLI options (preferred)' 'ok' `
        'Works on both the CPU and NVENC lanes.'
} else {
    Write-Item 'route' 'svtav1-params fallback (CPU lane only)' 'warn' `
        'HDR jobs will be steered to the CPU lane; the NVENC lane cannot carry colour volume on this build.'
    $script:warnings.Add('static HDR10 metadata limited to the CPU lane on this ffmpeg build')
}

# ---------------------------------------------------------------------------
Write-Head "Dolby Vision"

$bsfs = (& $ffmpeg -hide_banner -bsfs 2>&1 | Out-String)
$hasSplit = $bsfs -match '(?im)\bdovi_split\b'
$hasRpu   = $bsfs -match '(?im)\bdovi_rpu\b'

Write-Item 'dovi_split bsf' $(if ($hasSplit) { 'available' } else { 'missing' }) $(if ($hasSplit) { 'ok' } else { 'warn' }) `
    $(if ($hasSplit) { '' } else { 'Needed to extract the HDR10 base layer from Profile 7 (UHD Blu-ray) sources.' })
Write-Item 'dovi_rpu bsf'   $(if ($hasRpu)   { 'available' } else { 'missing' }) $(if ($hasRpu)   { 'ok' } else { 'warn' })

if ($hasSplit -or $hasRpu) {
    Write-Item 'DV -> HDR10 conversion' 'supported for Profiles 7 and 8' 'ok' `
        'Profile 5 is always refused: its base layer has no HDR10-compatible form.'
} else {
    Write-Item 'DV -> HDR10 conversion' 'unavailable' 'warn' 'Dolby Vision sources will be skipped.'
    $script:warnings.Add('no Dolby Vision bitstream filters; DV sources will be skipped')
}

$doviTool = Find-Binary 'dovi_tool'
Write-Item 'dovi_tool (optional)' $(if ($doviTool) { $doviTool } else { 'not found' }) 'info'

# ---------------------------------------------------------------------------
Write-Head "HDR10+ dynamic metadata (SMPTE ST 2094-40)"

$encHelp = (& $ffmpeg -hide_banner -h encoder=libsvtav1 2>&1 | Out-String)
$svtHdr10Plus = $encHelp -match '(?im)hdr10plus[-_]json'
$hasLibSvt = $encHelp -notmatch '(?i)is not recognized|unknown encoder'

Write-Item 'libsvtav1 encoder' $(if ($hasLibSvt) { 'present' } else { 'MISSING' }) $(if ($hasLibSvt) { 'ok' } else { 'bad' })
if (-not $hasLibSvt) { $script:issues.Add('libsvtav1 encoder not available') }

Write-Item 'SVT-AV1 hdr10plus-json' $(if ($svtHdr10Plus) { 'supported' } else { 'not supported' }) $(if ($svtHdr10Plus) { 'ok' } else { 'info' }) `
    $(if ($svtHdr10Plus) { '' } else { 'Mainline SVT-AV1 does not have this. It needs svt-av1-hdr or SVT-AV1-PSY built with enable-hdr10plus.' })

$h10Tool = Find-Binary 'hdr10plus_tool'
$h10Av1 = $false
if ($h10Tool) {
    try {
        $injectHelp = (& $h10Tool inject --help 2>&1 | Out-String)
        $h10Av1 = $injectHelp -match '(?im)\bav1\b|\bivf\b'
    } catch { $h10Av1 = $false }
}
Write-Item 'hdr10plus_tool' $(if ($h10Tool) { $h10Tool } else { 'not found' }) $(if ($h10Tool) { 'ok' } else { 'warn' })
if ($h10Tool) {
    Write-Item 'hdr10plus_tool AV1 support' $(if ($h10Av1) { 'yes' } else { 'no (HEVC only)' }) $(if ($h10Av1) { 'ok' } else { 'warn' })
}

$route = if ($svtHdr10Plus -and $h10Tool) { 'inline via SVT-AV1 (best)' }
         elseif ($h10Tool -and $h10Av1)   { 'post-encode injection' }
         else                             { 'NONE - HDR10+ will be dropped' }
Write-Item 'HDR10+ route' $route $(if ($route -like 'NONE*') { 'warn' } else { 'ok' })
if ($route -like 'NONE*') {
    $script:warnings.Add('HDR10+ cannot be preserved; output will be static HDR10 only')
    Write-Host ""
    Write-Host "  To preserve HDR10+, add ONE of the following:" -ForegroundColor DarkGray
    Write-Host "    a) an ffmpeg linked against svt-av1-hdr / SVT-AV1-PSY built with" -ForegroundColor DarkGray
    Write-Host "       enable-hdr10plus, plus hdr10plus_tool for extraction; or" -ForegroundColor DarkGray
    Write-Host "    b) an hdr10plus_tool build with AV1 inject support." -ForegroundColor DarkGray
    Write-Host "  Without either, HDR output is still correct -- just static HDR10." -ForegroundColor DarkGray
}

# ---------------------------------------------------------------------------
Write-Head "NVENC (AV1)"

$nvHelp = ''
try { $nvHelp = (& $ffmpeg -hide_banner -h encoder=av1_nvenc 2>&1 | Out-String) } catch { $nvHelp = '' }
$hasNvenc = $nvHelp -match '(?m)^\s+-preset\b'

if (-not $hasNvenc) {
    Write-Item 'av1_nvenc' 'not available' 'info' 'The Nvidia lane will be disabled; the CPU lane is unaffected.'
} else {
    Write-Item 'av1_nvenc' 'available' 'ok'
    foreach ($opt in 'preset','tune','cq','multipass','b_ref_mode','highbitdepth','split_encode_mode') {
        $present = $nvHelp -match "(?m)^\s+-$([Regex]::Escape($opt))\b"
        Write-Item "  -$opt" $(if ($present) { 'yes' } else { 'no' }) $(if ($present) { 'ok' } else { 'info' })
    }

    # -bf is a GENERIC AVCodecContext option, so it never appears in
    # "-h encoder=av1_nvenc" output -- searching the help text for it always
    # says "no" even on builds where it works. And FFmpeg 9's AV1 hierarchical
    # B-frames have no dedicated option: they are driven by -bf plus
    # -b_ref_mode middle.
    #
    # This is a DIFFERENTIAL probe. It encodes twice -- once plain, once with
    # B-frames -- and only blames B-frames when the plain run succeeds and the
    # B-frame run does not. A single probe cannot tell "B-frames unsupported"
    # apart from "the probe itself was invalid", and the previous version got
    # this wrong twice over: a 64x64 source (below NVENC's driver-side minimum
    # frame size for AV1) and a single frame (nothing for a B-frame to sit
    # between). 1280x720 x 16 frames avoids both and still takes under a second.
    Write-Host ""
    function Invoke-NvencProbe {
        param([string[]]$Extra)
        $probe = @('-hide_banner','-nostdin','-f','lavfi','-i','color=c=black:s=1280x720:r=24:d=1',
                   '-c:v','av1_nvenc','-preset','p1','-frames:v','16') + $Extra + @('-f','null','-')
        try {
            $psi = [System.Diagnostics.ProcessStartInfo]::new()
            $psi.FileName = $ffmpeg
            foreach ($a in $probe) { $psi.ArgumentList.Add($a) }
            $psi.RedirectStandardError = $true; $psi.RedirectStandardOutput = $true; $psi.UseShellExecute = $false
            $pr = [System.Diagnostics.Process]::Start($psi)
            $err = $pr.StandardError.ReadToEnd(); $null = $pr.StandardOutput.ReadToEnd()
            if (-not $pr.WaitForExit(60000)) { try { $pr.Kill($true) } catch {}; return @{ Ok=$false; Diag='timed out' } }
            # ffmpeg logs "Max B-frames N exceed M" as a WARNING and then fails,
            # so the final line is the useless generic "Conversion failed!".
            $lines = @($err -split "`n" | Where-Object {
                $_ -match '(?i)b-?frames|exceed|not supported|invalid|no capable|cannot|failed to|error'
            } | Where-Object { $_ -notmatch '(?i)^\s*Conversion failed' } | Select-Object -First 3)
            $diag = if ($lines.Count) { ($lines -join ' || ').Trim() } elseif ($pr.ExitCode -ne 0) { "exit code $($pr.ExitCode)" } else { '' }
            return @{ Ok = ($pr.ExitCode -eq 0); Diag = $diag }
        } catch { return @{ Ok=$false; Diag=$_.Exception.Message } }
    }

    $ctl = Invoke-NvencProbe -Extra @()
    if (-not $ctl.Ok) {
        Write-Item 'B-frame differential probe' 'INCONCLUSIVE' 'warn' `
            "av1_nvenc could not complete even a plain test encode, so nothing can be concluded about B-frames. $($ctl.Diag)"
        $script:warnings.Add('av1_nvenc could not complete a test encode; B-frame support undetermined')
    } else {
        Write-Item '  control encode (no B-frames)' 'succeeded' 'ok'
        $bf = Invoke-NvencProbe -Extra @('-bf','2','-b_ref_mode','middle')
        if ($bf.Ok) {
            Write-Item '  with -bf 2 -b_ref_mode middle' 'succeeded' 'ok' `
                'Hierarchical B-frames active: compression gain at the same CQ, near-free on Ada.'
        } else {
            Write-Item '  with -bf 2 -b_ref_mode middle' 'rejected' 'warn' `
                "Encodes fine without B-frames but rejects them. $($bf.Diag)"
            $script:warnings.Add('av1_nvenc will not use B-frames; some compression is left on the table')
        }
    }
}

# ---------------------------------------------------------------------------
if ($TestFile) {
    Write-Head "Source analysis: $([System.IO.Path]::GetFileName($TestFile))"

    if (-not (Test-Path -LiteralPath $TestFile)) {
        Write-Item 'file' 'NOT FOUND' 'bad'
    } else {
        $json = & $ffprobe -v error -print_format json -show_streams -show_format $TestFile
        $probe = $json | ConvertFrom-Json -Depth 100
        $v = @($probe.streams | Where-Object { $_.codec_type -eq 'video' })[0]

        Write-Item 'codec'      ([string]$v.codec_name) 'info'
        Write-Item 'resolution' ("{0}x{1}" -f $v.width, $v.height) 'info'
        foreach ($f in 'pix_fmt','color_primaries','color_transfer','color_space') {
            $val = $v.PSObject.Properties[$f]
            Write-Item $f $(if ($val -and $val.Value) { [string]$val.Value } else { '(unset)' }) 'info'
        }

        # Static payload: stream level first, then frame level. The frame-level
        # probe is the one that matters for HEVC remuxes, where the metadata is
        # carried in SEI rather than container elements.
        function Get-SideData([string]$Origin) {
            if ($Origin -eq 'stream') {
                $p = $v.PSObject.Properties['side_data_list']
                if ($p -and $p.Value) { return @($p.Value) }
                return @()
            }
            $fj = & $ffprobe -v error -print_format json -show_frames -read_intervals '%+#1' -select_streams "$($v.index)" $TestFile
            if (-not $fj) { return @() }
            $fp = $fj | ConvertFrom-Json -Depth 100
            foreach ($fr in @($fp.frames)) {
                $sp = $fr.PSObject.Properties['side_data_list']
                if ($sp -and $sp.Value) { return @($sp.Value) }
            }
            return @()
        }

        $found = @{ md = $null; cl = $null; h10 = $false; dv = $null; origin = 'none' }
        foreach ($origin in 'stream','frame') {
            foreach ($sd in (Get-SideData $origin)) {
                $t = [string]$sd.side_data_type
                if ($t -match '(?i)mastering\s*display' -and -not $found.md) { $found.md = $sd; $found.origin = $origin }
                elseif ($t -match '(?i)content\s*light'  -and -not $found.cl) { $found.cl = $sd; $found.origin = $origin }
                elseif ($t -match '(?i)HDR10\+|2094-40|Dynamic\s*HDR')        { $found.h10 = $true }
                elseif ($t -match '(?i)DOVI|Dolby\s*Vision' -and -not $found.dv) { $found.dv = $sd }
            }
        }

        Write-Host ""
        Write-Item 'metadata found at' $found.origin 'info'
        Write-Item 'mastering display' $(if ($found.md) { 'present' } else { 'absent' }) $(if ($found.md) { 'ok' } else { 'warn' }) `
            $(if ($found.md) { '' } else { 'Nothing to carry. The output cannot have colour volume the source never had.' })
        if ($found.cl) {
            Write-Item 'MaxCLL / MaxFALL' ("{0} / {1}" -f $found.cl.max_content, $found.cl.max_average) 'ok'
        } else {
            Write-Item 'MaxCLL / MaxFALL' 'absent' 'warn'
        }
        Write-Item 'HDR10+ (ST 2094-40)' $(if ($found.h10) { 'present' } else { 'absent' }) 'info'

        if ($found.dv) {
            $prof = $found.dv.PSObject.Properties['dv_profile']
            $compat = $found.dv.PSObject.Properties['dv_bl_signal_compatibility_id']
            $profVal = if ($prof) { [int]$prof.Value } else { -1 }
            $compatVal = if ($compat) { [int]$compat.Value } else { -1 }
            Write-Item 'Dolby Vision profile' "$profVal (base-layer compat id $compatVal)" 'info'
            $verdict = switch ($profVal) {
                7 { if ($hasSplit -or $hasRpu) { 'will convert to HDR10 via base layer','ok' } else { 'will be SKIPPED (no dovi bsf)','warn' } }
                8 { if ($compatVal -in @(1,2,4)) { 'will convert via base layer','ok' } else { 'will be SKIPPED (unknown compat id)','warn' } }
                5 { 'will be SKIPPED - no HDR10-compatible base layer','warn' }
                default { 'unrecognised configuration; will be SKIPPED','warn' }
            }
            Write-Item 'DV handling' $verdict[0] $verdict[1]
        }

        Write-Host ""
        $target = if ($found.dv) { 'per DV plan above' }
                  elseif ([string]$v.color_transfer -match 'arib-std-b67') { 'HLG (preserved)' }
                  elseif ([string]$v.color_transfer -match 'smpte2084') { 'HDR10' }
                  elseif ([string]$v.color_primaries -match 'bt2020') { 'HDR10' }
                  else { 'SDR passthrough' }
        Write-Item 'PREDICTED OUTPUT' $target 'info'
        $carry = if (-not $found.md -and -not $found.cl) { 'nothing to carry' }
                 elseif ($hasMD -and $hasCL) { 'static metadata via ffmpeg options' }
                 else { 'static metadata via svtav1-params (CPU lane)' }
        Write-Item 'static metadata' $carry 'info'
        $h10out = if (-not $found.h10) { 'n/a (source has none)' }
                  elseif ($route -like 'NONE*') { 'DROPPED - see HDR10+ section' }
                  else { "preserved ($route)" }
        Write-Item 'HDR10+' $h10out $(if ($h10out -like 'DROPPED*') { 'warn' } else { 'info' })
    }
}

# ---------------------------------------------------------------------------
Write-Head "Perceptual quality measurement"

# Quality targeting is the mechanism that answers "is this still visually
# transparent?" rather than only "how big is it?". Both metrics are checked
# functionally, not by name: a filter can be listed and still be unusable,
# libvmaf in particular because it needs its model files.
function Test-MeasurementFilter {
    param([string]$Spec)
    $probeArgs = @(
        '-hide_banner','-nostdin','-nostats','-loglevel','info','-y',
        '-f','lavfi','-i','testsrc2=size=256x144:rate=24:duration=1',
        '-f','lavfi','-i','testsrc2=size=256x144:rate=24:duration=1',
        '-lavfi', ("[0:v]format=yuv420p10le[a];[1:v]format=yuv420p10le[b];[a][b]{0}" -f $Spec),
        '-frames:v','4','-f','null','-'
    )
    try {
        $psi = [System.Diagnostics.ProcessStartInfo]::new()
        $psi.FileName = $ffmpeg
        foreach ($a in $probeArgs) { $psi.ArgumentList.Add($a) }
        $psi.RedirectStandardError = $true; $psi.RedirectStandardOutput = $true; $psi.UseShellExecute = $false
        $p = [System.Diagnostics.Process]::Start($psi)
        $err = $p.StandardError.ReadToEnd(); $null = $p.StandardOutput.ReadToEnd()
        if (-not $p.WaitForExit(60000)) { try { $p.Kill($true) } catch {}; return [pscustomobject]@{ Ok=$false; Detail='timed out' } }
        $last = (($err -split "\r?\n" | Where-Object { $_.Trim() }) | Select-Object -Last 1)
        return [pscustomobject]@{ Ok = ($p.ExitCode -eq 0); Detail = [string]$last }
    } catch {
        return [pscustomobject]@{ Ok = $false; Detail = $_.Exception.Message }
    }
}

$filterList = ''
try { $filterList = (& $ffmpeg -hide_banner -filters 2>&1 | Out-String) } catch { $filterList = '' }

$xpsnrListed = ($filterList -match '(?im)^\s*\S+\s+xpsnr\s')
$vmafListed  = ($filterList -match '(?im)^\s*\S+\s+libvmaf\s')

if ($xpsnrListed) {
    $r = Test-MeasurementFilter -Spec 'xpsnr'
    if ($r.Ok) { Write-Item 'xpsnr filter' 'available' 'ok' 'used for HDR sources, and as the SDR fallback' }
    else { Write-Item 'xpsnr filter' 'listed but unusable' 'warn' $r.Detail; $script:warnings.Add('xpsnr filter is listed but failed a test run') }
} else {
    Write-Item 'xpsnr filter' 'not present' 'warn' 'needs FFmpeg 7.1 or newer; HDR quality targeting will be unavailable'
    $script:warnings.Add('no xpsnr filter: HDR quality targeting unavailable')
}

if ($vmafListed) {
    $r = Test-MeasurementFilter -Spec 'libvmaf=n_threads=2'
    if ($r.Ok) {
        Write-Item 'libvmaf filter' 'available' 'ok' 'used for SDR sources, absolute target'
        $r4k = Test-MeasurementFilter -Spec "libvmaf=model='version=vmaf_4k_v0.6.1':n_threads=2"
        if ($r4k.Ok) { Write-Item 'vmaf_4k_v0.6.1 model' 'available' 'ok' 'used for UHD sources' }
        else { Write-Item 'vmaf_4k_v0.6.1 model' 'unavailable' 'warn' 'UHD will be scored with the 1080p model, which reads optimistic'; $script:warnings.Add('4K VMAF model unavailable') }
    } else {
        Write-Item 'libvmaf filter' 'listed but unusable' 'warn' $r.Detail
        $script:warnings.Add('libvmaf is listed but failed a test run (missing model files?)')
    }
} else {
    Write-Item 'libvmaf filter' 'not present' 'warn' 'build lacks --enable-libvmaf; SDR sources will fall back to anchored XPSNR'
    $script:warnings.Add('no libvmaf: SDR quality targeting falls back to XPSNR')
}

if (-not $xpsnrListed -and -not $vmafListed) {
    $script:issues.Add('no usable quality metric: quality targeting cannot run at all on this build')
}

# ---------------------------------------------------------------------------
Write-Head "SVT-AV1 parameter support"

# This has to be probed by reading ffmpeg's log, not by exit code and not from
# help text. Verified against SVT-AV1 4.2: an unknown -svtav1-params key is
# reported as "Error parsing option <key>" at BELOW error level, and the encode
# then succeeds with exit code 0. A probe that trusts the exit code therefore
# reports every key as supported -- including keys the library silently threw
# away. And ffmpeg's own help text never lists these keys at all, because they
# belong to the SVT library rather than to ffmpeg.
function Test-SvtParam {
    param([string]$Pair)
    $key = ($Pair -split '=', 2)[0]
    try {
        $probeArgs = @(
            '-hide_banner','-nostdin','-nostats','-loglevel','warning','-y',
            '-f','lavfi','-i','color=c=black:s=256x144:r=24:d=1',
            '-frames:v','4','-c:v','libsvtav1','-preset','12',
            '-svtav1-params', $Pair, '-f','null','-'
        )
        $psi = [System.Diagnostics.ProcessStartInfo]::new()
        $psi.FileName = $ffmpeg
        foreach ($a in $probeArgs) { $psi.ArgumentList.Add($a) }
        $psi.RedirectStandardError = $true; $psi.RedirectStandardOutput = $true; $psi.UseShellExecute = $false
        $p = [System.Diagnostics.Process]::Start($psi)
        $err = $p.StandardError.ReadToEnd(); $null = $p.StandardOutput.ReadToEnd()
        if (-not $p.WaitForExit(30000)) { try { $p.Kill($true) } catch {}; return $false }
        return -not ($err -match ('(?im)Error\s+parsing\s+option\s+' + [Regex]::Escape($key) + '\s*:'))
    } catch { return $false }
}

# The control comes first. If a key that must exist is reported missing, the
# probe itself is broken and nothing below it can be trusted.
$controlOk = Test-SvtParam 'tune=2'
$sentinelRejected = -not (Test-SvtParam 'this-key-does-not-exist=1')

if (-not $controlOk -or -not $sentinelRejected) {
    Write-Item 'parameter probe' 'INCONCLUSIVE' 'bad' 'the probe could not distinguish a real key from a fake one; treat the results below as unknown'
    $script:issues.Add('svtav1-params capability probe is not working on this build')
} else {
    Write-Item 'parameter probe' 'working' 'ok' 'a known key is accepted and a fake key is rejected'

    foreach ($pair in @('tune=2','keyint=240','enable-variance-boost=1','variance-boost-strength=2',
                        'scd=1','enable-overlays=1','qp-scale-compress-strength=2',
                        'mastering-display=G(0.265,0.690)B(0.150,0.060)R(0.680,0.320)WP(0.3127,0.3290)L(1000,0.0001)',
                        'content-light=1000,400','film-grain=8','film-grain-denoise=1')) {
        $key = ($pair -split '=', 2)[0]
        if (Test-SvtParam $pair) { Write-Item $key 'supported' 'ok' }
        else { Write-Item $key 'not supported' 'warn' 'this setting will be omitted rather than silently discarded' }
    }

    # HDR10+ inline is the one that matters for an HDR10+ display, and the one
    # the old help-text check could never detect.
    $svtHdr10Plus = (Test-SvtParam 'enable-hdr10plus=1') -and (Test-SvtParam 'hdr10plus-json=x.json')
    if ($svtHdr10Plus) {
        Write-Item 'hdr10plus-json (inline HDR10+)' 'supported' 'ok' 'this SVT-AV1 can carry HDR10+ during the encode'
    } else {
        Write-Item 'hdr10plus-json (inline HDR10+)' 'not supported' 'info' 'mainline SVT-AV1; HDR10+ needs hdr10plus_tool re-injection, or an svt-av1-hdr build'
    }
}

$encHelp = ''
try { $encHelp = (& $ffmpeg -hide_banner -h encoder=libsvtav1 2>&1 | Out-String) } catch { $encHelp = '' }
if ($encHelp -match '(?m)^\s+-dolbyvision\b') {
    Write-Item '-dolbyvision option' 'present' 'ok' 'defaults to auto; the script sets it to 0 when the target is HDR10'
} else {
    Write-Item '-dolbyvision option' 'absent' 'info' 'older libsvtav1; nothing to suppress'
}

# ---------------------------------------------------------------------------
Write-Head "Summary"

if ($script:issues.Count -eq 0 -and $script:warnings.Count -eq 0) {
    Write-Host "  Toolchain is fully capable. No issues found." -ForegroundColor Green
} else {
    if ($script:issues.Count -gt 0) {
        Write-Host "  Blocking issues:" -ForegroundColor Red
        foreach ($i in $script:issues) { Write-Host "    - $i" -ForegroundColor Red }
    }
    if ($script:warnings.Count -gt 0) {
        Write-Host "  Reduced capability (encoding still works):" -ForegroundColor Yellow
        foreach ($w in $script:warnings) { Write-Host "    - $w" -ForegroundColor Yellow }
    }
}
Write-Host ""
$script:LastToolExitCode = $(if ($script:issues.Count -gt 0) { 1 } else { 0 }); return

}

# =============================================================================
# Candidates
# Merged from Media2AV1Queue-Candidates.ps1. Body unchanged; see Build-Single.ps1.
# =============================================================================
function Invoke-CandidatesTool {
param(
    # Library roots. Accepts several, comma-separated, because "pwsh -File"
    # does not bind array arguments -- "-Path A,B" arrives as one token.
    [Parameter(Mandatory = $true)]
    [string[]]$Path,

    # How many candidates to print. The CSV always contains everything.
    [int]$Top = 40,

    # Payoff  = expected GiB returned per hour of encoding  (default)
    # Saved   = expected GiB returned, ignoring how long it takes
    # Waste   = most bloated first, by bits per pixel per frame
    # Size    = largest files first
    [ValidateSet('Payoff', 'Saved', 'Waste', 'Size')]
    [string]$SortBy = 'Payoff',

    # Drop candidates whose expected saving is below this, in GiB. Filters out
    # the long tail of files that are technically improvable and not worth the
    # queue slot.
    [double]$MinSavedGiB = 0.25,

    # Ignore files smaller than this. Trailers, extras and sample files.
    [double]$MinSourceGiB = 0.2,

    # Which lane the time estimate assumes. CPU is the conservative answer and
    # the one that matters for a big backlog; Nvidia is much faster per file.
    [ValidateSet('CPU', 'Nvidia')]
    [string]$Lane = 'CPU',

    # Full results as CSV.
    [string]$Csv = '',

    # Write the top N paths, one per line, ready to feed straight to the queue.
    [string]$OutList = '',

    # Parallel ffprobe workers. 0 = half the logical processors, minimum 4.
    [int]$ThrottleLimit = 0,

    # Include the scripts' own working folders in the scan.
    [switch]$IncludeWorkFolders,

    # Path to encode_log.csv, if it is not in the usual place.
    [string]$HistoryCsv = ''
)



$VideoExtensions = @('.mkv', '.mp4', '.m4v', '.ts', '.m2ts', '.avi', '.mov', '.wmv', '.webm', '.mpg', '.mpeg', '.vob')


function Write-SessionTextLogMessage { param($Level, $Message) }

# =============================================================================
# Tools
# =============================================================================
$FfprobePath = Join-Path $PSScriptRoot 'ffprobe.exe'
if (-not (Test-Path -LiteralPath $FfprobePath)) {
    $c = Get-Command ffprobe -ErrorAction SilentlyContinue
    if (-not $c) { throw 'ffprobe not found next to this script or on PATH.' }
    $FfprobePath = $c.Source
}

if ($ThrottleLimit -le 0) {
    $ThrottleLimit = [Math]::Max(4, [int]([Environment]::ProcessorCount / 2))
}

$normalisedPaths = New-Object System.Collections.Generic.List[string]
foreach ($rawPath in $Path) {
    if ([string]::IsNullOrWhiteSpace($rawPath)) { continue }
    foreach ($piece in ([string]$rawPath -split ',')) {
        $clean = $piece.Trim().Trim('"')
        if (-not [string]::IsNullOrWhiteSpace($clean)) { $normalisedPaths.Add($clean) }
    }
}

Write-Host ''
Write-Host '=============================================================' -ForegroundColor Cyan
Write-Host ' Media2AV1Queue - AV1 conversion candidates' -ForegroundColor Cyan
Write-Host '=============================================================' -ForegroundColor Cyan
Write-Host ("ffprobe      : {0}" -f $FfprobePath) -ForegroundColor DarkGray
Write-Host ("parallelism  : {0}" -f $ThrottleLimit) -ForegroundColor DarkGray
Write-Host ("time estimate: {0} lane" -f $Lane) -ForegroundColor DarkGray
Write-Host ''

# =============================================================================
# Learn from your own completed encodes
# =============================================================================
# Everything here is derived from encode_log.csv. Where there is no history the
# fallbacks below are used, and the output labels which is which -- a number
# learned from forty of your own encodes and a number I guessed should never
# look the same on screen.
if ([string]::IsNullOrWhiteSpace($HistoryCsv)) {
    $HistoryCsv = Join-Path $PSScriptRoot '.queue\encode_log.csv'
}

$script:HistorySavings = @{}   # "tier|codecClass|bppTier" -> @{ Median; Count }
$script:HistorySpeed   = @{}   # "tier|lane"              -> @{ Median; Count }
$script:HistoryRows    = 0

function Get-MedianOf {
    param($Values)
    $sorted = @($Values | Sort-Object)
    if ($sorted.Count -eq 0) { return 0.0 }
    if ($sorted.Count % 2 -eq 1) { return [double]$sorted[[int][Math]::Floor($sorted.Count / 2)] }
    return ([double]$sorted[($sorted.Count / 2) - 1] + [double]$sorted[$sorted.Count / 2]) / 2.0
}

if (Test-Path -LiteralPath $HistoryCsv) {
    try {
        $history = @(Import-Csv -LiteralPath $HistoryCsv | Where-Object { $_.Status -eq 'SUCCESS' })
        $script:HistoryRows = $history.Count

        $savingsBuckets = @{}
        $speedBuckets   = @{}

        foreach ($row in $history) {
            $tier  = [string](Get-OptionalProperty -InputObject $row -PropertyName 'ResolutionTier' -Default '')
            $codec = [string](Get-OptionalProperty -InputObject $row -PropertyName 'CodecClass' -Default '')

            $bpp = 0.0
            [void][double]::TryParse([string](Get-OptionalProperty -InputObject $row -PropertyName 'BPP' -Default '0'),
                [System.Globalization.NumberStyles]::Float, [System.Globalization.CultureInfo]::InvariantCulture, [ref]$bpp)

            $reduction = 0.0
            $haveReduction = [double]::TryParse([string](Get-OptionalProperty -InputObject $row -PropertyName 'ReductionPercent' -Default ''),
                [System.Globalization.NumberStyles]::Float, [System.Globalization.CultureInfo]::InvariantCulture, [ref]$reduction)

            # A reduction outside 0-99 is not a data point about compression --
            # it is a row from a job that was refused, or one whose source was
            # replaced between runs. Including those would drag every estimate.
            if ($haveReduction -and $reduction -gt 0 -and $reduction -lt 99 -and $tier -and $codec) {
                $key = "{0}|{1}|{2}" -f $tier, $codec, (Get-BppTier -Bpp $bpp)
                if (-not $savingsBuckets.ContainsKey($key)) { $savingsBuckets[$key] = [System.Collections.Generic.List[double]]::new() }
                $savingsBuckets[$key].Add($reduction)
            }

            $srcDur = 0.0
            $elapsed = 0.0
            $haveDur = [double]::TryParse([string](Get-OptionalProperty -InputObject $row -PropertyName 'SourceDurationSec' -Default ''),
                [System.Globalization.NumberStyles]::Float, [System.Globalization.CultureInfo]::InvariantCulture, [ref]$srcDur)
            $haveElapsed = [double]::TryParse([string](Get-OptionalProperty -InputObject $row -PropertyName 'ElapsedSec' -Default ''),
                [System.Globalization.NumberStyles]::Float, [System.Globalization.CultureInfo]::InvariantCulture, [ref]$elapsed)
            $lane = [string](Get-OptionalProperty -InputObject $row -PropertyName 'ResolvedEncodeLane' -Default '')

            # Speed is kept as a multiple of realtime rather than as pixels per
            # second: the log records duration and elapsed time, not dimensions,
            # and "this machine encodes UHD at 0.15x realtime" is both directly
            # measurable here and directly useful.
            if ($haveDur -and $haveElapsed -and $srcDur -gt 0 -and $elapsed -gt 0 -and $tier -and $lane) {
                $key = "{0}|{1}" -f $tier, $lane
                if (-not $speedBuckets.ContainsKey($key)) { $speedBuckets[$key] = [System.Collections.Generic.List[double]]::new() }
                $speedBuckets[$key].Add($srcDur / $elapsed)
            }
        }

        foreach ($key in $savingsBuckets.Keys) {
            $script:HistorySavings[$key] = @{ Median = (Get-MedianOf -Values $savingsBuckets[$key]); Count = $savingsBuckets[$key].Count }
        }
        foreach ($key in $speedBuckets.Keys) {
            $script:HistorySpeed[$key] = @{ Median = (Get-MedianOf -Values $speedBuckets[$key]); Count = $speedBuckets[$key].Count }
        }

        Write-Host ("History      : {0} successful encode(s) in {1}" -f $script:HistoryRows, (Split-Path -Leaf $HistoryCsv)) -ForegroundColor DarkCyan
        if ($script:HistorySavings.Count -gt 0) {
            Write-Host ("               savings learned for {0} resolution/codec/bitrate group(s)" -f $script:HistorySavings.Count) -ForegroundColor DarkGray
        }
        if ($script:HistorySpeed.Count -gt 0) {
            $speedSummary = @($script:HistorySpeed.GetEnumerator() | Sort-Object Name | ForEach-Object {
                "{0} {1:F2}x realtime (n={2})" -f $_.Key, $_.Value.Median, $_.Value.Count
            })
            Write-Host ("               speed learned: {0}" -f ($speedSummary -join ' | ')) -ForegroundColor DarkGray
        }
    } catch {
        Write-Warning ("Could not read encode history ({0}); falling back to built-in estimates." -f $_.Exception.Message)
    }
} else {
    Write-Host "History      : none yet -- using built-in estimates, which are priors, not measurements." -ForegroundColor Yellow
    Write-Host "               Re-run this after a few dozen encodes and the numbers become yours." -ForegroundColor DarkGray
}
Write-Host ''

# --- fallbacks, used only where history is silent ---------------------------
# Deliberately conservative. Over-promising here sends you off to encode a
# hundred files for a saving that never arrives.
function Get-FallbackReductionPercent {
    param([string]$CodecClass, [string]$BppTier, [string]$ResolutionTier)

    $base = switch ($CodecClass) {
        'legacy'   { 62.0 }   # MPEG-2, VC-1: enormous headroom
        'standard' { 50.0 }   # H.264: the common case
        'modern'   { 30.0 }   # HEVC/VP9: already good, less to win
        default    { 45.0 }
    }
    $base += switch ($BppTier) {
        'high'    {  10.0 }   # lots of bits per pixel: room to give back
        'medium'  {   0.0 }
        'low'     { -18.0 }   # already lean
        default   {  -5.0 }
    }
    if ($ResolutionTier -eq 'SD') { $base -= 8.0 }
    return [Math]::Max(5.0, [Math]::Min(85.0, $base))
}

function Get-FallbackSpeedFactor {
    param([string]$ResolutionTier, [string]$LaneName)

    # Multiples of realtime. Order-of-magnitude figures for a modern many-core
    # desktop at the presets Auto tends to choose; the whole point of the
    # history model above is to replace these with your machine's real numbers.
    $cpu = switch ($ResolutionTier) {
        'UHD'   { 0.12 }
        'HD'    { 0.45 }
        default { 1.10 }
    }
    if ($LaneName -eq 'Nvidia') { return $cpu * 8.0 }
    return $cpu
}

function Resolve-ExpectedReduction {
    param([string]$ResolutionTier, [string]$CodecClass, [string]$BppTier)

    $key = "{0}|{1}|{2}" -f $ResolutionTier, $CodecClass, $BppTier
    if ($script:HistorySavings.ContainsKey($key) -and $script:HistorySavings[$key].Count -ge 3) {
        return [pscustomobject]@{
            Percent = [double]$script:HistorySavings[$key].Median
            Source  = 'history'
            Count   = [int]$script:HistorySavings[$key].Count
        }
    }
    return [pscustomobject]@{
        Percent = (Get-FallbackReductionPercent -CodecClass $CodecClass -BppTier $BppTier -ResolutionTier $ResolutionTier)
        Source  = 'estimate'
        Count   = 0
    }
}

function Resolve-SpeedFactor {
    param([string]$ResolutionTier, [string]$LaneName)

    $key = "{0}|{1}" -f $ResolutionTier, $LaneName
    if ($script:HistorySpeed.ContainsKey($key) -and $script:HistorySpeed[$key].Count -ge 3) {
        return [pscustomobject]@{
            Factor = [double]$script:HistorySpeed[$key].Median
            Source = 'history'
            Count  = [int]$script:HistorySpeed[$key].Count
        }
    }
    return [pscustomobject]@{
        Factor = (Get-FallbackSpeedFactor -ResolutionTier $ResolutionTier -LaneName $LaneName)
        Source = 'estimate'
        Count  = 0
    }
}

# =============================================================================
# How much of a file is audio
# =============================================================================
# This decides whether a bloated-looking file is actually worth queueing. Audio
# and subtitles are stream-copied, so they cannot shrink: a 60 GiB remux that is
# 22 GiB of TrueHD can only ever give back part of the remaining 38.
#
# Get-StreamEstimatedSizeBytes is tried first, because it uses real metadata --
# mkvmerge writes BPS and NUMBER_OF_BYTES tags, and most Blu-ray remuxes have
# them. But plenty of files, including anything remuxed with plain ffmpeg, carry
# no per-stream bitrate at all: Matroska does not require one. On those,
# Get-StreamEstimatedSizeBytes returns 0, and treating 0 as "no audio" would
# quietly assume the file is 100% video and over-promise the saving on exactly
# the files where audio matters most.
#
# So there is a model underneath it, and the caller is told which one answered.
function Get-AudioBytesEstimate {
    param($Stream, [double]$DurationSec)

    $tagged = Get-StreamEstimatedSizeBytes -Stream $Stream -DurationSec $DurationSec
    if ($tagged -gt 0) {
        return [pscustomobject]@{ Bytes = [double]$tagged; Source = 'tags' }
    }
    if ($DurationSec -le 0) {
        return [pscustomobject]@{ Bytes = 0.0; Source = 'none' }
    }

    $codec    = ([string](Get-StreamProp $Stream 'codec_name' '')).ToLowerInvariant()
    $channels = [int](Get-StreamProp $Stream 'channels' 0)
    if ($channels -le 0) { $channels = 2 }
    $sampleRate = [int](Convert-ToInvariantInt64 (Get-StreamProp $Stream 'sample_rate' 0) 48000)
    if ($sampleRate -le 0) { $sampleRate = 48000 }
    # Clamped to 24: real lossless film audio is 16- or 24-bit, and a header
    # claiming 32 (or nothing at all) would inflate the whole estimate.
    $bitsPerSample = [int](Convert-ToInvariantInt64 (Get-StreamProp $Stream 'bits_per_raw_sample' 0) 0)
    if ($bitsPerSample -le 0 -or $bitsPerSample -gt 24) { $bitsPerSample = 24 }

    # Uncompressed PCM is arithmetic, not estimation.
    $pcmBitrate = [double]$channels * $sampleRate * $bitsPerSample

    $bitrate = switch -Regex ($codec) {
        '^pcm'                { $pcmBitrate }
        '^(truehd|mlp)$'      { $pcmBitrate * 0.62 }   # lossless, typical packing ratio
        '^flac$'              { $pcmBitrate * 0.58 }
        '^(dts)$'             { 1509000.0 }            # core DTS; DTS-HD MA is usually tagged
        '^(eac3|ec-3)$'       {  768000.0 }
        '^(ac3)$'             {  640000.0 }
        '^(aac|opus|vorbis)$' { [double]$channels * 64000.0 }
        default               { [double]$channels * 128000.0 }
    }

    return [pscustomobject]@{
        Bytes  = ($bitrate * $DurationSec) / 8.0
        Source = 'modelled'
    }
}

# =============================================================================
# Enumerate
# =============================================================================
$workFolderNames = @('.queue', '.verify', '_parked', '_to_delete')

function Test-InWorkFolder {
    param([string]$FullPath)
    foreach ($seg in ($FullPath -split '[\\/]')) {
        if ($workFolderNames -contains $seg.ToLowerInvariant()) { return $true }
    }
    if ([System.IO.Path]::GetFileName($FullPath) -match '(?i)\.encoding\.tmp\.') { return $true }
    return $false
}

Write-Host 'Enumerating files...' -ForegroundColor DarkCyan
$files = [System.Collections.Generic.List[string]]::new()
foreach ($root in $normalisedPaths) {
    if (-not (Test-Path -LiteralPath $root)) {
        Write-Warning "Path not found, skipping: $root"
        continue
    }
    $item = Get-Item -LiteralPath $root
    if ($item.PSIsContainer) {
        Get-ChildItem -LiteralPath $item.FullName -File -Recurse -ErrorAction SilentlyContinue |
            Where-Object { $VideoExtensions -contains $_.Extension.ToLowerInvariant() } |
            Where-Object { $IncludeWorkFolders -or -not (Test-InWorkFolder -FullPath $_.FullName) } |
            ForEach-Object { $files.Add($_.FullName) }
    } elseif ($VideoExtensions -contains $item.Extension.ToLowerInvariant()) {
        $files.Add($item.FullName)
    }
}

if ($files.Count -eq 0) {
    Write-Warning 'No video files found under the given path(s).'
    $script:LastToolExitCode = 0; return
}
Write-Host ("  {0} video file(s) found" -f $files.Count) -ForegroundColor Gray
Write-Host ''

# =============================================================================
# Probe (header only, in parallel)
# =============================================================================
# -show_format -show_streams reads the container header and the codec
# parameters. It does not decode a single frame, which is what keeps this to
# roughly 50ms per file on local storage. Network shares are slower, but the
# cost is metadata latency rather than throughput, so parallelism helps a lot.
$progress = [System.Collections.Concurrent.ConcurrentDictionary[string,int]]::new()
$null = $progress.TryAdd('done', 0)
$total = $files.Count

Write-Host 'Reading container headers...' -ForegroundColor DarkCyan
$sw = [System.Diagnostics.Stopwatch]::StartNew()

$probeResults = $files | ForEach-Object -ThrottleLimit $ThrottleLimit -Parallel {
    $ffprobe = $using:FfprobePath
    $counter = $using:progress
    $total   = $using:total
    $path    = $_

    $done = $counter.AddOrUpdate('done', 1, { param($k, $v) $v + 1 })
    if (($done % 100) -eq 0 -or $done -eq $total) {
        Write-Host ("  {0} / {1}" -f $done, $total) -ForegroundColor DarkGray
    }

    try {
        $json = & $ffprobe -v error -print_format json -show_format -show_streams $path 2>$null
        if (-not $json) { return [pscustomobject]@{ Path = $path; Json = ''; Error = 'ffprobe returned nothing' } }
        return [pscustomobject]@{ Path = $path; Json = ($json -join "`n"); Error = '' }
    } catch {
        return [pscustomobject]@{ Path = $path; Json = ''; Error = $_.Exception.Message }
    }
}

$sw.Stop()
Write-Host ("  probed {0} file(s) in {1:F1}s ({2:F0} ms each)" -f $total, $sw.Elapsed.TotalSeconds, (($sw.Elapsed.TotalMilliseconds) / [Math]::Max(1, $total))) -ForegroundColor Gray
Write-Host ''

# =============================================================================
# Analyse
# =============================================================================
Write-Host 'Scoring...' -ForegroundColor DarkCyan

$rows      = New-Object System.Collections.Generic.List[object]
$excluded  = @{ AlreadyAv1 = 0; NoVideo = 0; NoAudio = 0; TooSmall = 0; ProbeFailed = 0; AutoSkip = 0; BelowThreshold = 0; DolbyVisionP5 = 0 }

foreach ($probeResult in $probeResults) {
    if ([string]::IsNullOrWhiteSpace($probeResult.Json)) { $excluded.ProbeFailed++; continue }

    try   { $probe = $probeResult.Json | ConvertFrom-Json -Depth 100 }
    catch { $excluded.ProbeFailed++; continue }

    # The encoder's own stream selection, not a second implementation of it.
    # It knows to skip cover-art video streams and which audio tracks would
    # actually be kept, and both of those change the numbers below.
    #
    # It throws on a file with no usable audio, and that is worth reporting
    # separately rather than lumping in with "no video": the encoder maps an
    # audio stream unconditionally, so a video-only file cannot go through this
    # pipeline at all. Counting them here is how you find out you have any.
    $selected = $null
    try {
        $selected = Select-Streams -Probe $probe
    } catch {
        if ($_.Exception.Message -match '(?i)audio') { $excluded.NoAudio++ } else { $excluded.NoVideo++ }
        continue
    }

    $video = $selected.Video
    if ($null -eq $video) { $excluded.NoVideo++; continue }

    $keptAudio = @(@($selected.MainAudio, $selected.FallbackAudio) | Where-Object { $_ })

    $codecName = ([string](Get-StreamProp $video 'codec_name' '')).ToLowerInvariant()
    if ($codecName -eq 'av1') { $excluded.AlreadyAv1++; continue }

    $sourceItem = Get-Item -LiteralPath $probeResult.Path -ErrorAction SilentlyContinue
    if ($null -eq $sourceItem) { $excluded.ProbeFailed++; continue }
    $sourceGiB = $sourceItem.Length / 1GB
    if ($sourceGiB -lt $MinSourceGiB) { $excluded.TooSmall++; continue }

    $format = Get-OptionalProperty -InputObject $probe -PropertyName 'format' -Default ([pscustomobject]@{})
    $durationSec = Convert-ToInvariantDouble (Get-OptionalProperty $format 'duration' 0) 0.0
    if ($durationSec -le 0) { $excluded.NoVideo++; continue }

    $sourceProfile = Get-SourceProfile -Probe $probe -VideoStream $video -InputPath $probeResult.Path

    # Profile 5 has no HDR10-compatible base layer, so the queue refuses it by
    # design. Recommending it would be recommending a guaranteed skip.
    $dvPlan = Resolve-DolbyVisionSourcePlan -Probe $probe -VideoStream $video
    if ($dvPlan.IsDolbyVision -and -not $dvPlan.CanConvert) { $excluded.DolbyVisionP5++; continue }

    # ConfiguredFilmGrain is pinned to 0 rather than left on Auto purely to
    # suppress the grain pre-scan, which decodes frames and would turn a
    # four-minute pass into an overnight one. Everything else is exactly what
    # the encoder would resolve, including the Auto-skip verdict below.
    # KeptAudioStreams matters: Get-EffectiveVideoBitrate subtracts them from
    # the container total when no per-stream video bitrate is published, which
    # is the common case in Matroska. Leaving it empty credits the audio's bits
    # to the video and inflates BPP, which then picks the wrong CRF tier.
    $auto = Get-AutoEncodeSettings `
        -Probe $probe `
        -VideoStream $video `
        -SourceProfile $sourceProfile `
        -KeptAudioStreams $keptAudio `
        -InputPath $probeResult.Path `
        -ConfiguredCRF 'Auto' `
        -ConfiguredPreset 'Auto' `
        -ConfiguredFilmGrain 0 `
        -ConfiguredAutoCRFOffset 'Auto'

    if ($auto.Skip) { $excluded.AutoSkip++; continue }

    $width      = [int](Get-StreamProp $video 'width' 0)
    $height     = [int](Get-StreamProp $video 'height' 0)
    $tier       = [string]$auto.ResolutionTier
    $codecClass = [string]$auto.CodecClass
    $bpp        = [double]$auto.BPP
    $bppTier    = [string]$auto.BPPTier
    $videoRate  = [double]$auto.VideoBitrate

    # Video's share of the file, taken as "everything that is not copied".
    # Derived from the copied streams rather than from the video bitrate,
    # because the video bitrate is itself often inferred from the container
    # total and would make this circular.
    $copiedBytes = 0.0
    $copiedSource = if ($keptAudio.Count -eq 0) { 'no audio' } else { 'tags' }
    foreach ($audioStream in $keptAudio) {
        $estimate = Get-AudioBytesEstimate -Stream $audioStream -DurationSec $durationSec
        $copiedBytes += [double]$estimate.Bytes
        if ($estimate.Source -ne 'tags') { $copiedSource = $estimate.Source }
    }
    foreach ($subStream in @(@($selected.MainSub, $selected.SdhSub) | Where-Object { $_ })) {
        $copiedBytes += [double](Get-StreamEstimatedSizeBytes -Stream $subStream -DurationSec $durationSec)
    }

    # A modelled figure is a guess about how well this particular audio
    # compressed, and a guess that swallows most of the file is wrong however
    # plausible its arithmetic looked. Tag-derived sizes are real measurements
    # and are left alone; modelled ones are bounded so they can demote a file
    # but never erase it. Rows that hit the bound say so.
    $videoFloorShare = 0.25
    if ($copiedSource -eq 'modelled' -and $copiedBytes -gt ($sourceItem.Length * (1.0 - $videoFloorShare))) {
        $copiedBytes = [double]$sourceItem.Length * (1.0 - $videoFloorShare)
        $copiedSource = 'modelled (bounded)'
    }

    $videoBytes = [double]$sourceItem.Length - $copiedBytes
    if ($videoBytes -le 0) { $videoBytes = [double]$sourceItem.Length * $videoFloorShare }
    if ($videoBytes -gt $sourceItem.Length) { $videoBytes = [double]$sourceItem.Length }
    $videoShare = $videoBytes / [double]$sourceItem.Length

    $reduction = Resolve-ExpectedReduction -ResolutionTier $tier -CodecClass $codecClass -BppTier $bppTier
    $speed     = Resolve-SpeedFactor -ResolutionTier $tier -LaneName $Lane

    # Capped at the video portion: the whole-file reduction learned from history
    # already reflects some average audio share, and applying it to a file whose
    # audio is half the bytes would promise a saving the encoder cannot deliver.
    $savedGiB = [Math]::Min(
        $sourceGiB * ($reduction.Percent / 100.0),
        ($videoBytes / 1GB) * 0.92
    )

    $encodeHours = if ($speed.Factor -gt 0) { ($durationSec / 3600.0) / $speed.Factor } else { 0.0 }
    $payoff = if ($encodeHours -gt 0) { $savedGiB / $encodeHours } else { 0.0 }

    if ($savedGiB -lt $MinSavedGiB) { $excluded.BelowThreshold++; continue }

    $rows.Add([pscustomobject][ordered]@{
        Path              = $probeResult.Path
        Name              = [System.IO.Path]::GetFileName($probeResult.Path)
        SourceGiB         = [Math]::Round($sourceGiB, 3)
        DurationMin       = [Math]::Round($durationSec / 60.0, 1)
        Codec             = (Get-CodecLabel -Stream $video)
        CodecClass        = $codecClass
        Resolution        = ("{0}x{1}" -f $width, $height)
        ResolutionTier    = $tier
        Profile           = [string]$sourceProfile.Profile
        BPP               = [Math]::Round($bpp, 4)
        BPPTier           = $bppTier
        SourceGiBPerHour  = [Math]::Round([double]$auto.VideoBitratePerHourGiB, 2)
        VideoSharePct     = [Math]::Round($videoShare * 100.0, 1)
        AudioSizeSource   = $copiedSource
        AutoCRF           = [int]$auto.CRF
        ExpectedSavingPct = [Math]::Round($reduction.Percent, 1)
        SavingSource      = ("{0}{1}" -f $reduction.Source, $(if ($reduction.Count -gt 0) { " n=$($reduction.Count)" } else { '' }))
        ExpectedSavedGiB  = [Math]::Round($savedGiB, 3)
        EncodeHours       = [Math]::Round($encodeHours, 2)
        SpeedSource       = ("{0}{1}" -f $speed.Source, $(if ($speed.Count -gt 0) { " n=$($speed.Count)" } else { '' }))
        PayoffGiBPerHour  = [Math]::Round($payoff, 3)
    })
}

if ($rows.Count -eq 0) {
    Write-Host ''
    Write-Warning 'No candidates passed the filters. Nothing in this library looks worth converting on these thresholds.'
    Write-Host ("Excluded: already AV1 {0} | auto-skip (already efficient) {1} | below saving threshold {2} | too small {3} | DV Profile 5 {4} | no video {5} | no audio {6} | probe failed {7}" -f `
        $excluded.AlreadyAv1, $excluded.AutoSkip, $excluded.BelowThreshold, $excluded.TooSmall, $excluded.DolbyVisionP5, $excluded.NoVideo, $excluded.NoAudio, $excluded.ProbeFailed) -ForegroundColor DarkGray
    $script:LastToolExitCode = 0; return
}

$sorted = switch ($SortBy) {
    'Saved' { @($rows | Sort-Object -Property ExpectedSavedGiB -Descending) }
    'Waste' { @($rows | Sort-Object -Property BPP -Descending) }
    'Size'  { @($rows | Sort-Object -Property SourceGiB -Descending) }
    default { @($rows | Sort-Object -Property PayoffGiBPerHour -Descending) }
}

# =============================================================================
# Report
# =============================================================================
$totalSaved = ($rows | Measure-Object -Property ExpectedSavedGiB -Sum).Sum
$totalHours = ($rows | Measure-Object -Property EncodeHours -Sum).Sum
$topN = @($sorted | Select-Object -First $Top)
$topSaved = ($topN | Measure-Object -Property ExpectedSavedGiB -Sum).Sum
$topHours = ($topN | Measure-Object -Property EncodeHours -Sum).Sum

Write-Host ''
Write-Host '--- Candidates ----------------------------------------------' -ForegroundColor Cyan
Write-Host ("sorted by {0}; showing {1} of {2}" -f $SortBy, $topN.Count, $rows.Count) -ForegroundColor DarkGray
Write-Host ''
Write-Host ('{0,-44} {1,8} {2,7} {3,6} {4,8} {5,8} {6,8} {7,8}' -f 'File', 'Codec', 'Tier', 'BPP', 'Size', 'Saves', 'Hours', 'GiB/hr') -ForegroundColor DarkGray
Write-Host ('{0,-44} {1,8} {2,7} {3,6} {4,8} {5,8} {6,8} {7,8}' -f ('-' * 44), '--------', '-------', '------', '--------', '--------', '--------', '--------') -ForegroundColor DarkGray

foreach ($row in $topN) {
    $display = if ($row.Name.Length -gt 44) { $row.Name.Substring(0, 41) + '...' } else { $row.Name }
    Write-Host ('{0,-44} {1,8} {2,7} {3,6:F3} {4,8:F2} {5,8:F2} {6,8:F2} {7,8:F2}' -f `
        $display, $row.Codec, $row.ResolutionTier, $row.BPP, $row.SourceGiB, $row.ExpectedSavedGiB, $row.EncodeHours, $row.PayoffGiBPerHour)
}

Write-Host ''
Write-Host '--- Totals --------------------------------------------------' -ForegroundColor Cyan
Write-Host ("Top {0,-6}          {1,8:F1} GiB back for {2,7:F1} encode-hours" -f $topN.Count, $topSaved, $topHours)
Write-Host ("All {0,-6}          {1,8:F1} GiB back for {2,7:F1} encode-hours" -f $rows.Count, $totalSaved, $totalHours)
if ($totalHours -gt 0 -and $topHours -gt 0) {
    $topRate = $topSaved / $topHours
    $allRate = $totalSaved / $totalHours
    Write-Host ("The top {0} return {1:F2} GiB per encode-hour against {2:F2} for the whole list." -f $topN.Count, $topRate, $allRate) -ForegroundColor DarkGray
}

Write-Host ''
Write-Host '--- Excluded ------------------------------------------------' -ForegroundColor Cyan
Write-Host ("already AV1                  {0,6}" -f $excluded.AlreadyAv1)
Write-Host ("already efficient (auto-skip){0,6}" -f $excluded.AutoSkip)
Write-Host ("saving below {0,5:F2} GiB       {1,6}" -f $MinSavedGiB, $excluded.BelowThreshold)
Write-Host ("smaller than {0,5:F2} GiB       {1,6}" -f $MinSourceGiB, $excluded.TooSmall)
Write-Host ("Dolby Vision Profile 5       {0,6}" -f $excluded.DolbyVisionP5)
Write-Host ("no usable video stream       {0,6}" -f $excluded.NoVideo)
Write-Host ("no audio track (unencodable) {0,6}" -f $excluded.NoAudio)
Write-Host ("probe failed                 {0,6}" -f $excluded.ProbeFailed)

$estimatedRows = @($rows | Where-Object { $_.SavingSource -like 'estimate*' }).Count
Write-Host ''
if ($estimatedRows -eq $rows.Count) {
    Write-Host 'Every saving figure above is a prior, not a measurement.' -ForegroundColor Yellow
    Write-Host 'Convert a few dozen files, then re-run: the estimates become yours.' -ForegroundColor DarkGray
} elseif ($estimatedRows -gt 0) {
    Write-Host ("{0} of {1} rows used a fallback estimate; the rest came from your own encode history." -f $estimatedRows, $rows.Count) -ForegroundColor DarkGray
} else {
    Write-Host 'All saving figures came from your own encode history.' -ForegroundColor Green
}
Write-Host 'These are predictions. The preflight and quality search still decide per file,' -ForegroundColor DarkGray
Write-Host 'and can still refuse a file this list likes -- that is them working correctly.' -ForegroundColor DarkGray

if (-not [string]::IsNullOrWhiteSpace($Csv)) {
    $sorted | Export-Csv -LiteralPath $Csv -NoTypeInformation -Encoding UTF8
    Write-Host ''
    Write-Host ("Full results: {0}" -f $Csv) -ForegroundColor Green
}

if (-not [string]::IsNullOrWhiteSpace($OutList)) {
    $topN | ForEach-Object { $_.Path } | Set-Content -LiteralPath $OutList -Encoding UTF8
    Write-Host ''
    Write-Host ("Queue list ({0} paths): {1}" -f $topN.Count, $OutList) -ForegroundColor Green
    Write-Host 'Review it, then queue the lot with:' -ForegroundColor DarkGray
    Write-Host ("  & .\Media2AV1Queue.ps1 @(Get-Content '{0}')" -f $OutList) -ForegroundColor White
}

Write-Host ''

}

# =============================================================================
# Loudness
# Merged from Media2AV1Queue-Loudness.ps1. Body unchanged; see Build-Single.ps1.
# =============================================================================
function Invoke-LoudnessTool {
param(
    # Files or folders. Comma-separated works, because "pwsh -File" does not
    # bind array arguments.
    [Parameter(Mandatory = $true)]
    [string[]]$Path,

    # Which film standard to master to. See the header for what each one is.
    # Custom uses -TargetLufs / -DialogueTargetLufs / -TruePeak directly.
    [ValidateSet('HomeTheatre', 'EBU-R128', 'ATSC-A85', 'Netflix', 'Custom')]
    [string]$Standard = 'HomeTheatre',

    # Used when -Standard is Custom, or to override one value of a preset.
    [double]$TargetLufs = 0.0,
    [double]$DialogueTargetLufs = 0.0,
    [double]$TruePeak = 0.0,

    # What to anchor the gain on.
    #   Auto       centre channel when the source has one, programme otherwise
    #   Dialogue   force the centre channel; falls back where there is none
    #   Integrated force programme loudness, the old behaviour
    [ValidateSet('Auto', 'Dialogue', 'Integrated')]
    [string]$Anchor = 'Auto',

    # How far from target a file may sit and still be left completely alone.
    # This is the "listenable, not identical" dial: widen it to touch fewer
    # files, narrow it to even them out more aggressively.
    [double]$Tolerance = 2.0,

    # Extra gain applied to the centre channel relative to the rest of the mix,
    # in dB. This is the only thing that helps a film whose dialogue is buried
    # under its own effects, because no global gain can close that gap. It
    # DOES change the mix, so it is off by default.
    [double]$CentreBoostDb = 0.0,

    # Without this, the script only measures and reports. Nothing is written.
    [switch]$Apply,

    # Linear   = one static gain, dynamics untouched (default)
    # Compress = two-pass loudnorm, reduces loudness range for night listening
    [ValidateSet('Linear', 'Compress')]
    [string]$Mode = 'Linear',

    # Add     = keep the original track, add a normalised one, make it default
    # Replace = replace the audio track outright
    [ValidateSet('Add', 'Replace')]
    [string]$TrackMode = 'Add',

    # Loudness range target, used by -Mode Compress only. 11 LU is a reasonable
    # night-listening figure; broadcast uses 7.
    [double]$CompressLra = 11.0,

    # Sampled measurement: seeks to a handful of places instead of decoding the
    # whole file. Roughly an order of magnitude faster on a feature, and within
    # about 1 LU in testing. Triage only -- refused with -Apply, because a
    # sampled pass can miss the true peak and a missed peak means clipping.
    [switch]$Quick,
    [int]$SampleCount = 6,
    [int]$SampleDurationSec = 30,

    # Where normalised files go. Default is beside the source, suffixed.
    [string]$OutputDir = '',

    # Overwrite the source file. Off by default and deliberately awkward.
    #
    # Named -OverwriteSource, not -ReplaceOriginal, because the main script's
    # settings block defines $ReplaceOriginal = $true, and that block is
    # dot-sourced into this scope -- the name collision silently turned this on.
    [switch]$OverwriteSource,

    # Codec for the normalised track. Auto picks FLAC for lossless sources and
    # for anything above 5.1, E-AC3 otherwise.
    [ValidateSet('Auto', 'eac3', 'ac3', 'flac', 'opus', 'aac')]
    [string]$AudioCodec = 'Auto',

    [string]$Csv = '',
    [string]$OutList = '',
    [int]$ThrottleLimit = 0,
    [switch]$IncludeWorkFolders
)



$VideoExtensions = @('.mkv', '.mp4', '.m4v', '.ts', '.m2ts', '.avi', '.mov', '.wmv', '.webm', '.mpg', '.mpeg', '.vob')

if ($Quick -and $Apply) {
    throw "-Quick cannot be combined with -Apply. A sampled measurement can miss the true peak, and applying a gain computed from a missed peak clips the output. Scan with -Quick, then apply without it."
}


function Write-SessionTextLogMessage { param($Level, $Message) }

# =============================================================================
# Resolve the standard
# =============================================================================
# Every preset carries a programme target AND a dialogue target, because a
# stereo file has no centre channel to anchor on and has to fall back to
# programme loudness whatever the standard nominally prefers.
$standardTable = @{
    'EBU-R128'    = @{ Programme = -23.0; Dialogue = -26.0; Peak = -1.0; Note = 'European broadcast (EBU R128)' }
    'ATSC-A85'    = @{ Programme = -24.0; Dialogue = -27.0; Peak = -2.0; Note = 'US broadcast (ATSC A/85)' }
    'Netflix'     = @{ Programme = -24.0; Dialogue = -27.0; Peak = -2.0; Note = 'streaming delivery, dialogue-anchored' }
    'HomeTheatre' = @{ Programme = -20.0; Dialogue = -24.0; Peak = -1.5; Note = 'broadcast practice nudged up for a living room' }
    'Custom'      = @{ Programme = -20.0; Dialogue = -24.0; Peak = -1.5; Note = 'your numbers' }
}

$preset = $standardTable[$Standard]
# An explicitly supplied value wins over the preset. 0.0 is the "not supplied"
# sentinel, which is safe here: no real loudness target is 0 LUFS.
$programmeTarget = if ($TargetLufs -ne 0.0)         { $TargetLufs }         else { [double]$preset.Programme }
$dialogueTarget  = if ($DialogueTargetLufs -ne 0.0) { $DialogueTargetLufs } else { [double]$preset.Dialogue }
$peakCeiling     = if ($TruePeak -ne 0.0)           { $TruePeak }           else { [double]$preset.Peak }

if ($Standard -eq 'Custom' -and $TargetLufs -eq 0.0 -and $DialogueTargetLufs -eq 0.0) {
    throw '-Standard Custom needs at least one of -TargetLufs or -DialogueTargetLufs.'
}

# =============================================================================
# Tools
# =============================================================================
$FfmpegPath  = Join-Path $PSScriptRoot 'ffmpeg.exe'
$FfprobePath = Join-Path $PSScriptRoot 'ffprobe.exe'
if (-not (Test-Path -LiteralPath $FfmpegPath)) {
    $c = Get-Command ffmpeg -ErrorAction SilentlyContinue
    if (-not $c) { throw 'ffmpeg not found next to this script or on PATH.' }
    $FfmpegPath = $c.Source
}
if (-not (Test-Path -LiteralPath $FfprobePath)) {
    $c = Get-Command ffprobe -ErrorAction SilentlyContinue
    if (-not $c) { throw 'ffprobe not found next to this script or on PATH.' }
    $FfprobePath = $c.Source
}

if ($ThrottleLimit -le 0) { $ThrottleLimit = [Math]::Max(2, [int]([Environment]::ProcessorCount / 2)) }

$normalisedPaths = New-Object System.Collections.Generic.List[string]
foreach ($rawPath in $Path) {
    if ([string]::IsNullOrWhiteSpace($rawPath)) { continue }
    foreach ($piece in ([string]$rawPath -split ',')) {
        $clean = $piece.Trim().Trim('"')
        if (-not [string]::IsNullOrWhiteSpace($clean)) { $normalisedPaths.Add($clean) }
    }
}

# =============================================================================
# Measurement
# =============================================================================
# ebur128 rather than loudnorm for the measuring pass: it reports the same
# integrated loudness, loudness range and true peak, and measured here it ran
# about six times faster (189x realtime against 25x) because it is only
# analysing rather than also preparing a normalisation curve.
function ConvertFrom-Ebur128Output {
    param([string]$Text)

    if ([string]::IsNullOrWhiteSpace($Text)) { return $null }

    $num = '-?[0-9]+(?:\.[0-9]+)?|-?inf'
    $iMatch    = [Regex]::Match($Text, "(?im)^\s*I:\s*($num)\s*LUFS")
    $lraMatch  = [Regex]::Match($Text, "(?im)^\s*LRA:\s*($num)\s*LU")
    $peakMatch = [Regex]::Match($Text, "(?im)^\s*Peak:\s*($num)\s*dBFS")
    # (?ims): without the singleline flag the .*? cannot cross the newline
    # between the heading and its Threshold line, and this silently fell back
    # to the -70 default on every file.
    $thrMatch  = [Regex]::Match($Text, "(?ims)Integrated loudness:.*?Threshold:\s*($num)\s*LUFS")

    if (-not $iMatch.Success) { return $null }

    $parse = {
        param([string]$Raw)
        if ($Raw -match '(?i)^-?inf$') { return [double]::NegativeInfinity }
        $v = 0.0
        if ([double]::TryParse($Raw, [System.Globalization.NumberStyles]::Float,
                               [System.Globalization.CultureInfo]::InvariantCulture, [ref]$v)) { return $v }
        return $null
    }

    $integrated = & $parse $iMatch.Groups[1].Value
    if ($null -eq $integrated) { return $null }

    return [pscustomobject][ordered]@{
        Integrated = $integrated
        Lra        = if ($lraMatch.Success)  { & $parse $lraMatch.Groups[1].Value }  else { 0.0 }
        TruePeak   = if ($peakMatch.Success) { & $parse $peakMatch.Groups[1].Value } else { 0.0 }
        Threshold  = if ($thrMatch.Success)  { & $parse $thrMatch.Groups[1].Value }  else { -70.0 }
        # A silent or near-silent track measures as -inf and must never be
        # gained: the required gain is infinite and the result is amplified
        # noise floor.
        IsSilent   = ([double]::IsNegativeInfinity($integrated) -or $integrated -lt -60.0)
    }
}

# =============================================================================
# Enumerate
# =============================================================================
$workFolderNames = @('.queue', '.verify', '_parked', '_to_delete', '.loudness')

function Test-InWorkFolder {
    param([string]$FullPath)
    foreach ($seg in ($FullPath -split '[\\/]')) {
        if ($workFolderNames -contains $seg.ToLowerInvariant()) { return $true }
    }
    $leaf = [System.IO.Path]::GetFileName($FullPath)
    if ($leaf -match '(?i)\.encoding\.tmp\.') { return $true }
    # Don't re-normalise our own output.
    if ($leaf -match '(?i)\.normalized\.') { return $true }
    return $false
}

Write-Host ''
Write-Host '=============================================================' -ForegroundColor Cyan
Write-Host ' Media2AV1Queue - loudness' -ForegroundColor Cyan
Write-Host '=============================================================' -ForegroundColor Cyan
Write-Host ("standard     : {0} -- {1}" -f $Standard, $preset.Note)
Write-Host ("target       : {0:F1} LUFS programme / {1:F1} LUFS dialogue, peak ceiling {2:F1} dBTP" -f $programmeTarget, $dialogueTarget, $peakCeiling)
Write-Host ("anchor       : {0}, tolerance +/-{1:F1} LU" -f $Anchor, $Tolerance)
Write-Host ("mode         : {0}{1}" -f $(if ($Apply) { "APPLY / $Mode / track $TrackMode" } else { 'scan only (read-only)' }), $(if ($Quick) { ' / sampled' } else { '' }))
Write-Host ("parallelism  : {0}" -f $ThrottleLimit) -ForegroundColor DarkGray
if ($programmeTarget -gt -16.0) {
    Write-Host ''
    Write-Host ("Note: {0:F1} LUFS programme is well above broadcast film practice (-23 to -24)." -f $programmeTarget) -ForegroundColor Yellow
    Write-Host '      Expect a good share of real films to come out headroom-limited.' -ForegroundColor DarkGray
}
if ($CentreBoostDb -ne 0.0) {
    Write-Host ''
    Write-Host ("Centre channel will be boosted {0:+0.0;-0.0} dB relative to the rest of the mix." -f $CentreBoostDb) -ForegroundColor Yellow
    Write-Host '      This changes the mix. It is the only thing that helps buried dialogue.' -ForegroundColor DarkGray
}
Write-Host ''

Write-Host 'Enumerating files...' -ForegroundColor DarkCyan
$files = [System.Collections.Generic.List[string]]::new()
foreach ($root in $normalisedPaths) {
    if (-not (Test-Path -LiteralPath $root)) {
        Write-Warning "Path not found, skipping: $root"
        continue
    }
    $item = Get-Item -LiteralPath $root
    if ($item.PSIsContainer) {
        Get-ChildItem -LiteralPath $item.FullName -File -Recurse -ErrorAction SilentlyContinue |
            Where-Object { $VideoExtensions -contains $_.Extension.ToLowerInvariant() } |
            Where-Object { $IncludeWorkFolders -or -not (Test-InWorkFolder -FullPath $_.FullName) } |
            ForEach-Object { $files.Add($_.FullName) }
    } elseif ($VideoExtensions -contains $item.Extension.ToLowerInvariant()) {
        $files.Add($item.FullName)
    }
}

if ($files.Count -eq 0) {
    Write-Warning 'No video files found under the given path(s).'
    $script:LastToolExitCode = 0; return
}
Write-Host ("  {0} file(s) found" -f $files.Count) -ForegroundColor Gray
Write-Host ''

# =============================================================================
# Measure
# =============================================================================
Write-Host $(if ($Quick) { 'Measuring loudness (sampled)...' } else { 'Measuring loudness (full decode -- this is the slow part)...' }) -ForegroundColor DarkCyan
$swAll = [System.Diagnostics.Stopwatch]::StartNew()

$progress = [System.Collections.Concurrent.ConcurrentDictionary[string,int]]::new()
$null = $progress.TryAdd('done', 0)
$total = $files.Count

$measured = $files | ForEach-Object -ThrottleLimit $ThrottleLimit -Parallel {
    $ffmpeg   = $using:FfmpegPath
    $ffprobe  = $using:FfprobePath
    $quick    = $using:Quick
    $nSamples = $using:SampleCount
    $sampleSec= $using:SampleDurationSec
    $counter  = $using:progress
    $total    = $using:total
    $path     = $_

    $done = $counter.AddOrUpdate('done', 1, { param($k, $v) $v + 1 })
    if (($done % 25) -eq 0 -or $done -eq $total) {
        Write-Host ("  {0} / {1}" -f $done, $total) -ForegroundColor DarkGray
    }

    $result = [ordered]@{ Path = $path; Json = ''; Ebur = ''; EburCentre = ''; Error = '' }

    try {
        $probeJson = & $ffprobe -v error -print_format json -show_format -show_streams $path 2>$null
        if (-not $probeJson) { $result.Error = 'ffprobe returned nothing'; return [pscustomobject]$result }
        $result.Json = ($probeJson -join "`n")
        $probe = $result.Json | ConvertFrom-Json -Depth 100
    } catch {
        $result.Error = "probe failed: $($_.Exception.Message)"
        return [pscustomobject]$result
    }

    $audio = @($probe.streams | Where-Object { $_.codec_type -eq 'audio' })
    if ($audio.Count -eq 0) { $result.Error = 'no audio stream'; return [pscustomobject]$result }

    $duration = 0.0
    [void][double]::TryParse([string]$probe.format.duration, [System.Globalization.NumberStyles]::Float,
        [System.Globalization.CultureInfo]::InvariantCulture, [ref]$duration)

    # Stream index is resolved in the serial pass; measure the first audio
    # stream's absolute index here so the two agree.
    $audioIndex = [int]$audio[0].index
    $audioChannels = [int]($audio[0].channels ?? 0)

    try {
        if ($quick -and $duration -gt ($sampleSec * $nSamples * 1.5)) {
            # Seeked inputs concatenated into ONE ebur128. Measuring the samples
            # separately and averaging is not the same thing -- integrated
            # loudness is gated across the whole programme, so each fragment
            # would carry its own gate and the numbers would be meaningless.
            $args = @('-hide_banner', '-nostats')
            $labels = ''
            for ($i = 0; $i -lt $nSamples; $i++) {
                $frac = 0.08 + (0.84 * ($i / [Math]::Max(1, $nSamples - 1)))
                $start = [Math]::Max(0.0, [Math]::Min($duration - $sampleSec, $duration * $frac))
                $args += @('-ss', ("{0:0.###}" -f $start), '-t', "$sampleSec", '-i', $path)
                $labels += "[{0}:{1}]" -f $i, $audioIndex
            }
            $args += @('-filter_complex', ("{0}concat=n={1}:v=0:a=1[a];[a]ebur128=peak=true:framelog=quiet" -f $labels, $nSamples),
                       '-f', 'null', '-')
        } else {
            $args = @('-hide_banner', '-nostats', '-i', $path,
                      '-map', "0:$audioIndex", '-vn', '-sn', '-dn',
                      '-af', 'ebur128=peak=true:framelog=quiet', '-f', 'null', '-')
        }

        $runFfmpeg = {
            param([string[]]$FfArgs)
            $psi = [System.Diagnostics.ProcessStartInfo]::new()
            $psi.FileName = $ffmpeg
            foreach ($a in $FfArgs) { $psi.ArgumentList.Add([string]$a) }
            $psi.RedirectStandardError = $true
            $psi.RedirectStandardOutput = $true
            $psi.UseShellExecute = $false
            $p = [System.Diagnostics.Process]::Start($psi)
            $err = $p.StandardError.ReadToEnd()
            $null = $p.StandardOutput.ReadToEnd()
            $p.WaitForExit()
            return [pscustomobject]@{ Stderr = $err; ExitCode = $p.ExitCode }
        }

        $mix = & $runFfmpeg $args
        $result.Ebur = $mix.Stderr
        if ($mix.ExitCode -ne 0) { $result.Error = 'ebur128 measurement failed' }

        # Second pass over the centre channel, where dialogue lives. Only for
        # sources that have one: 3.0 and above. Measured as dual-mono rather
        # than mono so the number is directly comparable with the programme
        # figure -- the same signal reads about 3 LU lower as plain mono.
        #
        # A separate invocation rather than two ebur128 instances in one graph:
        # both print an identically-shaped summary, and the only thing telling
        # them apart is a filter index that moves when the graph changes.
        if ($audioChannels -ge 3) {
            $centreArgs = @('-hide_banner', '-nostats', '-i', $path,
                            '-map', "0:$audioIndex", '-vn', '-sn', '-dn',
                            '-af', 'pan=stereo|c0=FC|c1=FC,ebur128=peak=true:framelog=quiet',
                            '-f', 'null', '-')
            $centre = & $runFfmpeg $centreArgs
            if ($centre.ExitCode -eq 0) { $result.EburCentre = $centre.Stderr }
        }
    } catch {
        $result.Error = "measurement failed: $($_.Exception.Message)"
    }

    return [pscustomobject]$result
}

$swAll.Stop()
Write-Host ("  measured {0} file(s) in {1:F1}s" -f $total, $swAll.Elapsed.TotalSeconds) -ForegroundColor Gray
Write-Host ''

# =============================================================================
# Classify
# =============================================================================
# -----------------------------------------------------------------------------
# Decides what gain a file needs, how much of it can actually be applied, and
# what to call the result.
#
# Split out from the loop so it can be tested. The arithmetic is simple enough
# to look obviously right and still be wrong by a sign, and the failure mode is
# silent: the file plays, it is just at the wrong level.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# loudnorm's own pass-1 measurement, for the compression path only.
#
# ebur128 and loudnorm both report an "integrated loudness threshold", and they
# do not mean quite the same thing by it. Feeding ebur128's number into
# loudnorm's measured_thresh made compression measurably worse here -- on a test
# file it undershot the target by 0.9 dB and came out with a WIDER loudness
# range than it started with. Each tool is given its own numbers instead.
#
# This costs an extra decode, which is why it runs only for -Mode Compress. The
# linear path needs no loudnorm measurement at all: it is one static gain.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# A pan filter that raises the centre channel relative to everything else.
#
# The one thing that helps a mix whose dialogue is buried under its own
# effects. It is not normalisation -- it re-balances the mix, which is a real
# edit and audibly so past a few dB. Returns an empty string when the layout
# has no centre channel to raise, so the caller silently does nothing rather
# than mangling a stereo track.
# -----------------------------------------------------------------------------
function Get-CentreBoostFilter {
    param([int]$Channels, [double]$BoostDb)

    if ($BoostDb -eq 0.0) { return '' }

    $gain = [Math]::Pow(10.0, $BoostDb / 20.0)
    $g = $gain.ToString('0.####', [System.Globalization.CultureInfo]::InvariantCulture)

    switch ($Channels) {
        6 { return "pan=5.1|FL=FL|FR=FR|FC=$g*FC|LFE=LFE|BL=BL|BR=BR" }
        8 { return "pan=7.1|FL=FL|FR=FR|FC=$g*FC|LFE=LFE|BL=BL|BR=BR|SL=SL|SR=SR" }
        default { return '' }
    }
}

function Measure-LoudnormJson {
    param([string]$InputPath, [int]$AudioStreamIndex)

    $args = @(
        '-hide_banner', '-nostats', '-nostdin', '-i', $InputPath,
        '-map', "0:$AudioStreamIndex", '-vn', '-sn', '-dn',
        '-af', 'loudnorm=print_format=json', '-f', 'null', '-'
    )

    $psi = [System.Diagnostics.ProcessStartInfo]::new()
    $psi.FileName = $FfmpegPath
    foreach ($a in $args) { $psi.ArgumentList.Add([string]$a) }
    $psi.RedirectStandardError = $true
    $psi.RedirectStandardOutput = $true
    $psi.UseShellExecute = $false
    $proc = [System.Diagnostics.Process]::Start($psi)
    $stderr = $proc.StandardError.ReadToEnd()
    $null = $proc.StandardOutput.ReadToEnd()
    $proc.WaitForExit()

    $jsonMatch = [Regex]::Match($stderr, '(?s)\{.*?"normalization_type".*?\}')
    if (-not $jsonMatch.Success) { return $null }

    try { $parsed = $jsonMatch.Value | ConvertFrom-Json } catch { return $null }

    $read = {
        param([string]$Name)
        $raw = [string](Get-OptionalProperty -InputObject $parsed -PropertyName $Name -Default '')
        $v = 0.0
        if ([double]::TryParse($raw, [System.Globalization.NumberStyles]::Float,
                               [System.Globalization.CultureInfo]::InvariantCulture, [ref]$v)) { return $v }
        return $null
    }

    $i = & $read 'input_i'; $tp = & $read 'input_tp'; $lra = & $read 'input_lra'; $thr = & $read 'input_thresh'
    if ($null -eq $i -or $null -eq $tp -or $null -eq $lra -or $null -eq $thr) { return $null }

    return [pscustomobject][ordered]@{ I = $i; TP = $tp; LRA = $lra; Thresh = $thr }
}

function Resolve-LoudnessPlan {
    param(
        [double]$Integrated,
        [double]$TruePeakIn,
        [double]$Lra,
        [double]$TargetLufsValue,
        [double]$CeilingDbtp,
        [double]$ToleranceLu,
        # Dialogue loudness, when the source has a centre channel to measure.
        # $null means there was none, and the plan falls back to programme.
        $DialogueLufs = $null,
        [double]$DialogueTargetValue = 0.0,
        # Any boost applied to the centre channel lands on top of the global
        # gain, and the centre contributes to the peak, so it has to come out
        # of the headroom budget before the gain is decided.
        [double]$CentreBoost = 0.0
    )

    # Anchoring on dialogue where it exists: the gap between programme and
    # dialogue loudness is what decides whether a film is listenable, and it
    # varies enormously between titles.
    $usedDialogue = ($null -ne $DialogueLufs -and $DialogueTargetValue -ne 0.0)
    $anchorValue  = if ($usedDialogue) { [double]$DialogueLufs } else { $Integrated }
    $anchorTarget = if ($usedDialogue) { $DialogueTargetValue } else { $TargetLufsValue }

    # How far dialogue sits below the mix as a whole. Around 8-14 LU is normal
    # for film; far more than that is a mix you cannot fix with a volume knob.
    $dialogueGap = if ($null -ne $DialogueLufs) { $Integrated - [double]$DialogueLufs } else { 0.0 }

    $requiredGain = $anchorTarget - $anchorValue

    # The most a static gain may add before the loudest sample in the file
    # crosses the ceiling. Negative when the source already peaks above it,
    # which is why attenuation is always allowed through unchanged.
    #
    # A centre boost of N dB can raise the peak by as much as N dB, so it is
    # subtracted here rather than discovered by the verification step after the
    # encode has already happened. Measured without this, a +6 dB boost put the
    # output exactly on the ceiling with nothing to spare.
    $headroomGain = $CeilingDbtp - $TruePeakIn - [Math]::Max(0.0, $CentreBoost)
    $appliedGain  = if ($requiredGain -le $headroomGain) { $requiredGain } else { $headroomGain }
    $shortfall    = [Math]::Max(0.0, $requiredGain - $appliedGain)

    # Buried dialogue is checked before anything else, because it is the one
    # verdict a gain cannot resolve: raising the whole mix raises the effects
    # by exactly as much, and the gap stays where it was.
    $verdict =
        if ($usedDialogue -and $dialogueGap -ge 20.0)           { 'Dialogue buried' }
        elseif ([Math]::Abs($requiredGain) -le $ToleranceLu)    { 'OK' }
        elseif ($requiredGain -lt 0)                            { 'Loud' }
        elseif ($shortfall -gt 0.5 -and $Lra -ge 15.0)          { 'Headroom-limited (wide range)' }
        elseif ($shortfall -gt 0.5)                             { 'Headroom-limited' }
        elseif ($Lra -ge 20.0)                                  { 'Quiet + very wide range' }
        else                                                    { 'Quiet' }

    return [pscustomobject][ordered]@{
        RequiredGainDb = $requiredGain
        AppliedGainDb  = $appliedGain
        ShortfallDb    = $shortfall
        Verdict        = $verdict
        AnchoredOn     = if ($usedDialogue) { 'dialogue' } else { 'programme' }
        DialogueGapLu  = $dialogueGap
    }
}

function Get-NormalisedAudioCodec {
    param($AudioStream, [string]$Requested)

    $codec = ([string](Get-StreamProp $AudioStream 'codec_name' '')).ToLowerInvariant()
    $channels = [int](Get-StreamProp $AudioStream 'channels' 2)
    $isLossless = $codec -match '^(truehd|mlp|flac|alac|pcm_|wavpack|tta)' -or
                  ((Get-StreamProp $AudioStream 'profile' '') -match '(?i)MA|lossless')

    if ($Requested -ne 'Auto') {
        return [pscustomobject]@{ Codec = $Requested; Bitrate = $(if ($channels -gt 2) { '640k' } else { '256k' }); Reason = 'codec chosen by -AudioCodec' }
    }

    # E-AC3 tops out at 5.1. Handing it a 7.1 track does not fail -- it
    # downmixes, quietly. FLAC carries any layout untouched, and in Add mode
    # the original bitstream is still there for the AVR to decode.
    if ($channels -gt 6) {
        return [pscustomobject]@{ Codec = 'flac'; Bitrate = ''; Reason = "$channels channels: E-AC3 would downmix to 5.1, so FLAC" }
    }
    if ($isLossless) {
        return [pscustomobject]@{ Codec = 'flac'; Bitrate = ''; Reason = 'lossless source: FLAC keeps it lossless' }
    }
    return [pscustomobject]@{
        Codec = 'eac3'
        Bitrate = $(if ($channels -gt 2) { '640k' } else { '256k' })
        Reason = 'lossy source: E-AC3 for broad player support'
    }
}

$rows     = New-Object System.Collections.Generic.List[object]
$excluded = @{ NoAudio = 0; ProbeFailed = 0; Silent = 0; MeasureFailed = 0 }

foreach ($m in $measured) {
    if ($m.Error -eq 'no audio stream') { $excluded.NoAudio++; continue }
    if ([string]::IsNullOrWhiteSpace($m.Json)) { $excluded.ProbeFailed++; continue }

    try { $probe = $m.Json | ConvertFrom-Json -Depth 100 } catch { $excluded.ProbeFailed++; continue }

    $stats = ConvertFrom-Ebur128Output -Text $m.Ebur
    if ($null -eq $stats) { $excluded.MeasureFailed++; continue }
    if ($stats.IsSilent) { $excluded.Silent++; continue }

    # The track the encoder would keep, so both tools mean the same thing by
    # "the audio". Falls back to the first audio stream when Select-Streams
    # declines the file for reasons that do not matter here.
    $audioStream = $null
    try { $audioStream = (Select-Streams -Probe $probe).MainAudio } catch { $audioStream = $null }
    if ($null -eq $audioStream) {
        $audioStream = @($probe.streams | Where-Object { $_.codec_type -eq 'audio' })[0]
    }
    if ($null -eq $audioStream) { $excluded.NoAudio++; continue }

    $centreStats = if ([string]::IsNullOrWhiteSpace($m.EburCentre)) { $null } else { ConvertFrom-Ebur128Output -Text $m.EburCentre }
    $dialogueLufs = if ($null -ne $centreStats -and -not $centreStats.IsSilent) { [double]$centreStats.Integrated } else { $null }

    # -Anchor Integrated throws the dialogue figure away for the decision, but
    # it is still measured and still reported: knowing the gap is useful even
    # when you have chosen not to anchor on it.
    $anchorDialogue = ($Anchor -ne 'Integrated')

    $plan = Resolve-LoudnessPlan `
        -Integrated ([double]$stats.Integrated) `
        -TruePeakIn ([double]$stats.TruePeak) `
        -Lra ([double]$stats.Lra) `
        -TargetLufsValue $programmeTarget `
        -CeilingDbtp $peakCeiling `
        -ToleranceLu $Tolerance `
        -DialogueLufs $(if ($anchorDialogue) { $dialogueLufs } else { $null }) `
        -DialogueTargetValue $dialogueTarget `
        -CentreBoost $CentreBoostDb

    $codecPlan = Get-NormalisedAudioCodec -AudioStream $audioStream -Requested $AudioCodec

    $rows.Add([pscustomobject][ordered]@{
        Path             = $m.Path
        Name             = [System.IO.Path]::GetFileName($m.Path)
        AudioIndex       = [int](Get-StreamProp $audioStream 'index' 0)
        AudioCodec       = [string](Get-StreamProp $audioStream 'codec_name' '')
        Channels         = [int](Get-StreamProp $audioStream 'channels' 0)
        IntegratedLufs   = [Math]::Round([double]$stats.Integrated, 2)
        DialogueLufs     = if ($null -ne $dialogueLufs) { [Math]::Round([double]$dialogueLufs, 2) } else { '' }
        DialogueGapLu    = if ($null -ne $dialogueLufs) { [Math]::Round([double]$stats.Integrated - [double]$dialogueLufs, 2) } else { '' }
        AnchoredOn       = $plan.AnchoredOn
        LoudnessRangeLu  = [Math]::Round([double]$stats.Lra, 2)
        TruePeakDbtp     = [Math]::Round([double]$stats.TruePeak, 2)
        ThresholdLufs    = [Math]::Round([double]$stats.Threshold, 2)
        RequiredGainDb   = [Math]::Round($plan.RequiredGainDb, 2)
        AppliedGainDb    = [Math]::Round($plan.AppliedGainDb, 2)
        ShortfallDb      = [Math]::Round($plan.ShortfallDb, 2)
        Verdict          = $plan.Verdict
        Measurement      = $(if ($Quick) { 'sampled' } else { 'full' })
        PlannedCodec     = $codecPlan.Codec
        PlannedBitrate   = $codecPlan.Bitrate
        CodecReason      = $codecPlan.Reason
    })
}

if ($rows.Count -eq 0) {
    Write-Warning 'Nothing measurable was found.'
    $script:LastToolExitCode = 0; return
}

# =============================================================================
# Report
# =============================================================================
$needWork = @($rows | Where-Object { $_.Verdict -ne 'OK' })
# Sorted by the figure that actually drove each decision, so a film with fine
# programme loudness and inaudible dialogue does not hide in the middle.
$sorted   = @($rows | Sort-Object -Property @{ Expression = { if ($_.DialogueLufs -ne '') { [double]$_.DialogueLufs } else { [double]$_.IntegratedLufs } } })

Write-Host '--- Loudness distribution -----------------------------------' -ForegroundColor Cyan
$buckets = [ordered]@{
    'louder than -14'   = @($rows | Where-Object { $_.IntegratedLufs -gt -14 })
    '-14 to -18'        = @($rows | Where-Object { $_.IntegratedLufs -le -14 -and $_.IntegratedLufs -gt -18 })
    '-18 to -22'        = @($rows | Where-Object { $_.IntegratedLufs -le -18 -and $_.IntegratedLufs -gt -22 })
    '-22 to -26'        = @($rows | Where-Object { $_.IntegratedLufs -le -22 -and $_.IntegratedLufs -gt -26 })
    'quieter than -26'  = @($rows | Where-Object { $_.IntegratedLufs -le -26 })
}
foreach ($b in $buckets.GetEnumerator()) {
    $count = $b.Value.Count
    $bar = '#' * [Math]::Min(40, [int]($count * 40 / [Math]::Max(1, $rows.Count)))
    Write-Host ('{0,-18} {1,5}  {2}' -f $b.Key, $count, $bar)
}

Write-Host ''
Write-Host '--- Verdicts ------------------------------------------------' -ForegroundColor Cyan
foreach ($group in ($rows | Group-Object Verdict | Sort-Object Count -Descending)) {
    Write-Host ('{0,-32} {1,5}' -f $group.Name, $group.Count)
}

$limited = @($rows | Where-Object { $_.Verdict -like 'Headroom-limited*' })
if ($limited.Count -gt 0) {
    $medianShort = @($limited.ShortfallDb | Sort-Object)[[int]($limited.Count / 2)]
    Write-Host ''
    Write-Host ("{0} file(s) cannot reach the target without limiting; median shortfall {1:F1} dB." -f $limited.Count, $medianShort) -ForegroundColor Yellow
    Write-Host '      They will be gained as far as the peak ceiling allows, dynamics intact.' -ForegroundColor DarkGray
    Write-Host ("      A target around {0:F0} LUFS would clear most of them." -f ($programmeTarget - $medianShort)) -ForegroundColor DarkGray
}

$wide = @($rows | Where-Object { $_.LoudnessRangeLu -ge 20 })
if ($wide.Count -gt 0) {
    Write-Host ''
    Write-Host ("{0} file(s) have a loudness range of 20 LU or more." -f $wide.Count) -ForegroundColor Yellow
    Write-Host '      Loud and quiet scenes are far apart. A gain moves both ends together;' -ForegroundColor DarkGray
    Write-Host '      -Mode Compress is what narrows the distance between them.' -ForegroundColor DarkGray
}

$buried = @($rows | Where-Object { $_.Verdict -eq 'Dialogue buried' })
if ($buried.Count -gt 0) {
    Write-Host ''
    Write-Host ("{0} file(s) have dialogue 20 LU or more below the programme level." -f $buried.Count) -ForegroundColor Yellow
    Write-Host '      These are the ones you turn up and still cannot follow. No global gain' -ForegroundColor DarkGray
    Write-Host '      helps: it lifts the effects by exactly as much and the gap survives.' -ForegroundColor DarkGray
    Write-Host '      -CentreBoostDb 3 raises the centre channel relative to the rest, which' -ForegroundColor DarkGray
    Write-Host '      does change the mix. Try it on one film before running it over a library.' -ForegroundColor DarkGray
}

$measuredDialogue = @($rows | Where-Object { $_.AnchoredOn -eq 'dialogue' })
if ($measuredDialogue.Count -gt 0) {
    $gaps = @($measuredDialogue | ForEach-Object { [double]$_.DialogueGapLu } | Sort-Object)
    Write-Host ''
    Write-Host ("Dialogue was measured on {0} of {1} file(s); median gap below programme level {2:F1} LU." -f `
        $measuredDialogue.Count, $rows.Count, $gaps[[int]($gaps.Count / 2)]) -ForegroundColor DarkGray
    Write-Host '      Stereo sources have no centre channel, so those fall back to programme loudness.' -ForegroundColor DarkGray
}

Write-Host ''
Write-Host '--- Quietest files ------------------------------------------' -ForegroundColor Cyan
Write-Host ('{0,-40} {1,8} {2,9} {3,7} {4,7} {5,7}  {6}' -f 'File', 'LUFS', 'Dialogue', 'LRA', 'dBTP', 'Gain', 'Verdict') -ForegroundColor DarkGray
foreach ($row in ($sorted | Select-Object -First 25)) {
    $display = if ($row.Name.Length -gt 40) { $row.Name.Substring(0, 37) + '...' } else { $row.Name }
    $dlg = if ($row.DialogueLufs -ne '') { ('{0:F1}' -f [double]$row.DialogueLufs) } else { '-' }
    Write-Host ('{0,-40} {1,8:F1} {2,9} {3,7:F1} {4,7:F1} {5,7:F1}  {6}' -f `
        $display, $row.IntegratedLufs, $dlg, $row.LoudnessRangeLu, $row.TruePeakDbtp, $row.AppliedGainDb, $row.Verdict)
}

Write-Host ''
if ($excluded.NoAudio + $excluded.ProbeFailed + $excluded.Silent + $excluded.MeasureFailed -gt 0) {
    Write-Host ("Excluded: no audio {0} | silent/near-silent {1} | probe failed {2} | measurement failed {3}" -f `
        $excluded.NoAudio, $excluded.Silent, $excluded.ProbeFailed, $excluded.MeasureFailed) -ForegroundColor DarkGray
}
if ($Quick) {
    Write-Host 'Measurements are SAMPLED. Good enough to decide what to fix and what target to' -ForegroundColor Yellow
    Write-Host 'aim for; not good enough to compute a gain from. -Apply re-measures in full.' -ForegroundColor DarkGray
}

if (-not [string]::IsNullOrWhiteSpace($Csv)) {
    $sorted | Export-Csv -LiteralPath $Csv -NoTypeInformation -Encoding UTF8
    Write-Host ''
    Write-Host ("Full results: {0}" -f $Csv) -ForegroundColor Green
}
if (-not [string]::IsNullOrWhiteSpace($OutList)) {
    $needWork | ForEach-Object { $_.Path } | Set-Content -LiteralPath $OutList -Encoding UTF8
    Write-Host ("Needs work ({0} paths): {1}" -f $needWork.Count, $OutList) -ForegroundColor Green
}

if (-not $Apply) {
    Write-Host ''
    Write-Host ("{0} of {1} file(s) would be changed. Nothing was written." -f $needWork.Count, $rows.Count) -ForegroundColor Cyan
    Write-Host 'Re-run with -Apply once you have picked a target.' -ForegroundColor DarkGray
    Write-Host ''
    $script:LastToolExitCode = 0; return
}

# =============================================================================
# Apply
# =============================================================================
Write-Host ''
Write-Host '--- Applying ------------------------------------------------' -ForegroundColor Cyan
if ($OverwriteSource) {
    Write-Host 'OverwriteSource is ON: source files will be overwritten after a successful write.' -ForegroundColor Yellow
} else {
    Write-Host 'Originals are left alone; normalised copies are written alongside.' -ForegroundColor DarkGray
}
Write-Host ''

$applied = 0; $failed = 0; $skipped = 0

foreach ($row in $needWork) {
    $name = $row.Name
    Write-Host ("{0}" -f $name) -ForegroundColor White
    $anchorText = if ($row.AnchoredOn -eq 'dialogue') { ("dialogue {0:F1} LUFS" -f [double]$row.DialogueLufs) } else { ("programme {0:F1} LUFS" -f $row.IntegratedLufs) }
    Write-Host ("   {0} -> gain {1:+0.0;-0.0} dB{2}" -f `
        $anchorText, $row.AppliedGainDb, $(if ($row.ShortfallDb -gt 0.5) { (" (short of target by {0:F1} dB, peak-limited)" -f $row.ShortfallDb) } else { '' })) -ForegroundColor DarkGray

    $sourceItem = Get-Item -LiteralPath $row.Path
    $targetDir = if ([string]::IsNullOrWhiteSpace($OutputDir)) { $sourceItem.DirectoryName } else { $OutputDir }
    if (-not (Test-Path -LiteralPath $targetDir)) { $null = New-Item -ItemType Directory -Force -Path $targetDir }

    $baseName = [System.IO.Path]::GetFileNameWithoutExtension($sourceItem.Name)
    $finalPath = Join-Path $targetDir ($baseName + '.normalized' + $sourceItem.Extension)
    $tempPath  = Join-Path $targetDir ($baseName + '.loudness.tmp' + $sourceItem.Extension)

    if ((Test-Path -LiteralPath $finalPath) -and -not $OverwriteSource) {
        Write-Host '   already normalised, skipping' -ForegroundColor DarkGray
        $skipped++
        continue
    }

    try {
        $ffArgs = New-Object System.Collections.Generic.List[string]
        $ffArgs.AddRange([string[]]@('-hide_banner', '-nostdin', '-y', '-i', $row.Path))

        # Video, subtitles and chapters are copied untouched. Only audio is
        # rebuilt, and in Add mode the original audio is copied too.
        $ffArgs.AddRange([string[]]@('-map', '0:v?', '-map', '0:s?', '-map_chapters', '0'))

        $filter = if ($Mode -eq 'Compress') {
            # loudnorm measures the file itself, then is handed its own numbers
            # back. The LRA target here is deliberately BELOW the source's range:
            # narrowing the range is the entire point of this mode.
            $ln = Measure-LoudnormJson -InputPath $row.Path -AudioStreamIndex $row.AudioIndex
            if ($null -eq $ln) { throw 'loudnorm measurement pass failed' }
            ("loudnorm=I={0}:TP={1}:LRA={2}:measured_I={3}:measured_TP={4}:measured_LRA={5}:measured_thresh={6}:linear=false" -f `
                $programmeTarget, $peakCeiling, $CompressLra, $ln.I, $ln.TP, $ln.LRA, $ln.Thresh)
        } else {
            # One static gain. Nothing is reshaped, so the loudness range comes
            # out of the far end exactly as it went in.
            ("volume={0:F2}dB" -f $row.AppliedGainDb)
        }

        # The centre boost runs BEFORE the gain, so the gain still lands the
        # anchor where it was measured to land.
        $centreFilter = Get-CentreBoostFilter -Channels ([int]$row.Channels) -BoostDb $CentreBoostDb
        if (-not [string]::IsNullOrWhiteSpace($centreFilter)) {
            $filter = "$centreFilter,$filter"
        } elseif ($CentreBoostDb -ne 0.0) {
            Write-Host ("   centre boost skipped: {0}-channel layout has no centre to raise" -f $row.Channels) -ForegroundColor DarkYellow
        }

        if ($TrackMode -eq 'Add') {
            $ffArgs.AddRange([string[]]@('-map', "0:$($row.AudioIndex)", '-map', "0:$($row.AudioIndex)"))
            $ffArgs.AddRange([string[]]@('-filter:a:1', $filter))
            $ffArgs.AddRange([string[]]@('-c:a:0', 'copy'))
            $ffArgs.AddRange([string[]]@('-c:a:1', $row.PlannedCodec))
            if (-not [string]::IsNullOrWhiteSpace($row.PlannedBitrate)) {
                $ffArgs.AddRange([string[]]@('-b:a:1', $row.PlannedBitrate))
            }
            $ffArgs.AddRange([string[]]@(
                '-metadata:s:a:0', 'title=Original',
                '-metadata:s:a:1', ("title=Normalized ({0})" -f $Standard),
                '-disposition:a:0', '0',
                '-disposition:a:1', 'default'
            ))
        } else {
            $ffArgs.AddRange([string[]]@('-map', "0:$($row.AudioIndex)"))
            $ffArgs.AddRange([string[]]@('-filter:a', $filter))
            $ffArgs.AddRange([string[]]@('-c:a', $row.PlannedCodec))
            if (-not [string]::IsNullOrWhiteSpace($row.PlannedBitrate)) {
                $ffArgs.AddRange([string[]]@('-b:a', $row.PlannedBitrate))
            }
            $ffArgs.AddRange([string[]]@('-metadata:s:a:0', ("title=Normalized ({0})" -f $Standard), '-disposition:a:0', 'default'))
        }

        $ffArgs.AddRange([string[]]@('-c:v', 'copy', '-c:s', 'copy', '-max_muxing_queue_size', '4096', $tempPath))

        $psi = [System.Diagnostics.ProcessStartInfo]::new()
        $psi.FileName = $FfmpegPath
        foreach ($a in $ffArgs) { $psi.ArgumentList.Add([string]$a) }
        $psi.RedirectStandardError = $true
        $psi.RedirectStandardOutput = $true
        $psi.UseShellExecute = $false
        $proc = [System.Diagnostics.Process]::Start($psi)
        $stderr = $proc.StandardError.ReadToEnd()
        $null = $proc.StandardOutput.ReadToEnd()
        $proc.WaitForExit()

        if ($proc.ExitCode -ne 0 -or -not (Test-Path -LiteralPath $tempPath)) {
            $lastLines = (($stderr -split "\r?\n" | Where-Object { $_.Trim() }) | Select-Object -Last 2) -join ' || '
            throw "ffmpeg failed: $lastLines"
        }

        # ---- Verify, rather than assume --------------------------------
        # Re-measure the track that was just written. A gain is easy to get
        # wrong by a sign or a factor, and the failure is silent: the file
        # plays, it is just wrong. Checking costs one more decode.
        $verifyArgs = @('-hide_banner', '-nostats', '-i', $tempPath, '-map', $(if ($TrackMode -eq 'Add') { '0:a:1' } else { '0:a:0' }),
                        '-vn', '-sn', '-dn', '-af', 'ebur128=peak=true:framelog=quiet', '-f', 'null', '-')
        $vpsi = [System.Diagnostics.ProcessStartInfo]::new()
        $vpsi.FileName = $FfmpegPath
        foreach ($a in $verifyArgs) { $vpsi.ArgumentList.Add([string]$a) }
        $vpsi.RedirectStandardError = $true
        $vpsi.RedirectStandardOutput = $true
        $vpsi.UseShellExecute = $false
        $vproc = [System.Diagnostics.Process]::Start($vpsi)
        $vstderr = $vproc.StandardError.ReadToEnd()
        $null = $vproc.StandardOutput.ReadToEnd()
        $vproc.WaitForExit()

        $after = ConvertFrom-Ebur128Output -Text $vstderr
        if ($null -eq $after) { throw 'could not verify the normalised track' }

        # A static gain moves programme loudness by exactly that gain, so this
        # is a real check -- but only when nothing else touched the mix. A
        # centre boost deliberately changes the balance, so the identity no
        # longer holds and the check is skipped rather than made meaningless.
        $expected  = [double]$row.IntegratedLufs + [double]$row.AppliedGainDb
        # Not $error: that is an automatic variable and assigning to it throws.
        $deltaLufs = [double]$after.Integrated - $expected
        if ([Math]::Abs($deltaLufs) -gt 1.5 -and $Mode -eq 'Linear' -and $CentreBoostDb -eq 0.0) {
            throw ("verification failed: expected {0:F1} LUFS, measured {1:F1}" -f $expected, $after.Integrated)
        }
        if ([double]$after.TruePeak -gt ($peakCeiling + 0.5)) {
            throw ("verification failed: true peak {0:F1} dBTP exceeds the {1:F1} ceiling" -f $after.TruePeak, $peakCeiling)
        }

        Write-Host ("   -> {0:F1} LUFS, peak {1:F1} dBTP, range {2:F1} LU  (was {3:F1} LU)" -f `
            $after.Integrated, $after.TruePeak, $after.Lra, $row.LoudnessRangeLu) -ForegroundColor Green

        # loudnorm's loudness-range targeting is approximate, and on some
        # material it widens the range instead of narrowing it. Saying so beats
        # letting the user assume the option did what its name promises.
        if ($Mode -eq 'Compress' -and [double]$after.Lra -ge [double]$row.LoudnessRangeLu) {
            Write-Host ("   note: the loudness range did not narrow ({0:F1} -> {1:F1} LU). loudnorm's" -f $row.LoudnessRangeLu, $after.Lra) -ForegroundColor Yellow
            Write-Host '         LRA targeting is approximate on some material; check this one by ear.' -ForegroundColor DarkGray
        }

        if ($OverwriteSource) {
            Move-Item -LiteralPath $tempPath -Destination $row.Path -Force
            Write-Host '   replaced the original' -ForegroundColor DarkGray
        } else {
            Move-Item -LiteralPath $tempPath -Destination $finalPath -Force
            Write-Host ("   wrote {0}" -f [System.IO.Path]::GetFileName($finalPath)) -ForegroundColor DarkGray
        }
        $applied++
    } catch {
        Write-Host ("   FAILED: {0}" -f $_.Exception.Message) -ForegroundColor Red
        if (Test-Path -LiteralPath $tempPath) { Remove-Item -LiteralPath $tempPath -Force -ErrorAction SilentlyContinue }
        $failed++
    }
}

Write-Host ''
Write-Host ("Applied {0} | skipped {1} | failed {2}" -f $applied, $skipped, $failed) -ForegroundColor Cyan
Write-Host ''

}

# =============================================================================
# Quality
# Merged from Media2AV1Queue-Quality.ps1. Body unchanged; see Build-Single.ps1.
# =============================================================================
function Invoke-QualityTool {
param(
    # The file to profile. One file at a time: the point is to look closely.
    [Parameter(Mandatory = $true)]
    [string]$Path,

    # CRFs to measure. Defaults to a spread wide enough to show the knee.
    #
    # Taken as a string and split here rather than declared [int[]]. Launching a
    # script with "pwsh -File" does not bind array arguments: "-CrfList 22,26,30"
    # arrives as the single token "22,26,30", which an [int[]] parameter parses
    # as the number 222630. Splitting explicitly works from both -File and a
    # normal call.
    [string]$CrfList = '18,22,26,30,34,38,42',

    # Sample positions per CRF. More is steadier and slower.
    [int]$SampleCount = 2,

    # Seconds per sample.
    [int]$SampleDurationSec = 10,

    # Encoder preset for the samples. Lower is slower and closer to a real
    # encode; the default follows what Auto usually lands on.
    #
    # Named -SamplePreset rather than -Preset on purpose: this script loads the
    # main script's settings block into its own scope, and that block assigns
    # $Preset = Auto. A parameter called $Preset would be overwritten by it,
    # and being [int]-typed it would throw on the string 'Auto' instead of
    # failing visibly. Same reasoning for -SampleFilmGrain.
    [int]$SamplePreset = 6,

    # Film grain synthesis strength for the samples.
    [int]$SampleFilmGrain = 0,

    # Keep the sample encodes so you can watch them yourself. This is the part
    # no metric can do for you.
    [switch]$KeepSamples,

    # Write the curve to a CSV as well as the console.
    [string]$Csv = ''
)




# A handful of the lifted functions reach for the live queue. Replaced with
# quiet local equivalents AFTER the lift, so these definitions win.
function Write-SessionTextLogMessage { param($Level, $Message) }
function Test-QueueShutdownRequested { return $false }

function Invoke-FfmpegSync {
    param([string[]]$Arguments)

    $psi = [System.Diagnostics.ProcessStartInfo]::new()
    $psi.FileName = $FfmpegPath
    foreach ($a in $Arguments) { $psi.ArgumentList.Add($a) }
    $psi.RedirectStandardError  = $true
    $psi.RedirectStandardOutput = $true
    $psi.UseShellExecute = $false
    $proc = [System.Diagnostics.Process]::Start($psi)
    $err = $proc.StandardError.ReadToEnd()
    $null = $proc.StandardOutput.ReadToEnd()
    $proc.WaitForExit()
    return [pscustomobject]@{
        ExitCode = $proc.ExitCode
        Stderr   = $err
        LogLines = @(($err -split "\r?\n") | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    }
}

# =============================================================================
# Tools
# =============================================================================
$FfmpegPath  = Join-Path $PSScriptRoot 'ffmpeg.exe'
$FfprobePath = Join-Path $PSScriptRoot 'ffprobe.exe'
if (-not (Test-Path -LiteralPath $FfmpegPath)) {
    $c = Get-Command ffmpeg -ErrorAction SilentlyContinue
    if (-not $c) { throw 'ffmpeg not found next to this script or on PATH.' }
    $FfmpegPath = $c.Source
}
if (-not (Test-Path -LiteralPath $FfprobePath)) {
    $c = Get-Command ffprobe -ErrorAction SilentlyContinue
    if (-not $c) { throw 'ffprobe not found next to this script or on PATH.' }
    $FfprobePath = $c.Source
}

$PreflightDir = Join-Path $PSScriptRoot '.queue\quality'
$null = New-Item -ItemType Directory -Force -Path $PreflightDir

if (-not (Test-Path -LiteralPath $Path)) { throw "Source not found: $Path" }
$sourceItem = Get-Item -LiteralPath $Path

Write-Host ''
Write-Host '=============================================================' -ForegroundColor Cyan
Write-Host ' Media2AV1Queue - quality calibration' -ForegroundColor Cyan
Write-Host '=============================================================' -ForegroundColor Cyan
Write-Host ("Source     : {0}" -f $sourceItem.Name)
Write-Host ("Size       : {0:F2} GiB" -f ($sourceItem.Length / 1GB))
Write-Host ("ffmpeg     : {0}" -f $FfmpegPath)
Write-Host ''

# =============================================================================
# Probe the source the same way the queue does
# =============================================================================
$probe = Invoke-FfprobeJson -InputPath $Path
if ($null -eq $probe) { throw "ffprobe could not read $Path" }

$video = @($probe.streams | Where-Object { $_.codec_type -eq 'video' })[0]
if ($null -eq $video) { throw 'No video stream found.' }

$durationSec = [double](Convert-ToInvariantDouble (Get-OptionalProperty -InputObject $probe.format -PropertyName 'duration' -Default 0))
if ($durationSec -le 0) { throw 'Could not determine the source duration.' }

$sourceProfile = Get-SourceProfile -Probe $probe -VideoStream $video -InputPath $Path
$frameRate = [double](Get-FrameRate -Stream $video)
$resolutionTier = Get-ResolutionTier -Width ([int](Get-StreamProp $video 'width' 0))
$sourceGiBPerHour = ($sourceItem.Length / 1GB) / ($durationSec / 3600.0)

$hdrPlan = Resolve-HdrEncodePlan -InputPath $Path -Probe $probe -VideoStream $video -SourceProfile $sourceProfile -EncodeMode 'software'
if ($hdrPlan.Skip) {
    Write-Host ("This source would be skipped by the queue: {0}" -f $hdrPlan.SkipReason) -ForegroundColor Yellow
    Write-Host 'Measuring it anyway, since you asked, but the queue will not encode it.' -ForegroundColor Yellow
    Write-Host ''
}

$selected = [pscustomobject]@{ Video = $video }
$autoSettings = [pscustomobject]@{ ResolutionTier = $resolutionTier }

$metricPlan = Resolve-QualityMetricPlan -SourceProfile $sourceProfile -AutoSettings $autoSettings -EncodeMode 'software'
$toolchain  = Get-QualityToolchainEnvironment

Write-Host ("Profile    : {0} / {1} / {2:F3} fps / {3:F2} GiB/hr" -f $resolutionTier, $sourceProfile.Profile, $frameRate, $sourceGiBPerHour)
Write-Host ("HDR plan   : {0}" -f (Get-HdrPlanSummary -HdrPlan $hdrPlan))
Write-Host ("Metrics    : xpsnr {0} / libvmaf {1}" -f `
    $(if ($toolchain.SupportsXpsnr) { 'yes' } else { 'NO -- ' + $toolchain.XpsnrDetail }), `
    $(if ($toolchain.SupportsVmaf) { 'yes' } else { 'NO -- ' + $toolchain.VmafDetail })) -ForegroundColor DarkCyan
if ($metricPlan.Enabled) {
    Write-Host ("Configured : {0} / {1} -- {2}" -f $metricPlan.Metric, $metricPlan.Mode, $metricPlan.Reason) -ForegroundColor DarkCyan
} else {
    Write-Host ("Configured : quality targeting is OFF -- {0}" -f $metricPlan.Reason) -ForegroundColor Yellow
}

$svtPairs = Get-SvtAv1EfficiencyParamPairs -SourceProfile $sourceProfile -FrameRate $frameRate
Write-Host ("Encoder    : preset {0} / film-grain {1} / {2}" -f $SamplePreset, $SampleFilmGrain, (($svtPairs -join ':')))
Write-Host ''

# =============================================================================
# Sweep
# =============================================================================
$crfValues = New-Object System.Collections.Generic.List[int]
foreach ($token in ($CrfList -split '[,;\s]+')) {
    if ([string]::IsNullOrWhiteSpace($token)) { continue }
    $parsed = 0
    if (-not [int]::TryParse($token.Trim(), [ref]$parsed)) { throw "Not a CRF value: '$token'" }
    if ($parsed -lt 0 -or $parsed -gt 63) { throw "CRF out of range (0-63): $parsed" }
    if (-not $crfValues.Contains($parsed)) { $crfValues.Add($parsed) }
}
if ($crfValues.Count -lt 1) { throw 'No CRF values to measure.' }

$positions = Get-QualitySamplePositions -SourceDurationSec $durationSec -SampleDurationSec $SampleDurationSec -RequestedCount $SampleCount
if ($positions.Count -lt 1) { throw 'The source is too short for the requested sample spacing.' }

Write-Host ("Sampling at {0} position(s), {1}s each: {2}" -f `
    $positions.Count, $SampleDurationSec, (($positions | ForEach-Object { (Format-Duration -Seconds $_) }) -join ', ')) -ForegroundColor DarkGray
Write-Host ("Measuring {0} CRF value(s): {1}" -f $crfValues.Count, (($crfValues | Sort-Object) -join ', ')) -ForegroundColor DarkGray
Write-Host ''

$xpsnrPlan = [pscustomobject]@{ Enabled = $true; Metric = 'XPSNR'; FilterSpec = 'xpsnr' }
$vmafModel = Get-VmafModelName -ResolutionTier $resolutionTier
if ($vmafModel -eq 'vmaf_4k_v0.6.1' -and -not $toolchain.SupportsVmaf4kModel) { $vmafModel = 'vmaf_v0.6.1' }
$vmafPlan  = [pscustomobject]@{
    Enabled = $true; Metric = 'VMAF'
    FilterSpec = ("libvmaf=model='version={0}':n_threads={1}" -f $vmafModel, [Math]::Max(1, [Math]::Min(16, [int]$QualityVmafThreads)))
}

$rows = New-Object System.Collections.Generic.List[object]
$keptDir = Join-Path $PreflightDir ('samples_' + (Get-Date -Format 'yyyyMMdd_HHmmss'))
if ($KeepSamples) { $null = New-Item -ItemType Directory -Force -Path $keptDir }

$bsf = [string](Get-OptionalProperty -InputObject $hdrPlan -PropertyName 'InputBitstreamFilter' -Default '')
$videoIndex = [int](Get-StreamProp $video 'index' 0)

foreach ($crf in ($crfValues | Sort-Object)) {
    $xpsnrValues = [System.Collections.Generic.List[double]]::new()
    $vmafValues  = [System.Collections.Generic.List[double]]::new()
    $rateValues  = [System.Collections.Generic.List[double]]::new()
    $failures    = [System.Collections.Generic.List[string]]::new()

    Write-Host ("CRF {0,2} ... " -f $crf) -NoNewline -ForegroundColor DarkCyan

    for ($i = 0; $i -lt $positions.Count; $i++) {
        $startSec = [double]$positions[$i]
        $samplePath = if ($KeepSamples) {
            Join-Path $keptDir ("crf{0:d2}_pos{1}.mkv" -f $crf, $i)
        } else {
            Join-Path $PreflightDir ("cal_{0}_{1}.mkv" -f $crf, [Guid]::NewGuid().ToString('N'))
        }

        try {
            $ffArgs = Build-PreflightSampleArgs `
                -InputPath $Path -Selected $selected -SourceProfile $sourceProfile `
                -EncodeMode 'software' -StartSec $startSec -DurationSec $SampleDurationSec `
                -ResolvedCRF $crf -ResolvedPreset $SamplePreset -ResolvedFilmGrain $SampleFilmGrain `
                -HdrPlan $hdrPlan -OutputPath $samplePath

            $enc = Invoke-FfmpegSync -Arguments $ffArgs
            if ($enc.ExitCode -ne 0 -or -not (Test-Path -LiteralPath $samplePath)) {
                $failures.Add(("encode failed at {0}s: {1}" -f [int]$startSec, (($enc.LogLines | Select-Object -Last 1) -join '')))
                continue
            }

            $item = Get-Item -LiteralPath $samplePath
            if ($item.Length -le 0) { $failures.Add('empty sample'); continue }
            $rateValues.Add($item.Length / [double]$SampleDurationSec)

            if ($toolchain.SupportsXpsnr) {
                $mx = Measure-SampleQuality -SamplePath $samplePath -SourcePath $Path -StartSec $startSec `
                        -DurationSec $SampleDurationSec -SourceVideoStreamIndex $videoIndex `
                        -InputBitstreamFilter $bsf -MetricPlan $xpsnrPlan
                if ($mx.Measured) { $xpsnrValues.Add([double]$mx.Value) } else { $failures.Add("xpsnr: $($mx.Detail)") }
            }
            if ($toolchain.SupportsVmaf) {
                $mv = Measure-SampleQuality -SamplePath $samplePath -SourcePath $Path -StartSec $startSec `
                        -DurationSec $SampleDurationSec -SourceVideoStreamIndex $videoIndex `
                        -InputBitstreamFilter $bsf -MetricPlan $vmafPlan
                if ($mv.Measured) { $vmafValues.Add([double]$mv.Value) } else { $failures.Add("vmaf: $($mv.Detail)") }
            }
        } finally {
            if (-not $KeepSamples -and (Test-Path -LiteralPath $samplePath)) {
                Remove-Item -LiteralPath $samplePath -Force -ErrorAction SilentlyContinue
            }
        }
    }

    if ($rateValues.Count -eq 0) {
        Write-Host ("no measurement ({0})" -f (($failures | Select-Object -First 1) -join '')) -ForegroundColor Red
        continue
    }

    $bytesPerSec = Get-MedianValue -Values $rateValues
    $gibPerHour  = ($bytesPerSec * 3600.0) / 1GB
    $pctOfSource = if ($sourceGiBPerHour -gt 0) { ($gibPerHour / $sourceGiBPerHour) * 100.0 } else { 0.0 }

    $row = [pscustomobject][ordered]@{
        CRF            = $crf
        GiBPerHour     = [Math]::Round($gibPerHour, 3)
        PctOfSource    = [Math]::Round($pctOfSource, 1)
        SavingsPercent = [Math]::Round(100.0 - $pctOfSource, 1)
        VMAF           = if ($vmafValues.Count  -gt 0) { [Math]::Round((Get-MedianValue -Values $vmafValues), 3) }  else { $null }
        XPSNR          = if ($xpsnrValues.Count -gt 0) { [Math]::Round((Get-MedianValue -Values $xpsnrValues), 3) } else { $null }
        Samples        = $rateValues.Count
        Notes          = (($failures | Select-Object -First 1) -join '')
    }
    $rows.Add($row)

    Write-Host ("{0,6:F2} GiB/hr  {1,5:F1}% of source   VMAF {2}   XPSNR {3}" -f `
        $row.GiBPerHour, $row.PctOfSource,
        $(if ($null -ne $row.VMAF)  { ("{0,7:F3}" -f $row.VMAF) }  else { '      -' }),
        $(if ($null -ne $row.XPSNR) { ("{0,7:F3}" -f $row.XPSNR) } else { '      -' })) -ForegroundColor Gray
}

if ($rows.Count -eq 0) { throw 'No CRF produced a measurement. Nothing to report.' }

# =============================================================================
# Report
# =============================================================================
Write-Host ''
Write-Host '--- Curve ---------------------------------------------------' -ForegroundColor Cyan
# Rendered by hand rather than with Format-Table: Format-Table's output is
# emitted through the formatting pipeline and does not interleave predictably
# with the Write-Host lines above and below it, so the table can end up blank
# or out of order depending on how the script was launched.
Write-Host ('{0,4}  {1,11}  {2,9}  {3,9}  {4,9}  {5,9}  {6,7}' -f 'CRF', 'GiB/hr', '% of src', 'saved %', 'VMAF', 'XPSNR', 'samples') -ForegroundColor DarkGray
Write-Host ('{0,4}  {1,11}  {2,9}  {3,9}  {4,9}  {5,9}  {6,7}' -f '----', '-----------', '---------', '---------', '---------', '---------', '-------') -ForegroundColor DarkGray
foreach ($row in $rows) {
    Write-Host ('{0,4}  {1,11:F3}  {2,9:F1}  {3,9:F1}  {4,9}  {5,9}  {6,7}' -f `
        $row.CRF, $row.GiBPerHour, $row.PctOfSource, $row.SavingsPercent,
        $(if ($null -ne $row.VMAF)  { ('{0:F3}' -f $row.VMAF) }  else { '-' }),
        $(if ($null -ne $row.XPSNR) { ('{0:F3}' -f $row.XPSNR) } else { '-' }),
        $row.Samples)
}
Write-Host ('{0,4}  {1,11:F3}   <- the source itself' -f 'src', $sourceGiBPerHour) -ForegroundColor DarkGray
Write-Host ''

Write-Host '--- Where your configured thresholds land -------------------' -ForegroundColor Cyan

$withVmaf = @($rows | Where-Object { $null -ne $_.VMAF })
if ($withVmaf.Count -gt 0) {
    $vmafPassing = @($withVmaf | Where-Object { [double]$_.VMAF -ge [double]$QualityVmafTarget } | Sort-Object -Property CRF -Descending)
    if ($vmafPassing.Count -gt 0) {
        $pick = $vmafPassing[0]
        Write-Host ("VMAF >= {0}  -> CRF {1}, {2:F2} GiB/hr, {3:F1}% smaller than the source" -f `
            $QualityVmafTarget, $pick.CRF, $pick.GiBPerHour, $pick.SavingsPercent) -ForegroundColor Green
        if ($pick.CRF -eq ($rows | Sort-Object CRF -Descending)[0].CRF) {
            Write-Host '  Note: that is the highest CRF measured, so the real limit is higher. Extend -CrfList to find it.' -ForegroundColor Yellow
        }
    } else {
        Write-Host ("VMAF >= {0}  -> not reached at any measured CRF. The lowest CRF measured scored {1:F3}." -f `
            $QualityVmafTarget, ($withVmaf | Sort-Object CRF)[0].VMAF) -ForegroundColor Yellow
        Write-Host '  This source needs a lower CRF than you measured, or it cannot be shrunk transparently.' -ForegroundColor Yellow
    }
}

$withXpsnr = @($rows | Where-Object { $null -ne $_.XPSNR })
$anchorRow = @($withXpsnr | Where-Object { $_.CRF -eq [int]$QualityAnchorCRF })
if ($withXpsnr.Count -gt 0) {
    if ($anchorRow.Count -eq 1) {
        $threshold = [double]$anchorRow[0].XPSNR - [double]$QualityXpsnrAnchorDropDb
        $xpsnrPassing = @($withXpsnr | Where-Object { [double]$_.XPSNR -ge $threshold } | Sort-Object -Property CRF -Descending)
        $pick = if ($xpsnrPassing.Count -gt 0) { $xpsnrPassing[0] } else { $null }
        Write-Host ("XPSNR anchor CRF {0} = {1:F3} dB, threshold {2:F3} dB (drop {3})" -f `
            $QualityAnchorCRF, $anchorRow[0].XPSNR, $threshold, $QualityXpsnrAnchorDropDb) -ForegroundColor DarkCyan
        if ($null -ne $pick) {
            Write-Host ("  -> CRF {0}, {1:F2} GiB/hr, {2:F1}% smaller than the source" -f `
                $pick.CRF, $pick.GiBPerHour, $pick.SavingsPercent) -ForegroundColor Green
        } else {
            Write-Host '  -> no measured CRF met it.' -ForegroundColor Yellow
        }
    } else {
        Write-Host ("XPSNR anchor: CRF {0} was not in the measured list, so the anchored threshold cannot be shown." -f $QualityAnchorCRF) -ForegroundColor Yellow
        Write-Host ("  Re-run with -CrfList including {0} to see it." -f $QualityAnchorCRF) -ForegroundColor Yellow
    }

    # The slope is the number that says whether an absolute dB threshold could
    # ever work on this content, and it is why the anchored form exists.
    $sortedX = @($withXpsnr | Sort-Object CRF)
    if ($sortedX.Count -ge 2) {
        $lo = $sortedX[0]; $hi = $sortedX[$sortedX.Count - 1]
        $slope = ([double]$lo.XPSNR - [double]$hi.XPSNR) / [double]($hi.CRF - $lo.CRF)
        Write-Host ("XPSNR slope on this content: {0:F3} dB per CRF step (range {1:F2}-{2:F2} dB)" -f `
            $slope, $hi.XPSNR, $lo.XPSNR) -ForegroundColor DarkGray
        Write-Host '  A fixed dB threshold is only meaningful if this number is similar across your library. It usually is not.' -ForegroundColor DarkGray
    }
}

Write-Host ''
Write-Host '--- What to do with this ------------------------------------' -ForegroundColor Cyan
Write-Host 'If the chosen CRF looks too aggressive, lower $QualityAnchorCRF (HDR) or raise'
Write-Host '$QualityVmafTarget (SDR). If output is bigger than you want, do the opposite.'
if ($KeepSamples) {
    Write-Host ''
    Write-Host ("Samples kept in: {0}" -f $keptDir) -ForegroundColor Green
    Write-Host 'Watch them on the display you actually use. No metric replaces that step.'
} else {
    Write-Host ''
    Write-Host 'Re-run with -KeepSamples to keep the clips and watch them on your own TV.' -ForegroundColor DarkGray
}

if (-not [string]::IsNullOrWhiteSpace($Csv)) {
    $rows | Export-Csv -LiteralPath $Csv -NoTypeInformation -Encoding UTF8
    Write-Host ''
    Write-Host ("Curve written to: {0}" -f $Csv) -ForegroundColor Green
}

Write-Host ''

}

# =============================================================================
# LibraryScan
# Merged from Media2AV1Queue-LibraryScan.ps1. Body unchanged; see Build-Single.ps1.
# =============================================================================
function Invoke-LibraryScanTool {
param(
    [Parameter(Mandatory = $true)][string[]]$Path,
    [string]$Csv = '',
    [int]$ThrottleLimit = 0,
    [switch]$QuickScan,
    [switch]$IncludeWorkFolders,
    [string[]]$Extension = @('.mkv','.mp4','.m4v','.ts','.m2ts','.avi','.mov','.wmv','.webm','.mpg','.mpeg','.vob')
)



# --- normalise -Path -------------------------------------------------------
# `pwsh -File script.ps1 -Path "A","B"` does NOT bind an array. The shell hands
# the whole thing over as a single string, quotes and comma included, so the
# script sees one nonexistent path literally named:
#     "G:\Movies","G:\TV"
# Splitting on commas and stripping stray quotes makes both invocation styles
# work -- the dot-sourced/`-Command` form that really does pass an array, and
# the `-File` form that passes one string.
$normalisedPaths = New-Object System.Collections.Generic.List[string]
foreach ($rawPath in @($Path)) {
    if ([string]::IsNullOrWhiteSpace($rawPath)) { continue }
    foreach ($piece in ([string]$rawPath -split ',')) {
        $clean = $piece.Trim().Trim('"').Trim("'").Trim()
        if (-not [string]::IsNullOrWhiteSpace($clean)) { $normalisedPaths.Add($clean) }
    }
}
if ($normalisedPaths.Count -eq 0) { throw "No usable path was given to -Path." }
$Path = $normalisedPaths.ToArray()

# --- locate ffprobe, same convention as the other scripts --------------------
$ffprobe = $null
foreach ($c in @((Join-Path $PSScriptRoot 'ffprobe.exe'), (Join-Path $PSScriptRoot 'ffprobe'))) {
    if (Test-Path -LiteralPath $c -PathType Leaf) { $ffprobe = (Get-Item -LiteralPath $c).FullName; break }
}
if (-not $ffprobe) {
    $cmd = Get-Command ffprobe -ErrorAction SilentlyContinue
    if ($cmd) { $ffprobe = $cmd.Source }
}
if (-not $ffprobe) { throw "ffprobe was not found next to this script or on PATH." }

if ($ThrottleLimit -le 0) {
    # Each worker is a short-lived ffprobe process, so oversubscribing slightly
    # past the physical core count keeps the disk queue busy without thrashing.
    $ThrottleLimit = [Math]::Max(4, [Environment]::ProcessorCount / 2)
}

if ([string]::IsNullOrWhiteSpace($Csv)) {
    $Csv = Join-Path (Get-Location) ("hdr_census_{0}.csv" -f (Get-Date -Format 'yyyyMMdd_HHmmss'))
}

Write-Host ""
Write-Host "Media2AV1Queue library HDR census" -ForegroundColor Cyan
Write-Host ("  ffprobe      : {0}" -f $ffprobe) -ForegroundColor DarkGray
Write-Host ("  roots        : {0}" -f ($Path -join ', ')) -ForegroundColor DarkGray
Write-Host ("  parallelism  : {0}" -f $ThrottleLimit) -ForegroundColor DarkGray
Write-Host ("  mode         : {0}" -f $(if ($QuickScan) { 'quick (no frame probe, HDR10+ not detectable)' } else { 'full (frame probe on HDR candidates)' })) -ForegroundColor DarkGray
Write-Host ""

# --- exclusions --------------------------------------------------------------
# The scripts keep working folders under the script directory: .queue for the
# job queue and preflight samples, .verify for the verifier's sandbox copies.
# Those contain real video files with real HDR metadata, so a plain recursive
# walk counts them as library content -- which is how a verifier run ended up
# adding phantom HDR10+ and Dolby Vision entries to a census. They are excluded
# by default; -IncludeWorkFolders overrides that if you ever want to see them.
$workFolderNames = @('.queue', '.verify', '_parked', '_to_delete')

function Test-InWorkFolder {
    param([string]$FullPath)

    $segments = $FullPath -split '[\\/]'
    foreach ($seg in $segments) {
        if ($workFolderNames -contains $seg.ToLowerInvariant()) { return $true }
    }
    # Also catch the encoder's in-progress temp outputs wherever they land.
    $leaf = [System.IO.Path]::GetFileName($FullPath)
    if ($leaf -match '(?i)\.encoding\.tmp\.') { return $true }
    return $false
}

# --- enumerate ---------------------------------------------------------------
Write-Host "Enumerating files..." -ForegroundColor DarkCyan
$files = [System.Collections.Generic.List[string]]::new()
foreach ($root in $Path) {
    if (-not (Test-Path -LiteralPath $root)) {
        Write-Warning "Path not found, skipping: $root"
        continue
    }
    $item = Get-Item -LiteralPath $root
    if ($item.PSIsContainer) {
        Get-ChildItem -LiteralPath $item.FullName -File -Recurse -ErrorAction SilentlyContinue |
            Where-Object { $Extension -contains $_.Extension.ToLowerInvariant() } |
            Where-Object { $IncludeWorkFolders -or -not (Test-InWorkFolder -FullPath $_.FullName) } |
            ForEach-Object { $files.Add($_.FullName) }
    } elseif ($Extension -contains $item.Extension.ToLowerInvariant()) {
        $files.Add($item.FullName)
    }
}

if ($files.Count -eq 0) {
    Write-Warning "No video files found under the given path(s)."
    $script:LastToolExitCode = 0; return
}
Write-Host ("  {0} video file(s) found" -f $files.Count) -ForegroundColor Gray
if (-not $IncludeWorkFolders) {
    Write-Host ("  (excluding the scripts' own working folders: {0})" -f ($workFolderNames -join ', ')) -ForegroundColor DarkGray
}
Write-Host ""

# --- probe in parallel -------------------------------------------------------
# Runs in separate runspaces, so everything the worker needs is either passed
# with $using: or defined inline. No helper functions from this scope are
# visible inside the block.
$progressCounter = [System.Collections.Concurrent.ConcurrentDictionary[string,int]]::new()
$null = $progressCounter.TryAdd('done', 0)
$total = $files.Count

Write-Host "Probing..." -ForegroundColor DarkCyan

$results = $files | ForEach-Object -ThrottleLimit $ThrottleLimit -Parallel {
    $file      = $_
    $ffprobe   = $using:ffprobe
    $quick     = $using:QuickScan
    $counter   = $using:progressCounter
    $total     = $using:total

    $row = [ordered]@{
        File            = $file
        Name            = [System.IO.Path]::GetFileName($file)
        SizeGiB         = ''
        DurationMin     = ''
        Codec           = ''
        Width           = ''
        Height          = ''
        BitDepth        = ''
        HdrFormat       = 'unknown'
        Transfer        = ''
        Primaries       = ''
        HasMasteringDisplay = ''
        MasteringMaxNits = ''
        MasteringMinNits = ''
        MaxCLL          = ''
        MaxFALL         = ''
        HasHDR10Plus    = ''
        DvProfile       = ''
        DvCompatId      = ''
        MetadataOrigin  = ''
        Convertible     = ''
        Note            = ''
    }

    try {
        $row.SizeGiB = [Math]::Round((Get-Item -LiteralPath $file).Length / 1GB, 3)

        # ---- Phase 1: cheap header probe -----------------------------------
        $json = & $ffprobe -v error -print_format json -show_streams -show_format $file 2>$null
        if (-not $json) { $row.Note = 'ffprobe returned nothing'; $row.HdrFormat = 'probe failed'; return [pscustomobject]$row }

        $probe = $json | ConvertFrom-Json -Depth 100
        $video = @($probe.streams | Where-Object { $_.codec_type -eq 'video' }) | Select-Object -First 1
        if (-not $video) { $row.Note = 'no video stream'; $row.HdrFormat = 'no video'; return [pscustomobject]$row }

        function Prop($obj, $name) {
            if ($null -eq $obj) { return $null }
            $p = $obj.PSObject.Properties[$name]
            if ($null -eq $p -or $null -eq $p.Value) { return $null }
            return $p.Value
        }

        $row.Codec  = [string](Prop $video 'codec_name')
        $row.Width  = [string](Prop $video 'width')
        $row.Height = [string](Prop $video 'height')
        $pixFmt     = [string](Prop $video 'pix_fmt')
        $row.BitDepth = if ($pixFmt -match '10') { 10 } elseif ($pixFmt -match '12') { 12 } else { 8 }

        $transfer  = [string](Prop $video 'color_transfer')
        $primaries = [string](Prop $video 'color_primaries')
        $row.Transfer  = $transfer
        $row.Primaries = $primaries

        $dur = Prop $probe.format 'duration'
        if ($dur) { $row.DurationMin = [Math]::Round([double]$dur / 60.0, 1) }

        # Dolby Vision configuration record, if present at stream level.
        $dvProfile = $null; $dvCompat = $null
        $streamSide = Prop $video 'side_data_list'
        $mdFound = $false; $clContent = $null; $clAverage = $null; $h10 = $false
        $mdMaxNits = $null; $mdMinNits = $null
        $origin = 'none'

        foreach ($sd in @($streamSide)) {
            if ($null -eq $sd) { continue }
            $t = [string](Prop $sd 'side_data_type')
            if ($t -match '(?i)DOVI|Dolby\s*Vision') {
                $dvProfile = Prop $sd 'dv_profile'
                $dvCompat  = Prop $sd 'dv_bl_signal_compatibility_id'
                $origin = 'stream'
            } elseif ($t -match '(?i)mastering\s*display') {
                $mdFound = $true; $origin = 'stream'
                # Declared grading-monitor luminance, in nits. ffprobe reports it
                # as a rational in units of 0.0001 cd/m^2.
                foreach ($lk in @(@('max_luminance','maxNits'), @('min_luminance','minNits'))) {
                    $raw = Prop $sd $lk[0]
                    if ($null -ne $raw -and "$raw" -match '^\s*(-?\d+)\s*/\s*(-?\d+)\s*$' -and [double]$Matches[2] -ne 0) {
                        $val = [double]$Matches[1] / [double]$Matches[2]
                        if ($lk[1] -eq 'maxNits') { $mdMaxNits = $val } else { $mdMinNits = $val }
                    }
                }
            } elseif ($t -match '(?i)content\s*light') {
                $clContent = Prop $sd 'max_content'; $clAverage = Prop $sd 'max_average'; $origin = 'stream'
            } elseif ($t -match '(?i)HDR10\+|2094-40|Dynamic\s*HDR') {
                $h10 = $true; $origin = 'stream'
            }
        }

        # Codec tag can also mark Dolby Vision.
        $codecTag = [string](Prop $video 'codec_tag_string')
        $taggedDv = ($row.Codec -match 'dvhe|dvav') -or ($codecTag -match 'dvhe|dvav|dvh1|dav1')

        $looksHdr = ($transfer -match 'smpte2084|arib-std-b67|bt2020-10') -or ($primaries -match 'bt2020') -or $taggedDv -or ($null -ne $dvProfile)

        # ---- Phase 2: frame probe, HDR candidates only ---------------------
        # Skipped for SDR (nothing to find) and in -QuickScan mode.
        if ($looksHdr -and -not $quick -and (-not $mdFound -or -not $h10 -or ($null -eq $dvProfile -and $taggedDv))) {
            $idx = [string](Prop $video 'index')
            $fj = & $ffprobe -v error -print_format json -show_frames -read_intervals '%+#1' -select_streams "$idx" $file 2>$null
            if ($fj) {
                try {
                    $fp = $fj | ConvertFrom-Json -Depth 100
                    foreach ($fr in @($fp.frames)) {
                        foreach ($sd in @(Prop $fr 'side_data_list')) {
                            if ($null -eq $sd) { continue }
                            $t = [string](Prop $sd 'side_data_type')
                            if ($t -match '(?i)mastering\s*display') {
                                if (-not $mdFound) { $mdFound = $true; if ($origin -eq 'none') { $origin = 'frame' } }
                                foreach ($lk in @(@('max_luminance','maxNits'), @('min_luminance','minNits'))) {
                                    $raw = Prop $sd $lk[0]
                                    if ($null -ne $raw -and "$raw" -match '^\s*(-?\d+)\s*/\s*(-?\d+)\s*$' -and [double]$Matches[2] -ne 0) {
                                        $val = [double]$Matches[1] / [double]$Matches[2]
                                        if ($lk[1] -eq 'maxNits' -and $null -eq $mdMaxNits) { $mdMaxNits = $val }
                                        elseif ($lk[1] -eq 'minNits' -and $null -eq $mdMinNits) { $mdMinNits = $val }
                                    }
                                }
                            }
                            elseif ($t -match '(?i)content\s*light') { if ($null -eq $clContent) { $clContent = Prop $sd 'max_content'; $clAverage = Prop $sd 'max_average'; if ($origin -eq 'none') { $origin = 'frame' } } }
                            elseif ($t -match '(?i)HDR10\+|2094-40|Dynamic\s*HDR') { $h10 = $true; if ($origin -eq 'none') { $origin = 'frame' } }
                            elseif ($t -match '(?i)DOVI|Dolby\s*Vision') {
                                if ($null -eq $dvProfile) { $dvProfile = Prop $sd 'dv_profile'; $dvCompat = Prop $sd 'dv_bl_signal_compatibility_id'; if ($origin -eq 'none') { $origin = 'frame' } }
                            }
                        }
                    }
                } catch { }
            }
        }

        $row.HasMasteringDisplay = "$mdFound"
        $row.MasteringMaxNits = if ($null -ne $mdMaxNits) { [Math]::Round($mdMaxNits, 1) } else { '' }
        $row.MasteringMinNits = if ($null -ne $mdMinNits) { $mdMinNits.ToString('0.#####', [System.Globalization.CultureInfo]::InvariantCulture) } else { '' }
        $row.MaxCLL  = if ($null -ne $clContent) { "$clContent" } else { '' }
        $row.MaxFALL = if ($null -ne $clAverage) { "$clAverage" } else { '' }
        $row.HasHDR10Plus = if ($quick) { 'not checked' } else { "$h10" }
        $row.DvProfile  = if ($null -ne $dvProfile) { "$dvProfile" } else { '' }
        $row.DvCompatId = if ($null -ne $dvCompat)  { "$dvCompat"  } else { '' }
        $row.MetadataOrigin = $origin

        # ---- Classify -------------------------------------------------------
        $isDv = ($null -ne $dvProfile) -or $taggedDv
        $row.HdrFormat =
            if ($isDv)                                  { 'Dolby Vision' }
            elseif ($h10)                               { 'HDR10+' }
            elseif ($transfer -match 'arib-std-b67')    { 'HLG' }
            elseif ($transfer -match 'smpte2084')       { 'HDR10' }
            elseif ($transfer -match 'bt2020-10' -or $primaries -match 'bt2020') { 'HDR10 (implied)' }
            else                                        { 'SDR' }

        # ---- What the encoder would do with it ------------------------------
        if ($isDv) {
            $p = if ($null -ne $dvProfile) { [int]$dvProfile } else { -1 }
            $c = if ($null -ne $dvCompat)  { [int]$dvCompat }  else { -1 }
            switch ($p) {
                7 { $row.Convertible = 'yes'; $row.Note = 'Profile 7: base layer extracted via dovi_split -> HDR10' }
                8 {
                    if ($c -eq 1)     { $row.Convertible = 'yes'; $row.Note = 'Profile 8.1: RPU stripped -> HDR10' }
                    elseif ($c -eq 4) { $row.Convertible = 'yes'; $row.Note = 'Profile 8.4: -> HLG' }
                    elseif ($c -eq 2) { $row.Convertible = 'yes'; $row.Note = 'Profile 8.2: -> SDR' }
                    else              { $row.Convertible = 'NO';  $row.Note = 'Profile 8 with unusable compat id -> SKIPPED' }
                }
                # Profile 10 is Dolby Vision carried in AV1 rather than HEVC. Its
                # cross-compatibility IDs mean the same things as Profile 8's, so
                # 10.1 has an HDR10-compatible base layer and converts cleanly.
                # A P10 file is also already AV1, so re-encoding it usually gains
                # nothing -- flagged here so it is an informed decision.
                10 {
                    if ($c -eq 1)     { $row.Convertible = 'yes'; $row.Note = 'Profile 10.1 (DV in AV1): HDR10-compatible base layer. NOTE: already AV1 -- re-encoding likely pointless' }
                    elseif ($c -eq 4) { $row.Convertible = 'yes'; $row.Note = 'Profile 10.4 (DV in AV1): -> HLG. NOTE: already AV1 -- re-encoding likely pointless' }
                    elseif ($c -eq 2) { $row.Convertible = 'yes'; $row.Note = 'Profile 10.2 (DV in AV1): -> SDR. NOTE: already AV1 -- re-encoding likely pointless' }
                    else              { $row.Convertible = 'NO';  $row.Note = 'Profile 10.0 (DV in AV1): no compatible base layer -> SKIPPED (and already AV1)' }
                }
                5 { $row.Convertible = 'NO'; $row.Note = 'Profile 5: no HDR10-compatible base layer -> SKIPPED' }
                default { $row.Convertible = 'NO'; $row.Note = "Unrecognised DV configuration (profile $p) -> SKIPPED" }
            }
        } elseif ($h10) {
            $row.Convertible = 'yes'
            $row.Note = 'HDR10+ source: dynamic metadata needs the standalone SVT-AV1 route, otherwise static HDR10'
        } else {
            $row.Convertible = 'yes'
            if ($row.HdrFormat -like 'HDR10*' -and -not $mdFound) {
                $row.Note = 'HDR10 but NO mastering display metadata in the source -- nothing to carry'
            }
        }
    } catch {
        $row.HdrFormat = 'probe failed'
        $row.Note = $_.Exception.Message
    }

    # Printed every 25 completions. Under parallelism the counter rarely lands
    # exactly on $total inside the same iteration that prints, so the tail is
    # reported once after the pipeline drains rather than relied on here.
    $n = $counter.AddOrUpdate('done', 1, { param($k, $v) $v + 1 })
    if (($n % 25) -eq 0) {
        Write-Host ("`r  {0} / {1} probed ({2:P0})" -f $n, $total, ($n / $total)) -NoNewline
    }

    return [pscustomobject]$row
}

Write-Host ("`r  {0} / {1} probed (100%)   " -f @($results).Count, $total)
Write-Host ""

# --- report ------------------------------------------------------------------
$results | Export-Csv -LiteralPath $Csv -NoTypeInformation -Encoding UTF8

function Bar([int]$n, [int]$max, [int]$width = 34) {
    if ($max -le 0) { return '' }
    $filled = [int][Math]::Round($width * ($n / [double]$max))
    return ([string][char]0x2588 * $filled) + ([string][char]0x2591 * ($width - $filled))
}

# Measure-Object returns nothing at all for an empty input set, so reading .Sum
# off it throws under Set-StrictMode -Version Latest. Summed explicitly instead,
# skipping the blanks that probe failures leave behind.
function Sum-SizeGiB {
    param($Rows)

    $total = 0.0
    foreach ($r in @($Rows)) {
        if ($null -eq $r) { continue }
        $v = $r.SizeGiB
        if ($null -eq $v -or "$v" -eq '') { continue }
        $parsed = 0.0
        if ([double]::TryParse("$v", [System.Globalization.NumberStyles]::Float,
                               [System.Globalization.CultureInfo]::InvariantCulture, [ref]$parsed)) {
            $total += $parsed
        }
    }
    return [Math]::Round($total, 1)
}

Write-Host ("=" * 78) -ForegroundColor DarkCyan
Write-Host " HDR format census" -ForegroundColor Cyan
Write-Host ("=" * 78) -ForegroundColor DarkCyan

$groups = $results | Group-Object HdrFormat | Sort-Object Count -Descending
$maxCount = 0
foreach ($g in $groups) { if ($g.Count -gt $maxCount) { $maxCount = $g.Count } }
foreach ($g in $groups) {
    $gib = Sum-SizeGiB -Rows $g.Group
    $colour = switch -Wildcard ($g.Name) {
        'HDR10+'        { 'Magenta' }
        'Dolby Vision'  { 'Blue' }
        'HDR10*'        { 'Cyan' }
        'HLG'           { 'DarkCyan' }
        'SDR'           { 'Gray' }
        default         { 'Yellow' }
    }
    Write-Host ("  {0,-16} {1,5}  {2,7:P1}  {3,9} GiB  {4}" -f `
        $g.Name, $g.Count, ($g.Count / [double]$results.Count), $gib, (Bar $g.Count $maxCount)) -ForegroundColor $colour
}

Write-Host ""
Write-Host ("=" * 78) -ForegroundColor DarkCyan
Write-Host " What this means for the encode" -ForegroundColor Cyan
Write-Host ("=" * 78) -ForegroundColor DarkCyan

$hdrAll   = @($results | Where-Object { $_.HdrFormat -notin @('SDR','probe failed','no video','unknown') })
$h10plus  = @($results | Where-Object { $_.HdrFormat -eq 'HDR10+' })
$dv       = @($results | Where-Object { $_.HdrFormat -eq 'Dolby Vision' })
$dvSkip   = @($dv | Where-Object { $_.Convertible -eq 'NO' })
$noMd     = @($hdrAll | Where-Object { $_.HasMasteringDisplay -eq 'False' -and $_.HdrFormat -ne 'HLG' })
$failed   = @($results | Where-Object { $_.HdrFormat -eq 'probe failed' })

Write-Host ("  HDR sources of any kind      : {0} of {1}" -f $hdrAll.Count, $results.Count)
Write-Host ("  Static HDR10 metadata to carry: {0}" -f @($hdrAll | Where-Object { $_.HasMasteringDisplay -eq 'True' }).Count) -ForegroundColor Green
if ($noMd.Count -gt 0) {
    Write-Host ("  HDR but no mastering display  : {0}  (nothing to carry; not a bug)" -f $noMd.Count) -ForegroundColor DarkGray
}

Write-Host ""
if ($QuickScan) {
    Write-Host "  HDR10+ : not checked (-QuickScan). Re-run without it to count them." -ForegroundColor Yellow
} else {
    $pct = if ($results.Count -gt 0) { $h10plus.Count / [double]$results.Count } else { 0 }
    Write-Host ("  HDR10+ sources                : {0} ({1:P1} of the library)" -f $h10plus.Count, $pct) -ForegroundColor Magenta
    # Judged on share of the library as well as raw count. Thirty HDR10+ files in
    # a 200-file library is a different proposition from thirty in 5000, and a
    # count alone gets that wrong.
    $h10Gib = Sum-SizeGiB -Rows $h10plus
    if ($h10plus.Count -eq 0) {
        Write-Host "    -> Nothing to preserve. The standalone SVT-AV1 pipeline would buy you nothing." -ForegroundColor Green
    } elseif ($pct -lt 0.02 -or $h10plus.Count -le 20) {
        Write-Host ("    -> A small, bounded set ({0} files, {1} GiB)." -f $h10plus.Count, $h10Gib) -ForegroundColor Yellow
        Write-Host "       Building the standalone SVT-AV1 route into the queue means new code in" -ForegroundColor Yellow
        Write-Host "       the encoder worker -- the riskiest part of the script -- to benefit well" -ForegroundColor Yellow
        Write-Host "       under 1% of the library. A separate one-off script for just these files," -ForegroundColor Yellow
        Write-Host "       or simply accepting static HDR10 for them, is the better trade." -ForegroundColor Yellow
        Write-Host "       Check the list below for samples and duplicates first -- the unique count" -ForegroundColor DarkGray
        Write-Host "       is often lower than it looks." -ForegroundColor DarkGray
    } else {
        Write-Host ("    -> {0} files ({1:P1}, {2} GiB): enough to justify the standalone SVT-AV1 route." -f $h10plus.Count, $pct, $h10Gib) -ForegroundColor Cyan
    }
    if ($h10plus.Count -gt 0) {
        Write-Host ""
        Write-Host "    HDR10+ titles:" -ForegroundColor DarkGray
        foreach ($f in ($h10plus | Select-Object -First 15)) { Write-Host ("      {0}" -f $f.Name) -ForegroundColor DarkGray }
        if ($h10plus.Count -gt 15) { Write-Host ("      ... and {0} more (see the CSV)" -f ($h10plus.Count - 15)) -ForegroundColor DarkGray }
    }
}

# ---- declared grading-monitor luminance -------------------------------------
# ST 2086 max_display_mastering_luminance states which monitor the colourist
# graded on. The encoder copies it verbatim, so a source that declares the wrong
# figure propagates that error into the output -- and the display then tone-maps
# against a false premise. Grouping the declarations makes outliers obvious.
$withNits = @($hdrAll | Where-Object { $_.MasteringMaxNits -ne '' })
if ($withNits.Count -gt 0) {
    Write-Host ""
    Write-Host "  Declared mastering-display peak (the monitor the content was graded on):" -ForegroundColor Cyan
    foreach ($g in ($withNits | Group-Object MasteringMaxNits | Sort-Object { [double]$_.Name })) {
        $nits = [double]$g.Name
        # 1000 and 4000 are the overwhelmingly common grading targets; 10000 is
        # the PQ container maximum and is almost always a lazy default rather
        # than a real monitor. Anything else is worth a look.
        $note, $colour = if ($nits -ge 9999) {
            'PQ container maximum -- almost certainly a placeholder, not a real grading monitor', 'Yellow'
        } elseif ($nits -in @(1000, 4000)) {
            'standard grading target', 'Green'
        } elseif ($nits -lt 400) {
            'unusually low for HDR -- named below', 'Yellow'
        } else {
            'uncommon but plausible', 'Gray'
        }
        Write-Host ("    {0,8:N0} nits  {1,4} file(s)   {2}" -f $nits, $g.Count, $note) -ForegroundColor $colour
    }
    # Content cannot be brighter than the monitor it was graded on, so MaxCLL
    # above the declared mastering peak means one of the two figures is wrong.
    # This is the same class of error as a 10000-nit placeholder, but it shows
    # up on files that otherwise look perfectly reasonable.
    $inconsistent = New-Object System.Collections.Generic.List[object]
    foreach ($r in $withNits) {
        if ([string]::IsNullOrWhiteSpace($r.MaxCLL)) { continue }
        $cll = 0.0; $mx = 0.0
        if (-not [double]::TryParse($r.MaxCLL, [System.Globalization.NumberStyles]::Float, [System.Globalization.CultureInfo]::InvariantCulture, [ref]$cll)) { continue }
        if (-not [double]::TryParse($r.MasteringMaxNits, [System.Globalization.NumberStyles]::Float, [System.Globalization.CultureInfo]::InvariantCulture, [ref]$mx)) { continue }
        # 2% slack absorbs rounding in the metadata round-trip.
        if ($mx -gt 0 -and $cll -gt ($mx * 1.02)) {
            $inconsistent.Add([pscustomobject]@{ Name = $r.Name; MaxCLL = $cll; Peak = $mx; Ratio = ($cll / $mx) })
        }
    }
    if ($inconsistent.Count -gt 0) {
        Write-Host ""
        Write-Host ("    {0} file(s) declare a MaxCLL brighter than their own grading monitor:" -f $inconsistent.Count) -ForegroundColor Yellow
        Write-Host  "    content cannot exceed the display it was graded on, so one of the two" -ForegroundColor DarkGray
        Write-Host  "    figures is wrong in the source. Preserved faithfully either way." -ForegroundColor DarkGray
        foreach ($f in ($inconsistent | Sort-Object Ratio -Descending | Select-Object -First 8)) {
            Write-Host ("      MaxCLL {0,6:N0} vs peak {1,6:N0} ({2:F1}x)  {3}" -f $f.MaxCLL, $f.Peak, $f.Ratio, $f.Name) -ForegroundColor DarkGray
        }
        if ($inconsistent.Count -gt 8) { Write-Host ("      ... and {0} more (see the CSV)" -f ($inconsistent.Count - 8)) -ForegroundColor DarkGray }
    }

    $lowNits = @($withNits | Where-Object { [double]$_.MasteringMaxNits -lt 400 })
    if ($lowNits.Count -gt 0) {
        Write-Host ""
        Write-Host ("    {0} file(s) declare an unusually low grading peak:" -f $lowNits.Count) -ForegroundColor Yellow
        foreach ($f in $lowNits) {
            Write-Host ("      {0,6:N0} nits  MaxCLL {1,-7} {2}" -f [double]$f.MasteringMaxNits, $(if ($f.MaxCLL) { $f.MaxCLL } else { '-' }), $f.Name) -ForegroundColor DarkGray
        }
        Write-Host  "    Sometimes genuine (an SDR-referred or low-peak grade), sometimes a" -ForegroundColor DarkGray
        Write-Host  "    mis-tagged conversion. Worth eyeballing on the TV before trusting it." -ForegroundColor DarkGray
    }

    $placeholder = @($withNits | Where-Object { [double]$_.MasteringMaxNits -ge 9999 })
    if ($placeholder.Count -gt 0) {
        Write-Host ""
        Write-Host ("    {0} file(s) declare 10000 nits. Your display will assume highlights may" -f $placeholder.Count) -ForegroundColor Yellow
        Write-Host  "    reach 10000 and reserve headroom for highlights that are not there, which" -ForegroundColor Yellow
        Write-Host  "    flattens the range that IS used. This is a flaw in the source, not the" -ForegroundColor Yellow
        Write-Host  "    encode -- but it is preserved faithfully, so it is worth knowing about." -ForegroundColor Yellow
        foreach ($f in ($placeholder | Select-Object -First 8)) { Write-Host ("      {0}" -f $f.Name) -ForegroundColor DarkGray }
        if ($placeholder.Count -gt 8) { Write-Host ("      ... and {0} more (see the CSV)" -f ($placeholder.Count - 8)) -ForegroundColor DarkGray }
    }
}

Write-Host ""
Write-Host ("  Dolby Vision sources          : {0}" -f $dv.Count) -ForegroundColor Blue
if ($dv.Count -gt 0) {
    foreach ($g in ($dv | Group-Object DvProfile | Sort-Object Name)) {
        $label = if ($g.Name) { "profile $($g.Name)" } else { 'profile not reported' }
        Write-Host ("      {0,-22} {1}" -f $label, $g.Count) -ForegroundColor DarkGray
    }
    if ($dvSkip.Count -gt 0) {
        Write-Host ("    -> {0} will be SKIPPED (no HDR10-compatible base layer)" -f $dvSkip.Count) -ForegroundColor Yellow
        foreach ($f in ($dvSkip | Select-Object -First 10)) { Write-Host ("      {0}" -f $f.Name) -ForegroundColor DarkGray }
        if ($dvSkip.Count -gt 10) { Write-Host ("      ... and {0} more" -f ($dvSkip.Count - 10)) -ForegroundColor DarkGray }
    } else {
        Write-Host "    -> all convertible to HDR10/HLG" -ForegroundColor Green
    }
}

if ($failed.Count -gt 0) {
    Write-Host ""
    Write-Host ("  Files ffprobe could not read  : {0}" -f $failed.Count) -ForegroundColor Yellow
    foreach ($f in ($failed | Select-Object -First 5)) { Write-Host ("      {0}" -f $f.Name) -ForegroundColor DarkGray }
}

Write-Host ""
Write-Host ("Full detail written to: {0}" -f $Csv) -ForegroundColor Green
Write-Host ""

}

# =============================================================================
# Bench
# Merged from Media2AV1Queue-Bench.ps1. Body unchanged; see Build-Single.ps1.
# =============================================================================
function Invoke-BenchTool {
param(
    [string]$Source = '',
    [switch]$Synthetic,
    [int]$Preset = 4,
    [int]$Crf = 28,
    [int]$SampleSeconds = 20,
    [double]$StartFraction = 0.35,
    [int]$Repeat = 2,
    [string]$WorkDir = ''
)



# --- tool discovery ---------------------------------------------------------
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
if (-not $ffmpeg)  { throw "ffmpeg was not found next to this script or on PATH." }
if (-not $ffprobe) { throw "ffprobe was not found next to this script or on PATH." }

$logicalCores = [Environment]::ProcessorCount

Write-Host ""
Write-Host "SVT-AV1 threading benchmark" -ForegroundColor Cyan
Write-Host ("  logical processors : {0}" -f $logicalCores) -ForegroundColor DarkGray
Write-Host ("  preset / crf       : {0} / {1}" -f $Preset, $Crf) -ForegroundColor DarkGray

if ([string]::IsNullOrWhiteSpace($WorkDir)) {
    $WorkDir = Join-Path ([System.IO.Path]::GetTempPath()) ("m2av1bench_" + [Guid]::NewGuid().ToString('N').Substring(0,8))
}
$null = New-Item -ItemType Directory -Force -Path $WorkDir

try {
    # --- prepare a lossless sample -----------------------------------------
    # Extracted once, losslessly, so every configuration encodes byte-identical
    # input. Decoding the original inside each timed run would measure the
    # decoder as much as the encoder.
    $sample = Join-Path $WorkDir 'sample.mkv'

    if ($Synthetic -or [string]::IsNullOrWhiteSpace($Source)) {
        if (-not $Synthetic) {
            throw "Provide -Source <file>, or pass -Synthetic to benchmark against a generated clip."
        }
        Write-Host "  source             : synthetic 3840x2160 test pattern" -ForegroundColor Yellow
        Write-Host "                       (compresses nothing like real film -- treat results as indicative only)" -ForegroundColor DarkYellow
        & $ffmpeg -hide_banner -loglevel error -y `
            -f lavfi -i "testsrc2=s=3840x2160:r=24:d=$SampleSeconds" `
            -c:v ffv1 -pix_fmt yuv420p10le $sample
    } else {
        if (-not (Test-Path -LiteralPath $Source)) { throw "Source not found: $Source" }
        Write-Host ("  source             : {0}" -f [System.IO.Path]::GetFileName($Source)) -ForegroundColor DarkGray

        $durJson = & $ffprobe -v error -print_format json -show_format $Source
        $dur = 0.0
        if ($durJson) {
            $fmt = ($durJson | ConvertFrom-Json -Depth 20).format
            $p = $fmt.PSObject.Properties['duration']
            if ($p -and $p.Value) { $dur = [double]$p.Value }
        }
        $startSec = if ($dur -gt ($SampleSeconds * 2)) { [Math]::Max(0.0, $dur * $StartFraction) } else { 0.0 }

        Write-Host ("  sample             : {0}s from {1:F0}s in, decoded to lossless FFV1" -f $SampleSeconds, $startSec) -ForegroundColor DarkGray
        & $ffmpeg -hide_banner -loglevel error -y `
            -ss ("{0:0.###}" -f $startSec) -t "$SampleSeconds" -i $Source `
            -map 0:v:0 -an -sn -dn `
            -c:v ffv1 -pix_fmt yuv420p10le $sample
    }

    if (-not (Test-Path -LiteralPath $sample)) { throw "Failed to build the benchmark sample." }

    $sInfo = & $ffprobe -v error -print_format json -show_streams -select_streams v:0 $sample
    $sv = ($sInfo | ConvertFrom-Json -Depth 20).streams[0]
    $sampleW = [int]$sv.width; $sampleH = [int]$sv.height
    # nb_frames is frequently absent -- notably for FFV1 in Matroska, which is
    # exactly what the sample is. Derived from duration x frame rate instead,
    # and only used for the fps column, so an estimate is fine.
    $frameCount = 0
    $nbf = $sv.PSObject.Properties['nb_frames']
    if ($nbf -and $nbf.Value -and [int]$nbf.Value -gt 0) {
        $frameCount = [int]$nbf.Value
    } else {
        $rateText = ''
        foreach ($f in 'avg_frame_rate','r_frame_rate') {
            $pp = $sv.PSObject.Properties[$f]
            if ($pp -and $pp.Value -and "$($pp.Value)" -ne '0/0') { $rateText = [string]$pp.Value; break }
        }
        $fps = 0.0
        if ($rateText -match '^(\d+)/(\d+)$' -and [double]$Matches[2] -ne 0) {
            $fps = [double]$Matches[1] / [double]$Matches[2]
        }
        $sampleDur = 0.0
        $dp = $sv.PSObject.Properties['duration']
        if ($dp -and $dp.Value) { $sampleDur = [double]$dp.Value }
        if ($sampleDur -le 0) { $sampleDur = [double]$SampleSeconds }
        if ($fps -gt 0 -and $sampleDur -gt 0) { $frameCount = [int][Math]::Round($fps * $sampleDur) }
    }
    Write-Host ("  sample resolution  : {0}x{1}{2}" -f $sampleW, $sampleH, $(if ($frameCount) { ", $frameCount frames" } else { '' })) -ForegroundColor DarkGray
    Write-Host ("  work dir           : {0}" -f $WorkDir) -ForegroundColor DarkGray
    Write-Host ""

    # --- one timed encode ---------------------------------------------------
    function Invoke-Encode {
        param(
            [string]$Label,
            [string[]]$SvtParams,
            [string]$OutFile,
            [switch]$PassThruObject
        )

        $ffArgs = New-Object System.Collections.Generic.List[string]
        $ffArgs.AddRange([string[]]@(
            '-hide_banner', '-loglevel', 'error', '-nostdin', '-y',
            '-i', $sample, '-map', '0:v:0', '-an', '-sn', '-dn',
            '-c:v', 'libsvtav1', '-preset', "$Preset", '-crf', "$Crf",
            '-pix_fmt', 'yuv420p10le'
        ))
        if ($SvtParams.Count -gt 0) {
            $ffArgs.AddRange([string[]]@('-svtav1-params', ($SvtParams -join ':')))
        }
        $ffArgs.Add($OutFile)

        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        $psi = [System.Diagnostics.ProcessStartInfo]::new()
        $psi.FileName = $ffmpeg
        foreach ($a in $ffArgs) { $psi.ArgumentList.Add($a) }
        $psi.RedirectStandardError = $true
        $psi.RedirectStandardOutput = $true
        $psi.UseShellExecute = $false
        $proc = [System.Diagnostics.Process]::Start($psi)
        $errText = $proc.StandardError.ReadToEnd()
        $null = $proc.StandardOutput.ReadToEnd()
        $proc.WaitForExit()
        $sw.Stop()

        $ok = ($proc.ExitCode -eq 0) -and (Test-Path -LiteralPath $OutFile)
        $bytes = if ($ok) { (Get-Item -LiteralPath $OutFile).Length } else { 0 }

        return [pscustomobject][ordered]@{
            Label   = $Label
            Ok      = $ok
            Seconds = [Math]::Round($sw.Elapsed.TotalSeconds, 2)
            Bytes   = $bytes
            Fps     = if ($ok -and $sw.Elapsed.TotalSeconds -gt 0 -and $frameCount -gt 0) {
                          [Math]::Round($frameCount / $sw.Elapsed.TotalSeconds, 2)
                      } else { 0 }
            Error   = if ($ok) { '' } else { (($errText -split "`n" | Where-Object { $_.Trim() }) | Select-Object -Last 1) }
        }
    }

    # --- configurations to compare -----------------------------------------
    # lp and pin are threading-only, so their output sizes must match the
    # baseline. tile-columns is included specifically to demonstrate the
    # efficiency cost that makes tiles the wrong tool here.
    $halfCores = [Math]::Max(1, [int]($logicalCores / 2))

    $configs = @(
        @{ Label = 'baseline (lp=0 auto, no pin)';    Params = @() }
        @{ Label = 'lp=6 (max parallelism)';          Params = @('lp=6') }
        @{ Label = "pin=$halfCores (half the cores)"; Params = @("pin=$halfCores") }
        @{ Label = 'tile-columns=1 (2 tile cols)';    Params = @('tile-columns=1') }
    )

    Write-Host ("=" * 78) -ForegroundColor DarkCyan
    Write-Host " Single-encode configurations" -ForegroundColor Cyan
    Write-Host ("=" * 78) -ForegroundColor DarkCyan

    # Each configuration is run $Repeat times and the MEDIAN wall time is used.
    # A single sample routinely varies by several percent from run to run --
    # enough to invent a winner that does not exist -- and the whole point of
    # measuring instead of guessing is defeated if the measurement is noise.
    function Get-Median {
        param([double[]]$Values)
        $sorted = @($Values | Sort-Object)
        if ($sorted.Count -eq 0) { return 0.0 }
        if ($sorted.Count % 2 -eq 1) { return $sorted[[int][Math]::Floor($sorted.Count / 2)] }
        return (($sorted[($sorted.Count / 2) - 1] + $sorted[$sorted.Count / 2]) / 2.0)
    }

    $single = New-Object System.Collections.Generic.List[object]
    $i = 0
    foreach ($c in $configs) {
        $i++
        Write-Host ("  [{0}/{1}] {2}" -f $i, $configs.Count, $c.Label)
        $times = New-Object System.Collections.Generic.List[double]
        $lastOk = $null
        $failure = ''
        for ($rep = 1; $rep -le $Repeat; $rep++) {
            Write-Host ("        run {0}/{1} ..." -f $rep, $Repeat) -NoNewline
            $out = Join-Path $WorkDir ("single_{0}_{1}.mkv" -f $i, $rep)
            $r = Invoke-Encode -Label $c.Label -SvtParams $c.Params -OutFile $out
            Remove-Item -LiteralPath $out -Force -ErrorAction SilentlyContinue
            if ($r.Ok) {
                $times.Add($r.Seconds)
                $lastOk = $r
                Write-Host ("  {0,7:F2}s  {1,8:F2} fps  {2,9:F2} MiB" -f $r.Seconds, $r.Fps, ($r.Bytes / 1MB)) -ForegroundColor DarkGray
            } else {
                $failure = $r.Error
                Write-Host ("  FAILED: {0}" -f $r.Error) -ForegroundColor Red
                break
            }
        }

        if ($null -eq $lastOk) {
            $single.Add([pscustomobject][ordered]@{
                Label = $c.Label; Ok = $false; Seconds = 0; Bytes = 0; Fps = 0
                Spread = 0; Runs = 0; Error = $failure })
            continue
        }

        $median = Get-Median -Values $times.ToArray()
        $spread = if ($times.Count -gt 1) {
            ((($times | Measure-Object -Maximum).Maximum - ($times | Measure-Object -Minimum).Minimum) / $median)
        } else { 0.0 }

        $agg = [pscustomobject][ordered]@{
            Label   = $c.Label
            Ok      = $true
            Seconds = [Math]::Round($median, 2)
            Bytes   = $lastOk.Bytes
            Fps     = if ($median -gt 0 -and $frameCount -gt 0) { [Math]::Round($frameCount / $median, 2) } else { 0 }
            Spread  = $spread
            Runs    = $times.Count
            Error   = ''
        }
        $single.Add($agg)
        Write-Host ("        median {0,7:F2}s   run-to-run spread {1:P1}" -f $agg.Seconds, $spread) -ForegroundColor Green
    }

    # --- two encodes in parallel, one per CCD -------------------------------
    Write-Host ""
    Write-Host ("=" * 78) -ForegroundColor DarkCyan
    Write-Host " Throughput: one wide encode vs two pinned encodes" -ForegroundColor Cyan
    Write-Host ("=" * 78) -ForegroundColor DarkCyan
    Write-Host "  Compared on TOTAL throughput -- two jobs finishing together can beat one" -ForegroundColor DarkGray
    Write-Host "  finishing sooner, which is what matters for a queue." -ForegroundColor DarkGray
    Write-Host ""

    $script:throughputVerdict = 'not measured'
    $script:throughputRatio = 1.0

    $baseline = $single | Where-Object { $_.Label -like 'baseline*' } | Select-Object -First 1
    $maxSpread = 0.0
    foreach ($r in $single) { if ($r.Ok -and $r.Spread -gt $maxSpread) { $maxSpread = $r.Spread } }

    Write-Host ("  two parallel encodes, pin={0} each ..." -f $halfCores) -NoNewline
    $swPar = [System.Diagnostics.Stopwatch]::StartNew()
    $jobs = @(1, 2) | ForEach-Object -ThrottleLimit 2 -Parallel {
        $idx = $_
        $ffmpeg = $using:ffmpeg
        $sample = $using:sample
        $WorkDir = $using:WorkDir
        $Preset = $using:Preset
        $Crf = $using:Crf
        $halfCores = $using:halfCores

        $out = Join-Path $WorkDir ("par_{0}.mkv" -f $idx)
        $a = @('-hide_banner','-loglevel','error','-nostdin','-y','-i',$sample,'-map','0:v:0','-an','-sn','-dn',
               '-c:v','libsvtav1','-preset',"$Preset",'-crf',"$Crf",'-pix_fmt','yuv420p10le',
               '-svtav1-params',"pin=$halfCores",$out)
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        $psi = [System.Diagnostics.ProcessStartInfo]::new()
        $psi.FileName = $ffmpeg
        foreach ($x in $a) { $psi.ArgumentList.Add($x) }
        $psi.RedirectStandardError = $true; $psi.RedirectStandardOutput = $true; $psi.UseShellExecute = $false
        $p = [System.Diagnostics.Process]::Start($psi)
        $null = $p.StandardError.ReadToEnd(); $null = $p.StandardOutput.ReadToEnd()
        $p.WaitForExit()
        $sw.Stop()
        $bytes = if ((Test-Path -LiteralPath $out)) { (Get-Item -LiteralPath $out).Length } else { 0 }
        Remove-Item -LiteralPath $out -Force -ErrorAction SilentlyContinue
        [pscustomobject]@{ Index = $idx; Seconds = [Math]::Round($sw.Elapsed.TotalSeconds,2); Bytes = $bytes; Ok = ($p.ExitCode -eq 0) }
    }
    $swPar.Stop()
    $parWall = [Math]::Round($swPar.Elapsed.TotalSeconds, 2)
    $parOk = @($jobs | Where-Object { -not $_.Ok }).Count -eq 0
    if ($parOk) {
        Write-Host ("  {0,7:F2}s wall for 2 clips" -f $parWall) -ForegroundColor Green
    } else {
        Write-Host "  FAILED" -ForegroundColor Red
    }

    # --- results ------------------------------------------------------------
    Write-Host ""
    Write-Host ("=" * 78) -ForegroundColor DarkCyan
    Write-Host " Results" -ForegroundColor Cyan
    Write-Host ("=" * 78) -ForegroundColor DarkCyan

    if (-not $baseline -or -not $baseline.Ok) {
        Write-Host "  Baseline encode failed; nothing can be concluded." -ForegroundColor Red
        return
    }

    Write-Host ("  {0,-34} {1,9} {2,10} {3,11} {4,10}" -f 'configuration','median s','vs base','size MiB','vs base')
    Write-Host ("  " + ("-" * 76)) -ForegroundColor DarkGray
    foreach ($r in $single) {
        if (-not $r.Ok) {
            Write-Host ("  {0,-34} {1,9}" -f $r.Label, 'FAILED') -ForegroundColor Red
            continue
        }
        $spd = $baseline.Seconds / [double]$r.Seconds
        $szD = ($r.Bytes - $baseline.Bytes) / [double]$baseline.Bytes
        $spdLabel = if ($r.Label -like 'baseline*') { '--' } else { "{0:F2}x" -f $spd }
        $szLabel  = if ($r.Label -like 'baseline*') { '--' } else { "{0:+0.00;-0.00;0.00}%" -f ($szD * 100) }
        $colour = if ($r.Label -like 'baseline*') { 'Gray' }
                  elseif ([Math]::Abs($szD) -gt 0.002) { 'Yellow' }
                  elseif ($spd -gt 1.03) { 'Green' }
                  else { 'Gray' }
        Write-Host ("  {0,-34} {1,9:F2} {2,10} {3,11:F2} {4,10}" -f `
            $r.Label, $r.Seconds, $spdLabel, ($r.Bytes / 1MB), $szLabel) -ForegroundColor $colour
    }

    Write-Host ""
    Write-Host "  Methodology self-check" -ForegroundColor Cyan
    $lpRow   = $single | Where-Object { $_.Label -like 'lp=6*' } | Select-Object -First 1
    $pinRow  = $single | Where-Object { $_.Label -like 'pin=*' } | Select-Object -First 1
    $tileRow = $single | Where-Object { $_.Label -like 'tile-columns*' } | Select-Object -First 1

    foreach ($pair in @(@{R=$lpRow;N='lp=6'}, @{R=$pinRow;N='pin'})) {
        if ($pair.R -and $pair.R.Ok) {
            $same = ($pair.R.Bytes -eq $baseline.Bytes)
            $msg = if ($same) { 'identical output size, as expected for a threading-only control' }
                   else { ("output size differs by {0:P3} -- unexpected; treat the numbers with caution" -f (($pair.R.Bytes - $baseline.Bytes) / [double]$baseline.Bytes)) }
            Write-Host ("    {0,-8} {1}" -f $pair.N, $msg) -ForegroundColor $(if ($same) { 'Green' } else { 'Yellow' })
        }
    }
    if ($tileRow -and $tileRow.Ok) {
        $d = ($tileRow.Bytes - $baseline.Bytes) / [double]$baseline.Bytes
        $expected = $d -gt 0
        Write-Host ("    {0,-8} {1}" -f 'tiles', $(if ($expected) {
            "output {0:P2} LARGER -- the efficiency cost of tiling, as expected" -f $d
        } else {
            "output not larger ({0:P2}); at this resolution tiling may be a no-op" -f $d
        })) -ForegroundColor $(if ($expected) { 'Green' } else { 'Gray' })
    }

    Write-Host ""
    Write-Host "  Throughput" -ForegroundColor Cyan
    if ($parOk) {
        # One wide encode does 1 clip in baseline.Seconds; two pinned encodes do
        # 2 clips in parWall. Compare clips per second.
        $oneRate = 1.0 / $baseline.Seconds
        $twoRate = 2.0 / $parWall
        $ratio = $twoRate / $oneRate
        Write-Host ("    one wide encode      : {0,7:F2}s per clip  ({1:F4} clips/s)" -f $baseline.Seconds, $oneRate)
        Write-Host ("    two pinned encodes   : {0,7:F2}s for two   ({1:F4} clips/s)" -f $parWall, $twoRate)
        $noiseGate = [Math]::Max(0.08, $maxSpread * 1.5)
        $script:throughputVerdict = 'inconclusive'
        $script:throughputRatio = $ratio
        if ($ratio -gt (1 + $noiseGate)) {
            $script:throughputVerdict = 'two-workers'
        } elseif ($ratio -lt (1 - $noiseGate)) {
            $script:throughputVerdict = 'one-worker'
        }
        if ($ratio -gt (1 + $noiseGate)) {
            Write-Host ("    -> two parallel encodes are {0:P0} faster in total throughput." -f ($ratio - 1)) -ForegroundColor Green
            Write-Host ("       Worth raising the CPU lane to 2 workers with pin=$halfCores each." -f $halfCores) -ForegroundColor Green
        } elseif ($ratio -lt (1 - $noiseGate)) {
            Write-Host ("    -> one wide encode is {0:P0} faster in total throughput." -f ((1 / $ratio) - 1)) -ForegroundColor Green
            Write-Host  "       Keep the CPU lane at 1 worker using all cores (current behaviour)." -ForegroundColor Green
        } else {
            Write-Host ("    -> within noise ({0:P0} difference, {1:P0} gate). Keep the CPU lane at 1 worker:" -f [Math]::Abs($ratio - 1), $noiseGate) -ForegroundColor Gray
            Write-Host  "       equal throughput, but lower latency per file and simpler behaviour." -ForegroundColor Gray
        }
    } else {
        Write-Host "    parallel run failed; no throughput comparison available." -ForegroundColor Yellow
    }

    Write-Host ""
    Write-Host "  Recommendation" -ForegroundColor Cyan
    Write-Host ("    measurement noise floor: {0:P1} (worst run-to-run spread over {1} runs each)" -f $maxSpread, $Repeat) -ForegroundColor DarkGray

    $best = $single | Where-Object { $_.Ok -and $_.Label -notlike 'tile*' } | Sort-Object Seconds | Select-Object -First 1
    $gain = if ($best -and $best.Seconds -gt 0) { ($baseline.Seconds / [double]$best.Seconds) - 1 } else { 0 }

    # A winner only counts if it beats baseline by more than the noise floor.
    # Otherwise the honest answer is "no measurable difference".
    # Two independent questions, reported separately because the answers can
    # differ and an earlier version of this script made them look contradictory:
    #
    #   (a) per-encode tuning  -- does lp or pin make ONE encode faster?
    #   (b) lane concurrency   -- do TWO encodes move more total work?
    #
    # "Leave lp alone" and "run two workers" are both perfectly consistent
    # answers; they are simply answers to different questions.
    Write-Host "    (a) Per-encode settings:" -ForegroundColor White
    if ($best.Label -like 'baseline*' -or $gain -le $maxSpread) {
        Write-Host "        No configuration beat the defaults by more than measurement noise." -ForegroundColor Green
        Write-Host "        Leave lp and pin at their defaults for a single encode." -ForegroundColor Green
    } else {
        Write-Host ("        '{0}' was fastest: {1:P0} over baseline, above the {2:P1} noise floor." -f `
            $best.Label, $gain, $maxSpread) -ForegroundColor Green
        Write-Host  "        Worth adding to the software lane's svtav1-params." -ForegroundColor Green
    }

    Write-Host ""
    Write-Host "    (b) CPU lane concurrency:" -ForegroundColor White
    switch ($script:throughputVerdict) {
        'two-workers' {
            $pinSuggest = $halfCores
            Write-Host ("        Two concurrent encodes moved {0:P0} more total work than one wide" -f ($script:throughputRatio - 1)) -ForegroundColor Green
            Write-Host  "        encode, above the noise gate. In Media2AV1Queue.ps1 set:" -ForegroundColor Green
            Write-Host  "" -ForegroundColor Green
            Write-Host  "            `$CpuMaxParallel   = 2" -ForegroundColor Cyan
            Write-Host ("            `$SoftwarePinCores = {0}" -f $pinSuggest) -ForegroundColor Cyan
            Write-Host  "" -ForegroundColor Green
            Write-Host  "        Trade-off: throughput improves, but each individual file takes" -ForegroundColor DarkGray
            Write-Host  "        proportionally longer and memory use roughly doubles. Good for a" -ForegroundColor DarkGray
            Write-Host  "        bulk library conversion, worse if you want one file back quickly." -ForegroundColor DarkGray
        }
        'one-worker' {
            Write-Host ("        One wide encode moved {0:P0} more total work. Keep `$CpuMaxParallel = 1." -f ((1 / $script:throughputRatio) - 1)) -ForegroundColor Green
        }
        'inconclusive' {
            Write-Host ("        Difference was {0:P0}, within the noise gate. Keep `$CpuMaxParallel = 1:" -f [Math]::Abs($script:throughputRatio - 1)) -ForegroundColor Gray
            Write-Host  "        same throughput, lower per-file latency, simpler behaviour." -ForegroundColor Gray
        }
        default {
            Write-Host "        Not measured (the parallel run failed)." -ForegroundColor Yellow
        }
    }

    Write-Host ""
    Write-Host "    (c) Tiles: do NOT enable tile-rows/tile-columns." -ForegroundColor Yellow
    if ($tileRow -and $tileRow.Ok) {
        $tSpeed = $baseline.Seconds / [double]$tileRow.Seconds
        $tSize  = ($tileRow.Bytes - $baseline.Bytes) / [double]$baseline.Bytes
        if ($tSpeed -lt 1.0) {
            Write-Host ("        Measured here as {0:P0} SLOWER and {1:P2} larger -- worse on both" -f (1 - $tSpeed), $tSize) -ForegroundColor Yellow
            Write-Host  "        counts. SVT-AV1's segment parallelism already saturates this many" -ForegroundColor Yellow
            Write-Host  "        cores, so tiles add coordination overhead and buy nothing." -ForegroundColor Yellow
        } else {
            Write-Host ("        {0:P0} faster but {1:P2} larger -- the speed is not worth the" -f ($tSpeed - 1), $tSize) -ForegroundColor Yellow
            Write-Host  "        efficiency loss when SVT-AV1 scales without them." -ForegroundColor Yellow
        }
    }
    Write-Host ""
}
finally {
    if (Test-Path -LiteralPath $WorkDir) {
        Remove-Item -LiteralPath $WorkDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}

}

# =============================================================================
# Verify
# Merged from Media2AV1Queue-Verify.ps1. Body unchanged; see Build-Single.ps1.
# =============================================================================
function Invoke-VerifyTool {
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
    $script:LastToolExitCode = 0; return
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
$script:LastToolExitCode = $(if ($fail -gt 0) { 1 } else { 0 }); return

}

# =============================================================================
# The queue session
#
# This was the engine's top-level flow. Wrapped in a function so that picking
# a tool from the menu does not acquire the queue mutex and start encoding.
# =============================================================================
function Invoke-QueueSession {
# ---- Which queue loop this session runs ---------------------------------
# $EncoderPreference selects the processing loop, not just a bias, and only the
# Auto lane scheduler reads a per-job lane preference. So when this drop -- or
# anything already sitting in the queue -- asks for a lane the configured loop
# cannot serve, the session runs the Auto scheduler instead. Jobs without an
# override still resolve to $EncoderPreference inside that scheduler, so a plain
# drag-drop behaves exactly as before.
$script:QueueLoopPreference = $EncoderPreference
$laneOverrideInPlay = (-not [string]::IsNullOrWhiteSpace($EncoderPreferenceOverride) -and $EncoderPreferenceOverride -ne $EncoderPreference)
if (-not $laneOverrideInPlay) {
    $laneOverrideInPlay = Test-QueueHasLanePreferenceConflict
}
if ($laneOverrideInPlay -and $EncoderPreference -ne 'Auto') {
    $script:QueueLoopPreference = 'Auto'
    Write-Host ("Encoder preference is '{0}', but a queued drop asked for a different lane. Using the automatic lane scheduler so both are honoured." -f $EncoderPreference) -ForegroundColor DarkCyan
}

$script:NvencEnvironment = $null
if ($script:QueueLoopPreference -eq 'Nvidia') {
    $script:NvencEnvironment = Get-NvencEnvironment
    $startupTuneResolution = Resolve-NvencTune -ConfiguredNvencTune $NvencTune -NvencEnvironment $script:NvencEnvironment
    if ($startupTuneResolution.Warning) {
        Write-Warning $startupTuneResolution.Warning
    }
} elseif ($script:QueueLoopPreference -eq 'Auto') {
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
        Add-QueueInputs -Paths $InputPaths -AutoCRFOffsetOverrideValue $AutoCRFOffsetOverride -TargetGiBPerHourOverrideValue $TargetGiBPerHourOverride -EncoderPreferenceOverrideValue $EncoderPreferenceOverride
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
        Write-SessionSummary
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

}

# =============================================================================
# The front door
#
# Everything above this point is engine and tools. This is the part a person
# actually meets, and its job is to make sure nobody has to know which of nine
# scripts to run, or which folder to drag a film onto.
#
# Three rules it follows:
#   - Files dropped on it encode. No menu, no questions, exactly as before.
#   - A bare run opens the menu.
#   - Every destructive thing is behind a confirmation that says what it will do
#     to which files.
# =============================================================================

# Tools signal pass/fail with an exit code. Inside a function that code has
# nowhere to go, so it is parked here and handed to the shell by the dispatcher
# when the script was invoked with an explicit -Mode.
$script:LastToolExitCode = 0

function Read-MenuChoice {
    param(
        [string]$Prompt,
        [string[]]$Valid,
        [string]$Default = ''
    )

    while ($true) {
        Write-Host ''
        $answer = Read-Host $Prompt

        # Read-Host returns $null, not an empty string, when there is no console
        # to read from -- a piped run, a scheduled task, a closed stdin. Calling
        # .Trim() on that throws, and the script dies with a null-reference
        # error instead of doing anything useful.
        if ($null -eq $answer) {
            if ($Default) { return $Default }
            Write-Host '  No console input available; leaving the menu.' -ForegroundColor DarkGray
            return 'Q'
        }
        if ([string]::IsNullOrWhiteSpace($answer) -and $Default) { return $Default }
        $answer = $answer.Trim()
        foreach ($v in $Valid) {
            if ($answer -ieq $v) { return $v }
        }
        Write-Host ("  Enter one of: {0}" -f ($Valid -join ', ')) -ForegroundColor Yellow
    }
}

# Console paths arrive in every shape a person can produce: dragged in (which
# quotes anything with a space), pasted with a trailing backslash, typed with
# neither. All three have to work.
function Read-PathList {
    param([string]$Prompt, [switch]$AllowEmpty)

    while ($true) {
        Write-Host ''
        Write-Host '  Tip: you can drag a file or folder from Explorer straight into this window.' -ForegroundColor DarkGray
        $raw = Read-Host $Prompt
        if ($null -eq $raw) { $none = [string[]]@(); return ,$none }
        if ([string]::IsNullOrWhiteSpace($raw)) {
            # Comma-returned. A bare "return @()" unrolls to nothing, the caller
            # gets $null, and reading .Count on it throws under StrictMode --
            # which is precisely what "press Enter to skip" does.
            if ($AllowEmpty) { $none = [string[]]@(); return ,$none }
            Write-Host '  Give me at least one path, or press Ctrl+C to go back.' -ForegroundColor Yellow
            continue
        }

        # A dragged path is quoted only when it needs to be, so split on the
        # quotes first and fall back to splitting on commas.
        $found = New-Object System.Collections.Generic.List[string]
        $quoted = [Regex]::Matches($raw, '"([^"]+)"')
        if ($quoted.Count -gt 0) {
            foreach ($m in $quoted) { $found.Add($m.Groups[1].Value) }
            $raw = [Regex]::Replace($raw, '"[^"]+"', ' ')
        }
        foreach ($piece in ($raw -split ',')) {
            $clean = $piece.Trim().Trim('"').TrimEnd('\')
            if (-not [string]::IsNullOrWhiteSpace($clean)) { $found.Add($clean) }
        }

        $missing = @($found | Where-Object { -not (Test-Path -LiteralPath $_) })
        if ($missing.Count -gt 0) {
            foreach ($m in $missing) { Write-Host ("  Not found: {0}" -f $m) -ForegroundColor Yellow }
            continue
        }
        if ($found.Count -eq 0) {
            Write-Host '  Nothing usable in that. Try again.' -ForegroundColor Yellow
            continue
        }
        $out = [string[]]$found.ToArray()
        return ,$out
    }
}

function Read-Confirmation {
    param([string]$Prompt, [string]$Expected = 'yes')

    Write-Host ''
    $answer = Read-Host ("{0} (type '{1}' to continue)" -f $Prompt, $Expected)
    if ($null -eq $answer) { return $false }
    return ($answer.Trim() -ieq $Expected)
}

function Get-PendingQueueCount {
    try {
        return @(Get-ChildItem -LiteralPath $QueuePendingDir -Filter *.json -File -ErrorAction SilentlyContinue).Count
    } catch {
        return 0
    }
}

function Write-MenuHeader {
    param([string]$Title)

    Write-Host ''
    Write-Host '===============================================================' -ForegroundColor Cyan
    Write-Host (" {0}" -f $Title) -ForegroundColor Cyan
    Write-Host '===============================================================' -ForegroundColor Cyan
}

# -----------------------------------------------------------------------------
# Tools
# -----------------------------------------------------------------------------
function Show-ToolsMenu {
    while ($true) {
        Write-MenuHeader 'Tools'
        Write-Host '  Everything here is read-only unless it says otherwise.' -ForegroundColor DarkGray
        Write-Host ''
        Write-Host '  [1] Doctor         Check FFmpeg and this machine can do what the encoder needs'
        Write-Host '  [2] Candidates     Scan a library and rank what is worth converting'
        Write-Host '  [3] Loudness       Measure audio levels, and even them out'
        Write-Host '  [4] Quality        Calibrate the quality thresholds against your own films'
        Write-Host '  [5] Library scan   HDR / Dolby Vision census of a library'
        Write-Host '  [6] Benchmark      Find the fastest CPU threading for this machine'
        Write-Host '  [7] Verify         End-to-end self-test on throwaway copies'
        Write-Host ''
        Write-Host '  [B] Back'

        $choice = Read-MenuChoice -Prompt 'Choose a tool' -Valid @('1','2','3','4','5','6','7','B')

        try {
            switch ($choice) {
                '1' { Invoke-DoctorMenuAction }
                '2' { Invoke-CandidatesMenuAction }
                '3' { Invoke-LoudnessMenuAction }
                '4' { Invoke-QualityMenuAction }
                '5' { Invoke-LibraryScanMenuAction }
                '6' { Invoke-BenchMenuAction }
                '7' { Invoke-VerifyMenuAction }
                'B' { return }
            }
        } catch {
            # A tool that throws should not take the menu down with it: the
            # whole point of one file is that you can try the next thing.
            Write-Host ''
            Write-Host ("The tool stopped with an error: {0}" -f $_.Exception.Message) -ForegroundColor Red
        }

        Write-Host ''
        $null = Read-Host 'Press Enter to return to the menu'
    }
}

function Invoke-DoctorMenuAction {
    Write-MenuHeader 'Doctor'
    Write-Host 'Checks your FFmpeg build, GPU and optional tools against what the encoder needs.'
    Write-Host 'Read-only. Nothing is encoded and nothing is written.' -ForegroundColor DarkGray
    Write-Host ''
    Write-Host 'Optionally point it at one file and it will also tell you what it would do'
    Write-Host 'with that file and why.'

    $paths = Read-PathList -Prompt 'File to analyse (Enter to skip)' -AllowEmpty
    if ($paths.Count -gt 0) { Invoke-DoctorTool -TestFile $paths[0] } else { Invoke-DoctorTool }
}

function Invoke-CandidatesMenuAction {
    Write-MenuHeader 'Candidates'
    Write-Host 'Reads container headers only -- no decoding -- and ranks files by how much'
    Write-Host 'space they give back per hour of encoding. Read-only.' -ForegroundColor DarkGray

    $paths = Read-PathList -Prompt 'Library folder(s) to scan'
    $csv = Join-Path $PSScriptRoot ('candidates_{0}.csv' -f (Get-Date -Format 'yyyyMMdd_HHmmss'))
    $list = Join-Path $PSScriptRoot ('candidates_{0}.txt' -f (Get-Date -Format 'yyyyMMdd_HHmmss'))

    Invoke-CandidatesTool -Path $paths -Csv $csv -OutList $list

    Write-Host ''
    if ((Test-Path -LiteralPath $list) -and (Read-Confirmation -Prompt 'Queue the shortlist for encoding now?')) {
        $queueThese = @(Get-Content -LiteralPath $list | Where-Object { $_.Trim() })
        if ($queueThese.Count -gt 0) {
            Add-QueueInputs -Paths $queueThese
            Write-Host ("Queued {0} file(s). Choose Auto from the main menu to start." -f $queueThese.Count) -ForegroundColor Green
        }
    }
}

function Invoke-LoudnessMenuAction {
    Write-MenuHeader 'Loudness'
    Write-Host 'Measures how loud things actually are, anchored on dialogue where there is a'
    Write-Host 'centre channel to measure. Scanning is read-only.' -ForegroundColor DarkGray
    Write-Host ''
    Write-Host '  [1] Scan (fast, sampled)     get the lay of the land'
    Write-Host '  [2] Scan (full measurement)  slower, exact'
    Write-Host '  [3] Fix files                measure, then write normalised copies'
    Write-Host ''
    Write-Host '  [B] Back'
    $what = Read-MenuChoice -Prompt 'Choose' -Valid @('1','2','3','B')
    if ($what -eq 'B') { return }

    $paths = Read-PathList -Prompt 'File(s) or folder(s)'
    $csv = Join-Path $PSScriptRoot ('loudness_{0}.csv' -f (Get-Date -Format 'yyyyMMdd_HHmmss'))

    switch ($what) {
        '1' { Invoke-LoudnessTool -Path $paths -Quick -Csv $csv }
        '2' { Invoke-LoudnessTool -Path $paths -Csv $csv }
        '3' {
            Write-Host ''
            Write-Host 'Normalised copies are written alongside the originals, which are not touched.' -ForegroundColor DarkGray
            Write-Host 'The original audio track is kept inside the new file as well, so your receiver' -ForegroundColor DarkGray
            Write-Host 'can still bitstream it.' -ForegroundColor DarkGray
            if (Read-Confirmation -Prompt 'Write normalised copies?') {
                Invoke-LoudnessTool -Path $paths -Apply -Csv $csv
            } else {
                Write-Host 'Nothing written.' -ForegroundColor DarkGray
            }
        }
    }
}

function Invoke-QualityMenuAction {
    Write-MenuHeader 'Quality calibration'
    Write-Host 'Encodes short samples of ONE file at several CRFs and measures each against the'
    Write-Host 'source, so you can see the size-versus-quality curve for your own content.'
    Write-Host 'Read-only with respect to your library. Takes a few minutes.' -ForegroundColor DarkGray

    $paths = Read-PathList -Prompt 'One file to profile'
    $keep = Read-Confirmation -Prompt 'Keep the sample clips so you can watch them?' -Expected 'yes'
    if ($keep) {
        Invoke-QualityTool -Path $paths[0] -KeepSamples
    } else {
        Invoke-QualityTool -Path $paths[0]
    }
}

function Invoke-LibraryScanMenuAction {
    Write-MenuHeader 'Library scan'
    Write-Host 'HDR, HDR10+ and Dolby Vision census of a library. Read-only.' -ForegroundColor DarkGray

    $paths = Read-PathList -Prompt 'Library folder(s) to scan'
    $csv = Join-Path $PSScriptRoot ('hdr_census_{0}.csv' -f (Get-Date -Format 'yyyyMMdd_HHmmss'))
    Invoke-LibraryScanTool -Path $paths -Csv $csv
}

function Invoke-BenchMenuAction {
    Write-MenuHeader 'Benchmark'
    Write-Host 'Measures SVT-AV1 threading on this machine to answer whether one wide encode'
    Write-Host 'or two narrower ones finish more work per hour. Takes a while.' -ForegroundColor DarkGray

    Write-Host ''
    Write-Host 'Give it a real file for a representative answer, or press Enter to use a'
    Write-Host 'synthetic clip, which is faster but less like your own content.'

    $paths = Read-PathList -Prompt 'File to benchmark with (Enter for synthetic)' -AllowEmpty
    if ($paths.Count -gt 0) { Invoke-BenchTool -Source $paths[0] } else { Invoke-BenchTool -Synthetic }
}

function Invoke-VerifyMenuAction {
    Write-MenuHeader 'Verify'
    Write-Host 'Copies short samples into a sandbox, encodes them, and checks the output really'
    Write-Host 'kept its HDR metadata, audio, subtitles and chapters.' -ForegroundColor DarkGray
    Write-Host 'Your originals are copied, never encoded in place.' -ForegroundColor DarkGray

    if ((Get-PendingQueueCount) -gt 0) {
        Write-Host ''
        Write-Host 'The queue is not idle. Verify needs it empty so its own encodes are the only' -ForegroundColor Yellow
        Write-Host 'thing running. Finish or clear the queue first.' -ForegroundColor Yellow
        return
    }

    $paths = Read-PathList -Prompt 'File(s) to test with (Enter to let it choose)' -AllowEmpty
    if ($paths.Count -gt 0) { Invoke-VerifyTool -Files $paths } else { Invoke-VerifyTool }
}

# -----------------------------------------------------------------------------
# Encoding
# -----------------------------------------------------------------------------
function Invoke-AutoMenuAction {
    $pending = Get-PendingQueueCount

    if ($pending -gt 0) {
        Write-MenuHeader 'Auto'
        Write-Host ("There are already {0} file(s) waiting in the queue." -f $pending)
        Write-Host ''
        Write-Host '  [1] Start working through them'
        Write-Host '  [2] Add more files first'
        Write-Host '  [B] Back'
        $what = Read-MenuChoice -Prompt 'Choose' -Valid @('1','2','B') -Default '1'
        if ($what -eq 'B') { return }
        if ($what -eq '2') {
            $paths = Read-PathList -Prompt 'File(s) or folder(s) to add'
            Add-QueueInputs -Paths (Expand-DroppedPaths -Paths $paths)
        }
    } else {
        Write-MenuHeader 'Auto'
        Write-Host 'Everything decided automatically: quality, encoder lane, HDR handling.'
        Write-Host 'This is the same as dropping files on the .bat.' -ForegroundColor DarkGray

        $paths = Read-PathList -Prompt 'File(s) or folder(s) to encode'
        $expanded = Expand-DroppedPaths -Paths $paths
        if ($expanded.Count -eq 0) {
            Write-Host 'No video files found in that.' -ForegroundColor Yellow
            return
        }
        Write-Host ("{0} video file(s) found." -f $expanded.Count) -ForegroundColor Gray

        if ($ReplaceOriginal) {
            Write-Host ''
            Write-Host 'Note: ReplaceOriginal is ON, so each source file is replaced by its AV1' -ForegroundColor Yellow
            Write-Host 'version once the encode succeeds. Change $ReplaceOriginal at the top of' -ForegroundColor Yellow
            Write-Host 'this script to keep the originals.' -ForegroundColor Yellow
            if (-not (Read-Confirmation -Prompt 'Continue?')) { return }
        }
        Add-QueueInputs -Paths $expanded
    }

    Invoke-QueueSession
}

function Invoke-InteractiveMenuAction {
    Write-MenuHeader 'Interactive'
    Write-Host 'Same encoder, but you choose the size/quality trade and which encoder to use.'
    Write-Host 'Both choices apply only to the files you add now.' -ForegroundColor DarkGray

    $paths = Read-PathList -Prompt 'File(s) or folder(s) to encode'
    $expanded = Expand-DroppedPaths -Paths $paths
    if ($expanded.Count -eq 0) {
        Write-Host 'No video files found in that.' -ForegroundColor Yellow
        return
    }
    Write-Host ("{0} video file(s) found." -f $expanded.Count) -ForegroundColor Gray

    Write-Host ''
    Write-Host 'How should these be encoded?'
    Write-Host '  [1] Auto        let the script decide everything'
    Write-Host '  [2] Aggressive  smaller files, more quality given up'
    Write-Host '  [3] Balanced    a middle ground'
    Write-Host '  [4] Quality     larger files, less quality given up'
    Write-Host '  [5] Target      name an exact output rate in GiB/hr'
    $tier = Read-MenuChoice -Prompt 'Choose 1-5' -Valid @('1','2','3','4','5') -Default '1'

    $offsetOverride = switch ($tier) { '1' { '' } '2' { '2' } '3' { '0' } '4' { '-2' } '5' { '' } }
    $rateOverride = ''
    if ($tier -eq '5') {
        while ($true) {
            Write-Host ''
            Write-Host '  For reference: a 2h film at 5 GiB/hr lands around 10 GiB.' -ForegroundColor DarkGray
            $entered = Read-Host 'Target GiB/hr (blank to cancel back to Auto)'
            if ([string]::IsNullOrWhiteSpace($entered)) { break }
            $parsed = 0.0
            if ([double]::TryParse($entered.Trim(), [System.Globalization.NumberStyles]::Float,
                                   [System.Globalization.CultureInfo]::InvariantCulture, [ref]$parsed) -and
                $parsed -gt 0 -and $parsed -le 100) {
                $rateOverride = $parsed.ToString('0.###', [System.Globalization.CultureInfo]::InvariantCulture)
                break
            }
            Write-Host '  Enter a positive number up to 100.' -ForegroundColor Yellow
        }
    }

    Write-Host ''
    Write-Host 'Which encoder?'
    Write-Host '  [1] Auto    pick per file, keeps both busy'
    Write-Host '  [2] CPU     SVT-AV1. Best compression, slower'
    Write-Host '  [3] NVENC   GPU. Much faster, larger files at similar quality'
    $lane = Read-MenuChoice -Prompt 'Choose 1-3' -Valid @('1','2','3') -Default '1'
    $laneOverride = switch ($lane) { '1' { '' } '2' { 'CPU' } '3' { 'Nvidia' } }

    if ($ReplaceOriginal) {
        Write-Host ''
        Write-Host 'Note: ReplaceOriginal is ON -- sources are replaced on success.' -ForegroundColor Yellow
        if (-not (Read-Confirmation -Prompt 'Continue?')) { return }
    }

    Add-QueueInputs -Paths $expanded `
        -AutoCRFOffsetOverrideValue $offsetOverride `
        -TargetGiBPerHourOverrideValue $rateOverride `
        -EncoderPreferenceOverrideValue $laneOverride

    # A per-drop lane only takes effect if the session runs the scheduler that
    # reads it, which is decided inside Invoke-QueueSession from this variable.
    if ($laneOverride) { $script:EncoderPreferenceOverride = $laneOverride }

    Invoke-QueueSession
}

# Folders are expanded here rather than rejected, because "encode this season"
# is the obvious thing to want and the queue only accepts individual files.
function Expand-DroppedPaths {
    param([string[]]$Paths)

    $videoExtensions = @('.mkv', '.mp4', '.m4v', '.ts', '.m2ts', '.avi', '.mov', '.wmv', '.webm', '.mpg', '.mpeg', '.vob')
    $results = New-Object System.Collections.Generic.List[string]
    $seen = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
    $workFiles = New-Object System.Collections.Generic.List[string]

    foreach ($p in $Paths) {
        if ([string]::IsNullOrWhiteSpace($p) -or -not (Test-Path -LiteralPath $p)) { continue }
        $item = Get-Item -LiteralPath $p
        if ($item.PSIsContainer) {
            Write-Host ("  Expanding {0}" -f $item.FullName) -ForegroundColor DarkGray
            Get-ChildItem -LiteralPath $item.FullName -File -Recurse -ErrorAction SilentlyContinue |
                Where-Object { $videoExtensions -contains $_.Extension.ToLowerInvariant() } |
                Sort-Object FullName |
                ForEach-Object {
                    # A folder scan sees this script's own leftovers as ordinary
                    # .mkv files. Queueing a half-written .encoding.tmp.mkv from
                    # an interrupted run once put a worker on the same bytes
                    # another worker was still writing; it failed the output
                    # duration check after burning an encode slot. Folder
                    # expansion is where that has to be caught, because by the
                    # time it is a queued path it looks like any other job.
                    if (Test-EncoderWorkPath -Path $_.FullName) {
                        $workFiles.Add($_.FullName)
                    } elseif ($seen.Add($_.FullName)) {
                        $results.Add($_.FullName)
                    }
                }
        } elseif (Test-EncoderWorkPath -Path $item.FullName) {
            $workFiles.Add($item.FullName)
        } elseif ($videoExtensions -contains $item.Extension.ToLowerInvariant()) {
            if ($seen.Add($item.FullName)) { $results.Add($item.FullName) }
        } else {
            Write-Host ("  Skipping non-video file: {0}" -f $item.Name) -ForegroundColor DarkGray
        }
    }

    if ($workFiles.Count -gt 0) {
        Write-Host ("  Ignored {0} unfinished encode file(s) left by an earlier run:" -f $workFiles.Count) -ForegroundColor DarkYellow
        foreach ($leftover in ($workFiles | Select-Object -First 5)) {
            Write-Host ("    {0}" -f [System.IO.Path]::GetFileName($leftover)) -ForegroundColor DarkGray
        }
        if ($workFiles.Count -gt 5) {
            Write-Host ("    ... and {0} more" -f ($workFiles.Count - 5)) -ForegroundColor DarkGray
        }
        Write-Host "  They are partial output, not sources. Delete them if the encode is not coming back." -ForegroundColor DarkGray
    }

    $out = [string[]]$results.ToArray()
    return ,$out
}

# -----------------------------------------------------------------------------
# Main menu
# -----------------------------------------------------------------------------
function Show-MainMenu {
    while ($true) {
        $pending = Get-PendingQueueCount

        Write-MenuHeader 'Media2AV1Queue'
        Write-Host ('  FFmpeg: {0}' -f $FfmpegPath) -ForegroundColor DarkGray
        if ($pending -gt 0) {
            Write-Host ('  {0} file(s) waiting in the queue' -f $pending) -ForegroundColor Yellow
        }
        Write-Host ''
        Write-Host '  [1] Auto           Encode files. Everything decided for you.'
        Write-Host '  [2] Interactive    Encode files, choosing quality and encoder yourself.'
        Write-Host '  [3] Tools          Check, scan, measure, calibrate, verify.'
        Write-Host ''
        Write-Host '  [Q] Quit'

        $choice = Read-MenuChoice -Prompt 'Choose' -Valid @('1','2','3','Q')

        try {
            switch ($choice) {
                '1' { Invoke-AutoMenuAction }
                '2' { Invoke-InteractiveMenuAction }
                '3' { Show-ToolsMenu }
                'Q' { return }
            }
        } catch {
            if ($_.Exception.Message -eq $script:QueueShutdownSentinel) { return }
            Write-Host ''
            Write-Host ("Stopped with an error: {0}" -f $_.Exception.Message) -ForegroundColor Red
            Write-Host ''
            $null = Read-Host 'Press Enter to return to the menu'
        }
    }
}

# =============================================================================
# Dispatch
#
# The order here is the whole user-facing contract:
#   an explicit -Mode wins; otherwise dropped files encode; otherwise, menu.
# Dragging files onto the .bat therefore behaves exactly as it always has, and
# double-clicking it -- which used to silently resume an empty queue and exit --
# now shows something useful.
# =============================================================================
# Folders are expanded in place before the session runs, so the queue receives
# individual files -- the only thing it accepts -- while the session keeps doing
# its own enqueueing exactly as it did when this was a standalone script.
$script:DroppedSomething = ($InputPaths -and $InputPaths.Count -gt 0)
if ($script:DroppedSomething) {
    $script:InputPaths = Expand-DroppedPaths -Paths $InputPaths
}

if ($Mode) {
    switch ($Mode) {
        'Auto'        { Invoke-QueueSession }
        'Interactive' { Invoke-InteractiveMenuAction }
        'Tools'       { Show-ToolsMenu }
        'Doctor'      { Invoke-DoctorMenuAction }
        'Candidates'  { Invoke-CandidatesMenuAction }
        'Loudness'    { Invoke-LoudnessMenuAction }
        'Quality'     { Invoke-QualityMenuAction }
        'LibraryScan' { Invoke-LibraryScanMenuAction }
        'Bench'       { Invoke-BenchMenuAction }
        'Verify'      { Invoke-VerifyMenuAction }
    }
    # Only when a mode was named explicitly: an interactive session ending is
    # not a failure, whatever the last tool happened to report.
    exit $script:LastToolExitCode
} elseif ($InputPaths -and $InputPaths.Count -gt 0) {
    Invoke-QueueSession
} else {
    # Dropping a folder that contains no encodable video used to land silently
    # on the main menu, which reads as "it ignored me".
    if ($script:DroppedSomething) {
        Write-Host ''
        Write-Host 'Nothing to encode: no video files were found in what you dropped.' -ForegroundColor Yellow
        Write-Host 'Unfinished .encoding.tmp. files are skipped on purpose -- they are partial output, not sources.' -ForegroundColor DarkGray
        Write-Host ''
    }
    Show-MainMenu
}


