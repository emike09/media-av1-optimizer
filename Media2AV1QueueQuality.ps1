#requires -Version 7.0
# =============================================================================
# Media2AV1Queue-Quality.ps1
#
# Measures the size-versus-quality curve of a real file from your own library,
# so the quality thresholds in Media2AV1Queue.ps1 can be set from your content
# and your own eyes instead of from someone else's defaults -- including mine.
#
# READ-ONLY. It never writes to, moves, or replaces the source. Sample encodes
# go to a scratch folder under .queue\quality and are deleted as they are
# measured, unless -KeepSamples is given.
#
# What it does:
#   1. Loads the encoder settings and functions from Media2AV1Queue.ps1 itself,
#      so the samples are encoded exactly the way the queue would encode them.
#      A calibration tool that used its own arguments would calibrate the wrong
#      encoder.
#   2. Encodes short samples at a range of CRFs.
#   3. Measures each against the source with BOTH metrics that the build
#      supports, and records the size.
#   4. Prints the curve, and marks where the currently configured thresholds
#      land -- so you can see what you are giving up and what you are gaining.
#
# Usage:
#   .\Media2AV1Queue-Quality.ps1 -Path "G:\Movies\Films\Some Film\Some Film.mkv"
#   .\Media2AV1Queue-Quality.ps1 -Path "..." -CrfList 20,24,28,32,36,40
#   .\Media2AV1Queue-Quality.ps1 -Path "..." -KeepSamples -SampleCount 3
#   .\Media2AV1Queue-Quality.ps1 -Path "..." -Csv "curve.csv"
# =============================================================================

[CmdletBinding()]
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
    [string]$Csv = '',

    # Path to the main script, if it is not next to this one.
    [string]$QueueScript = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# =============================================================================
# Load the production settings and functions
# =============================================================================
if ([string]::IsNullOrWhiteSpace($QueueScript)) {
    $QueueScript = Join-Path $PSScriptRoot 'Media2AV1Queue.ps1'
}
if (-not (Test-Path -LiteralPath $QueueScript)) {
    throw "Could not find Media2AV1Queue.ps1. Put this script next to it, or pass -QueueScript."
}

$parseErrors = $null
$queueAst = [System.Management.Automation.Language.Parser]::ParseFile($QueueScript, [ref]$null, [ref]$parseErrors)
if ($parseErrors) {
    throw ("Media2AV1Queue.ps1 has parse errors: " + (($parseErrors | ForEach-Object { "line $($_.Extent.StartLineNumber): $($_.Message)" }) -join '; '))
}

function Auto { return 'Auto' }

# The user-configurable settings block, verbatim, so this tool reports against
# the thresholds actually in force rather than against a copy of the defaults.
$queueText = Get-Content -LiteralPath $QueueScript -Raw
$startIdx = $queueText.IndexOf('# User-configurable settings')
$endIdx   = $queueText.IndexOf('# End of user-configurable settings')
if ($startIdx -lt 0 -or $endIdx -lt 0) { throw 'Could not locate the settings block in Media2AV1Queue.ps1.' }
. ([scriptblock]::Create($queueText.Substring($startIdx, $endIdx - $startIdx)))

# Script-scope state the lifted functions expect to already exist. Declared
# here for the same reason the main script declares them up front: under
# StrictMode, reading a never-assigned script variable throws.
$script:HdrChromaUnit             = 0.00002
$script:HdrLuminanceUnit          = 0.0001
$script:HdrToolchainCache         = $null
$script:HdrStaticMetadataCache    = @{}
$script:QualityToolchainCache     = $null
$script:SvtParamSupportCache      = @{}
$script:QualityLosslessDbSentinel = 140.0
$script:SessionLogPath            = $null
$script:QueueShutdownRequested    = $false
$script:QueueShutdownSentinel     = '__QUEUE_SHUTDOWN__'
# The lifted functions cannot see this script's $PSScriptRoot -- a function
# built from a scriptblock has no file, so its $PSScriptRoot is empty. This is
# how they are told where hdr10plus_tool / dovi_tool live.
$script:HdrToolSearchRoot         = $PSScriptRoot

$lifted = 0
foreach ($fn in $queueAst.FindAll({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] }, $true)) {
    if ($null -ne $fn.Parent -and $null -ne $fn.Parent.Parent -and
        $fn.Parent.Parent -is [System.Management.Automation.Language.FunctionDefinitionAst]) { continue }
    . ([scriptblock]::Create($fn.Extent.Text))
    $lifted++
}

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
Write-Host ("Loaded     : {0} functions from {1}" -f $lifted, [System.IO.Path]::GetFileName($QueueScript)) -ForegroundColor DarkGray
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
