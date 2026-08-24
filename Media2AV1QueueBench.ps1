#requires -Version 7.0
# =============================================================================
# Media2AV1Queue-Bench.ps1
#
# Measures SVT-AV1 threading behaviour on THIS machine and recommends settings.
# Writes only into its own temp working folder, which it cleans up.
#
# Why measure instead of just prescribing settings:
#
# SVT-AV1's parallelism is picture- and segment-based, so unlike tile-based
# threading it costs no compression efficiency -- the documentation notes it can
# "comfortably utilize up to 16 cores given 1080p source video" without tiling.
# That means the usual advice for a high-core-count CPU is simply "one encode,
# all cores, no tiles", and the defaults already do that.
#
# What is genuinely uncertain on a part like the 9950X3D is the effect of its
# dual-CCD layout and asymmetric L3: only one CCD carries the extra V-Cache, and
# cross-CCD traffic is not free. Whether one 16-core encode beats two 8-core
# encoders pinned one per CCD depends on the content, the resolution, the preset
# and the scheduler. That is a measurement, not a rule of thumb.
#
# The benchmark is also self-checking. lp and pin are pure threading controls,
# so at a fixed CRF they must produce the SAME output size; tiles are not, so
# they should produce a LARGER one. If the numbers do not show that, distrust
# the run rather than the conclusion.
#
# Usage:
#   pwsh -File Media2AV1Queue-Bench.ps1 -Source "G:\Movies\Some UHD Movie.mkv"
#   pwsh -File Media2AV1Queue-Bench.ps1 -Source "..." -Preset 4 -SampleSeconds 30
#   pwsh -File Media2AV1Queue-Bench.ps1 -Synthetic          # no real source needed
#
# A real source is strongly preferred: synthetic test patterns compress nothing
# like film and will mislead you about both speed and size.
# =============================================================================

[CmdletBinding()]
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

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

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
