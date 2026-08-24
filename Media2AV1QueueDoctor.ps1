#requires -Version 7.0
# =============================================================================
# Media2AV1Queue-Doctor.ps1
#
# Read-only diagnostic for the Media2AV1Queue toolchain. Encodes nothing and
# writes nothing outside its own console output.
#
# Media2AV1Queue decides what it can do by probing the local binaries at run
# time rather than assuming a build. That is the right behaviour -- it degrades
# instead of failing -- but it also means a missing capability shows up as
# quietly reduced output rather than an error. This script makes the same
# probes and prints the answers, so "why did my HDR10+ file come out as plain
# HDR10?" has a direct answer.
#
# Usage:
#   pwsh -File Media2AV1Queue-Doctor.ps1
#   pwsh -File Media2AV1Queue-Doctor.ps1 -TestFile "D:\Media\Some Movie.mkv"
#
# With -TestFile it also reports what it detects in that specific source and
# what the encode would do with it.
# =============================================================================

[CmdletBinding()]
param(
    [string]$TestFile = '',
    [string]$HdrToolsDir = $null
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

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
    exit 1
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
exit $(if ($script:issues.Count -gt 0) { 1 } else { 0 })
