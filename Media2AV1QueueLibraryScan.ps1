#requires -Version 7.0
# =============================================================================
# Media2AV1Queue-LibraryScan.ps1
#
# Read-only HDR census of a media library. Encodes nothing, modifies nothing,
# and writes exactly one file: the CSV report you ask for.
#
# The point is to make the HDR10+ question a data question. Building the
# standalone SVT-AV1 pipeline needed to preserve HDR10+ is a few hundred lines
# touching the encoder worker plumbing. That is worth doing if a meaningful
# share of the library actually carries SMPTE ST 2094-40 -- and worth skipping
# entirely if it turns out four films have it.
#
# Usage:
#   pwsh -File Media2AV1Queue-LibraryScan.ps1 -Path "G:\Movies"
#   pwsh -File Media2AV1Queue-LibraryScan.ps1 -Path "G:\Movies","G:\TV" -Csv "G:\hdr_census.csv"
#   pwsh -File Media2AV1Queue-LibraryScan.ps1 -Path "G:\Movies" -QuickScan
#
# Notes on cost:
#   HDR10+ metadata usually lives in HEVC SEI, not in container elements, so
#   detecting it requires decoding a frame. That is far slower than reading
#   container headers. The scan is therefore two-phase: every file gets a cheap
#   header probe, and only files that look HDR get the expensive frame probe.
#   SDR files -- normally most of a library -- never pay for it.
#
#   -QuickScan skips the frame probe entirely. It still classifies HDR10 vs HLG
#   vs Dolby Vision vs SDR correctly; it just cannot see HDR10+, which is the
#   one thing you are probably running this for.
# =============================================================================

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string[]]$Path,
    [string]$Csv = '',
    [int]$ThrottleLimit = 0,
    [switch]$QuickScan,
    [switch]$IncludeWorkFolders,
    [string[]]$Extension = @('.mkv','.mp4','.m4v','.ts','.m2ts','.avi','.mov','.wmv','.webm','.mpg','.mpeg','.vob')
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

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
    exit 0
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
