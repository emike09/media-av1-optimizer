# 🎬 Media AV1 Optimizer

A queue-based AV1 encoding pipeline for video libraries with:

- software AV1 via `libsvtav1`
- hardware AV1 via NVIDIA `av1_nvenc`
- **measured** perceptual quality targeting, not just size estimation
- full HDR10 static metadata preservation, and Dolby Vision → HDR10 conversion
- source-aware Auto mode
- queue persistence
- live console controls

Built for large media libraries where quality, automation, and safe batch handling all matter.

**The goal is one sentence:** convert H.264/H.265 to AV1 and make the file smaller *without losing anything you can see.* Everything below exists to serve that.

> Optimized for English-language media libraries.

---

## 🚀 Highlights

- AV1 encoding with **SVT-AV1** or **NVIDIA NVENC**
- **Quality targeting**: sample encodes are *measured* against the source with VMAF or XPSNR, and the script picks the highest CRF that is still visually transparent
- **Files that cannot be shrunk transparently are left alone** rather than re-encoded at reduced quality
- **HDR10 static metadata preserved** — mastering display colour volume and MaxCLL/MaxFALL survive the re-encode
- **Dolby Vision converted**, not skipped — Profiles 7, 8 and 10 become correct HDR10; Profile 5 is refused by design
- **HDR10+ detection and preservation** where the toolchain supports it
- **HLG stays HLG** instead of being mislabelled as PQ
- **Auto lane selection**: CPU or Nvidia chosen per file
- **Per-file Auto analysis** for `CRF`, `Preset`, `FilmGrain`, and `AutoCRFOffset`
- **Concurrent workers**: CPU + multiple NVENC workers when available
- **Preflight estimation** with source-relative size targets
- **Auto skip** for already-efficient files
- **Colour-aware UI** with SDR / HDR / HDR10 / HDR10+ output info
- **Interactive live controls** for workers and queue
- **CSV logging** plus a readable per-session text log
- **Five companion tools** for diagnosis, calibration, library survey, benchmarking, and verification

---

## ⚡ Software vs NVENC

### CPU / SVT-AV1

- best compression efficiency
- best choice for difficult or high-value encodes
- required for HDR10+ inline preservation and for static metadata on some FFmpeg builds
- slower

### Nvidia / AV1 NVENC

- much faster
- lower compression efficiency than SVT-AV1 at similar quality
- good for throughput-friendly files
- supports multiple concurrent workers on supported GPUs
- hierarchical B-frames are enabled where the GPU supports them

The script can run either mode directly, or choose automatically per file.

---

## 🧰 Requirements

- Windows
- PowerShell 7+
- full **FFmpeg 9.0.1 or newer** build with `ffmpeg.exe` and `ffprobe.exe`

FFmpeg 8.x runs with **reduced capability** — no `-mastering_display` / `-content_light`, no Dolby Vision bitstream filters. The script detects this and says so rather than silently producing worse output. FFmpeg 6.x / 7.x and stripped or "essentials" builds are not supported.

For the quality measurement to work, the build also needs:

- the `xpsnr` filter (stock FFmpeg 7.1+, no extra dependency) — used for HDR
- `libvmaf` (`--enable-libvmaf`) — used for SDR

Most full builds (gyan.dev "full", BtbN "gpl") have both. `Media2AV1QueueDoctor.ps1` tells you exactly which of these your build has.

For NVENC:

- NVIDIA GPU with AV1 encode support
- current NVIDIA driver
- `nvidia-smi` available

Recommended:

- place `ffmpeg.exe` and `ffprobe.exe` in the same folder as the script

### Optional external tools

Two optional binaries unlock extra capability. Neither is bundled — download them from upstream and drop the `.exe` next to the script (or set `$HdrToolsDir`):

| Tool | Purpose | Source |
|---|---|---|
| `hdr10plus_tool.exe` | Extract HDR10+ (ST 2094-40) dynamic metadata, and re-inject it into AV1 on builds that support it | [quietvoid/hdr10plus_tool](https://github.com/quietvoid/hdr10plus_tool/releases) |
| `dovi_tool.exe` | Inspect Dolby Vision RPU details for better logging | [quietvoid/dovi_tool](https://github.com/quietvoid/dovi_tool/releases) |

Both are MIT-licensed. Without them, HDR output is still **correct static HDR10** — you just lose the dynamic metadata layer. The Doctor reports which route it can take.

---

## 📦 Installation

```bash
git clone https://github.com/emike09/media-av1-optimizer.git
```

Place `ffmpeg.exe` and `ffprobe.exe` next to the script, or make sure both are on `PATH`.

---

## ✅ First run — do this before touching your library

`$ReplaceOriginal = $true` by default. The script replaces your source files in place. Three steps, in order:

**1. Check the toolchain.**

```powershell
pwsh -File .\Media2AV1QueueDoctor.ps1
pwsh -File .\Media2AV1QueueDoctor.ps1 -TestFile "D:\Movies\Some HDR Movie.mkv"
```

Read-only. It probes the same capabilities the encoder does and prints the answers, then tells you what it *would* do with that specific file and why. Because the script degrades gracefully rather than erroring, this is how you tell "working" apart from "working at reduced capability".

**2. Calibrate the quality thresholds against your own content.**

```powershell
pwsh -File .\Media2AV1QueueQuality.ps1 -Path "D:\Movies\A Film You Know Well.mkv" -KeepSamples
```

Run it on two or three genuinely different files — a clean modern UHD HDR title, something grainy, something animated. It prints the size-versus-quality curve, shows where your configured thresholds land on it, and keeps the sample clips so you can watch them on the display you actually use. **The shipped defaults are starting points, not settled constants** — see [Quality targeting](#-quality-targeting) for why that matters more than it sounds.

**3. Encode a handful of files with `$ReplaceOriginal = $false`.**

Check the outputs. Then turn it back on.

---

## 🖱️ Usage

### Drag and Drop

Drop one or many files onto:

```text
Media2AV1Queue.bat
```

You can also run the batch file with no dropped files to resume an existing queue.

### Interactive

```text
Media2AV1Queue-Interactive.bat
```

Prompts for a quality tier per drop:

| Tier | Effect |
|---|---|
| Auto | Full automatic behaviour |
| Aggressive / Balanced / Quality | Applies a CRF bias to this drop |
| Target | Prompts for an explicit GiB/hr target for this drop, overriding both the ladder and the source-rate cap |

### CLI

```powershell
pwsh .\Media2AV1Queue.ps1 "D:\Movies\SomeMovie.mkv"
pwsh .\Media2AV1Queue.ps1 "D:\Movies\A.mkv" "D:\Movies\B.mkv"
```

---

## 🎯 Quality targeting

This is the part that turns "make the file smaller" into "make the file smaller **without losing anything you can see**".

Before the size projection runs, the script:

1. Encodes short samples at the CRF that Auto chose.
2. **Measures** each one against the same source segment, decoded the same way.
3. Moves CRF and measures again, until it finds the highest CRF that is still visually transparent.
4. Hands that CRF to the size projection, which keeps its veto.

Higher CRF means a smaller file, so "the highest CRF that still looks the same" *is* the objective, expressed as a search. It converges in two or three probes using a secant method over the measured points.

This moves CRF in **both** directions. The Auto ladder is driven by resolution, codec and bits-per-pixel — none of which knows how hard the picture is actually to encode. Easy content is routinely handed far more bitrate than it needs.

### Two metrics, each used only where it is valid

| Source | Metric | Why |
|---|---|---|
| SDR | **VMAF**, absolute target 95 | Its scale is calibrated against human scores, so a fixed target means something. Its models are trained on SDR. |
| HDR | **XPSNR**, anchored | VMAF's models are not trained on PQ or HLG, so an absolute VMAF number on HDR is not interpretable. XPSNR was developed and validated on UHD/HDR material. |

UHD SDR sources use the `vmaf_4k_v0.6.1` model; HD uses `vmaf_v0.6.1`.

### Why XPSNR is anchored, and why you should calibrate

The widely quoted rule is "above 42 dB XPSNR is visually lossless". Measured with identical encoder settings on two different sources:

| Source | CRF 18 | high CRF |
|---|---|---|
| flat, low detail | 64.5 dB | 49.9 dB at CRF 50 |
| heavily grained | 35.1 dB | 32.0 dB at CRF 46 |

A fixed 42 dB threshold would **refuse to compress the grained source at any CRF at all**, and would **wave the flat source through at CRF 50**. The absolute number carries almost no information across content.

So `$QualityAnchorCRF` names a reference *quality level* instead. The search asks "how good would CRF 22 have looked on **this** content?" and finds the cheapest CRF that still looks that good. It costs one extra sample encode per HDR file.

It has a second benefit: the anchor is encoded with the same film-grain settings as the candidates, so the pixel-fidelity penalty that grain synthesis inflicts on *any* reference-based metric is common to both sides and cancels out.

**`$QualityAnchorCRF` is the knob to turn if HDR output is consistently bigger or smaller than you want.** Raise it for smaller files, lower it for higher quality.

### Files that cannot be shrunk are left alone

If no CRF in the permitted range stays transparent, the file is skipped with status `PRECHECK_SKIPPED_QUALITY_FLOOR` instead of being re-encoded at reduced quality. Some sources genuinely cannot be made smaller without visible loss, and re-encoding those is the one outcome this script should never produce.

```powershell
$QualitySkipIfFloorUnreachable = $true
```

### Settings

```powershell
$EnableQualityTargeting = $true    # master switch
$QualityMetric = Auto              # Auto | VMAF | XPSNR | Off
$QualityMode = Auto                # Auto | Absolute | Anchor
$QualitySampleCount = 2            # sample positions per CRF probe
$QualitySampleDurationSec = 15
$QualityMaxSearchPasses = 3        # CRF probes after the first
$QualityMaxCrfStep = 6
$QualityMaxCrfAboveAuto = 12       # never raise CRF more than this above Auto
$QualityMaxCrfBelowAuto = 8
$QualitySkipIfFloorUnreachable = $true
$QualityReportSecondMetric = $true # log the other metric too, as a cross-check

$QualityVmafTarget = 95.0          # SDR
$QualityAnchorCRF = 22             # HDR reference quality level
$QualityXpsnrAnchorDropDb = 0.25   # dB allowed below the anchor
```

Quality targeting only runs when `$CRF = Auto`. A CRF you pinned by hand is treated as an instruction, not a starting guess.

**Cost:** roughly 1.5–3× the old preflight time per file. `$QualityMaxSearchPasses` and `$QualitySampleCount` are the dials.

---

## 🌈 HDR / colour handling

### Static HDR10 metadata

The single most important setting for HDR output quality:

```powershell
$PreserveHdrStaticMetadata = $true
```

Without it, the encode carries PQ/BT.2020 signalling but **no mastering-display colour volume and no MaxCLL/MaxFALL**, so your display falls back to generic tone-mapping assumptions. That is the usual cause of an AV1 re-encode looking flatter than its source on an HDR TV.

The payload usually lives in HEVC SEI rather than in the container, so the script reads it at frame level when the stream-level lookup comes up empty.

### HDR10+ (SMPTE ST 2094-40)

```powershell
$PreserveHDR10Plus = 'Auto'   # Auto | $true | $false
```

Stock FFmpeg and mainline SVT-AV1 cannot carry HDR10+ into AV1. Preserving it needs one of:

- an SVT-AV1 built from `svt-av1-hdr` / SVT-AV1-PSY with `enable-hdr10plus`, which accepts `hdr10plus-json` during the encode, or
- an `hdr10plus_tool` build with AV1 support, used to re-inject the metadata after encoding

The script probes for both and reports which route it took. With neither, output is still correct static HDR10 — it just has no dynamic layer.

### HLG

```powershell
$PreserveHLG = $true
```

HLG sources stay tagged HLG. Setting this to `$false` restores the old behaviour of labelling them PQ.

### MaxCLL sanity check

Some masters declare a MaxCLL brighter than their own mastering-display peak, which cannot be true — the content was graded on that display, so it cannot exceed it.

This is off by default, and deliberately conservative when on:

```powershell
$ClampMaxCllToMasteringPeak = $false
$ClampMaxCllMinPeakNits = 400      # only clamp when the peak is credible
$ClampMaxCllMinOvershoot = 1.5     # AND the overshoot is large
```

Both conditions must hold. A file with a 200-nit peak and a 574-nit MaxCLL is left untouched, because there it is the *peak* that looks wrong and clamping MaxCLL down to 200 would be the damaging edit. Every clamp is written to the log.

---

## 🎥 Dolby Vision

**Dolby Vision sources are now converted rather than skipped.**

```powershell
$DolbyVisionMode = 'HDR10'   # Skip | HDR10 | Passthrough
```

| Profile | Handling |
|---|---|
| **7** (dual layer, UHD Blu-ray) | Base layer extracted with `dovi_split`; output is correct HDR10 |
| **8** (single layer, cross-compatible) | RPU stripped with `dovi_rpu=strip=1`; output is correct HDR10 |
| **10** (DV in AV1) | Converted to HDR10 |
| **5** | **Refused by design.** Its base layer has no HDR10-compatible representation, so re-tagging it as HDR10 produces visibly wrong colour. |

`Passthrough` falls back to base-layer conversion and says so: AV1 can carry a DV RPU as Profile 10, but nothing in this pipeline produces a conformant P10 stream, and emitting a file that *claims* Dolby Vision without being it is worse than converting honestly.

The legacy `$SkipDolbyVisionSources = $true` still works and forces `Skip`.

---

## 🧰 Companion tools

All five are standalone and safe to run at any time. None of them modify your sources.

| Tool | What it is for | When to run it |
|---|---|---|
| **`Media2AV1QueueDoctor.ps1`** | Read-only toolchain diagnosis. Probes FFmpeg version, static HDR10 options and their accepted syntax, DV bitstream filters, HDR10+ routes, quality metrics, every `-svtav1-params` key, and NVENC B-frame support. `-TestFile` adds a per-file "here is what I would do and why". | First, and after any FFmpeg upgrade |
| **`Media2AV1QueueQuality.ps1`** | Measures the size-versus-quality curve of one real file and shows where your thresholds land on it. `-KeepSamples` keeps the clips so you can watch them. | Before committing a library; whenever output size feels wrong |
| **`Media2AV1QueueLibraryScan.ps1`** | Read-only census of a whole library: HDR format breakdown, DV profiles, HDR10+ counts, mastering-peak distribution, MaxCLL contradictions. Writes a CSV. | Before planning a bulk run |
| **`Media2AV1QueueBench.ps1`** | SVT-AV1 threading benchmark. Establishes a noise floor from repeat runs and only calls a winner when the margin beats it. Answers "should `$CpuMaxParallel` be 1 or 2 on this CPU?" | Once, when tuning concurrency |
| **`Media2AV1QueueVerify.ps1`** | End-to-end smoke test. Copies short samples into a sandbox, encodes them, and checks the output really is AV1 10-bit with the right transfer, mastering display, MaxCLL/MaxFALL, audio, subtitles and chapters — and that nothing was fabricated. | After changing HDR settings or upgrading FFmpeg |

`Media2AV1QueueQuality.ps1` loads its encoder settings and functions **out of `Media2AV1Queue.ps1` itself**, so its samples are encoded exactly the way the queue would encode them. A calibration tool with its own argument builder would calibrate the wrong encoder.

`Media2AV1QueueVerify.ps1` requires the queue to be idle and works only on sandbox copies — your originals are never handed to the encoder.

---

## ⚙️ Main settings

The main settings are near the top of `Media2AV1Queue.ps1`.

### Auto-capable quality settings

```powershell
$CRF = Auto
$Preset = Auto
$FilmGrain = Auto
$AutoCRFOffset = Auto
```

`AutoCRFOffset` only applies when `CRF = Auto`.

### Encoder selection

```powershell
$EncoderPreference = 'Auto'   # Auto | CPU | Nvidia
$CpuMaxParallel = 1           # 1-4 simultaneous CPU encodes
$SoftwarePinCores = 0         # cores per CPU encode; only meaningful when CpuMaxParallel > 1
```

Two concurrent SVT-AV1 encodes can beat one wide encode on **total throughput**, because a single instance does not scale perfectly to very high core counts. It is hardware- and content-dependent — `Media2AV1QueueBench.ps1` measures it rather than guessing.

### SVT-AV1 compression efficiency

Every one of these is verified against your encoder before it is used, so an unsupported setting is reported rather than silently discarded.

```powershell
$SoftwareTune = Auto                    # 0 = VQ, 1 = PSNR, 2 = SSIM. Auto = 2
$SoftwareKeyintSeconds = 10             # library default is 161 frames regardless of frame rate
$SoftwareVarianceBoost = Auto           # protects flat and dark areas at higher CRF
$SoftwareVarianceBoostStrength = 2
$SoftwareFilmGrainDenoise = $false      # see Film Grain below
$SoftwareSceneChangeDetection = $false  # off by default; measure before enabling
$SoftwareEnableOverlays = $false        # off by default; measure before enabling
$SoftwareQpScaleCompressStrength = $null
```

`tune` deliberately avoids 1 (PSNR): tuning the encoder for the same family of metric the acceptance test uses optimises the *score* rather than the picture.

### Preflight settings

```powershell
$EnablePreflightEstimate = $true
$EnablePreflightAutoTune = $true
$EnableSecondPreflightPass = $true
$PreflightAutoTuneQuality = 'High'          # Low | Medium | High
$PreflightMaxFractionOfSourceRate = 0.65    # cap the target at this share of the SOURCE's own rate
```

That last one matters. The ladder is resolution- and HDR-based; it has no idea how efficiently the source was already encoded. A 2160p WEB-DL sitting at 6.4 GiB/hr against a ladder target of 10 GiB/hr can only *inflate*. The cap keeps the target both achievable and a genuine saving.

### Queue / file handling

```powershell
$DolbyVisionMode = 'HDR10'      # Skip | HDR10 | Passthrough
$KeepBackupOriginal = $false
$ReplaceOriginal = $true
$KeepEnglishSDH = $false
$KeepEnglishFallbackAudio = $true
```

### Process priority

```powershell
$SoftwareEncodePriority = 'BelowNormal'
$HardwareEncodePriority = 'Normal'
$ScriptProcessPriority = 'Normal'
$ApplyProcessPriority = $true
```

---

## 🤖 Auto mode

Auto mode is resolved **per file when that file begins encoding**, including queued items.

It uses:

- ffprobe stream/format inspection
- bitrate fallback logic
- frame rate parsing
- BPP analysis
- resolution tier classification
- codec class classification
- FFmpeg-only grain pre-scan
- preflight sample encodes
- **measured quality probes**

Auto mode can resolve `CRF`, `Preset`, `FilmGrain`, `AutoCRFOffset`, skip decisions, and the CPU vs Nvidia lane choice. Manual values still stay manual.

---

## 🎞️ Film grain

| Value | Use case |
|------:|----------|
| 0 | Clean CGI / animation |
| 4–8 | Light grain |
| 8–15 | Typical Blu-ray grain |
| 15–25 | Heavy grain |
| 25+ | Extreme / degraded sources |

Auto film grain is capped at `16` by default.

**A measurement worth knowing:** with `film-grain-denoise=0` — the default — the encoder adds synthetic grain *on top of* the grain it already coded. Measured at fixed CRF, film grain 0 → 8 → 16 changed output size by **under 1%**. The look is preserved; essentially nothing is saved.

The size win lives in `$SoftwareFilmGrainDenoise = $true`, where the encoder denoises first (cheap to code) and re-synthesises grain afterwards. That is a real trade — it *replaces* the original grain rather than reproducing it, so reference-based metrics score it lower even when it looks fine. Measure it on your own grainy titles with `Media2AV1QueueQuality.ps1` before turning it on library-wide.

---

## 🚀 NVIDIA NVENC

NVENC mode uses `av1_nvenc`. The script checks FFmpeg encoder support, local `av1_nvenc` options, GPU availability, and GPU model via `nvidia-smi`, then maps the model to a built-in engine-count table.

| GPU | NVENC workers |
|---|---|
| RTX 4090 / 5080 | 2 |
| RTX 4080 / 4070 / 4060 | 1 |
| RTX 5090 | 3 |

Unknown GPUs fall back to `1` worker with a logged warning.

Notes:

- hierarchical B-frames are enabled where supported, detected by a differential test encode rather than by help-text parsing (the relevant option never appears in `-h encoder=av1_nvenc`)
- `-dolbyvision 0` is set explicitly when the target is HDR10, so a surviving RPU cannot be re-emitted into the AV1 stream
- tune is only passed if supported by the local FFmpeg build
- split-frame is disabled by default
- film grain synthesis may be unavailable and can be forced to `0`
- some FFmpeg builds cannot write static HDR10 metadata on the NVENC path; the script holds those files for the CPU lane and says why

---

## 🖥️ Live console controls

During an active queue session:

- `1-9` select a worker, then `p` pause / `r` resume / `s` stop
- `q` then `p` pause queue, `r` resume queue, `c` clear pending queue
- `x` soft-exit after active jobs finish
- `h` toggle help overlay

Pausing suspends the active `ffmpeg` process; resuming resumes that same process. Stopping cancels the job and holds the worker; resuming a held worker restarts that job from scratch. All operator actions are logged.

---

## 📋 Queue behaviour

- drag-and-drop anytime, including while a session is running
- queue persists on disk in `.queue`
- interrupted working items are recovered on restart
- session log written to `.queue\HH-mm-yyyy-MM-dd.log`

---

## 🏷️ Output naming

Output filenames replace common source codec tags with `AV1`:

- `Movie.x265.mkv` → `Movie.AV1.mkv`
- `Show.H.264.1080p.mkv` → `Show.AV1.1080p.mkv`

Handled tokens: `x264`, `x265`, `H.264`, `H.265`, `H264`, `H265`, `HEVC`.

---

## 📊 Logging

### CSV log — `.queue/encode_log.csv`

Alongside the existing size, lane, and Auto-reason columns:

**HDR columns** — `SourceHdrFormat`, `HdrTargetFormat`, `HdrStaticMetadata`, `HdrMaxCLL`, `HdrMaxFALL`, `HdrHDR10PlusSource`, `HdrHDR10PlusOutput`, `DolbyVisionProfile`, `DolbyVisionStrategy`, `HdrPlanSummary`, `MaxCllClamped`

**Quality columns** — `QualityMetric`, `QualityMode`, `QualityThreshold`, `QualityMeasured`, `QualityAnchorCRF`, `QualityAnchorMetric`, `QualityTransparencyMet`, `QualityProbeCount`, `QualityCrfDelta`, `QualitySecondMetric`, `QualitySecondMetricValue`, `SvtEfficiencyParams`

Together these make "did this file actually keep its HDR metadata, and did it actually stay transparent?" answerable from the log alone, months later, without re-probing the output.

**`QualityCrfDelta` is the one to watch.** It records how far the measurement moved CRF away from what the ladder guessed. A consistent pattern in that column across your library tells you whether your Auto ladder is systematically too generous or too tight.

### Session log — `.queue\HH-mm-yyyy-MM-dd.log`

Readable text log for the active session: queue additions, lane decisions, HDR plan, quality search progress, resolved settings, worker actions, final outcomes.

---

## 🧠 Philosophy

- visual quality over maximum compression
- **measure rather than assume** — capabilities are probed functionally, not read out of help text; quality is measured, not estimated from bitrate
- when a capability is missing, say so and degrade honestly rather than silently producing worse output
- explainable heuristics over black-box tuning
- safe queue behaviour over risky shortcuts
- refuse to produce a file that claims to be something it is not

---

## 📜 License

MIT License

Optional external tools (`hdr10plus_tool`, `dovi_tool`) are separate MIT-licensed projects by [quietvoid](https://github.com/quietvoid) and are not distributed with this repository.

---

## 🙌 Contributing

Pull requests welcome.

Ideas:

- QuickSync lane
- AMD lane
- Linux support
- broader quality-threshold calibration data across content types

---

## ⭐ Support

If this helped you, give it a star.
