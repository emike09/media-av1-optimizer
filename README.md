# 🎬 Media AV1 Optimizer

A queue-based AV1 encoding pipeline for video libraries with:

- software AV1 via `libsvtav1`
- hardware AV1 via NVIDIA `av1_nvenc`
- source-aware Auto mode
- queue persistence
- live console controls

Built for large media libraries where quality, automation, and safe batch handling all matter.

> Optimized for English-language media libraries.

---

## 🚀 Highlights

- AV1 encoding with **SVT-AV1** or **NVIDIA NVENC**
- **Auto lane selection**: CPU or Nvidia chosen per file
- **Per-file Auto analysis** for `CRF`, `Preset`, `FilmGrain`, and `AutoCRFOffset`
- **Concurrent workers**: CPU + multiple NVENC workers when available
- **FFmpeg-only preflight estimation** with optional auto-retuning
- **Auto skip** for already-efficient files
- **Color-aware UI** with SDR / HDR / HDR10 / HDR10+ output info
- **Interactive live controls** for workers and queue
- **CSV logging** plus a readable per-session text log

---

## ⚡ Software vs NVENC

### CPU / SVT-AV1

- best compression efficiency
- best choice for difficult or high-value encodes
- slower

### Nvidia / AV1 NVENC

- much faster
- lower compression efficiency than SVT-AV1 at similar quality
- good for throughput-friendly files
- supports multiple concurrent workers on supported GPUs

The script can run either mode directly, or choose automatically per file.

---

## 🧰 Requirements

- Windows
- PowerShell 7+
- full **FFmpeg 8.1** build with `ffmpeg.exe` and `ffprobe.exe`

For NVENC:

- NVIDIA GPU with AV1 encode support
- current NVIDIA driver
- `nvidia-smi` available

Recommended:

- place `ffmpeg.exe` and `ffprobe.exe` in the same folder as the script

Older FFmpeg 6.x / 7.x and stripped/basic builds are not supported.

---

## 📦 Installation

```bash
git clone https://github.com/emike09/media-av1-optimizer.git
```

Place `ffmpeg.exe` and `ffprobe.exe` next to the script, or make sure both are on `PATH`.

---

## 🖱️ Usage

### Drag and Drop

Drop one or many files onto:

```text
Media2AV1Queue.bat
```

You can also run the batch file with no dropped files to resume an existing queue.

### CLI

```powershell
pwsh .\Media2AV1Queue.ps1 "D:\Movies\SomeMovie.mkv"
```

Multiple files are supported:

```powershell
pwsh .\Media2AV1Queue.ps1 "D:\Movies\A.mkv" "D:\Movies\B.mkv"
```

---

## ⚙️ Main Settings

The main settings are near the top of [Media2AV1Queue.ps1](</G:/Movies/Scripts/Media2AV1Queue.ps1>).

### Auto-capable Quality Settings

These accept an integer or `Auto`:

```powershell
$CRF = Auto
$Preset = Auto
$FilmGrain = Auto
$AutoCRFOffset = Auto
```

`AutoCRFOffset` only applies when `CRF = Auto`.

### Encoder Selection

```powershell
$EncoderPreference = 'Auto' # Auto | CPU | Nvidia
```

- `Auto` = choose CPU or Nvidia per file
- `CPU` = force software `libsvtav1`
- `Nvidia` = force `av1_nvenc`

### NVENC Settings

```powershell
$NvencMaxParallel = Auto
$NvencCQ = Auto
$NvencPreset = Auto
$NvencDecode = Auto
$NvencTune = 'auto'
$NvencAllowSplitFrame = $false
```

### Preflight Settings

```powershell
$EnablePreflightEstimate = $true
$EnablePreflightAutoTune = $true
$EnableSecondPreflightPass = $true
$PreflightAutoTuneQuality = 'High' # Low | Medium | High
```

- `Low` = smaller files
- `Medium` = balanced
- `High` = more quality-preserving

### Queue / File Handling

```powershell
$SkipDolbyVisionSources = $true
$KeepBackupOriginal = $false
$ReplaceOriginal = $true
$KeepEnglishSDH = $false
$KeepEnglishFallbackAudio = $true
```

### Process Priority

```powershell
$SoftwareEncodePriority = 'BelowNormal'
$HardwareEncodePriority = 'Normal'
$ScriptProcessPriority = 'Normal'
$ApplyProcessPriority = $true
```

By default, CPU-heavy software encodes run at a lower OS priority to keep the system responsive.

---

## 🤖 Auto Mode

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

Auto mode can resolve:

- `CRF`
- `Preset`
- `FilmGrain`
- `AutoCRFOffset`
- skip decisions
- CPU vs Nvidia lane choice

Manual values still stay manual.

---

## 🧪 Preflight

The script can run sample-based preflight encodes before the full encode starts.

Preflight is used for:

- projected final size
- projected savings
- projected GiB/hr
- warn / skip decisions
- Auto retuning before the real encode starts

If Auto retuning is enabled, preflight can conservatively adjust:

- CRF first
- FilmGrain second
- Preset only in limited cases

The script can also run a second preflight pass when the first pass suggests the initial Auto settings need correction.

---

## 🚀 NVIDIA NVENC

NVENC mode uses `av1_nvenc`.

The script checks:

- FFmpeg encoder support
- local `av1_nvenc` options
- NVIDIA GPU availability
- GPU model via `nvidia-smi`

It then maps the detected GPU model to a built-in NVENC engine-count table.

Examples:

- `RTX 4090` -> `2`
- `RTX 4080` -> `1`
- `RTX 4070` -> `1`
- `RTX 4060` -> `1`
- `RTX 5080` -> `2`
- `RTX 5090` -> `3`

If the GPU is unknown, the script falls back to `1` NVENC worker and logs a warning.

NVENC notes:

- tune is only passed if supported by the local FFmpeg build
- split-frame is disabled by default
- film grain synthesis may be unavailable and can be forced to `0`
- Dolby Vision is still not preserved

---

## 🧠 CPU / Nvidia Lane Selection

When `EncoderPreference = 'Auto'`, the script chooses a lane per file.

It evaluates:

- source complexity
- SDR vs HDR
- resolution tier
- codec efficiency
- grain class
- preflight results
- current CPU / Nvidia worker availability

This lets the script:

- keep CPU and Nvidia busy at the same time
- avoid weak NVENC fallbacks for bad-fit sources
- hold difficult files for CPU when quality/compression would suffer on NVENC

---

## 🎞️ Film Grain

AV1 film grain synthesis can save a lot of space on grainy material.

Manual examples:

| Value | Use Case |
|------:|----------|
| 0 | Clean CGI / animation |
| 4–8 | Light grain |
| 8–15 | Typical Blu-ray grain |
| 15–25 | Heavy grain |
| 25+ | Extreme / degraded sources |

Auto mode uses:

- grain pre-scan when available
- conservative fallback logic otherwise

Auto film grain is capped at `16` by default.

---

## 🌈 HDR / Color Handling

The script reports both source and output color information.

It shows:

- SDR / HDR / DV profile
- 8-bit / 10-bit
- Rec.709 / Rec.2020
- PQ / HLG when applicable
- HDR10 / HDR10+ labeling where detected

The live UI also highlights `HDR`, `HDR10`, and `HDR10+` with rainbow coloring.

Dolby Vision is not preserved in AV1 output.

---

## 🖥️ Live Console Controls

During an active queue session:

- `1-9` select a worker
- then `p` pause worker
- then `r` resume worker
- then `s` stop worker
- `q` then `p` pause queue
- `q` then `r` resume queue
- `q` then `c` clear pending queue
- `x` soft-exit after active jobs finish
- `h` toggle help overlay

Behavior:

- pausing a worker suspends the active `ffmpeg` process in the current session
- resuming a paused worker resumes that same process
- stopping a worker cancels that job and moves the worker to a held state
- resuming a held worker restarts the same job from scratch
- queue pause stops new assignments but leaves active workers alone
- soft exit stops new work and exits after active jobs finish

All operator actions are logged.

---

## 📋 Queue Behavior

- drag-and-drop anytime
- add files while another session is already running
- queue persists on disk in `.queue`
- interrupted working items are recovered on restart
- active session log is written to `.queue\HH-mm-yyyy-MM-dd.log`

The session log is intended to be human-readable and includes:

- queue additions
- lane decisions
- preflight decisions
- resolved settings
- worker actions
- final outcomes

---

## 🏷️ Output Naming

Output filenames replace common source codec tags with `AV1`.

Examples:

- `Movie.x265.mkv` -> `Movie.AV1.mkv`
- `Movie.HEVC.mkv` -> `Movie.AV1.mkv`
- `Show.H.264.1080p.mkv` -> `Show.AV1.1080p.mkv`

Handled tokens:

- `x264`
- `x265`
- `H.264`
- `H.265`
- `H264`
- `H265`
- `HEVC`

---

## 📊 Logging

### CSV Log

```text
.queue/encode_log.csv
```

Tracks:

- source/output path
- size reduction
- duration
- encode mode
- resolved lane
- resolved CRF / preset / film grain
- resolved NVENC CQ / preset / tune / decode path
- Auto reason
- preflight estimates and retuning
- bitrate / BPP / resolution / codec / grain diagnostics
- GPU / NVENC capacity information

### Session Log

```text
.queue\HH-mm-yyyy-MM-dd.log
```

Readable text log for the active session.

---

## ⚠️ Dolby Vision

Dolby Vision is not preserved during AV1 re-encoding.

By default:

- DV sources are skipped

Optional:

```powershell
$SkipDolbyVisionSources = $false
```

If disabled, the output is no longer Dolby Vision.

---

## 🧪 Tested / Covered Scenarios

- 4K HDR remux -> AV1
- SDR and HDR web encodes
- low-bitrate sources with Auto skip
- grain-heavy sources with grain pre-scan
- NVENC capability detection and worker scheduling
- CPU + Nvidia concurrent lane scheduling
- interrupted-job recovery
- bulk drag-and-drop queue additions

---

## 🧠 Philosophy

- visual quality over maximum compression
- quality-first automation
- explainable heuristics over black-box tuning
- safe queue behavior over risky shortcuts

---

## 📜 License

MIT License

---

## 🙌 Contributing

Pull requests welcome.

Ideas:

- QuickSync lane
- AMD lane
- more encode heuristics tuning
- Linux support

---

## ⭐ Support

If this helped you, give it a star.
