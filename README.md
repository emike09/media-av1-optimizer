# 🎬 Media AV1 Optimizer

A queue-based, drag-and-drop AV1 encoding pipeline for video libraries.

Built for high-quality archival compression using **SVT-AV1**, with intelligent stream selection, HDR handling, source-aware Auto mode, and safe batch processing.

If you have large Blu-ray libraries or high-bitrate video files and want to compress them using AV1, this script is for you.

> Optimized for English-language media libraries.

---

## ⚡ Performance

- CPU: Ryzen 9 9950X3D (PBO)
- Speed: ~1.4× realtime
- Recommended: **16+ core CPU**
- Average output size: **~11 GB/hour**

---

## 🚀 Features

- 🎬 **AV1 (SVT-AV1) encoding**
- 🧠 **Source-aware Auto mode** for `CRF`, `Preset`, `FilmGrain`, and `AutoCRFOffset`
- 📦 **Massive space savings** (5–10× vs H.265 typical)
- 🌈 **HDR-aware processing**
- 🧾 **Detailed encode logging**
- 🖥️ **Color-aware console UI** showing source/output color format

### 🧠 Intelligent Stream Selection

- Best English audio (TrueHD / DTS-HD / E-AC3 Atmos prioritized)
- Optional fallback audio track
- English subtitles + optional SDH

### 🧹 Automatic Cleanup

- Removes commentary tracks
- Removes foreign audio/subtitles
- Removes junk metadata streams

### 🌈 HDR Awareness

- Preserves HDR10-style signaling
- Detects Dolby Vision and skips by default
- Shows source/output color information in the console UI

### 🤖 Auto Mode

When set to `Auto`, the script evaluates each source independently using:

- ffprobe bitrate fallback logic
- frame rate and BPP analysis
- codec and resolution tier classification
- FFmpeg-only grain pre-scan
- skip detection for already-efficient sources

Auto mode is recalculated **per file when that file begins encoding**, including queued jobs.

### 📋 Queue System

- Drag-and-drop anytime
- Sequential processing
- Unlimited queue size
- Each queued title gets its own fresh Auto analysis

### 🔁 Safe Processing

- Temp file encoding
- Validation before replacement
- Interrupted-job state tracking

### 📝 Logging

- CSV log of all encodes and skips
- Stores resolved Auto settings and diagnostics

---

## 🧰 Requirements

- Windows
- PowerShell 7.0+ (tested on 7.x)
- `ffmpeg.exe` and `ffprobe.exe` from the FFmpeg suite

Recommended:
- place `ffmpeg.exe` and `ffprobe.exe` in the same folder as the script

---

## 📦 Installation

```bash
git clone https://github.com/emike09/media-av1-optimizer.git
```

Place `ffmpeg.exe` and `ffprobe.exe` in the same directory as the script, or ensure both are available on `PATH`.

---

## 🖱️ Usage

### Drag & Drop

Drop files onto:

```text
Media2AV1Queue.bat
```

### CLI

```powershell
pwsh .\Media2AV1Queue.ps1 "D:\Movies\SomeMovie.mkv"
```

---

## ⚙️ Configuration

The main user settings are near the top of [Media2AV1Queue.ps1](</G:/Movies/Scripts/Media2AV1Queue.ps1>).

These values accept either an integer or `Auto`:

```powershell
$CRF = Auto
$Preset = Auto
$FilmGrain = Auto
$AutoCRFOffset = Auto
```

Other important options:

```powershell
$SkipDolbyVisionSources = $true
$KeepEnglishSDH = $false
$KeepEnglishFallbackAudio = $true
$KeepBackupOriginal = $false
$ReplaceOriginal = $true
```

`AutoCRFOffset` only applies when `CRF = Auto`.

Examples:

```powershell
$AutoCRFOffset = -2
```

- makes Auto CRF 2 steps lower quality-number / higher quality

```powershell
$AutoCRFOffset = 3
```

- makes Auto CRF 3 steps higher quality-number / more aggressive compression

---

## 🤖 Auto Mode Details

Auto mode resolves final settings before ffmpeg starts.

### Auto CRF

Uses:

- resolution tier (`SD`, `HD`, `UHD`)
- BPP bucket (`low`, `medium`, `high`)
- SDR vs HDR
- codec class (`legacy`, `standard`, `modern`)

Then applies optional `AutoCRFOffset`.

### Auto Film Grain

Uses:

- FFmpeg-only grain pre-scan when `FilmGrain = Auto`
- conservative fallback logic if pre-scan fails

Film grain mapping:

- `none` -> `0`
- `light` -> `4`
- `moderate` -> `8`
- `heavy` -> `12`
- `extreme` -> `16`

### Auto Preset

Preset stays intentionally simple:

- `3` for harder/high-quality cases
- `4` as the balanced default
- `5` for lower-risk / speed-favored cases

### Auto Skip

Already-efficient low-bitrate sources may be skipped automatically instead of being re-encoded with avoidable generational loss.

Skipped jobs are logged as:

```text
AUTO_SKIPPED_ALREADY_EFFICIENT
```

---

## ⚠️ Dolby Vision

Dolby Vision is **not preserved** during AV1 re-encoding.

By default:

- DV sources are **skipped**

Optional:

- Allow fallback to HDR10-style output by setting:

```powershell
$SkipDolbyVisionSources = $false
```

If DV is not skipped, the output is not Dolby Vision anymore.

---

## 🎞️ Film Grain

AV1 supports **film grain synthesis**, allowing grain to be stored efficiently and reconstructed during playback instead of encoded pixel-by-pixel.

### Manual Configuration

```powershell
$FilmGrain = 0
```

### Recommended Manual Values

| Value | Use Case |
|------:|----------|
| 0 | Clean CGI / animation |
| 4–8 | Light grain |
| 8–15 | Typical Blu-ray grain |
| 15–25 | Heavy grain |
| 25+ | Extreme / degraded sources |

### Notes

- Too low = wasted bitrate
- Too high = artificial noise
- Auto mode caps its default film-grain selection at `16`

> A 70 GB encode at `FilmGrain=0` may drop to ~20–25 GB at `FilmGrain=12` with similar perceived quality.

### Film Grain Encoding Speed

- Without film grain synthesis (`FilmGrain=0`), the encoder tries to preserve every grain pixel
- With film grain synthesis (`FilmGrain > 0`), the encoder stores a compact grain model instead
- Encoding speed can improve significantly on grain-heavy sources

---

## 🎛️ Manual Encoding Profiles

If you prefer manual settings over Auto mode:

### 🔥 Archival Quality

```text
CRF: 10
Preset: 3
```

### ⚖️ High Quality / Compression Balance

```text
CRF: 14–15
Preset: 3
```

### ⚡ Balanced

```text
CRF: 10–12
Preset: 4
```

### 🚀 Faster Encoding

```text
CRF: 10–12
Preset: 5
```

### 📦 Aggressive Compression

```text
CRF: 16–18
Preset: 4–5
```

---

## 🖥️ Console Output

Before encoding, the script prints resolved values and reasoning such as:

- `Auto CRF: 24 (HD / SDR / AVC / medium BPP)`
- `Auto Preset: 4 (balanced default)`
- `Auto FilmGrain: 8 (pre-scan: moderate grain)`
- `Auto Skip: already efficient low-bitrate SDR AVC source`

The live UI also shows:

- resolved AV1 output filename
- source/output color format
- profile (`SDR`, `HDR`, `DV`)
- CRF / preset / elapsed time
- encoded size / speed / ETA

---

## 🏷️ Output Filename Behavior

The script now rewrites common source codec tags in the filename to `AV1` for the output.

Examples:

- `Interstellar.2014.2160p.uhd.bluray.x265.mkv` -> `Interstellar.2014.2160p.uhd.bluray.AV1.mkv`
- `Movie.1080p.HEVC.mkv` -> `Movie.1080p.AV1.mkv`
- `Show.S01E01.H.264.1080p.mkv` -> `Show.S01E01.AV1.1080p.mkv`

Handled tokens:

- `x264`
- `x265`
- `H.264`
- `H.265`
- `H264`
- `H265`
- `HEVC`

If no supported codec token exists in the filename, the basename is left unchanged.

---

## 📊 Logging

Log file:

```text
.queue/encode_log.csv
```

Tracks:

- source/output path
- size reduction
- duration
- HDR/DV detection
- selected streams
- resolved CRF / preset / film grain
- Auto reason
- effective video bitrate
- bitrate per hour
- BPP
- resolution tier
- codec class
- grain class / score
- Auto skip status

---

## 🧪 Tested Scenarios

- 4K HDR remux -> AV1
- Dolby Vision skip logic
- low-bitrate sources with Auto skip
- grain-heavy sources with FFmpeg-only grain pre-scan
- multi-audio / multi-subtitle cluttered files

---

## 🧠 Philosophy

- Visual quality over maximum compression
- Consistency over edge-case perfection
- Automation over manual tuning
- Explainable heuristics over black-box tooling

---

## 📜 License

MIT License

---

## 🙌 Contributing

Pull requests welcome.

Ideas:

- DV-safe workflows
- Auto heuristic refinement
- Linux support
- optional scene-aware FFmpeg sampling improvements

---

## ⭐ Support

If this helped you, give it a star.
