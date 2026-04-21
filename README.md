# 🎬 Media AV1 Optimizer

A queue-based, drag-and-drop AV1 encoding pipeline for video libraries.

Built for high-quality archival compression using **SVT-AV1**, with intelligent stream selection, HDR handling, and safe batch processing.

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
- ⚡ High-quality defaults (**CRF 10 / Preset 4**)
- 📦 **Massive space savings** (5–10× vs H.265 typical)

### 🧠 Intelligent Stream Selection

- Best English audio (TrueHD / DTS-HD / E-AC3 Atmos prioritized)
- Optional fallback audio track
- English subtitles + optional SDH

### 🧹 Automatic Cleanup

- Removes commentary tracks
- Removes foreign audio/subtitles
- Removes junk metadata streams

### 🌈 HDR Awareness

- Preserves HDR10 signaling
- Detects Dolby Vision and skips (configurable)

### 📋 Queue System

- Drag-and-drop anytime
- Sequential processing
- Unlimited queue size

### 🔁 Safe Processing

- Temp file encoding
- Validation before replacement

### 📝 Logging

- CSV log of all encodes
- Size reduction, duration, profile, etc.

---

## 🧰 Requirements

- Windows
- PowerShell 7.0+ (tested on 7.6)
- `ffmpeg` and `ffprobe` version 8+

---

## 📦 Installation

```bash
git clone https://github.com/yourusername/media-av1-optimizer.git
```

Place `ffmpeg.exe` and `ffprobe.exe` in the same directory (recommended).

---

## 🖱️ Usage

### Drag & Drop

Drop files onto:

```text
Drop_Encode_AV1.bat
```

### CLI

```powershell
pwsh Media2AV1Queue.ps1 "D:\Movies\SomeMovie.mkv"
```

---

## ⚙️ Default Behavior

### Encoding

- Codec: AV1 (SVT-AV1)
- CRF: 10
- Preset: 4
- Container: MKV

### Audio

- Keeps best English track
- Keeps optional fallback track

### Subtitles

- Keeps English default
- Optionally keeps SDH

### Video

- Preserves HDR10 metadata
- Skips Dolby Vision sources by default

---

## ⚠️ Dolby Vision

Dolby Vision is **not preserved** during AV1 re-encoding.

By default:
- DV sources are **skipped**

Optional:
- Allow fallback to HDR10

---

## 🎞️ Film Grain

AV1 supports **film grain synthesis**, allowing grain to be stored efficiently and reconstructed during playback instead of encoded pixel-by-pixel.

### Configuration

```powershell
$FilmGrain = 0  # 0 = disabled
```

### Recommended Values

| Value | Use Case |
|------:|----------|
| 0 | Clean CGI / animation |
| 4–8 | Light grain (modern films) |
| 8–15 | Typical Blu-ray grain |
| 15–25 | Heavy grain (e.g. *Saving Private Ryan*) |
| 25+ | Extreme / degraded sources |

### Notes

- Too low = wasted bitrate
- Too high = artificial noise

> A 70GB encode at `FilmGrain=0` may drop to ~20–25GB at `FilmGrain=12` with similar perceived quality.

### Film Grain Encoding Speed
- Without film grain synthesis (FilmGrain=0), the encoder tries to preserve every grain pixel.
- With film grain synthesis (FilmGrain > 0), the encoder removes grain during encoding and stores a compact grain model.
- The decoder later reconstructs this grain.
- Gains on encoding speed can be up to 30%. 

---

## ⚠️ Grain-Heavy Films

Some films contain heavy grain and complex motion.

These will:
- produce very large encodes
- especially at CRF 10–12

### Recommendation

```text
CRF: 14–16
```

or:

```text
FilmGrain: 8–12
```

> AV1 preserves grain extremely well — sometimes *too well*.

---

## 🎛️ Recommended Encoding Profiles

### 🔥 Archival Quality

```text
CRF: 10
Preset: 3
```

- Near-transparent quality
- 4–7× reduction from remux
- Slow but optimal

### ⚖️ High Quality / Compression Balance

```text
CRF: 14–15
Preset: 3
```

- ~30–50% smaller than x265
- Minimal visible loss

### ⚡ Balanced (Default Recommendation)

```text
CRF: 10–12
Preset: 4
```

- Excellent quality
- Good performance

### 🚀 Faster Encoding

```text
CRF: 10–12
Preset: 5
```

- Faster encoding
- Slightly larger files

### 📦 Aggressive Compression

```text
CRF: 16–18
Preset: 4–5
```

- Maximum savings
- Visible artifacts possible

### 🧊 Skip Re-encoding

```text
< ~8 GB/hour → Skip
```

- Minimal gains
- Guaranteed quality loss

---

## 🧠 Preset vs CRF

| Setting | Result |
|--------|--------|
| CRF 10 / Preset 5 | Higher quality, larger file |
| CRF 14 / Preset 3 | Smaller file, more loss |

> Rule: Lower CRF first, then lower preset.

---

## 📊 Logging

```text
.queue/encode_log.csv
```

Tracks:
- size reduction
- duration
- HDR/DV detection
- selected streams

---

## 🧪 Tested Scenarios

- 4K HDR remux → AV1
- Dolby Vision (skip logic)
- AI-upscaled content (Topaz)
- Multi-audio / multi-subtitle cluttered files

---

## 🛠️ Configuration

```powershell
$CRF = 10
$Preset = 4
$FilmGrain = 0
$SkipDolbyVisionSources = $true
$KeepEnglishSDH = $true
$KeepBackupOriginal = $false
```

---

## 🧠 Philosophy

- Visual quality over maximum compression
- Consistency over edge-case perfection
- Automation over manual tuning

---

## 📜 License

MIT License

---

## 🙌 Contributing

Pull requests welcome.

Ideas:
- DV-safe workflows
- HDR/SDR detection improvements
- GPU encoding modes (experimental)
- Linux support

---

## ⚡ Final Note

If it looks good on *Prometheus*, it’ll survive anything.

---

## ⭐ Support

If this helped you, give it a star.
