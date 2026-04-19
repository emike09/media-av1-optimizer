# media-av1-optimizer
Automated AV1 encoding pipeline for video libraries with intelligent stream selection, HDR handling, and queue-based processing.
# Plex AV1 Optimizer

A queue-based, drag-and-drop AV1 encoding pipeline for Plex libraries.

Built for high-quality archival compression using **SVT-AV1**, with intelligent stream selection, HDR handling, and safe batch processing.

---

## 🚀 Features

* 🎬 **AV1 (SVT-AV1) encoding**
* ⚡ Fixed high-quality profile (**CRF 10**, preset 4)
* 📦 **Massive space savings** (5–10x typical reduction)
* 🧠 Intelligent stream selection:

  * Best English audio (TrueHD / DTS-HD / E-AC3 Atmos prioritized)
  * Optional fallback audio track
  * English subtitles + optional SDH
* 🧹 Automatic cleanup:

  * Removes commentary tracks
  * Removes foreign audio/subtitles
  * Removes junk metadata streams
* 🌈 HDR-aware:

  * Preserves HDR10 signaling
  * Detects Dolby Vision and safely skips (configurable)
* 📋 Queue system:

  * Drag-and-drop files anytime
  * Processes sequentially
  * No limit on queue size
* 🔁 Safe processing:

  * Temp file encoding
  * Validation before replacement
* 📝 Logging:

  * CSV log of all encodes
  * Size reduction, duration, profile, etc.

---

## 🧰 Requirements

* Windows
* PowerShell 7.0+ (tested on 7.6)
* `ffmpeg` and `ffprobe`

  * Either in PATH or placed next to the script

---

## 📦 Installation

1. Clone the repo:

```bash
git clone https://github.com/yourusername/plex-av1-optimizer.git
```

2. Place `ffmpeg.exe` and `ffprobe.exe` in the same folder (optional but recommended)

3. Done.

---

## 🖱️ Usage

### Drag & Drop

Drag video files onto:

```
Drop_Encode_AV1.bat
```

That’s it.

---

### CLI

```powershell
pwsh PlexAV1Queue.ps1 "D:\Movies\SomeMovie.mkv"
```

---

## ⚙️ Default Behavior

### Encoding

* Codec: AV1 (SVT-AV1)
* CRF: 10
* Preset: 4
* Output: MKV

### Audio

* Keeps best English track
* Keeps optional fallback (AC3/E-AC3/etc.)

### Subtitles

* Keeps English default
* Optionally keeps SDH

### Video

* Preserves HDR10 metadata
* Skips Dolby Vision sources (by default)

---

## ⚠️ Dolby Vision

Dolby Vision is **not preserved** during AV1 re-encoding.

By default, DV sources are skipped to prevent accidental quality loss.

You can override this behavior in the script if you prefer HDR10 fallback.

---

## 📊 Logging

All encodes are logged to:

```
.queue/encode_log.csv
```

Includes:

* Input/output size
* Reduction %
* Duration
* HDR/DV detection
* Selected streams

---

## 🧪 Tested Scenarios

* 4K HDR remux (HEVC → AV1)
* Dolby Vision (skipped)
* AI-upscaled content (Topaz)
* Web-DL sources
* Multi-audio / multi-subtitle cluttered files

---

## 🛠️ Configuration

Edit the script:

```powershell
$CRF = 10
$Preset = 4
$SkipDolbyVisionSources = $true
$KeepEnglishSDH = $true
$KeepBackupOriginal = $false
```

---

## 🧠 Philosophy

This project prioritizes:

* Visual quality over maximum compression
* Consistency over edge-case perfection
* Automation over manual micromanagement

---

## 📜 License

MIT License (see LICENSE file)

---

## 🙌 Contributing

Pull requests welcome.

Ideas:

* DV-safe workflows
* SDR/HDR auto-detection improvements
* GPU encoding modes
* Linux/macOS support

---

## ⚡ Final Notes

If it looks good on *Prometheus*, it’ll survive anything.

---

## ⭐ If this helped you

Give it a star — it helps others find it.
