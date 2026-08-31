# 🎬 Media AV1 Optimizer

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](#-license)
[![PowerShell 7+](https://img.shields.io/badge/PowerShell-7%2B-5391FE.svg?logo=powershell&logoColor=white)](https://github.com/PowerShell/PowerShell)
[![FFmpeg 9+](https://img.shields.io/badge/FFmpeg-9.0.1%2B-007808.svg)](https://www.gyan.dev/ffmpeg/builds/)
[![Platform: Windows](https://img.shields.io/badge/Platform-Windows-0078D6.svg)](#requirements)

**Convert an H.264/H.265 library to AV1 and make the files smaller without losing anything you can see.**

That is the whole goal, and the thing that makes this different from a batch script is the second half of it. Most tools answer *"how big will this be?"* with a bitrate heuristic. This one encodes short samples, **measures** them against the source with VMAF or XPSNR, and searches for the highest CRF that still looks the same — then hands that to the size projection, which keeps its veto.

One PowerShell file and a launcher. Drag files on to encode; double-click for a menu.

> Built for large English-language media libraries where quality, automation and safe batch handling all matter.

---

## What it does

- **AV1 via SVT-AV1 or NVIDIA NVENC**, chosen per file, with concurrent workers
- **Measured quality targeting** — the CRF is searched for and verified, not guessed ([how](docs/quality.md))
- **Honest about the metric's limits** — where an absolute VMAF target cannot discriminate (grainy film transfers), the file is converted at the Auto CRF rather than refused, and the run says so
- **HDR10 static metadata survives** the re-encode — mastering display colour volume, MaxCLL/MaxFALL
- **Dolby Vision converted, not skipped** — Profiles 7, 8 and 10 become correct HDR10; Profile 5 is refused by design
- **HDR10+ preserved** where the toolchain supports it; **HLG stays HLG**
- **Per-file Auto analysis** for `CRF`, `Preset`, `FilmGrain` and `AutoCRFOffset`
- **Persistent queue** with live pause / resume / stop controls and crash recovery
- **Seven built-in tools** — diagnose the toolchain, rank conversion candidates, calibrate thresholds, normalise loudness, census a library, benchmark threading, verify output
- **CSV + session logging**, and a summary at the end of every run

---

## Requirements

| | |
|---|---|
| OS | Windows |
| Shell | PowerShell 7+ |
| Encoder | full **FFmpeg 9.0.1+** build (`ffmpeg.exe`, `ffprobe.exe`) with `libvmaf` and the `xpsnr` filter |
| NVENC *(optional)* | NVIDIA GPU with AV1 encode, current driver, `nvidia-smi` on `PATH` |

Most full builds — gyan.dev "full", BtbN "gpl" — have everything needed. FFmpeg 8.x runs with **reduced capability** and says so; 6.x / 7.x and "essentials" builds are not supported. The **Doctor** tool tells you exactly what your build can do.

Two optional binaries unlock HDR10+ preservation and richer Dolby Vision logging — see [HDR docs](docs/hdr.md#optional-external-tools).

---

## Install

Download **`Media2AV1Queue.ps1`** and **`Media2AV1Queue.bat`** from the [latest release](../../releases/latest) into one folder. That is the whole install.

```text
Media2AV1Queue.bat      drag files onto this, or double-click it
Media2AV1Queue.ps1      everything: encoder, tools, settings
ffmpeg.exe              (or on PATH)
ffprobe.exe
```

| | |
|---|---|
| **Drag files or folders onto the .bat** | They get encoded. No menu, no questions. |
| **Double-click the .bat** | A menu opens. |

```text
  [1] Auto           Encode files. Everything decided for you.
  [2] Interactive    Encode files, choosing quality and encoder yourself.
  [3] Tools          Check, scan, measure, calibrate, verify.
```

Settings live in the `User-configurable settings` block near the top of the `.ps1`.

---

## ✅ First run — do this before touching your library

`$ReplaceOriginal = $true` by default: **the script replaces your source files in place.** Four steps, in order.

**1. Check the toolchain.**

```powershell
pwsh -File .\Media2AV1Queue.ps1 -Mode Doctor
```

Read-only. It probes the same capabilities the encoder does and prints the answers, then — with `-TestFile` — tells you what it *would* do with that specific file and why. Because the script degrades gracefully rather than erroring, this is how you tell "working" apart from "working at reduced capability".

**2. Calibrate the quality thresholds against your own content.**

```powershell
pwsh -File .\Media2AV1Queue.ps1 -Mode Quality
```

Run it on two or three genuinely different files — a clean modern UHD HDR title, something grainy, something animated. **The shipped defaults are starting points, not settled constants**, and [Quality targeting](docs/quality.md) shows why that matters more than it sounds.

**3. Encode a handful of files with `$ReplaceOriginal = $false`.** Check the outputs. Then turn it back on.

**4. Find out what is actually worth converting.**

```powershell
pwsh -File .\Media2AV1Queue.ps1 -Mode Candidates
```

Container headers only — roughly 50 ms per file on local storage, so a few thousand files take minutes rather than hours.

---

## The idea in one table

Two measurements that shaped how this works, both taken with identical encoder settings:

| Source | CRF 22 | CRF 30 | What a fixed threshold does |
|---|---|---|---|
| clean 1080p | VMAF 98.4 | — | passes easily |
| heavily grained 1080p | VMAF 75.0 | VMAF 73.8, **6.8× smaller** | refuses it at every CRF |

VMAF moves under two points across sixteen CRF steps on grain, because the score is dominated by noise no bitrate reproduces exactly. A fixed "VMAF ≥ 95" gate therefore rejects every film-grain Blu-ray remux — the files most worth converting.

So the script does not treat "no CRF was transparent" as "this file cannot be converted". It keeps the CRF the Auto ladder chose, says so on one line, and carries on. If your library is mostly grainy film transfers, `$QualityMode = 'Anchor'` compares every CRF against a reference encode of the *same* content instead, which cancels the grain penalty entirely.

**[→ Full explanation, both metrics, and all the settings](docs/quality.md)**

---

## Documentation

| | |
|---|---|
| **[Quality targeting](docs/quality.md)** | How the CRF search works, VMAF vs XPSNR, why XPSNR is anchored, what to do about grain, all quality settings |
| **[HDR and Dolby Vision](docs/hdr.md)** | Static HDR10 metadata, HDR10+, HLG, the MaxCLL sanity check, DV profile handling |
| **[The seven tools](docs/tools.md)** | What each tool is for, plus deep dives on finding candidates and loudness normalisation |
| **[Settings reference](docs/settings.md)** | Every setting, Auto mode, film grain, SVT-AV1 efficiency, preflight, NVENC |
| **[Running it day to day](docs/operating.md)** | Drag-drop / interactive / CLI, queue behaviour, live controls, output naming, log schema |
| **[Release notes](RELEASE-NOTES.md)** | Changelog |

---

## Building it yourself

The single file is generated. The sources live in `src\`:

```powershell
pwsh -File .\Build-Single.ps1
```

Edit `src\*.ps1`, rebuild, ship the one file. The build refuses to write an output that does not parse, that lost a function, or that kept an `exit` where one would drop a user out of the menu.

---

## 🧠 Philosophy

- visual quality over maximum compression
- **measure rather than assume** — capabilities are probed functionally, not read out of help text; quality is measured, not inferred from bitrate
- when a capability is missing, say so and degrade honestly rather than silently producing worse output
- when a *measurement* cannot decide, say that too, and fall back to something defensible — never refuse work on the strength of a number that carries no information
- explainable heuristics over black-box tuning
- safe queue behaviour over risky shortcuts
- refuse to produce a file that claims to be something it is not

---

## 📜 License

MIT. Optional external tools (`hdr10plus_tool`, `dovi_tool`) are separate MIT-licensed projects by [quietvoid](https://github.com/quietvoid) and are not distributed with this repository.

## 🙌 Contributing

Pull requests welcome. Ideas: a QuickSync lane, an AMD lane, Linux support, broader quality-threshold calibration data across content types.

## ⭐ Support

If this helped you, give it a star.
