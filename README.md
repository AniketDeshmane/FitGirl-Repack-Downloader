# FitGirl Repack Downloader

A minimal, dark-themed download manager for FitGirl repacks. It resolves fuckingfast.co page links to direct download URLs and downloads files with an IDM-like interface: concurrent downloads, pause/resume, auto-retry, scheduler, and file categories.

## Features

- **Dual input:** Paste fuckingfast.co links directly, or paste a FitGirl repack page URL to auto-extract all links.
- **Link resolution:** Visits each fuckingfast.co page, extracts the real download URL from the page (handles JavaScript-based download buttons), and parses filename and size. URLs are sanitized (fragment stripped) before requests.
- **Download manager:** Queue multiple files, run up to 10 concurrent downloads (configurable), with per-file progress, speed, and ETA.
- **Pause and resume:** Pause or resume individual downloads or all; supports HTTP Range for resuming partial files.
- **Auto-retry:** Up to 3 retries with exponential backoff on network or server errors.
- **Scheduler:** Set a date and time to start all queued downloads automatically.
- **Categories:** Sidebar filters (All, Archives, Binaries, Videos, Other, Completed, Failed) with counts; files are auto-categorized by extension.
- **Logging:** Per-day rotating log files in `logs/` (e.g. `dm_2026-02-08.log`), kept for 30 days. Optional in-app log viewer.
- **Dark UI:** Single-window layout with toolbar, category sidebar, download table, and status bar. No emojis in the interface.

## Requirements

- Python 3.8+
- PyQt5
- requests
- beautifulsoup4

## Installation

Clone the repository and install dependencies:

```bash
git clone https://github.com/AniketDeshmane/FitGirl-Repack-Downloader.git
cd FitGirl-Repack-Downloader
pip install -r requirements.txt
```

## Running the application

**Option 1 – Python**

```bash
python download_manager.py
```

**Option 2 – Windows (build.cmd)**

Creates a virtual environment if needed, installs dependencies, and starts the app:

```cmd
build.cmd
```

**Option 3 – Linux / macOS / Git Bash (build.sh)**

Same as above for Unix-like environments:

```bash
chmod +x build.sh
./build.sh
```

## Workflow

1. Paste fuckingfast.co links in the text area, or paste a FitGirl repack page URL and click **Extract** to load links.
2. Click **Analyze** to resolve each link to a direct download URL (and optionally click **Download All** to start right after analysis).
3. Choose a download folder (toolbar **Folder**), set max concurrent downloads (1–10), then **Download All** (or **Scheduler** to start at a later time).
4. Use the table and toolbar to pause, resume, or cancel; right-click a row for per-file actions. Use the sidebar to filter by category or status.

## Build and releases

- **From source:** Use the run methods above; no separate build step.
- **Windows executable:** GitHub Actions build a single-file Windows exe:
  - **On every push to `main`:** The workflow runs and uploads the exe as an artifact. Open the [Actions](https://github.com/AniketDeshmane/FitGirl-Repack-Downloader/actions) tab, select the latest run, and download the artifact.
  - **On version tag push:** Push a tag like `v1.0.0` to trigger a build and create a GitHub Release with the exe attached:
    ```bash
    git tag v1.0.0
    git push origin v1.0.0
    ```
  - Release assets are named e.g. `FitGirl-Repack-Downloader-v1.0.0.exe`.

## Project structure

| Path | Description |
|------|-------------|
| `download_manager.py` | Main application (GUI, scrapers, download engine, logging). |
| `requirements.txt` | Python dependencies. |
| `build.cmd` / `build.sh` | Scripts to install deps and run the app (optional venv). |
| `.github/workflows/build-release.yml` | CI: build Windows exe on push to `main` and on tag `v*`; create release only on tag. |
| `logs/` | Per-day log files (created at runtime; see `.gitignore`). |

## Logging

- Logs are written to the `logs/` directory next to the script.
- Daily files are named `dm_YYYY-MM-DD.log` and rotate at midnight; the last 30 days are kept.
- Format: `[YYYY-MM-DD HH:MM:SS] [LEVEL] [DM] message`.
- A collapsible **Logs** panel in the app shows the same log stream.

## License

This project is provided as-is for personal use. Respect FitGirl Repacks and hosting sites’ terms of use and do not abuse their servers.
