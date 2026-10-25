<div align="center">

# 🕊️ Pigeonhole

**A tiny self-hosted home for your scripts.**
Write Python and bash in the browser, run them with live output, and put them on a schedule, all from one small Docker container.

[![Publish image](https://github.com/josh-uk/pigeonhole/actions/workflows/publish.yml/badge.svg)](https://github.com/josh-uk/pigeonhole/actions/workflows/publish.yml)
![Python 3.12](https://img.shields.io/badge/python-3.12-blue)
![Docker](https://img.shields.io/badge/docker-ghcr.io-2496ED)

</div>

---

## Features

- **Browser editor** for Python and bash scripts, with syntax highlighting and `Ctrl/Cmd+S` to save, `Ctrl/Cmd+Enter` to run.
- **Live output.** Watch stdout and stderr stream in as the script runs, and stop it at any time.
- **Human-friendly scheduling.** Pick "Every 5 minutes", "Weekdays at 08:00" or "Monthly on day 1" from a builder, or drop to raw cron when you need to. The script list shows each schedule in plain English.
- **Run history.** Every run (manual, scheduled or package install) keeps its log, exit code and duration.
- **Install what your scripts need.** Add Python packages with pip and system packages with apt (ffmpeg, jq, imagemagick and so on) from the UI. apt packages are remembered and put back if the container is recreated.
- **Failure alerts** to your [ntfy](https://ntfy.sh) topic when a script fails or times out.
- **Per-script timeouts** and an overlap guard, so a scheduled script never runs on top of itself.

> [!WARNING]
> Pigeonhole has **no built-in authentication** and runs scripts (and installs packages) as **root inside the container**. Only expose it on a trusted network, or put it behind a reverse proxy that handles auth.

## Quick start

### Docker Compose

```yaml
services:
  pigeonhole:
    image: ghcr.io/josh-uk/pigeonhole:latest
    container_name: pigeonhole
    ports:
      - "8080:8080"
    environment:
      - TZ=Europe/London
    volumes:
      - ./data:/data
    restart: unless-stopped
```

```bash
docker compose up -d
```

### Docker run

```bash
docker run -d --name pigeonhole -p 8080:8080 -e TZ=Europe/London -v "$PWD/data:/data" --restart unless-stopped ghcr.io/josh-uk/pigeonhole:latest
```

Then open **http://localhost:8080**.

### Build it yourself

```bash
git clone https://github.com/josh-uk/pigeonhole.git
cd pigeonhole
docker compose up -d --build
```

## Configuration

| Variable   | Default | Description                                                          |
|------------|---------|----------------------------------------------------------------------|
| `TZ`       | `UTC`   | Timezone used for cron schedules and timestamps.                     |
| `MAX_RUNS` | `500`   | How many runs of history (and their logs) to keep.                   |
| `DATA_DIR` | `/data` | Where everything is stored. Mount this as a volume.                  |

## How it works

| Thing | Where it lives |
|-------|----------------|
| Scripts, history, settings | SQLite database in `/data/pigeonhole.db` |
| Script files as they run | `/data/scripts/` |
| Run logs | `/data/runs/` |
| Python packages | A shared virtualenv in `/data/venv` (survives container recreation) |
| System packages | Installed with apt into the container; the list is stored in the database and re-installed on startup if missing |
| Working directory for scripts | `/data/work` |

- Python scripts run with the virtualenv's interpreter, so anything you install on the **Packages** page is importable straight away.
- Scripts can find the data directory via the `PIGEONHOLE_DATA` environment variable.
- Schedules use standard 5-field cron. Weekly schedules are stored with day names (`mon,wed,fri`).

## Notifications

Open **Settings**, paste your ntfy topic URL (for example `https://ntfy.sh/my-topic`) and press **Send test**. From then on a failed or timed-out script publishes a high-priority message.

## Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
DATA_DIR=./data uvicorn app.main:app --reload --port 8080
```

The backend is a single FastAPI app in [`app/main.py`](app/main.py) (APScheduler for cron, asyncio subprocesses for runs, SSE for live logs). The UI is plain HTML and JavaScript in [`app/static/`](app/static), with CodeMirror loaded from a CDN, so there is no build step.

## Image tags

Every merge to `master` publishes to GitHub Container Registry:

- `ghcr.io/josh-uk/pigeonhole:latest`
- `ghcr.io/josh-uk/pigeonhole:sha-<short-commit>` for pinning to an exact build

## Upgrading from Scriptorium

Pigeonhole was previously called Scriptorium. An existing `data/` folder is migrated automatically on first start. If you had scripts that read `SCRIPTORIUM_DATA`, switch them to `PIGEONHOLE_DATA`.
