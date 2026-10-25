<div align="center">

# 🐦 Pigeonhole

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
- **Environment variables per script.** Give a script API keys, URLs and other settings from the editor (one `KEY=value` per line) instead of hard-coding them. They are passed to every run, manual or scheduled.
- **Install what your scripts need.** Add Python packages with pip and system packages with apt (ffmpeg, jq, imagemagick and so on) from the UI. apt packages are remembered and put back if the container is recreated.
- **Failure alerts** to your [ntfy](https://ntfy.sh) topic when a script fails or times out.
- **Per-script timeouts** and an overlap guard, so a scheduled script never runs on top of itself.

> [!WARNING]
> Pigeonhole has **no built-in authentication**, and anyone who can open the UI can run code in the container. Scripts run as an unprivileged user, but that user may use `sudo apt-get` (this is how the Packages page installs system packages), so treat the container as trusted. Only expose it on a trusted network, or put it behind a reverse proxy that handles auth.

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
      - PUID=1000   # run as this user id (see `id -u`)
      - PGID=1000   # and this group id (see `id -g`)
    volumes:
      - ./data:/data
    restart: unless-stopped
```

```bash
docker compose up -d
```

### Docker run

```bash
docker run -d --name pigeonhole -p 8080:8080 -e TZ=Europe/London -e PUID=$(id -u) -e PGID=$(id -g) -v "$PWD/data:/data" --restart unless-stopped ghcr.io/josh-uk/pigeonhole:latest
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
| `PUID`     | `1000`  | User id the app and your scripts run as. Files in `/data` are owned by it. |
| `PGID`     | `1000`  | Group id to match.                                                   |
| `MAX_RUNS` | `500`   | How many runs of history (and their logs) to keep.                   |
| `DATA_DIR` | `/data` | Where everything is stored. Mount this as a volume.                  |

## How it works

| Thing | Where it lives |
|-------|----------------|
| Scripts, history, settings | SQLite database in `/data/pigeonhole.db` |
| Script files as they run | `/data/scripts/` |
| Run logs | `/data/runs/` |
| Python packages | A shared virtualenv in `/data/venv` (survives container recreation) |
| System packages | Installed with apt (via `sudo`) into the container; the list is stored in the database and re-installed on startup if missing |
| Working directory for scripts | `/data/work` |

- The container starts as root just long enough to map the `pigeonhole` user to `PUID`/`PGID` and fix ownership of `/data`, then drops to that user. The app and every script run unprivileged; only `apt-get` is allowed through `sudo`.
- Python scripts run with the virtualenv's interpreter, so anything you install on the **Packages** page is importable straight away.
- Scripts can find the data directory via the `PIGEONHOLE_DATA` environment variable.
- Per-script environment variables are stored as plain text in the database (`/data/pigeonhole.db`), next to the script, and are visible to anyone who can open the UI. They override the container's own variables of the same name. Lines starting with `#` are ignored, and values can be wrapped in quotes.
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
