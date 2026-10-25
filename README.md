# Scriptorium

A small, self-hosted web app (runs as a Docker image) for writing, running and scheduling Python and bash scripts.

## Features
- Browser editor for Python and bash scripts
- Run on demand with live streaming output, or on a cron schedule
- Run history with stored logs
- Install Python packages (pip) and OS packages (apt, e.g. ffmpeg) from the UI
- Failure notifications via [ntfy](https://ntfy.sh)

> No authentication is built in. Run it on a trusted network or behind a reverse proxy.

## Run

```bash
docker compose up -d --build
```

Open http://localhost:8080. Everything (scripts, history, the Python virtualenv, settings) lives in the `/data` volume.

- **Python packages** are installed into a shared virtualenv in `/data/venv`, so they survive container recreation.
- **System packages** (e.g. `ffmpeg`) are installed with apt as root; the list is remembered and re-installed automatically if the container is recreated.
- **Schedules** use standard 5-field cron syntax in the timezone set by `TZ`.
- `MAX_RUNS` (default 500) controls how many runs of history are kept.
