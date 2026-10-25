# Scriptorium

A small, self-hosted web app (runs as a Docker image) for writing, running and scheduling Python and bash scripts.

## Features
- Browser editor for Python and bash scripts
- Run on demand with live streaming output, or on a cron schedule
- Run history with stored logs
- Install Python packages (pip) and OS packages (apt, e.g. ffmpeg) from the UI
- Failure notifications via webhook or ntfy

> No authentication is built in. Run it on a trusted network or behind a reverse proxy.
