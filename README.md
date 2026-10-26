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
- **Global environment variables.** Set values every script needs (a shared API key, a base URL) once in Settings instead of copying them into each script. A script's own variable of the same name overrides the global one.
- **Webhook triggers.** Give a script a URL that anything able to send a POST can call to start it. Calls that arrive close together are batched into one run.
- **Chain scripts.** Optionally start another script when one finishes, on success, on failure or either way, so a cleanup can be followed by a report without timing two schedules.
- **Write scripts with an AI client.** A built-in MCP server lets Claude Code, Claude Desktop or another MCP client create, run and fix scripts here, using that client's own account.
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
- Environment variables, per script and global, are stored as plain text in the database (`/data/pigeonhole.db`) and are visible to anyone who can open the UI. Lines starting with `#` are ignored, and values can be wrapped in quotes.
- A run's environment is built in layers, each overriding the one before: the container's own variables, then the global variables from **Settings**, then the script's own. The editor lists the global names a script inherits and marks any it overrides.
- Schedules use standard 5-field cron. Weekly schedules are stored with day names (`mon,wed,fri`).

## Notifications

Open **Settings**, paste your ntfy topic URL (for example `https://ntfy.sh/my-topic`) and press **Send test**. From then on a failed or timed-out script publishes a high-priority message.

## Webhooks

Open a script, expand **Webhook trigger** and press **Create webhook URL** (save a new script first). You get a URL like `http://<server>:8080/api/hooks/<token>`; a `POST` (or `PUT`) to it starts the script. A run started this way shows `webhook` as its trigger in History.

```bash
curl -X POST http://localhost:8080/api/hooks/<token>
```

Calls are batched rather than run one by one, which matters when something sends a burst of them (a batch of files being imported, for example):

- The script runs once the calls have been quiet for the **quiet period** (60 seconds by default, set it next to the URL and press Save; `0` runs straight away). A steady stream cannot hold a run back for more than ten times the quiet period.
- Calls that arrive **while the script is running** trigger one more run after it finishes, again after a quiet period.
- Waiting calls are kept in memory only, so a restart of the container forgets them. Keep a scheduled run as a safety net for anything that matters.

A body is optional. When the call has one, the script receives it in the `WEBHOOK_BODIES` environment variable, a JSON list with the body of every call the run was started for (in order, each cut to 1000 characters, at most 100). A call with no body adds nothing, and the variable is `[]` when no call had a body. It is only set for runs started by a webhook. This lets one webhook carry a choice, for example an action button in a notification that posts the name of the thing to act on:

```python
import json, os
for name in json.loads(os.environ.get("WEBHOOK_BODIES", "[]")):
    print("asked to handle", name)
```

Anything that can send a `POST` or `PUT` can call it: an app's webhook settings, a CI job, a cron entry on another machine, or `curl`. A request whose JSON body has `"eventType": "Test"` (the test button of many apps) is answered with a 200 without running the script. Use an address the sender can reach, such as the server's LAN IP and port.

> [!WARNING]
> The token in the URL is the only protection on a webhook, and anyone who has the URL can start the script. Treat it like a password: keep it on a trusted network, and press **New URL** if it leaks. **Remove** deletes it. The token is also visible to anyone who can open the UI, and appears in the container's request log.

## Chaining scripts

Open a script and expand **After this script**. Choose when (it succeeds, it fails or times out, or either way) and which script to run, then press **Save**. Chaining is off until you pick a script.

- The next script starts as soon as this one finishes, and shows `chain` as its trigger in History. It can have its own next script, so you can build a longer chain.
- A run you stop never starts the next script.
- If the next script is already running, it runs once more when that run finishes.
- Loops (a script that leads back to itself) are refused when you save. Deleting a script clears it from any script that ran it next.

## AI clients (MCP)

Pigeonhole has a built-in [MCP](https://modelcontextprotocol.io) server, so an AI client can write a script, save it here, run it, read the output and fix it until it works. The client does the thinking with its own account; Pigeonhole only provides the tools.

Open **Settings**, find **AI client (MCP)** and press **Turn on**. You get a server URL (`http://<server>:8080/mcp`) and a token. In Claude Code, copy the command shown there, which looks like:

```bash
claude mcp add --transport http --scope user pigeonhole http://<server>:8080/mcp --header "Authorization: Bearer <token>"
```

Other clients: add a Streamable HTTP server with that URL and the header `Authorization: Bearer <token>`. Use an address the client can reach, such as the server's LAN IP and port.

The tools:

| Tool | What it does |
|------|--------------|
| `list_scripts`, `get_script` | Read scripts, their schedules, environment variable names and recent runs |
| `create_script`, `update_script` | Write or change a script, its schedule, timeout and the script to run after it |
| `set_env_vars` | Set or remove a script's environment variables. Values can be set but are never shown back, only the names |
| `run_script`, `get_run`, `list_runs`, `stop_run` | Start a run and wait for its output, follow or stop it, list past runs |
| `list_packages`, `install_packages` | See and install pip and apt packages |
| `create_webhook`, `update_webhook` | Give a script a webhook URL (or replace it) and set its quiet period. The URL is never shown to the client; it can be saved into a script's environment variable instead |
| `delete_webhook` | Remove a script's webhook URL |
| `delete_script` | Delete a script |

The client is also told how scripts run here (working directory, `PIGEONHOLE_DATA`, environment variables for settings and secrets, exit codes for alerts), so what it writes fits from the start. Runs it starts show `mcp` as their trigger in History.

Switches in Settings limit what a client may change: creating and changing scripts (and their webhooks) and running them are on by default, installing packages and deleting scripts or webhooks are off. Reading is always allowed while MCP is on. Most clients also ask you to approve each tool call.

> [!WARNING]
> A client with the token can run code in the container, as a person using the UI can. Keep the token private, use MCP on a trusted network, and press **New token** if it leaks; **Turn off** disables MCP entirely. The token is visible to anyone who can open the UI.

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
