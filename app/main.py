"""Pigeonhole: write, run and schedule Python/bash scripts from a web UI."""
import asyncio
import codecs
import json
import os
import re
import secrets
import signal
import sqlite3
import subprocess
import sys
import time
import urllib.request
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

DATA = Path(os.environ.get("DATA_DIR", "/data"))
SCRIPTS_DIR = DATA / "scripts"
RUNS_DIR = DATA / "runs"
WORK_DIR = DATA / "work"
VENV = DATA / "venv"
VENV_BIN = VENV / "bin"
TZ = os.environ.get("TZ", "UTC")
MAX_RUNS = int(os.environ.get("MAX_RUNS", "500"))
STATIC = Path(__file__).parent / "static"

for d in (SCRIPTS_DIR, RUNS_DIR, WORK_DIR):
    d.mkdir(parents=True, exist_ok=True)

db = sqlite3.connect(DATA / "pigeonhole.db", check_same_thread=False)
db.row_factory = sqlite3.Row
db.executescript(
    """
    CREATE TABLE IF NOT EXISTS scripts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        language TEXT NOT NULL,
        content TEXT NOT NULL DEFAULT '',
        schedule TEXT NOT NULL DEFAULT '',
        enabled INTEGER NOT NULL DEFAULT 1,
        timeout INTEGER NOT NULL DEFAULT 0,
        updated REAL NOT NULL
    );
    CREATE TABLE IF NOT EXISTS runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        script_id INTEGER,
        title TEXT NOT NULL,
        trigger TEXT NOT NULL,
        status TEXT NOT NULL,
        exit_code INTEGER,
        started REAL NOT NULL,
        finished REAL
    );
    CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS apt_packages (name TEXT PRIMARY KEY);
    """
)
_columns = {r["name"] for r in db.execute("PRAGMA table_info(scripts)")}
for _name, _ddl in (("env", "TEXT NOT NULL DEFAULT ''"),
                    ("hook_token", "TEXT NOT NULL DEFAULT ''"),
                    ("hook_delay", "INTEGER NOT NULL DEFAULT 60")):
    if _name not in _columns:
        db.execute(f"ALTER TABLE scripts ADD COLUMN {_name} {_ddl}")
db.commit()

scheduler = AsyncIOScheduler(timezone=TZ)


# ---------------------------------------------------------------- streaming
class Stream:
    """Live output of a run, fanned out to any number of SSE subscribers."""

    def __init__(self):
        self.chunks: list[str] = []
        self.done = False
        self.stopped = False
        self.proc: Optional[asyncio.subprocess.Process] = None
        self.cond = asyncio.Condition()

    async def write(self, text: str):
        self.chunks.append(text)
        async with self.cond:
            self.cond.notify_all()

    async def finish(self):
        self.done = True
        async with self.cond:
            self.cond.notify_all()


STREAMS: dict[int, Stream] = {}
RUNNING_SCRIPTS: dict[int, int] = {}  # script id -> run id


def log_path(run_id: int) -> Path:
    return RUNS_DIR / f"{run_id}.log"


def script_path(row) -> Path:
    return SCRIPTS_DIR / f"{row['id']}.{'py' if row['language'] == 'python' else 'sh'}"


def get_setting(key: str, default: str = "") -> str:
    r = db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return r["value"] if r else default


def child_env() -> dict:
    env = dict(os.environ)
    env["PATH"] = f"{VENV_BIN}{os.pathsep}{env.get('PATH', '')}"
    env["VIRTUAL_ENV"] = str(VENV)
    env["PYTHONUNBUFFERED"] = "1"
    env["PIGEONHOLE_DATA"] = str(DATA)
    return env


ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def parse_env(text: str) -> dict[str, str]:
    """Parse KEY=value lines (blank lines and # comments ignored, optional quotes and `export `)."""
    out: dict[str, str] = {}
    for n, line in enumerate(text.replace("\r\n", "\n").split("\n"), 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        key, sep, value = line.partition("=")
        key, value = key.strip(), value.strip()
        if not sep or not ENV_NAME.match(key):
            raise ValueError(f"line {n}: expected KEY=value with a name like MY_VAR")
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        if "\0" in value:
            raise ValueError(f"line {n}: value contains a null byte")
        out[key] = value
    return out


async def execute(run_id: int, stream: Stream, argv: list[str], timeout: int = 0,
                  env: Optional[dict] = None, cwd: Optional[Path] = None):
    logf = open(log_path(run_id), "ab")
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")

    async def emit(text: str):
        if text:
            logf.write(text.encode())
            logf.flush()
            await stream.write(text)

    status, code = "success", None
    try:
        proc = await asyncio.create_subprocess_exec(
            *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            stdin=asyncio.subprocess.DEVNULL, cwd=cwd or WORK_DIR, env=env or child_env(),
            start_new_session=True,
        )
        stream.proc = proc

        async def pump():
            while chunk := await proc.stdout.read(4096):
                await emit(decoder.decode(chunk))

        pump_task = asyncio.create_task(pump())
        try:
            await asyncio.wait_for(proc.wait(), timeout or None)
        except asyncio.TimeoutError:
            status = "timeout"
            kill_group(proc)
            await proc.wait()
            await emit(f"\n[timed out after {timeout}s]\n")
        try:
            await asyncio.wait_for(pump_task, 5)
        except asyncio.TimeoutError:
            pump_task.cancel()
        code = proc.returncode
        if stream.stopped:
            status = "stopped"
            await emit("\n[stopped]\n")
        elif status == "success" and code != 0:
            status = "failed"
        await emit(decoder.decode(b"", final=True))
        if status in ("failed", "success"):
            await emit(f"\n[exit code {code}]\n")
    except Exception as e:  # noqa: BLE001
        status = "failed"
        await emit(f"\n[error: {e}]\n")
    finally:
        logf.close()
        db.execute("UPDATE runs SET status=?, exit_code=?, finished=? WHERE id=?",
                   (status, code, time.time(), run_id))
        db.commit()
        await stream.finish()
        STREAMS.pop(run_id, None)
        prune_runs()
    return status


def kill_group(proc, sig=signal.SIGKILL):
    try:
        os.killpg(os.getpgid(proc.pid), sig)
    except (ProcessLookupError, PermissionError):
        pass


def prune_runs():
    rows = db.execute("SELECT id FROM runs ORDER BY id DESC LIMIT -1 OFFSET ?", (MAX_RUNS,)).fetchall()
    for r in rows:
        log_path(r["id"]).unlink(missing_ok=True)
        db.execute("DELETE FROM runs WHERE id=?", (r["id"],))
    if rows:
        db.commit()


def new_run(title: str, trigger: str, script_id: Optional[int] = None) -> tuple[int, Stream]:
    cur = db.execute("INSERT INTO runs (script_id,title,trigger,status,started) VALUES (?,?,?,?,?)",
                     (script_id, title, trigger, "running", time.time()))
    db.commit()
    stream = Stream()
    STREAMS[cur.lastrowid] = stream
    return cur.lastrowid, stream


def start_script(script_id: int, trigger: str) -> int:
    row = db.execute("SELECT * FROM scripts WHERE id=?", (script_id,)).fetchone()
    if not row:
        raise HTTPException(404, "script not found")
    if script_id in RUNNING_SCRIPTS:
        if trigger == "schedule":
            return RUNNING_SCRIPTS[script_id]
        raise HTTPException(409, "script is already running")
    path = script_path(row)
    path.write_text(row["content"].replace("\r\n", "\n"))
    argv = [str(VENV_BIN / "python"), "-u", str(path)] if row["language"] == "python" else ["bash", str(path)]
    env = child_env()
    env.update(parse_env(row["env"]))
    run_id, stream = new_run(row["name"], trigger, script_id)
    RUNNING_SCRIPTS[script_id] = run_id

    async def job():
        try:
            status = await execute(run_id, stream, argv, row["timeout"], env=env)
        finally:
            RUNNING_SCRIPTS.pop(script_id, None)
            hook_finished(script_id)
        if status in ("failed", "timeout"):
            await asyncio.to_thread(notify_failure, row["name"], status, run_id)

    asyncio.create_task(job())
    return run_id


def start_job(title: str, argv: list[str], on_success=None) -> int:
    run_id, stream = new_run(title, "system")

    async def job():
        status = await execute(run_id, stream, argv)
        if status == "success" and on_success:
            on_success()

    asyncio.create_task(job())
    return run_id


# ------------------------------------------------------------ notifications
def send_ntfy(message: str, title: str = "Pigeonhole", tags: str = "warning") -> Optional[str]:
    """Publish a message to the configured ntfy topic; returns an error string or None."""
    url = get_setting("ntfy_url")
    if not url:
        return "no ntfy URL configured"
    try:
        req = urllib.request.Request(url, data=message.encode(), method="POST",
                                     headers={"Title": title, "Priority": "high", "Tags": tags})
        urllib.request.urlopen(req, timeout=10).read()
    except Exception as e:  # noqa: BLE001
        return f"ntfy: {e}"
    return None


def notify_failure(name: str, status: str, run_id: int):
    if get_setting("ntfy_url"):
        send_ntfy(f"Script '{name}' {status} (run #{run_id})")


# ---------------------------------------------------------------- scheduling
def sync_schedule():
    for job in scheduler.get_jobs():
        job.remove()
    for r in db.execute("SELECT id, schedule FROM scripts WHERE enabled=1 AND schedule!=''"):
        try:
            scheduler.add_job(scheduled_start, CronTrigger.from_crontab(r["schedule"], timezone=TZ),
                              args=[r["id"], "schedule"], id=f"s{r['id']}", coalesce=True, misfire_grace_time=60)
        except ValueError:
            pass


async def scheduled_start(script_id: int, trigger: str):
    start_script(script_id, trigger)


def next_run(script_id: int) -> Optional[float]:
    job = scheduler.get_job(f"s{script_id}")
    return job.next_run_time.timestamp() if job and job.next_run_time else None


# ------------------------------------------------------------------ webhooks
# A webhook call does not run the script straight away. Calls are batched: the script runs once
# the calls have been quiet for hook_delay seconds, and calls that arrive while it is running
# trigger exactly one more run afterwards. All of this state lives in memory; a restart drops it.
HOOK_TIMERS: dict[int, asyncio.TimerHandle] = {}
HOOK_FIRST: dict[int, float] = {}  # script id -> when its oldest waiting call arrived
HOOK_AGAIN: set[int] = set()  # script ids called while running
HOOK_MAX_WAIT = 10  # a waiting run is held back at most this many times the quiet period


def hook_clear(script_id: int):
    timer = HOOK_TIMERS.pop(script_id, None)
    if timer:
        timer.cancel()
    HOOK_FIRST.pop(script_id, None)
    HOOK_AGAIN.discard(script_id)


def hook_arm(script_id: int, delay: int) -> float:
    """(Re)start the quiet-period timer and return how many seconds until the script runs."""
    now = time.monotonic()
    first = HOOK_FIRST.setdefault(script_id, now)
    wait = max(0.0, min(float(delay), first + delay * HOOK_MAX_WAIT - now))
    old = HOOK_TIMERS.pop(script_id, None)
    if old:
        old.cancel()
    HOOK_TIMERS[script_id] = asyncio.get_running_loop().call_later(wait, hook_fire, script_id)
    return wait


def hook_fire(script_id: int):
    HOOK_TIMERS.pop(script_id, None)
    HOOK_FIRST.pop(script_id, None)
    if script_id in RUNNING_SCRIPTS:  # a manual or scheduled run started in the meantime
        HOOK_AGAIN.add(script_id)
        return
    try:
        start_script(script_id, "webhook")
    except HTTPException:  # the script was deleted while waiting
        pass


def hook_finished(script_id: int):
    """Calls that arrived during a run get one more run, after a fresh quiet period."""
    if script_id not in HOOK_AGAIN:
        return
    HOOK_AGAIN.discard(script_id)
    row = db.execute("SELECT hook_token, hook_delay FROM scripts WHERE id=?", (script_id,)).fetchone()
    if row and row["hook_token"]:
        hook_arm(script_id, row["hook_delay"])


# ------------------------------------------------------------------- startup
def apt_installed(name: str) -> bool:
    try:
        return subprocess.run(["dpkg", "-s", name], capture_output=True).returncode == 0
    except FileNotFoundError:
        return False


def apt_argv(action: str, pkgs: list[str]) -> list[str]:
    # the app runs unprivileged; sudoers only allows apt-get
    pre = "sudo -n apt-get update && " if action == "install" else ""
    flags = "--no-install-recommends " if action == "install" else ""
    return ["bash", "-c", f'{pre}sudo -n apt-get {action} -y {flags}"$@"', "_", *pkgs]


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not (VENV_BIN / "python").exists():
        await asyncio.to_thread(subprocess.run, [sys.executable, "-m", "venv", str(VENV)], check=True)
    db.execute("UPDATE runs SET status='failed' WHERE status='running'")
    db.commit()
    scheduler.start()
    sync_schedule()
    missing = [r["name"] for r in db.execute("SELECT name FROM apt_packages")
               if not await asyncio.to_thread(apt_installed, r["name"])]
    if missing and shutil_which("apt-get"):
        start_job("restore apt packages", apt_argv("install", missing))
    yield
    scheduler.shutdown(wait=False)


def shutil_which(cmd: str) -> bool:
    from shutil import which
    return which(cmd) is not None


app = FastAPI(title="Pigeonhole", lifespan=lifespan)


# ----------------------------------------------------------------- scripts API
class ScriptIn(BaseModel):
    name: str
    language: str = "python"
    content: str = ""
    schedule: str = ""
    enabled: bool = True
    timeout: int = 0
    env: str = ""
    hook_delay: int = 60


def validate(s: ScriptIn):
    if s.language not in ("python", "bash"):
        raise HTTPException(400, "language must be python or bash")
    if not s.name.strip():
        raise HTTPException(400, "name is required")
    if not 0 <= s.hook_delay <= 3600:
        raise HTTPException(400, "webhook quiet period must be between 0 and 3600 seconds")
    try:
        parse_env(s.env)
    except ValueError as e:
        raise HTTPException(400, f"environment variables: {e}")
    s.schedule = s.schedule.strip()
    if s.schedule:
        try:
            CronTrigger.from_crontab(s.schedule, timezone=TZ)
        except ValueError as e:
            raise HTTPException(400, f"invalid cron expression: {e}")


def script_dict(r, with_content=True):
    d = dict(r)
    d["enabled"] = bool(d["enabled"])
    d["next_run"] = next_run(r["id"]) if r["enabled"] else None
    d["running_run"] = RUNNING_SCRIPTS.get(r["id"])
    last = db.execute("SELECT status FROM runs WHERE script_id=? AND status!='running' ORDER BY id DESC LIMIT 1",
                      (r["id"],)).fetchone()
    d["last_status"] = last["status"] if last else None
    if not with_content:
        d.pop("content")
    return d


@app.get("/api/scripts")
async def list_scripts():
    return [script_dict(r) for r in db.execute("SELECT * FROM scripts ORDER BY name COLLATE NOCASE")]


@app.post("/api/scripts")
async def create_script(s: ScriptIn):
    validate(s)
    cur = db.execute("INSERT INTO scripts (name,language,content,schedule,enabled,timeout,env,hook_delay,updated) VALUES (?,?,?,?,?,?,?,?,?)",
                     (s.name.strip(), s.language, s.content, s.schedule, int(s.enabled), s.timeout, s.env, s.hook_delay, time.time()))
    db.commit()
    sync_schedule()
    return script_dict(db.execute("SELECT * FROM scripts WHERE id=?", (cur.lastrowid,)).fetchone())


@app.put("/api/scripts/{sid}")
async def update_script(sid: int, s: ScriptIn):
    validate(s)
    if not db.execute("SELECT 1 FROM scripts WHERE id=?", (sid,)).fetchone():
        raise HTTPException(404)
    db.execute("UPDATE scripts SET name=?,language=?,content=?,schedule=?,enabled=?,timeout=?,env=?,hook_delay=?,updated=? WHERE id=?",
               (s.name.strip(), s.language, s.content, s.schedule, int(s.enabled), s.timeout, s.env, s.hook_delay,
                time.time(), sid))
    db.commit()
    sync_schedule()
    return script_dict(db.execute("SELECT * FROM scripts WHERE id=?", (sid,)).fetchone())


@app.delete("/api/scripts/{sid}")
async def delete_script(sid: int):
    row = db.execute("SELECT * FROM scripts WHERE id=?", (sid,)).fetchone()
    if row:
        script_path(row).unlink(missing_ok=True)
    db.execute("DELETE FROM scripts WHERE id=?", (sid,))
    db.commit()
    hook_clear(sid)
    sync_schedule()
    return {"ok": True}


@app.post("/api/scripts/{sid}/run")
async def run_script(sid: int):
    return {"run_id": start_script(sid, "manual")}


# -------------------------------------------------------------------- webhooks API
@app.post("/api/scripts/{sid}/hook")
async def create_hook(sid: int):
    """Create the script's webhook URL token, or replace it (the old URL stops working)."""
    if not db.execute("SELECT 1 FROM scripts WHERE id=?", (sid,)).fetchone():
        raise HTTPException(404)
    db.execute("UPDATE scripts SET hook_token=? WHERE id=?", (secrets.token_urlsafe(24), sid))
    db.commit()
    hook_clear(sid)
    return script_dict(db.execute("SELECT * FROM scripts WHERE id=?", (sid,)).fetchone())


@app.delete("/api/scripts/{sid}/hook")
async def delete_hook(sid: int):
    if not db.execute("SELECT 1 FROM scripts WHERE id=?", (sid,)).fetchone():
        raise HTTPException(404)
    db.execute("UPDATE scripts SET hook_token='' WHERE id=?", (sid,))
    db.commit()
    hook_clear(sid)
    return script_dict(db.execute("SELECT * FROM scripts WHERE id=?", (sid,)).fetchone())


@app.api_route("/api/hooks/{token}", methods=["POST", "PUT"])
async def webhook(token: str, request: Request):
    """Called by Radarr, Sonarr or anything else that can send a POST. The body is ignored."""
    row = db.execute("SELECT id, hook_delay FROM scripts WHERE hook_token=? AND hook_token!=''", (token,)).fetchone()
    if not row:
        raise HTTPException(404, "unknown webhook")
    try:
        payload = await request.json()
    except Exception:  # noqa: BLE001 - an empty or non-JSON body is fine
        payload = None
    if isinstance(payload, dict) and payload.get("eventType") == "Test":  # Radarr/Sonarr "Test" button
        return {"status": "test", "detail": "webhook reachable, script not run"}
    sid = row["id"]
    if sid in RUNNING_SCRIPTS:
        HOOK_AGAIN.add(sid)
        return {"status": "queued", "detail": "script is running, it will run once more afterwards"}
    return {"status": "scheduled", "runs_in": round(hook_arm(sid, row["hook_delay"]))}


# -------------------------------------------------------------------- runs API
@app.get("/api/runs")
async def list_runs(script_id: Optional[int] = None, limit: int = 100):
    q, args = "SELECT * FROM runs", []
    if script_id:
        q += " WHERE script_id=?"
        args.append(script_id)
    return [dict(r) for r in db.execute(q + " ORDER BY id DESC LIMIT ?", (*args, limit))]


@app.post("/api/runs/{rid}/stop")
async def stop_run(rid: int):
    s = STREAMS.get(rid)
    if not s or not s.proc:
        raise HTTPException(404, "run is not active")
    s.stopped = True
    kill_group(s.proc, signal.SIGTERM)
    asyncio.get_running_loop().call_later(5, kill_group, s.proc)
    return {"ok": True}


@app.get("/api/runs/{rid}/stream")
async def stream_run(rid: int):
    async def gen():
        s = STREAMS.get(rid)
        if s:
            i = 0
            while True:
                while i < len(s.chunks):
                    yield f"data: {json.dumps(s.chunks[i])}\n\n"
                    i += 1
                if s.done:
                    break
                async with s.cond:
                    if i >= len(s.chunks) and not s.done:
                        await s.cond.wait()
        elif log_path(rid).exists():
            yield f"data: {json.dumps(log_path(rid).read_text(errors='replace'))}\n\n"
        yield "event: done\ndata: {}\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})


# ------------------------------------------------------------------ packages
class Packages(BaseModel):
    packages: str


PIP_SPEC = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._\-]*(\[[A-Za-z0-9,._\-]+\])?([<>=!~]=?[A-Za-z0-9.*+!_\-]+(,[<>=!~]=?[A-Za-z0-9.*+!_\-]+)*)?$")
APT_NAME = re.compile(r"^[a-z0-9][a-z0-9+.\-]+$")


def parse(p: Packages, rx: re.Pattern) -> list[str]:
    items = p.packages.replace(",", " ").split()
    if not items:
        raise HTTPException(400, "no packages given")
    bad = [i for i in items if not rx.match(i)]
    if bad:
        raise HTTPException(400, f"invalid package name: {bad[0]}")
    return items


@app.get("/api/pip")
async def pip_list():
    out = await asyncio.to_thread(
        subprocess.run, [str(VENV_BIN / "python"), "-m", "pip", "list", "--format=json"], capture_output=True, text=True)
    pkgs = json.loads(out.stdout or "[]")
    return [p for p in pkgs if p["name"].lower() not in ("pip", "setuptools", "wheel")]


@app.post("/api/pip/install")
async def pip_install(p: Packages):
    pkgs = parse(p, PIP_SPEC)
    return {"run_id": start_job(f"pip install {' '.join(pkgs)}",
                                [str(VENV_BIN / "python"), "-m", "pip", "install", "--no-input", *pkgs])}


@app.post("/api/pip/uninstall")
async def pip_uninstall(p: Packages):
    pkgs = parse(p, PIP_SPEC)
    return {"run_id": start_job(f"pip uninstall {' '.join(pkgs)}",
                                [str(VENV_BIN / "python"), "-m", "pip", "uninstall", "-y", *pkgs])}


@app.get("/api/apt")
async def apt_list():
    names = [r["name"] for r in db.execute("SELECT name FROM apt_packages ORDER BY name")]
    return [{"name": n, "installed": await asyncio.to_thread(apt_installed, n)} for n in names]


@app.post("/api/apt/install")
async def apt_install(p: Packages):
    pkgs = parse(p, APT_NAME)

    def remember():
        db.executemany("INSERT OR IGNORE INTO apt_packages (name) VALUES (?)", [(n,) for n in pkgs])
        db.commit()

    return {"run_id": start_job(f"apt install {' '.join(pkgs)}", apt_argv("install", pkgs), remember)}


@app.post("/api/apt/remove")
async def apt_remove(p: Packages):
    pkgs = parse(p, APT_NAME)

    def forget():
        db.executemany("DELETE FROM apt_packages WHERE name=?", [(n,) for n in pkgs])
        db.commit()

    return {"run_id": start_job(f"apt remove {' '.join(pkgs)}", apt_argv("remove", pkgs), forget)}


# ------------------------------------------------------------------ settings
class Settings(BaseModel):
    ntfy_url: str = ""


@app.get("/api/settings")
async def read_settings():
    return {"ntfy_url": get_setting("ntfy_url"), "timezone": TZ}


@app.put("/api/settings")
async def write_settings(s: Settings):
    if s.ntfy_url.strip() and not s.ntfy_url.strip().startswith(("http://", "https://")):
        raise HTTPException(400, "ntfy URL must start with http:// or https://")
    for k, v in s.model_dump().items():
        db.execute("INSERT INTO settings (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                   (k, v.strip()))
    db.commit()
    return {"ok": True}


@app.post("/api/settings/test")
async def test_notification():
    error = await asyncio.to_thread(send_ntfy, "Test notification from Pigeonhole", "Pigeonhole", "white_check_mark")
    if error:
        raise HTTPException(502, error)
    return {"ok": True}


@app.get("/")
async def index():
    return FileResponse(STATIC / "index.html")


@app.middleware("http")
async def revalidate_static(request, call_next):
    response = await call_next(request)
    if request.url.path.startswith("/static/") or request.url.path == "/":
        response.headers["Cache-Control"] = "no-cache"
    return response


app.mount("/static", StaticFiles(directory=STATIC), name="static")
