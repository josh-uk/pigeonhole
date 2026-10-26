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
from typing import Any, Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations
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
                    ("hook_delay", "INTEGER NOT NULL DEFAULT 60"),
                    ("chain_next", "INTEGER NOT NULL DEFAULT 0"),
                    ("chain_on", "TEXT NOT NULL DEFAULT 'success'")):
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


def set_setting(key: str, value: str):
    db.execute("INSERT INTO settings (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
               (key, value))
    db.commit()


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
    env.update(parse_env(get_setting("global_env")))  # global variables, then the script's own on top
    env.update(parse_env(row["env"]))
    run_id, stream = new_run(row["name"], trigger, script_id)
    RUNNING_SCRIPTS[script_id] = run_id

    async def job():
        try:
            status = await execute(run_id, stream, argv, row["timeout"], env=env)
        finally:
            RUNNING_SCRIPTS.pop(script_id, None)
            hook_finished(script_id)
        chain_finished(script_id, status)
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


# ------------------------------------------------------------------ chaining
# A script can name another to start when it finishes (chain_next, 0 for none), on success, on
# failure or either way (chain_on). A stopped run never starts the next one. Loops are refused when
# a script is saved, so a chain always ends.
CHAIN_WHEN = {
    "success": lambda status: status == "success",
    "failure": lambda status: status in ("failed", "timeout"),
    "always": lambda status: status != "stopped",
}
CHAIN_AGAIN: set[int] = set()  # chained scripts that were already running when their turn came


def chain_start(script_id: int):
    try:
        start_script(script_id, "chain")
    except HTTPException as e:
        if e.status_code == 409:  # already running: run it once more when that run finishes
            CHAIN_AGAIN.add(script_id)


def chain_finished(script_id: int, status: str):
    if script_id in CHAIN_AGAIN:
        CHAIN_AGAIN.discard(script_id)
        chain_start(script_id)
    row = db.execute("SELECT chain_next, chain_on FROM scripts WHERE id=?", (script_id,)).fetchone()
    if row and row["chain_next"] and CHAIN_WHEN.get(row["chain_on"], CHAIN_WHEN["success"])(status):
        chain_start(row["chain_next"])


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
    async with mcp_server.session_manager.run():
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
    chain_next: int = 0
    chain_on: str = "success"


def validate(s: ScriptIn, sid: Optional[int] = None):
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
    if s.chain_on not in CHAIN_WHEN:
        raise HTTPException(400, "chain_on must be success, failure or always")
    if s.chain_next:
        validate_chain(s, sid)


def validate_chain(s: ScriptIn, sid: Optional[int]):
    """Refuse a next script that doesn't exist, or one whose own chain leads back here."""
    if s.chain_next == sid:
        raise HTTPException(400, "a script can't run itself next")
    names, seen, nxt = [s.name.strip()], {sid}, s.chain_next
    while nxt:
        row = db.execute("SELECT id, name, chain_next FROM scripts WHERE id=?", (nxt,)).fetchone()
        if not row:
            if nxt == s.chain_next:
                raise HTTPException(400, "the script to run next doesn't exist")
            break
        names.append(row["name"])
        if row["id"] in seen:
            raise HTTPException(400, f"that would make a loop: {' → '.join(names)}")
        seen.add(row["id"])
        nxt = row["chain_next"]


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
    cur = db.execute("INSERT INTO scripts (name,language,content,schedule,enabled,timeout,env,hook_delay,chain_next,chain_on,updated) "
                     "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                     (s.name.strip(), s.language, s.content, s.schedule, int(s.enabled), s.timeout, s.env, s.hook_delay,
                      s.chain_next, s.chain_on, time.time()))
    db.commit()
    sync_schedule()
    return script_dict(db.execute("SELECT * FROM scripts WHERE id=?", (cur.lastrowid,)).fetchone())


@app.put("/api/scripts/{sid}")
async def update_script(sid: int, s: ScriptIn):
    if not db.execute("SELECT 1 FROM scripts WHERE id=?", (sid,)).fetchone():
        raise HTTPException(404)
    validate(s, sid)
    db.execute("UPDATE scripts SET name=?,language=?,content=?,schedule=?,enabled=?,timeout=?,env=?,hook_delay=?,"
               "chain_next=?,chain_on=?,updated=? WHERE id=?",
               (s.name.strip(), s.language, s.content, s.schedule, int(s.enabled), s.timeout, s.env, s.hook_delay,
                s.chain_next, s.chain_on, time.time(), sid))
    db.commit()
    sync_schedule()
    return script_dict(db.execute("SELECT * FROM scripts WHERE id=?", (sid,)).fetchone())


@app.delete("/api/scripts/{sid}")
async def delete_script(sid: int):
    row = db.execute("SELECT * FROM scripts WHERE id=?", (sid,)).fetchone()
    if row:
        script_path(row).unlink(missing_ok=True)
    db.execute("DELETE FROM scripts WHERE id=?", (sid,))
    db.execute("UPDATE scripts SET chain_next=0 WHERE chain_next=?", (sid,))  # scripts that ran it next run nothing
    db.commit()
    hook_clear(sid)
    CHAIN_AGAIN.discard(sid)
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
    """Called by anything that can send a POST. The body is ignored."""
    row = db.execute("SELECT id, hook_delay FROM scripts WHERE hook_token=? AND hook_token!=''", (token,)).fetchone()
    if not row:
        raise HTTPException(404, "unknown webhook")
    try:
        payload = await request.json()
    except Exception:  # noqa: BLE001 - an empty or non-JSON body is fine
        payload = None
    if isinstance(payload, dict) and payload.get("eventType") == "Test":  # a sender's "Test" button
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
    # fields left out of a request are not changed
    ntfy_url: Optional[str] = None
    global_env: Optional[str] = None


@app.get("/api/settings")
async def read_settings():
    return {"ntfy_url": get_setting("ntfy_url"), "global_env": get_setting("global_env"), "timezone": TZ}


@app.put("/api/settings")
async def write_settings(s: Settings):
    if s.ntfy_url is not None:
        if s.ntfy_url.strip() and not s.ntfy_url.strip().startswith(("http://", "https://")):
            raise HTTPException(400, "ntfy URL must start with http:// or https://")
        set_setting("ntfy_url", s.ntfy_url.strip())
    if s.global_env is not None:
        try:
            parse_env(s.global_env)
        except ValueError as e:
            raise HTTPException(400, f"global environment variables: {e}")
        set_setting("global_env", s.global_env.rstrip())
    return {"ok": True}


@app.post("/api/settings/test")
async def test_notification():
    error = await asyncio.to_thread(send_ntfy, "Test notification from Pigeonhole", "Pigeonhole", "white_check_mark")
    if error:
        raise HTTPException(502, error)
    return {"ok": True}


# ----------------------------------------------------------------------- MCP
# An MCP server at /mcp lets an AI client (Claude Code, Claude Desktop, ...) list, write, run and fix
# scripts with the person's own account. It is off until a token is created in Settings, every
# request must carry that token, and switches in Settings limit what the client may change.
MCP_PERMISSIONS = {"write": "1", "run": "1", "packages": "0", "delete": "0"}  # defaults
MCP_WAIT_MAX = 600  # longest a tool call waits for a run or install to finish
MCP_INSTRUCTIONS = f"""Pigeonhole stores, runs and schedules Python and bash scripts on a self-hosted server.

How scripts run:
- Python scripts run with Python 3 in a shared virtualenv; add libraries with install_packages(manager="pip"). \
Bash scripts run with bash. System tools (ffmpeg, jq, ...) come from install_packages(manager="apt").
- Scripts run as an unprivileged user with {WORK_DIR} as the working directory. Keep files a script needs \
between runs under the directory in the PIGEONHOLE_DATA environment variable.
- Put settings and secrets in the script's environment variables with set_env_vars, never in the code. You can \
see variable names but never their values; ask the user for a value rather than guessing it.
- Global environment variables, set by the user in Pigeonhole's Settings, are passed to every script too; a \
script's own variable with the same name overrides them. get_script lists their names, so reuse one (for example \
an API key) instead of asking for the value again.
- Print progress while the script works; the output streams live to the user. Exit with a non-zero code on \
failure: that marks the run as failed and alerts the user.
- Schedules are 5-field cron expressions in the server's timezone ({TZ}). A script with an empty schedule only \
runs when started.

Working style:
- After writing or changing a script, run it with run_script and read the output; fix it and run it again until \
it works. For scripts that change or delete things, give them a dry-run mode and run that first.
- A script can start another when it finishes (next_script_id and next_when on create_script and \
update_script), for example a report after a cleanup. Loops are refused.
- Only set a schedule, chain scripts, delete a script or install packages when the user asks for it.
- The user can turn some actions off in Pigeonhole's Settings. If a tool says an action is turned off, tell the \
user instead of retrying."""

mcp_server = FastMCP(
    "Pigeonhole", instructions=MCP_INSTRUCTIONS, stateless_http=True, json_response=True,
    # requests are authenticated by a bearer token, so Host/Origin pinning (meant for unauthenticated
    # localhost servers) is not needed, and it would reject LAN addresses and reverse proxies
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
)
mcp_server.streamable_http_app()  # creates mcp_server.session_manager; requests reach it via McpEndpoint
READ_ONLY = ToolAnnotations(readOnlyHint=True)


def mcp_allowed(perm: str) -> bool:
    return get_setting(f"mcp_allow_{perm}", MCP_PERMISSIONS[perm]) == "1"


def mcp_require(perm: str, what: str):
    if not mcp_allowed(perm):
        raise ValueError(f"{what} from an AI client is turned off in Pigeonhole's Settings.")


async def api_call(coro):
    """Await one of the HTTP API handlers, turning its HTTP errors into tool errors."""
    try:
        return await coro
    except HTTPException as e:
        raise ValueError(e.detail) from None


def script_row(sid: int):
    row = db.execute("SELECT * FROM scripts WHERE id=?", (sid,)).fetchone()
    if not row:
        raise ValueError(f"there is no script with id {sid}")
    return row


def iso(ts: Optional[float]) -> Optional[str]:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)) if ts else None


def env_names(text: str) -> list[str]:
    try:
        return list(parse_env(text))
    except ValueError:
        return []


def script_info(row, with_content: bool = False) -> dict:
    d = {"id": row["id"], "name": row["name"], "language": row["language"], "schedule": row["schedule"],
         "schedule_enabled": bool(row["enabled"]), "next_run": iso(next_run(row["id"]) if row["enabled"] else None),
         "timeout_seconds": row["timeout"], "env_vars": env_names(row["env"]),
         "webhook": bool(row["hook_token"]), "running_run_id": RUNNING_SCRIPTS.get(row["id"]), "next_script": None}
    if row["chain_next"]:
        nxt = db.execute("SELECT name FROM scripts WHERE id=?", (row["chain_next"],)).fetchone()
        d["next_script"] = {"id": row["chain_next"], "name": nxt["name"] if nxt else None, "when": row["chain_on"]}
    last = db.execute("SELECT status FROM runs WHERE script_id=? AND status!='running' ORDER BY id DESC LIMIT 1",
                      (row["id"],)).fetchone()
    d["last_status"] = last["status"] if last else None
    if with_content:
        d["content"] = row["content"]
    return d


def run_output(rid: int, tail: int) -> str:
    s = STREAMS.get(rid)
    text = "".join(s.chunks) if s else (log_path(rid).read_text(errors="replace") if log_path(rid).exists() else "")
    return text if len(text) <= tail else "[... earlier output cut ...]\n" + text[-tail:]


def run_info(rid: int, output: bool = False, tail: int = 12000) -> dict:
    r = db.execute("SELECT * FROM runs WHERE id=?", (rid,)).fetchone()
    if not r:
        raise ValueError(f"there is no run with id {rid}")
    d = {"run_id": r["id"], "script_id": r["script_id"], "title": r["title"], "trigger": r["trigger"],
         "status": r["status"], "exit_code": r["exit_code"], "started": iso(r["started"]),
         "seconds": round((r["finished"] or time.time()) - r["started"], 1)}
    if output:
        d["output"] = run_output(rid, max(1000, min(tail, 100000)))
    return d


async def wait_for_run(rid: int, seconds: float):
    deadline = time.monotonic() + max(0, min(seconds, MCP_WAIT_MAX))
    while time.monotonic() < deadline:
        r = db.execute("SELECT status FROM runs WHERE id=?", (rid,)).fetchone()
        if not r or r["status"] != "running":
            return
        await asyncio.sleep(0.5)


def env_quote(value: str) -> str:
    if value != value.strip() or (len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'"):
        q = "'" if '"' in value else '"'
        return f"{q}{value}{q}"
    return value


def edit_env(text: str, set_vars: dict[str, str], unset: list[str]) -> str:
    """Set or remove variables in a script's KEY=value text, keeping its other lines and comments."""
    for key in [*set_vars, *unset]:
        if not ENV_NAME.match(key):
            raise ValueError(f"{key!r} is not a valid variable name (letters, digits and _, not starting with a digit)")
    for key, value in set_vars.items():
        if any(c in value for c in "\r\n\0"):
            raise ValueError(f"the value for {key} contains a line break or null byte, which can't be stored")
    out, done = [], set()
    for line in text.replace("\r\n", "\n").split("\n"):
        s = line.strip()
        key = None
        if s and not s.startswith("#"):
            key = (s[7:].lstrip() if s.startswith("export ") else s).partition("=")[0].strip()
        if key in unset or (key in set_vars and key in done):
            continue
        if key in set_vars:
            out.append(f"{key}={env_quote(set_vars[key])}")
            done.add(key)
        else:
            out.append(line)
    out += [f"{k}={env_quote(v)}" for k, v in set_vars.items() if k not in done]
    result = "\n".join(out).strip("\n")
    parsed = parse_env(result)
    for k, v in set_vars.items():
        if parsed.get(k) != v:
            raise ValueError(f"the value for {k} can't be stored exactly (it may contain a line break)")
    return result


@mcp_server.tool(name="list_scripts", annotations=READ_ONLY)
async def mcp_list_scripts() -> dict[str, Any]:
    """List every script with its schedule, environment variable names and last run status."""
    return {"scripts": [script_info(r) for r in db.execute("SELECT * FROM scripts ORDER BY name COLLATE NOCASE")]}


@mcp_server.tool(name="get_script", annotations=READ_ONLY)
async def mcp_get_script(script_id: int) -> dict[str, Any]:
    """Get one script's code and settings, plus its five most recent runs. global_env_vars lists the global
    variables every script receives; env_vars are the script's own, which win over globals of the same name."""
    d = script_info(script_row(script_id), with_content=True)
    d["global_env_vars"] = env_names(get_setting("global_env"))
    d["recent_runs"] = [run_info(r["id"]) for r in
                        db.execute("SELECT id FROM runs WHERE script_id=? ORDER BY id DESC LIMIT 5", (script_id,))]
    return d


@mcp_server.tool(name="create_script")
async def mcp_create_script(name: str, content: str, language: str = "python", schedule: str = "",
                            timeout_seconds: int = 0, next_script_id: int = 0,
                            next_when: str = "success") -> dict[str, Any]:
    """Create a script. language is "python" or "bash"; schedule is a 5-field cron expression or empty for
    manual runs only; timeout_seconds 0 means no limit. next_script_id starts that script when this one
    finishes (0 for none), when next_when is "success", "failure" or "always". Set its environment variables
    with set_env_vars."""
    mcp_require("write", "Creating scripts")
    s = ScriptIn(name=name, language=language, content=content, schedule=schedule, timeout=timeout_seconds,
                 chain_next=next_script_id, chain_on=next_when)
    created = await api_call(create_script(s))
    return script_info(script_row(created["id"]))


@mcp_server.tool(name="update_script")
async def mcp_update_script(script_id: int, content: Optional[str] = None, name: Optional[str] = None,
                            language: Optional[str] = None, schedule: Optional[str] = None,
                            schedule_enabled: Optional[bool] = None, timeout_seconds: Optional[int] = None,
                            next_script_id: Optional[int] = None, next_when: Optional[str] = None) -> dict[str, Any]:
    """Change a script. Only the fields you pass change. content replaces the whole script. schedule is a
    5-field cron expression ("" for manual only); schedule_enabled pauses or resumes it. next_script_id starts
    that script when this one finishes (0 for none), when next_when is "success", "failure" or "always"."""
    mcp_require("write", "Changing scripts")
    row = script_row(script_id)
    s = ScriptIn(name=row["name"] if name is None else name,
                 language=row["language"] if language is None else language,
                 content=row["content"] if content is None else content,
                 schedule=row["schedule"] if schedule is None else schedule,
                 enabled=bool(row["enabled"]) if schedule_enabled is None else schedule_enabled,
                 timeout=row["timeout"] if timeout_seconds is None else timeout_seconds,
                 env=row["env"], hook_delay=row["hook_delay"],
                 chain_next=row["chain_next"] if next_script_id is None else next_script_id,
                 chain_on=row["chain_on"] if next_when is None else next_when)
    await api_call(update_script(script_id, s))
    return script_info(script_row(script_id))


@mcp_server.tool(name="set_env_vars")
async def mcp_set_env_vars(script_id: int, set: Optional[dict[str, str]] = None,
                           unset: Optional[list[str]] = None) -> dict[str, Any]:
    """Set and/or remove a script's environment variables (settings, API keys, URLs). Variables you don't
    mention are kept. Values are never shown back, only the names."""
    mcp_require("write", "Changing scripts")
    row = script_row(script_id)
    env = edit_env(row["env"], set or {}, unset or [])
    db.execute("UPDATE scripts SET env=?, updated=? WHERE id=?", (env, time.time(), script_id))
    db.commit()
    return {"script_id": script_id, "env_vars": env_names(env)}


@mcp_server.tool(name="delete_script", annotations=ToolAnnotations(destructiveHint=True))
async def mcp_delete_script(script_id: int) -> dict[str, Any]:
    """Delete a script for good, with its schedule and webhook. Only when the user asked for it."""
    mcp_require("delete", "Deleting scripts")
    script_row(script_id)
    await api_call(delete_script(script_id))
    return {"deleted": script_id}


@mcp_server.tool(name="run_script")
async def mcp_run_script(script_id: int, wait_seconds: int = 120) -> dict[str, Any]:
    """Start a script and wait up to wait_seconds (max 600) for it to finish. Returns the run's status, exit
    code and the end of its output. If it is still running, follow it with get_run."""
    mcp_require("run", "Running scripts")
    script_row(script_id)
    try:
        rid = start_script(script_id, "mcp")
    except HTTPException as e:
        if e.status_code == 409:
            raise ValueError(f"the script is already running as run {RUNNING_SCRIPTS.get(script_id)}; "
                             "follow it with get_run") from None
        raise ValueError(e.detail) from None
    await wait_for_run(rid, wait_seconds)
    return run_info(rid, output=True)


@mcp_server.tool(name="get_run", annotations=READ_ONLY)
async def mcp_get_run(run_id: int, wait_seconds: int = 0, tail_chars: int = 12000) -> dict[str, Any]:
    """Get a run's status and the end of its output (tail_chars characters, 1000-100000). wait_seconds
    (max 600) waits for a running run to finish first."""
    await wait_for_run(run_id, wait_seconds)
    return run_info(run_id, output=True, tail=tail_chars)


@mcp_server.tool(name="list_runs", annotations=READ_ONLY)
async def mcp_list_runs(script_id: Optional[int] = None, limit: int = 20) -> dict[str, Any]:
    """List recent runs, newest first, for one script or for everything (including package installs)."""
    q, args = "SELECT id FROM runs", []
    if script_id is not None:
        q += " WHERE script_id=?"
        args.append(script_id)
    rows = db.execute(q + " ORDER BY id DESC LIMIT ?", (*args, max(1, min(limit, 100))))
    return {"runs": [run_info(r["id"]) for r in rows]}


@mcp_server.tool(name="stop_run")
async def mcp_stop_run(run_id: int) -> dict[str, Any]:
    """Stop a running script."""
    mcp_require("run", "Stopping runs")
    await api_call(stop_run(run_id))
    await wait_for_run(run_id, 10)
    return run_info(run_id)


@mcp_server.tool(name="list_packages", annotations=READ_ONLY)
async def mcp_list_packages() -> dict[str, Any]:
    """List the Python packages in the shared virtualenv and the system packages installed through Pigeonhole."""
    return {"pip": [f"{p['name']}=={p['version']}" for p in await pip_list()],
            "apt": [p["name"] + ("" if p["installed"] else " (not installed)") for p in await apt_list()]}


@mcp_server.tool(name="install_packages")
async def mcp_install_packages(manager: str, packages: list[str]) -> dict[str, Any]:
    """Install packages: manager "pip" for Python libraries (shared by all Python scripts), "apt" for system
    tools such as ffmpeg or jq. Waits up to 10 minutes and returns the install output."""
    mcp_require("packages", "Installing packages")
    if manager not in ("pip", "apt"):
        raise ValueError('manager must be "pip" or "apt"')
    p = Packages(packages=" ".join(packages))
    started = await api_call(pip_install(p) if manager == "pip" else apt_install(p))
    await wait_for_run(started["run_id"], MCP_WAIT_MAX)
    return run_info(started["run_id"], output=True)


class McpEndpoint:
    """ASGI endpoint for /mcp: checks the token, then hands the request to the MCP server."""

    async def __call__(self, scope, receive, send):
        token = get_setting("mcp_token")
        if not token:
            response = JSONResponse({"detail": "MCP is turned off. Turn it on in Pigeonhole's Settings."}, 404)
            return await response(scope, receive, send)
        auth = dict(scope.get("headers") or []).get(b"authorization", b"").decode("latin-1")
        given = auth[7:].strip() if auth[:7].lower() == "bearer " else ""
        if not given or not secrets.compare_digest(given.encode(), token.encode()):
            response = JSONResponse({"detail": "missing or wrong MCP token"}, 401, headers={"WWW-Authenticate": "Bearer"})
            return await response(scope, receive, send)
        await mcp_server.session_manager.handle_request(scope, receive, send)


for _path in ("/mcp", "/mcp/"):
    app.router.add_route(_path, McpEndpoint(), include_in_schema=False)


class McpPermissions(BaseModel):
    write: bool = True
    run: bool = True
    packages: bool = False
    delete: bool = False


def mcp_status() -> dict:
    token = get_setting("mcp_token")
    return {"enabled": bool(token), "token": token, "permissions": {p: mcp_allowed(p) for p in MCP_PERMISSIONS}}


@app.get("/api/mcp")
async def read_mcp():
    return mcp_status()


@app.post("/api/mcp/token")
async def create_mcp_token():
    """Turn MCP on, or replace the token (clients using the old one lose access)."""
    set_setting("mcp_token", secrets.token_urlsafe(32))
    return mcp_status()


@app.delete("/api/mcp/token")
async def delete_mcp_token():
    set_setting("mcp_token", "")
    return mcp_status()


@app.put("/api/mcp/permissions")
async def write_mcp_permissions(p: McpPermissions):
    for k, v in p.model_dump().items():
        set_setting(f"mcp_allow_{k}", "1" if v else "0")
    return mcp_status()


@app.get("/")
async def index():
    return FileResponse(STATIC / "index.html")


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    # for clients that ask for /favicon.ico instead of reading the <link> tags
    return FileResponse(STATIC / "favicon-32.png", media_type="image/png")


@app.middleware("http")
async def revalidate_static(request, call_next):
    response = await call_next(request)
    if request.url.path.startswith("/static/") or request.url.path == "/":
        response.headers["Cache-Control"] = "no-cache"
    return response


app.mount("/static", StaticFiles(directory=STATIC), name="static")
