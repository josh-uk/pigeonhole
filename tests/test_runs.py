import os
import tempfile
import time

os.environ["DATA_DIR"] = tempfile.mkdtemp(prefix="pigeonhole-test-")

from fastapi.testclient import TestClient  # noqa: E402

from app import main  # noqa: E402

client = TestClient(main.app)  # not entered as a context manager, so startup (venv, scheduler) is skipped


def add_run(status: str) -> int:
    cur = main.db.execute("INSERT INTO runs (title,trigger,status,started) VALUES (?,?,?,?)",
                          ("test", "manual", status, time.time()))
    main.db.commit()
    main.log_path(cur.lastrowid).write_text(f"output of a {status} run\n")
    return cur.lastrowid


def setup_function():
    main.db.execute("DELETE FROM runs")
    main.db.commit()


def test_clear_keeps_running_runs():
    running = add_run("running")
    past = [add_run(s) for s in ("success", "failed", "stopped", "timeout")]
    assert client.get("/api/runs/count").json() == {"past": 4, "running": 1}

    assert client.delete("/api/runs").json() == {"deleted": 4}

    assert [r["id"] for r in client.get("/api/runs").json()] == [running]
    assert main.log_path(running).exists()
    assert not any(main.log_path(rid).exists() for rid in past)
    assert client.get("/api/runs/count").json() == {"past": 0, "running": 1}


def test_clear_with_nothing_to_clear():
    assert client.get("/api/runs/count").json() == {"past": 0, "running": 0}
    assert client.delete("/api/runs").json() == {"deleted": 0}
