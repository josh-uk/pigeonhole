import io
import os
import sqlite3
import tempfile
import time
import zipfile

os.environ["DATA_DIR"] = tempfile.mkdtemp(prefix="pigeonhole-test-")

from fastapi.testclient import TestClient  # noqa: E402

from app import main  # noqa: E402

client = TestClient(main.app)  # not entered as a context manager, so startup (venv, scheduler) is skipped


def setup_function():
    main.db.execute("DELETE FROM scripts")
    main.db.execute("DELETE FROM runs")
    main.db.commit()


def download(**params) -> zipfile.ZipFile:
    r = client.get("/api/backup", params=params)
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/zip"
    assert 'filename="pigeonhole-backup-' in r.headers["content-disposition"]
    return zipfile.ZipFile(io.BytesIO(r.content))


def add_run() -> int:
    cur = main.db.execute("INSERT INTO runs (title,trigger,status,started) VALUES (?,?,?,?)",
                          ("test", "manual", "success", time.time()))
    main.db.commit()
    main.log_path(cur.lastrowid).write_text("output of a run\n")
    return cur.lastrowid


def test_backup_has_a_valid_database_with_the_scripts():
    # inserted directly: creating a scheduled script through the API needs the scheduler running
    sid = main.db.execute("INSERT INTO scripts (name,language,content,schedule,env,updated) VALUES (?,?,?,?,?,?)",
                          ("nightly", "python", "print('hi')", "0 3 * * *", "API_KEY=secret", time.time())).lastrowid
    main.db.commit()
    client.put("/api/settings", json={"global_env": "SHARED=1"})

    z = download()
    assert z.testzip() is None
    with tempfile.TemporaryDirectory() as tmp:
        path = z.extract("pigeonhole.db", tmp)
        con = sqlite3.connect(path)
        assert con.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert con.execute("SELECT name, content, schedule, env FROM scripts WHERE id=?", (sid,)).fetchone() == \
            ("nightly", "print('hi')", "0 3 * * *", "API_KEY=secret")
        assert con.execute("SELECT value FROM settings WHERE key='global_env'").fetchone() == ("SHARED=1",)
        con.close()


def test_backup_keeps_script_data_and_leaves_out_rebuildable_files():
    (main.DATA / "state").mkdir(exist_ok=True)
    (main.DATA / "state" / "seen.json").write_text('{"last": 3}')
    (main.DATA / "notes.txt").write_text("kept")
    (main.DATA / "venv" / "lib").mkdir(parents=True, exist_ok=True)
    (main.DATA / "venv" / "lib" / "big.py").write_text("x")
    (main.WORK_DIR / "tmp.bin").write_text("scratch")
    (main.SCRIPTS_DIR / "1.py").write_text("print()")
    (main.DATA / "pigeonhole.db-journal").write_text("")
    (main.DATA / "state" / "__pycache__").mkdir(exist_ok=True)
    (main.DATA / "state" / "__pycache__" / "x.pyc").write_text("")

    z = download()
    names = set(z.namelist())
    assert {"pigeonhole.db", "notes.txt", "state/seen.json"} <= names
    assert z.read("state/seen.json") == b'{"last": 3}'
    assert not [n for n in names if n.split("/")[0] in ("venv", "work", "scripts")]
    assert "pigeonhole.db-journal" not in names
    assert not [n for n in names if "__pycache__" in n]


def test_logs_only_when_asked():
    rid = add_run()
    assert not [n for n in download().namelist() if n.startswith("runs/")]
    assert download(logs="true").read(f"runs/{rid}.log") == b"output of a run\n"


def test_pip_packages_are_listed(monkeypatch):
    monkeypatch.setattr(main, "pip_freeze", lambda: "requests==2.32.3\n")
    assert download().read("pip-requirements.txt") == b"requests==2.32.3\n"


def test_no_pip_list_without_packages():
    assert "pip-requirements.txt" not in download().namelist()  # no virtualenv in the tests
