import os
import tempfile

os.environ.setdefault("DATA_DIR", tempfile.mkdtemp(prefix="pigeonhole-test-"))

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from app import main  # noqa: E402

client = TestClient(main.app)  # not entered as a context manager, so startup (venv, scheduler) is skipped

OURS = "a" * 40
LATEST = "b" * 40


def commit(title: str) -> dict:
    return {"commit": {"message": f"{title}\n\nCo-Authored-By: someone"}}


@pytest.fixture(autouse=True)
def fake_github(monkeypatch):
    """Answers GitHub API calls from `replies` (path prefix -> response) and records the paths asked for."""
    replies: dict = {}
    calls: list[str] = []

    def github(path):
        calls.append(path)
        for prefix, reply in replies.items():
            if path.startswith(prefix):
                if isinstance(reply, Exception):
                    raise reply
                return reply
        raise AssertionError(f"unexpected GitHub call {path}")

    monkeypatch.setattr(main, "github", github)
    monkeypatch.setattr(main, "VERSION", OURS)
    monkeypatch.setattr(main, "UPDATE_REPO", "someone/pigeonhole")
    main.update_cache.clear()
    main.set_setting("update_notify", "0")
    main.set_setting("update_notified", "")
    return replies, calls


def published(sha: str) -> dict:
    return {"workflow_runs": [{"head_sha": sha}]}


def test_up_to_date(fake_github):
    replies, calls = fake_github
    replies["/actions/"] = published(OURS)
    v = client.get("/api/version").json()
    assert v["version"] == "aaaaaaa" and v["latest"] == "aaaaaaa"
    assert v["update"] is False and v["error"] == ""
    assert len(calls) == 1  # no compare needed


def test_update_lists_changes_newest_first(fake_github):
    replies, _ = fake_github
    replies["/actions/"] = published(LATEST)
    replies["/compare/"] = {"status": "ahead", "ahead_by": 2, "commits": [commit("Older change"), commit("Newer change")]}
    v = client.get("/api/version").json()
    assert v["update"] is True and v["behind"] == 2
    assert v["tag"] == "sha-bbbbbbb"
    assert v["changes"] == ["Newer change", "Older change"]


def test_newer_than_published_is_not_an_update(fake_github):
    replies, _ = fake_github
    replies["/actions/"] = published(LATEST)
    replies["/compare/"] = {"status": "behind", "ahead_by": 0, "behind_by": 1, "commits": []}
    assert client.get("/api/version").json()["update"] is False


def test_result_is_cached_until_refreshed(fake_github):
    replies, calls = fake_github
    replies["/actions/"] = published(OURS)
    client.get("/api/version")
    client.get("/api/version")
    assert len(calls) == 1
    client.get("/api/version", params={"refresh": "true"})
    assert len(calls) == 2


def test_github_error_is_reported(fake_github):
    replies, _ = fake_github
    replies["/actions/"] = OSError("network down")
    v = client.get("/api/version").json()
    assert v["update"] is False
    assert "network down" in v["error"]


def test_development_build_skips_the_check(fake_github, monkeypatch):
    _, calls = fake_github
    monkeypatch.setattr(main, "VERSION", "")
    v = client.get("/api/version").json()
    assert v["version"] == "" and v["update"] is False
    assert calls == []


def test_update_notification_is_sent_once(fake_github, monkeypatch):
    sent = []
    monkeypatch.setattr(main, "send_ntfy", lambda *a: sent.append(a))
    v = {"update": True, "latest": "bbbbbbb", "version": "aaaaaaa", "behind": 3, "tag": "sha-bbbbbbb"}
    main.notify_update(v)
    assert sent == []  # off until turned on in Settings
    client.put("/api/settings", json={"update_notify": True})
    assert client.get("/api/settings").json()["update_notify"] is True
    main.notify_update(v)
    main.notify_update(v)
    assert len(sent) == 1
    assert "sha-bbbbbbb" in sent[0][0]
