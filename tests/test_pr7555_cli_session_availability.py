"""Availability regressions retained by PR #7555 after #7445.

Master owns the strict read-only state.db connection layer. These tests cover the
remaining cache and multi-profile availability semantics.
"""

import sqlite3
import time

import pytest

import api.agent_sessions as agent_sessions
import api.models as models


def _make_state_db(path, *, sessions=80, messages_per_session=3, create_messages_index=True, source="cli", session_source="cli"):
    conn = sqlite3.connect(str(path))
    conn.executescript(
        """
        CREATE TABLE sessions (
            id TEXT PRIMARY KEY,
            source TEXT,
            session_source TEXT,
            title TEXT,
            model TEXT,
            started_at REAL NOT NULL,
            message_count INTEGER DEFAULT 0,
            parent_session_id TEXT,
            ended_at REAL,
            end_reason TEXT
        );
        CREATE INDEX idx_sessions_started ON sessions(started_at);
        CREATE TABLE messages (
            id TEXT PRIMARY KEY,
            session_id TEXT,
            role TEXT,
            content TEXT,
            timestamp REAL
        );
        """
    )
    base = time.time() - sessions
    for i in range(sessions):
        sid = f"cli_perf_{i:04d}"
        started = base + i
        conn.execute(
            """
            INSERT INTO sessions
            (id, source, session_source, title, model, started_at, message_count, parent_session_id, ended_at, end_reason)
            VALUES (?, ?, ?, ?, 'openai/gpt-5', ?, ?, NULL, NULL, NULL)
            """,
            (sid, source, session_source, sid, started, messages_per_session),
        )
        for j in range(messages_per_session):
            conn.execute(
                "INSERT INTO messages (id, session_id, role, content, timestamp) VALUES (?, ?, ?, 'hello', ?)",
                (f"msg_{i:04d}_{j:02d}", sid, "user" if j == 0 else "assistant", started + j / 10),
            )
    if create_messages_index:
        conn.execute("CREATE INDEX idx_messages_session ON messages(session_id, timestamp)")
    conn.commit()
    conn.close()


def test_cache_owned_projection_preserves_rows_when_real_open_fails(tmp_path, monkeypatch):
    """A real read-only open failure must reach the stale-cache fallback."""
    db = tmp_path / "state.db"
    _make_state_db(db, sessions=1, messages_per_session=1, source="cli", session_source="cli")
    home = tmp_path / "home"
    home.mkdir()
    revision = ["warm"]
    monkeypatch.setattr(models, "_CLI_SESSIONS_CACHE_TTL_SECONDS", 0.001, raising=False)
    monkeypatch.setattr(models, "get_claude_code_sessions", lambda: [])
    monkeypatch.setattr(models, "_default_claude_code_projects_dir", lambda: None)
    monkeypatch.setattr(
        models,
        "_resolve_cli_sessions_context",
        lambda _source_filter=None, **_kwargs: (
            home,
            db,
            "default",
            (str(home), "default", str(db), "", revision[0], False, None, None, None),
        ),
    )
    models.clear_cli_sessions_cache()

    warm = models.get_cli_sessions()
    assert [row["session_id"] for row in warm] == ["cli_perf_0000"]

    def fail_open(*_args, **_kwargs):
        raise OSError("state.db temporarily unavailable")

    monkeypatch.setattr(agent_sessions, "open_state_db_readonly", fail_open)
    revision[0] = "failed"
    assert models.get_cli_sessions() == warm


def test_cache_owned_source_pass_failure_does_not_publish_partial_rows(tmp_path, monkeypatch):
    """A source-specific open failure must not cache the earlier partial pass."""
    db = tmp_path / "state.db"
    _make_state_db(db, sessions=1, messages_per_session=1, source="cron", session_source="cron")
    home = tmp_path / "home"
    home.mkdir()
    revision = ["warm"]
    monkeypatch.setattr(models, "_CLI_SESSIONS_CACHE_TTL_SECONDS", 0.001, raising=False)
    monkeypatch.setattr(models, "get_claude_code_sessions", lambda: [])
    monkeypatch.setattr(models, "_default_claude_code_projects_dir", lambda: None)
    monkeypatch.setattr(
        models,
        "_resolve_cli_sessions_context",
        lambda _source_filter=None, **_kwargs: (
            home,
            db,
            "default",
            (str(home), "default", str(db), "", revision[0], False, None, None, None),
        ),
    )
    models.clear_cli_sessions_cache()
    warm = models.get_cli_sessions()
    assert [row["session_id"] for row in warm] == ["cli_perf_0000"]

    real_open = agent_sessions.open_state_db_readonly
    opens = 0

    def fail_second_open(*args, **kwargs):
        nonlocal opens
        opens += 1
        if opens == 2:
            raise OSError("state.db disappeared during cron pass")
        return real_open(*args, **kwargs)

    monkeypatch.setattr(agent_sessions, "open_state_db_readonly", fail_second_open)
    revision[0] = "failed"
    assert models.get_cli_sessions() == warm
    assert opens == 2


def test_all_profiles_partial_result_never_poison_ttl_or_stable_cache(monkeypatch, tmp_path):
    """A+B warm -> A-only partial -> recovery must retain complete authority."""
    homes = [tmp_path / "a", tmp_path / "b"]
    for home in homes:
        home.mkdir()
    revision = ["warm"]
    mode = ["warm"]
    calls = []

    def contexts():
        return (
            [(homes[0], homes[0] / "state.db", "a"), (homes[1], homes[1] / "state.db", "b")],
            ((str(homes[0]), "a", revision[0]), (str(homes[1]), "b", revision[0])),
        )

    monkeypatch.setattr(models, "_all_profiles_cli_contexts", contexts)
    monkeypatch.setattr(models, "_default_claude_code_projects_dir", lambda: None)
    monkeypatch.setattr(models, "get_claude_code_sessions", lambda: [])
    monkeypatch.setattr(models, "_CLI_SESSIONS_CACHE_TTL_SECONDS", 60.0, raising=False)
    models.clear_cli_sessions_cache()

    def load(_home, _db_path, profile, **_kwargs):
        calls.append((mode[0], profile))
        if mode[0] == "partial" and profile == "b":
            raise OSError("profile b unavailable")
        if mode[0] == "failed":
            raise OSError(f"profile {profile} unavailable")
        return [{"session_id": f"session-{profile}", "profile": profile}]

    monkeypatch.setattr(models, "_load_cli_sessions_uncached", load)
    complete = [
        {"session_id": "session-a", "profile": "a"},
        {"session_id": "session-b", "profile": "b"},
    ]
    assert models.get_cli_sessions(all_profiles=True) == complete

    mode[0] = "partial"
    revision[0] = "partial"
    assert models.get_cli_sessions(all_profiles=True) == complete

    mode[0] = "warm"
    # Same fingerprint/key as the partial attempt: it must rebuild because the
    # incomplete result was not published to the TTL cache.
    assert models.get_cli_sessions(all_profiles=True) == complete

    mode[0] = "failed"
    revision[0] = "failed"
    assert models.get_cli_sessions(all_profiles=True) == complete
    assert calls.count(("partial", "b")) == 1


def test_all_profiles_empty_success_counts_as_success(monkeypatch, tmp_path):
    """A healthy profile returning [] is success, not an all-failed signal."""
    homes = [tmp_path / "a", tmp_path / "b"]
    for home in homes:
        home.mkdir()
    revision = ["same"]
    mode = ["partial"]
    monkeypatch.setattr(
        models,
        "_all_profiles_cli_contexts",
        lambda: (
            [(homes[0], homes[0] / "state.db", "a"), (homes[1], homes[1] / "state.db", "b")],
            ((str(homes[0]), "a", revision[0]), (str(homes[1]), "b", revision[0])),
        ),
    )
    monkeypatch.setattr(models, "_default_claude_code_projects_dir", lambda: None)
    monkeypatch.setattr(models, "get_claude_code_sessions", lambda: [])
    monkeypatch.setattr(models, "_CLI_SESSIONS_CACHE_TTL_SECONDS", 60.0, raising=False)
    models.clear_cli_sessions_cache()

    def load(_home, _db_path, profile, **_kwargs):
        if profile == "a":
            return []
        if mode[0] == "partial":
            raise OSError("profile b unavailable")
        return [{"session_id": "b"}]

    monkeypatch.setattr(models, "_load_cli_sessions_uncached", load)
    assert models.get_cli_sessions(all_profiles=True) == []
    mode[0] = "recovered"
    assert models.get_cli_sessions(all_profiles=True) == [{"session_id": "b"}]


def test_all_profiles_scans_global_claude_when_first_profile_unavailable(monkeypatch, tmp_path):
    """Profile 0 failure must not suppress the independent Claude scan."""
    homes = [tmp_path / "a", tmp_path / "b"]
    for home in homes:
        home.mkdir()
    monkeypatch.setattr(
        models,
        "_all_profiles_cli_contexts",
        lambda: (
            [(homes[0], homes[0] / "state.db", "a"), (homes[1], homes[1] / "state.db", "b")],
            ((str(homes[0]), "a", "same"), (str(homes[1]), "b", "same")),
        ),
    )
    monkeypatch.setattr(models, "_default_claude_code_projects_dir", lambda: None)
    monkeypatch.setattr(models, "get_claude_code_sessions", lambda: [{"session_id": "claude-1"}])
    monkeypatch.setattr(models, "_CLI_SESSIONS_CACHE_TTL_SECONDS", 0.0, raising=False)
    monkeypatch.setattr(
        models,
        "_load_cli_sessions_uncached",
        lambda _home, _db_path, profile, **_kwargs: (
            (_ for _ in ()).throw(OSError("profile a unavailable"))
            if profile == "a"
            else [{"session_id": "profile-b"}]
        ),
    )
    assert models.get_cli_sessions(all_profiles=True) == [
        {"session_id": "profile-b"},
        {"session_id": "claude-1"},
    ]


def test_all_profiles_incomplete_load_prefers_stale_primary_over_evicted_lkg(monkeypatch, tmp_path):
    """A complete expired primary remains usable when its LKG twin was evicted."""
    home = tmp_path / "home"
    home.mkdir()
    mode = ["warm"]
    monkeypatch.setattr(models, "_all_profiles_cli_contexts", lambda: (
        [(home, home / "state.db", "default")],
        ((str(home), "default", "warm"),),
    ))
    monkeypatch.setattr(models, "_default_claude_code_projects_dir", lambda: None)
    monkeypatch.setattr(models, "get_claude_code_sessions", lambda: [])
    monkeypatch.setattr(models, "_CLI_SESSIONS_CACHE_TTL_SECONDS", 60.0, raising=False)
    models.clear_cli_sessions_cache()
    rows = [{"session_id": "complete"}]
    monkeypatch.setattr(models, "_load_cli_sessions_uncached", lambda *_a, **_k: (
        rows if mode[0] == "warm" else (_ for _ in ()).throw(OSError("unavailable"))
    ))
    assert models.get_cli_sessions(all_profiles=True) == rows
    cache_key = next(k for k in models._CLI_SESSIONS_CACHE if k[0] == "all_profiles")
    with models._CLI_SESSIONS_CACHE_LOCK:
        expires, stamp, cached = models._CLI_SESSIONS_CACHE[cache_key]
        models._CLI_SESSIONS_CACHE[cache_key] = (0.0, stamp, cached)
        models._CLI_SESSIONS_LAST_KNOWN_GOOD.clear()
    mode[0] = "failed"
    calls = []
    original_loader = models._load_cli_sessions_uncached
    def failing_loader(*args, **kwargs):
        calls.append(True)
        return original_loader(*args, **kwargs)
    monkeypatch.setattr(models, "_load_cli_sessions_uncached", failing_loader)
    assert models.get_cli_sessions(all_profiles=True) == rows
    assert calls == [True]


@pytest.mark.parametrize("source_filter", ["cron", "webhook", "kanban"])
def test_all_profiles_filtered_load_excludes_global_claude_on_first_load_and_cache_hit(
    monkeypatch, tmp_path, source_filter
):
    """Profile-source filters must not admit global Claude rows."""
    home = tmp_path / "home"
    home.mkdir()
    claude_calls = []
    monkeypatch.setattr(models, "_all_profiles_cli_contexts", lambda: (
        [(home, home / "state.db", "default")],
        ((str(home), "default", "same"),),
    ))
    monkeypatch.setattr(models, "_default_claude_code_projects_dir", lambda: None)
    monkeypatch.setattr(models, "_CLI_SESSIONS_CACHE_TTL_SECONDS", 60.0, raising=False)
    models.clear_cli_sessions_cache()
    monkeypatch.setattr(models, "get_claude_code_sessions", lambda: claude_calls.append(1) or [{"session_id": "claude"}])
    monkeypatch.setattr(
        models,
        "_load_cli_sessions_uncached",
        lambda *_a, **_k: [{"session_id": source_filter, "source": source_filter}],
    )
    expected = [{"session_id": source_filter, "source": source_filter}]
    assert models.get_cli_sessions(source_filter, all_profiles=True) == expected
    assert models.get_cli_sessions(source_filter, all_profiles=True) == expected
    assert claude_calls == []


def test_unavailable_profile_warning_names_profile_and_database_path(
    monkeypatch, tmp_path, caplog
):
    healthy_home = tmp_path / "healthy"
    failed_home = tmp_path / "failed"
    healthy_home.mkdir()
    failed_home.mkdir()
    failed_db = failed_home / "state.db"
    monkeypatch.setattr(
        models,
        "_all_profiles_cli_contexts",
        lambda: (
            [
                (healthy_home, healthy_home / "state.db", "healthy"),
                (failed_home, failed_db, "broken-profile"),
            ],
            ((str(healthy_home), "healthy", "same"), (str(failed_home), "broken-profile", "same")),
        ),
    )
    monkeypatch.setattr(models, "_default_claude_code_projects_dir", lambda: None)
    monkeypatch.setattr(models, "_cli_sessions_cache_ttl_seconds", lambda: 0.0)

    def load(_home, _db_path, profile, **_kwargs):
        if profile == "broken-profile":
            raise OSError("temporarily unavailable")
        return [{"session_id": "healthy-row"}]

    monkeypatch.setattr(models, "_load_cli_sessions_uncached", load)
    with caplog.at_level("WARNING"):
        rows = models.get_cli_sessions(all_profiles=True, include_claude_code=False)

    assert rows == [{"session_id": "healthy-row"}]
    warning = next(
        record for record in caplog.records
        if record.getMessage().startswith("get_cli_sessions() skipped unavailable profile")
    )
    assert "broken-profile" in warning.getMessage()
    assert str(failed_db) in warning.getMessage()


def test_all_profiles_keeps_healthy_profile_when_another_is_unavailable(monkeypatch, tmp_path):
    """An unavailable profile must not hide rows loaded from healthy profiles."""
    home_a = tmp_path / "profile-a"
    home_b = tmp_path / "profile-b"
    home_a.mkdir()
    home_b.mkdir()
    contexts = lambda: (
        [(home_a, home_a / "state.db", "a"), (home_b, home_b / "state.db", "b")],
        ((str(home_a), "a", "a-rev"), (str(home_b), "b", "b-rev")),
    )
    monkeypatch.setattr(models, "_all_profiles_cli_contexts", contexts)
    monkeypatch.setattr(models, "_default_claude_code_projects_dir", lambda: None)
    monkeypatch.setattr(models, "get_claude_code_sessions", lambda: [])
    monkeypatch.setattr(models, "_CLI_SESSIONS_CACHE_TTL_SECONDS", 0.0, raising=False)

    def load(home, _db_path, profile, **_kwargs):
        if profile == "b":
            raise OSError("profile b state.db unavailable")
        return [{"session_id": "healthy-a", "profile": "a"}]

    monkeypatch.setattr(models, "_load_cli_sessions_uncached", load)
    assert models.get_cli_sessions(all_profiles=True) == [
        {"session_id": "healthy-a", "profile": "a"}
    ]


def test_all_profiles_idle_and_streaming_fallback_share_stable_identity(monkeypatch, tmp_path):
    """Idle and streaming-frozen all-profile failures share last-known-good rows."""
    home = tmp_path / "home"
    home.mkdir()
    db = home / "state.db"
    db.write_text("placeholder", encoding="utf-8")
    marker: list[object] = [None]
    revision = ["warm"]
    calls = 0
    rows = [{"session_id": "all-profile-1", "title": "Known good"}]
    contexts = lambda: ([(home, db, "default")], ((str(home), "default", revision[0]),))
    monkeypatch.setattr(models, "_all_profiles_cli_contexts", contexts)
    monkeypatch.setattr(models, "_default_claude_code_projects_dir", lambda: None)
    monkeypatch.setattr(models, "get_claude_code_sessions", lambda: [])
    monkeypatch.setattr(models, "_cli_sessions_streaming_freeze_marker", lambda: marker[0])
    monkeypatch.setattr(models, "_CLI_SESSIONS_CACHE_TTL_SECONDS", 0.001, raising=False)

    def load(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if calls > 1:
            raise OSError("all-profile state.db unavailable")
        return list(rows)

    monkeypatch.setattr(models, "_load_cli_sessions_uncached", load)
    models.clear_cli_sessions_cache()
    assert models.get_cli_sessions(all_profiles=True) == rows
    revision[0] = "streaming"
    marker[0] = ("streaming", ("session-1",))
    assert models.get_cli_sessions(all_profiles=True) == rows
    revision[0] = "idle-again"
    marker[0] = None
    assert models.get_cli_sessions(all_profiles=True) == rows
    assert calls == 3
