"""Cancelled history through real SQLite writes, worker settlement, and HTTP."""

import sys
import copy, json, os, queue, sqlite3, subprocess, time
from pathlib import Path
import pytest
from tests.test_cancel_restart_journal_recovery import (
    _isolated_state,  # noqa: F401 -- imported autouse fixture
    _start_cancelled_turn,
    _simulate_restart,
)
from tests.test_cancelled_journal_owner_occurrences import _recover
from tests.test_webui_state_db_reconciliation import (
    _make_state_db,
    _append_state_db_rows,
)
from tests.test_recovered_surrogate_http import _new_server
from tests.test_recovered_export_share_and_gateway_http import _request
from api import config, models, profiles, routes, streaming
from api.run_journal import RunJournalWriter

ROOT = Path(models.__file__).resolve().parents[1]


def http_env(tmp):
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("AWS_", "GH_", "GITHUB_", "OPENAI_", "ANTHROPIC_"))
        and not k.endswith(("_API_KEY", "_AUTH_TOKEN", "_BOT_TOKEN", "_ACCESS_TOKEN"))
    }
    workspace = tmp / "workspace"
    workspace.mkdir(exist_ok=True)
    env.update(
        HERMES_HOME=str(tmp),
        HERMES_BASE_HOME=str(tmp),
        HERMES_CONFIG_PATH=str(tmp / "config.yaml"),
        HERMES_WEBUI_STATE_DIR=str(tmp),
        HERMES_WEBUI_DEFAULT_WORKSPACE=str(workspace),
        PYTHONPATH=str(ROOT) + os.pathsep + env.get("PYTHONPATH", ""),
        HERMES_WEBUI_TEST_NETWORK_BLOCK="1",
    )
    env.pop("HERMES_WEBUI_TEST_PORT", None)
    return env


def http_snapshots(tmp, sid, copies=False, limited=False):
    out = {}
    env = http_env(tmp)
    with _new_server(env, ROOT, tmp / "server.log") as base:
        queries = ["all", "all", "7", "7"] if limited else ["all"]
        for i, limit in enumerate(queries):
            status, body, _ = _request(
                base, "/api/session?session_id=" + sid + "&msg_limit=" + limit
            )
            assert status == 200, body
            out["GET-" + str(i) + "-" + limit] = json.loads(body)["session"]
        if copies:
            for endpoint in ["branch", "duplicate"]:
                status, body, _ = _request(
                    base, "/api/session/" + endpoint, {"session_id": sid}
                )
                assert status == 200, body
                payload = json.loads(body)
                child = (
                    payload["session_id"]
                    if endpoint == "branch"
                    else payload["session"]["session_id"]
                )
                code = "import sys,json;from api.models import Session;s=Session.load(sys.argv[1]);print(json.dumps({'messages':s.messages,'context_messages':s.context_messages}))"
                p = subprocess.run(
                    [sys.executable, "-c", code, child],
                    cwd=ROOT,
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                assert p.returncode == 0, p.stderr
                out[endpoint] = json.loads(p.stdout)
    return out


def actual_worker(session, tmp_path, monkeypatch):
    histories = []

    class Agent:
        def __init__(self, **kwargs):
            self.session_id = session.session_id
            self.platform = "webui"
            self.model = "test-model"
            self.context_compressor = None
            self.ephemeral_system_prompt = None

        def run_conversation(
            self,
            user_message,
            conversation_history=None,
            persist_user_message=None,
            persist_user_timestamp=None,
            **kwargs,
        ):
            histories.append(copy.deepcopy(conversation_history))
            return {
                "completed": True,
                "final_response": "LOCAL_NEXT_ANSWER",
                "messages": list(conversation_history)
                + [
                    {"role": "user", "content": user_message},
                    {"role": "assistant", "content": "LOCAL_NEXT_ANSWER"},
                ],
                "current_turn_user_idx": len(conversation_history),
            }

    monkeypatch.setattr(streaming, "_get_ai_agent", lambda: Agent)
    monkeypatch.setattr(streaming, "_build_session_db_for_stream", lambda _: None)
    monkeypatch.setattr(
        streaming, "resolve_model_provider", lambda *a, **k: ("test-model", None, None)
    )
    monkeypatch.setattr(streaming, "get_config", lambda: {})
    monkeypatch.setattr(config, "get_config", lambda: {})
    monkeypatch.setattr(config, "_resolve_cli_toolsets", lambda *a, **k: [])
    monkeypatch.setattr(profiles, "get_active_hermes_home", lambda: tmp_path)
    config.SESSION_AGENT_CACHE.clear()
    stream = session.session_id + "-next"
    models.SESSIONS[session.session_id] = session
    routes._prepare_chat_start_session_for_stream(
        session,
        msg="NEXT",
        attachments=[],
        workspace=str(tmp_path),
        model="test-model",
        model_provider=None,
        stream_id=stream,
        started_at=30.0,
    )
    config.STREAMS[stream] = queue.Queue()
    streaming._run_agent_streaming(
        session.session_id, "NEXT", "test-model", str(tmp_path), stream, []
    )
    assert len(histories) == 1, "actual production worker did not reach local provider"
    return histories[0]


@pytest.mark.parametrize(
    "ids", [False, True], ids=["ordinary-clock-skew", "known-private-ids-skew"]
)
def test_actual_settle_and_sqlite_flush_clocks_preserve_pre_stop_mirrors(
    tmp_path, monkeypatch, ids
):
    sid = "cancelled-producer-restamp-" + str(ids)
    db = tmp_path / "state.db"
    monkeypatch.setattr(models, "_active_state_db_path", lambda: db)
    _make_state_db(db, sid, [])
    histories = []
    appends = []

    class Agent:
        def __init__(self, **kw):
            self.session_id = sid
            self.platform = "webui"
            self.model = "test-model"
            self.context_compressor = None
            self.ephemeral_system_prompt = None

        def run_conversation(
            self,
            user_message,
            conversation_history=None,
            persist_user_message=None,
            persist_user_timestamp=None,
            **kw,
        ):
            histories.append(copy.deepcopy(conversation_history))
            n = len(histories)
            u = {"role": "user", "content": persist_user_message}
            a = {"role": "assistant", "content": "PRIOR_A" + str(n)}
            with sqlite3.connect(db) as conn:
                uid = conn.execute(
                    "INSERT INTO messages(session_id,role,content,timestamp)VALUES(?,?,?,?)",
                    (sid, "user", persist_user_message, persist_user_timestamp),
                ).lastrowid
                stamp = time.time()
                aid = conn.execute(
                    "INSERT INTO messages(session_id,role,content,timestamp)VALUES(?,?,?,?)",
                    (sid, "assistant", a["content"], stamp),
                ).lastrowid
            if ids:
                u["_row_id"] = uid
                a["_row_id"] = aid
            appends.append(
                {
                    "user_row_id": uid,
                    "assistant_row_id": aid,
                    "user_stamp": persist_user_timestamp,
                    "assistant_stamp": stamp,
                }
            )
            return {
                "completed": True,
                "final_response": a["content"],
                "messages": copy.deepcopy(conversation_history) + [u, a],
                "current_turn_user_idx": len(conversation_history),
            }

    monkeypatch.setattr(streaming, "_get_ai_agent", lambda: Agent)
    monkeypatch.setattr(streaming, "_build_session_db_for_stream", lambda _: None)
    monkeypatch.setattr(
        streaming, "resolve_model_provider", lambda *a, **k: ("test-model", None, None)
    )
    monkeypatch.setattr(streaming, "get_config", lambda: {})
    monkeypatch.setattr(config, "get_config", lambda: {})
    monkeypatch.setattr(config, "_resolve_cli_toolsets", lambda *a, **k: [])
    monkeypatch.setattr(profiles, "get_active_hermes_home", lambda: tmp_path)
    s = models.Session(session_id=sid, model="test-model")
    s.save()
    models.SESSIONS[sid] = s
    for i, clock in [(1, 1.0), (2, 3.0)]:
        config.SESSION_AGENT_CACHE.clear()
        stream = sid + "-settle" + str(i)
        routes._prepare_chat_start_session_for_stream(
            s,
            msg="PRIOR_Q" + str(i),
            attachments=[],
            workspace=str(tmp_path),
            model="test-model",
            model_provider=None,
            stream_id=stream,
            started_at=clock,
        )
        config.STREAMS[stream] = queue.Queue()
        streaming._run_agent_streaming(
            sid, "PRIOR_Q" + str(i), "test-model", str(tmp_path), stream, []
        )
        s = models.Session.load(sid)
        models.SESSIONS[sid] = s
    prior = copy.deepcopy(s.messages)
    prior_context = copy.deepcopy(s.context_messages)
    stream = sid + "-stop"
    s = _start_cancelled_turn(sid, stream)
    s.pending_started_at = 10.0
    s.pending_user_message = "STOP_Q"
    s.messages = prior
    s.context_messages = prior_context
    s.save()
    assert streaming.cancel_stream(stream)
    RunJournalWriter(sid, stream).append_sse_event("token", {"text": "STOP_OUTPUT"})
    _simulate_restart()
    s = models.get_session(sid)
    _append_state_db_rows(
        db,
        sid,
        [
            {"role": "user", "content": "GW_Q", "timestamp": 5},
            {"role": "assistant", "content": "GW_A", "timestamp": 6},
            {"role": "user", "content": "STOP_Q", "timestamp": 10},
            {"role": "assistant", "content": "CANCELLED_RAW", "timestamp": 11},
        ],
    )
    source = copy.deepcopy(s.messages)
    display = models.reconciled_state_db_messages_for_session(s)
    context = models.reconciled_state_db_messages_for_session(s, prefer_context=True)
    history = actual_worker(s, tmp_path, monkeypatch)
    # Restore stopped snapshot after the subsequent worker for independent HTTP/copy projection.
    s.messages = source
    s.context_messages = prior_context + [
        copy.deepcopy(r)
        for r in source
        if r.get("content") in {"STOP_Q", "STOP_OUTPUT"}
    ]
    s.active_stream_id = None
    s.pending_user_message = None
    s.save(touch_updated_at=False)
    snapshots = http_snapshots(tmp_path, sid, copies=True)
    checks = [("display", display), ("context", context), ("worker", history)]
    checks += [
        (name + "." + layer, data[layer])
        for name, data in snapshots.items()
        for layer in ["messages", "context_messages"]
        if layer in data
    ]
    for name, rows in checks:
        text = [r.get("content") for r in rows]
        assert text.count("GW_Q") == text.count("GW_A") == 1, (name, text)
        assert text.count("PRIOR_A1") == text.count("PRIOR_A2") == 1, (name, text)
        assert "CANCELLED_RAW" not in text


@pytest.mark.parametrize("live", [False, True], ids=["journal-control", "live-partial"])
def test_actual_stop_keeps_proved_later_gateway_in_worker(tmp_path, monkeypatch, live):
    sid = "cancelled-producer-live-suffix-" + str(live)
    db = tmp_path / "state.db"
    monkeypatch.setattr(models, "_active_state_db_path", lambda: db)
    s = _start_cancelled_turn(sid, sid + "-stop")
    s.pending_user_message = "STOP_Q"
    s.pending_started_at = 10.0
    s.save()
    if live:
        config.STREAM_PARTIAL_TEXT[sid + "-stop"] = "STOP_OUTPUT"
    assert streaming.cancel_stream(sid + "-stop")
    if not live:
        RunJournalWriter(sid, sid + "-stop").append_sse_event(
            "token", {"text": "STOP_OUTPUT"}
        )
    _simulate_restart()
    s = models.get_session(sid)
    _make_state_db(
        db,
        sid,
        [
            {"role": "user", "content": "STOP_Q", "timestamp": 10},
            {"role": "assistant", "content": "CANCELLED_RAW", "timestamp": 11},
            {"role": "user", "content": "GW_Q", "timestamp": 20},
            {"role": "assistant", "content": "GW_A", "timestamp": 21},
        ],
    )
    display = models.reconciled_state_db_messages_for_session(s)
    context = models.reconciled_state_db_messages_for_session(s, prefer_context=True)
    history = actual_worker(s, tmp_path, monkeypatch)
    for name, rows in [("display", display), ("context", context), ("worker", history)]:
        text = [r.get("content") for r in rows]
        assert text.count("GW_Q") == text.count("GW_A") == 1, (name, text)
        assert "CANCELLED_RAW" not in text


@pytest.mark.parametrize(
    "legacy", [False, True], ids=["exact-clock-control", "legacy-floor"]
)
def test_actual_older_stop_fractional_owner_does_not_drop_gateway(
    tmp_path, monkeypatch, legacy
):
    sid = "cancelled-producer-legacy-floor-" + str(legacy)
    db = tmp_path / "state.db"
    monkeypatch.setattr(models, "_active_state_db_path", lambda: db)
    prior = [
        {"role": "user", "content": "PRIOR_Q", "timestamp": 1},
        {"role": "assistant", "content": "PRIOR_A", "timestamp": 2},
    ]
    old = sid + "-old"
    s = _start_cancelled_turn(sid, old)
    s.pending_started_at = 3.5
    s.pending_user_message = "OLD_STOP"
    s.messages = copy.deepcopy(prior)
    s.context_messages = copy.deepcopy(prior)
    s.save()
    with monkeypatch.context() as m:
        if legacy:
            m.setattr(
                streaming, "_recovered_pending_timestamp", lambda stamp: int(stamp)
            )
        assert streaming.cancel_stream(old)
    RunJournalWriter(sid, old).append_sse_event("token", {"text": "OLD_OUTPUT"})
    _simulate_restart()
    older = models.get_session(sid)
    oldmessages = copy.deepcopy(older.messages)
    new = sid + "-new"
    s = _start_cancelled_turn(sid, new)
    s.pending_started_at = 10.0
    s.pending_user_message = "NEW_STOP"
    s.messages = oldmessages
    s.context_messages = [copy.deepcopy(r) for r in oldmessages if not r.get("_error")]
    s.save()
    assert streaming.cancel_stream(new)
    RunJournalWriter(sid, new).append_sse_event("token", {"text": "NEW_OUTPUT"})
    _simulate_restart()
    s = models.get_session(sid)
    _make_state_db(
        db,
        sid,
        [
            *prior,
            {"role": "user", "content": "OLD_STOP", "timestamp": 3.5},
            {"role": "assistant", "content": "OLD_RAW", "timestamp": 4},
            {"role": "user", "content": "GW_Q", "timestamp": 6},
            {"role": "assistant", "content": "GW_A", "timestamp": 7},
            {"role": "user", "content": "NEW_STOP", "timestamp": 10},
            {"role": "assistant", "content": "NEW_RAW", "timestamp": 11},
        ],
    )
    display = models.reconciled_state_db_messages_for_session(s)
    context = models.reconciled_state_db_messages_for_session(s, prefer_context=True)
    history = actual_worker(s, tmp_path, monkeypatch)
    for name, rows in [("display", display), ("context", context), ("worker", history)]:
        text = [r.get("content") for r in rows]
        assert text.count("GW_Q") == text.count("GW_A") == 1, (name, text)
        assert not {"OLD_RAW", "NEW_RAW"} & set(text)


def test_real_http_restored_tool_cards_follow_exact_assistant(
    tmp_path, monkeypatch
):
    sid = "cancelled-producer-http-card"
    db = tmp_path / "state.db"
    monkeypatch.setattr(models, "_active_state_db_path", lambda: db)
    prior = [
        {"role": "user", "content": "PRIOR_Q", "timestamp": 1},
        {"role": "assistant", "content": "PRIOR_A", "timestamp": 2},
        {"role": "user", "content": "CACHED_Q", "timestamp": 5},
        {"role": "assistant", "content": "CACHED_A", "timestamp": 6},
    ]
    s, owner = _recover(sid, prior)
    s.tool_calls = [
        {
            "id": "r23-card",
            "name": "read",
            "result": "LOCAL_CARD_RESULT",
            "done": True,
            "assistant_msg_idx": 3,
        }
    ]
    s.save(touch_updated_at=False)
    _make_state_db(
        db,
        sid,
        [
            *prior[:2],
            {"role": "user", "content": "GW_Q", "timestamp": 3},
            {"role": "assistant", "content": "GW_A", "timestamp": 4},
            owner,
            {"role": "assistant", "content": "CANCELLED_RAW", "timestamp": 11},
        ],
    )
    snapshots = http_snapshots(tmp_path, sid, limited=True)
    for name, data in snapshots.items():
        rows = data["messages"]
        cards = [c for c in data["tool_calls"] if c.get("id") == "r23-card"]
        assert len(cards) == 1, (name, data)
        idx = cards[0]["assistant_msg_idx"]
        assert rows[idx]["content"] == "CACHED_A", (
            name,
            idx,
            [r.get("content") for r in rows],
        )
