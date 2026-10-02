"""Focused restart durability for journal-only output after WebUI Stop."""

from __future__ import annotations

import copy
import queue
import threading
import time
from unittest.mock import Mock

import pytest

import api.config as config
import api.models as models
from api.models import Session
from api.helpers import public_session_projection
from api.run_journal import RunJournalWriter
from api.streaming import cancel_stream


@pytest.fixture(autouse=True)
def _isolated_state(tmp_path, monkeypatch):
    session_dir = tmp_path / "sessions"
    session_dir.mkdir()
    monkeypatch.setattr(models, "SESSION_DIR", session_dir)
    monkeypatch.setattr(models, "SESSION_INDEX_FILE", session_dir / "_index.json")

    models.SESSIONS.clear()
    models._JOURNAL_RETRY_LOCKS.clear()
    for name in (
        "STREAMS",
        "CANCEL_FLAGS",
        "AGENT_INSTANCES",
        "STREAM_PARTIAL_TEXT",
        "STREAM_REASONING_TEXT",
        "STREAM_LIVE_TOOL_CALLS",
        "ACTIVE_RUNS",
        "STREAM_SESSION_OWNERS",
        "SESSION_WRITEBACK_OWNERS",
    ):
        getattr(config, name).clear()
    config.SESSION_AGENT_LOCKS.clear()
    yield
    models.SESSIONS.clear()
    models._JOURNAL_RETRY_LOCKS.clear()
    for name in (
        "STREAMS",
        "CANCEL_FLAGS",
        "AGENT_INSTANCES",
        "STREAM_PARTIAL_TEXT",
        "STREAM_REASONING_TEXT",
        "STREAM_LIVE_TOOL_CALLS",
        "ACTIVE_RUNS",
        "STREAM_SESSION_OWNERS",
        "SESSION_WRITEBACK_OWNERS",
    ):
        getattr(config, name).clear()
    config.SESSION_AGENT_LOCKS.clear()


def _start_cancelled_turn(sid: str, stream_id: str) -> Session:
    session = Session(
        session_id=sid,
        title="cancel restart recovery",
        messages=[],
        context_messages=[],
        pending_user_message="Do the cancellable task.",
        pending_started_at=10.0,
        pending_user_source="webui",
        active_stream_id=stream_id,
    )
    session.save()
    models.SESSIONS[sid] = session

    config.STREAMS[stream_id] = queue.Queue()
    config.CANCEL_FLAGS[stream_id] = threading.Event()
    agent = Mock()
    agent.session_id = sid
    agent.interrupt = Mock()
    config.AGENT_INSTANCES[stream_id] = agent
    config.ACTIVE_RUNS[stream_id] = {
        "session_id": sid,
        "backend": "legacy",
        "phase": "running",
        "started_at": time.time(),
    }
    return session


def _cancel_marker(session: Session) -> tuple[int, dict]:
    for index, row in enumerate(session.messages):
        if not isinstance(row, dict) or row.get("role") != "assistant":
            continue
        if row.get("_error") is True and "cancel" in str(row.get("content") or "").lower():
            return index, row
    raise AssertionError("cancel marker missing")


def _simulate_restart() -> None:
    # The production token changes at interpreter restart. Rotate it in
    # process here so the durable sidecar exercises that exact ownership edge.
    models._JOURNAL_RECOVERY_PROCESS_TOKEN = f"restart-{time.time_ns()}"
    models.SESSIONS.clear()
    models._JOURNAL_RETRY_LOCKS.clear()
    config.ACTIVE_RUNS.clear()
    config.STREAMS.clear()
    config.CANCEL_FLAGS.clear()
    config.AGENT_INSTANCES.clear()
    config.STREAM_PARTIAL_TEXT.clear()
    config.STREAM_REASONING_TEXT.clear()
    config.STREAM_LIVE_TOOL_CALLS.clear()
    config.STREAM_SESSION_OWNERS.clear()
    config.SESSION_WRITEBACK_OWNERS.clear()
    config.SESSION_AGENT_LOCKS.clear()


@pytest.mark.parametrize("previous_exchange", [False, True])
@pytest.mark.parametrize("saved_context", [False, True])
@pytest.mark.parametrize("state_db_owner", [False, True])
@pytest.mark.parametrize("partial", [
    "A useful partial answer",
    "```python\nprint(42)\n```",
    "- first\n- second\n\n1. third",
])
def test_stop_saved_partial_survives_next_send(
    previous_exchange, saved_context, state_db_owner, partial,
):
    from api.streaming import (
        _build_partial_message,
        _sanitize_messages_for_agent,
        build_active_turn_token,
    )

    sid = "stop-saved-partial-history"
    stream_id = "stream-stop-saved-partial-history"
    session = _start_cancelled_turn(sid, stream_id)
    previous = [
        {"role": "user", "content": "Earlier question", "timestamp": 1},
        {"role": "assistant", "content": "Earlier answer", "timestamp": 2},
    ] if previous_exchange else []
    owner = {"role": "user", "content": session.pending_user_message, "timestamp": 10}
    models.stamp_message_source(
        owner, "webui", active_turn_token=build_active_turn_token(stream_id, 10),
    )
    session.messages = copy.deepcopy(previous)
    session.context_messages = (
        copy.deepcopy(previous + [owner, _build_partial_message(partial, "", [])])
        if saved_context else []
    )
    original_context = copy.deepcopy(session.context_messages)
    session.save()
    config.STREAM_PARTIAL_TEXT[stream_id] = partial

    assert cancel_stream(stream_id) is True
    # Stop must not change the authoritative provider snapshot for the live
    # partial path, or create the journal-only provisional user boundary.
    stopped = Session.load(sid)
    assert stopped.context_messages == original_context
    visible_owner = next(row for row in stopped.messages if row.get("content") == owner["content"])
    assert not visible_owner.get("_recovered")
    _, marker = _cancel_marker(stopped)
    assert not marker.get("_pending_journal_recovery")

    _simulate_restart()
    stopped = models.get_session(sid)
    state_messages = copy.deepcopy(previous + [owner]) if state_db_owner else []
    history = _sanitize_messages_for_agent(
        models.reconciled_state_db_messages_for_session(
            stopped, prefer_context=True, state_messages=state_messages,
        )
    )
    next_send = history + [{"role": "user", "content": "Next request"}]
    assert [(row["role"], row["content"]) for row in next_send] == [
        *((row["role"], row["content"]) for row in previous),
        ("user", owner["content"]),
        ("assistant", partial),
        ("user", "Next request"),
    ]


@pytest.mark.parametrize("previous_exchange", [False, True])
@pytest.mark.parametrize("raw_partial", ["   \n", "<think>unfinished trace</think>"])
def test_stop_without_model_visible_partial_keeps_journal_owner_provisional(
    previous_exchange, raw_partial,
):
    from api.streaming import _sanitize_messages_for_agent

    sid = "stop-empty-partial-history"
    stream_id = "stream-stop-empty-partial-history"
    session = _start_cancelled_turn(sid, stream_id)
    previous = [
        {"role": "user", "content": "Earlier question", "timestamp": 1},
        {"role": "assistant", "content": "Earlier answer", "timestamp": 2},
    ] if previous_exchange else []
    session.messages = copy.deepcopy(previous)
    session.context_messages = copy.deepcopy(previous)
    session.save()
    config.STREAM_PARTIAL_TEXT[stream_id] = raw_partial
    assert cancel_stream(stream_id) is True
    stopped = Session.load(sid)
    _, marker = _cancel_marker(stopped)
    assert marker.get("_pending_journal_recovery") is True
    _simulate_restart()
    stopped = models.get_session(sid)
    history = _sanitize_messages_for_agent(
        models.reconciled_state_db_messages_for_session(
            stopped, prefer_context=True, state_messages=[],
        )
    )
    assert [(row["role"], row["content"]) for row in history] == [
        (row["role"], row["content"]) for row in previous
    ]


@pytest.mark.parametrize("previous_exchange", [False, True])
@pytest.mark.parametrize("context_owner", ["absent", "tokenless", "exact", "missing-context"])
def test_stop_empty_journal_next_send_omits_unanswered_prompt(previous_exchange, context_owner):
    from api.streaming import _sanitize_messages_for_agent, build_active_turn_token

    sid = "stop-empty-journal-history"
    stream_id = "stream-stop-empty-journal-history"
    session = _start_cancelled_turn(sid, stream_id)
    previous = [
        {"role": "user", "content": "Earlier question", "timestamp": 1},
        {"role": "assistant", "content": "Earlier answer", "timestamp": 2},
    ] if previous_exchange else []
    session.messages = copy.deepcopy(previous)
    session.context_messages = copy.deepcopy(previous)
    if context_owner in {"tokenless", "exact"}:
        owner = {"role": "user", "content": session.pending_user_message, "timestamp": 10}
        models.stamp_message_source(owner, "webui")
        if context_owner == "exact":
            owner["_active_turn_token"] = build_active_turn_token(stream_id, 10)
        session.messages.append(copy.deepcopy(owner))
        session.context_messages.append(copy.deepcopy(owner))
    elif context_owner == "missing-context":
        session.context_messages = None
    session.save()

    assert cancel_stream(stream_id) is True
    # Real Stop, a cold sidecar read, then the same history boundaries used by
    # the next send. No journal assistant output exists to answer this prompt.
    _simulate_restart()
    stopped = models.get_session(sid)
    history = _sanitize_messages_for_agent(
        models.reconciled_state_db_messages_for_session(
            stopped, prefer_context=True, state_messages=[],
        )
    )
    next_send = history + [{"role": "user", "content": "Next request"}]
    assert [(row["role"], row["content"]) for row in next_send] == [
        *((row["role"], row["content"]) for row in previous),
        ("user", "Next request"),
    ]
    assert any(row.get("content") == "Do the cancellable task." for row in stopped.messages)


@pytest.mark.parametrize("previous_exchange", [False, True])
@pytest.mark.parametrize("journal_output", ["token", "reasoning"])
def test_stop_journal_owner_promotes_only_after_model_visible_answer(previous_exchange, journal_output):
    from api.streaming import _sanitize_messages_for_agent

    sid = "stop-journal-answer-history"
    stream_id = "stream-stop-journal-answer-history"
    session = _start_cancelled_turn(sid, stream_id)
    previous = [
        {"role": "user", "content": "Earlier question", "timestamp": 1},
        {"role": "assistant", "content": "Earlier answer", "timestamp": 2},
    ] if previous_exchange else []
    session.messages = copy.deepcopy(previous)
    session.context_messages = copy.deepcopy(previous)
    session.save()
    writer = RunJournalWriter(sid, stream_id)
    writer.append_sse_event(journal_output, {"text": "Recovered answer"})
    assert cancel_stream(stream_id) is True
    provisional = Session.load(sid)
    owner = next(row for row in provisional.context_messages if row.get("role") == "user" and row.get("content") == "Do the cancellable task.")
    assert owner.get("_recovered") is True
    _simulate_restart()
    recovered = models.get_session(sid)
    owner = next(row for row in recovered.context_messages if row.get("role") == "user" and row.get("content") == "Do the cancellable task.")
    history = _sanitize_messages_for_agent(
        models.reconciled_state_db_messages_for_session(recovered, prefer_context=True, state_messages=[])
    )
    expected = [(row["role"], row["content"]) for row in previous]
    if journal_output == "token":
        assert not owner.get("_recovered")
        expected += [("user", "Do the cancellable task."), ("assistant", "Recovered answer")]
    else:
        assert owner.get("_recovered") is True
    assert [(row["role"], row["content"]) for row in history] == expected


def test_cancel_retry_metadata_stays_server_private():
    sid = "cancel-restart-public-scrub"
    stream_id = "stream-cancel-restart-public-scrub"

    _start_cancelled_turn(sid, stream_id)
    RunJournalWriter(sid, stream_id).append_sse_event(
        "token", {"text": "private recovery metadata proof"}
    )
    assert cancel_stream(stream_id) is True

    durable = Session.load(sid)
    assert durable is not None
    _, marker = _cancel_marker(durable)
    assert marker["_pending_journal_recovery"] is True
    assert marker["_journal_retry_process_token"]
    assert marker["_journal_retry_owner_token"]

    public = public_session_projection({"messages": durable.messages})
    public_marker = next(
        row
        for row in public["messages"]
        if isinstance(row, dict) and row.get("_error") is True
    )
    for field in (
        "_pending_journal_recovery",
        "_journal_retry_stream_id",
        "_journal_retry_attempts",
        "_journal_retry_first_seen_ts",
        "_journal_retry_kind",
        "_journal_retry_owner_token",
        "_journal_retry_process_token",
    ):
        assert field not in public_marker


def test_cancel_restart_recovers_exact_journal_before_successor():
    sid = "cancel-restart-successor"
    stream_id = "stream-cancel-restart-successor"
    early = "Journal-only prefix before Stop."
    late = " Late suffix before process loss."

    _start_cancelled_turn(sid, stream_id)
    writer = RunJournalWriter(sid, stream_id)
    writer.append_sse_event("token", {"text": early})

    assert cancel_stream(stream_id) is True
    cancelled = Session.load(sid)
    assert cancelled is not None
    marker_index, marker = _cancel_marker(cancelled)
    assert marker.get("_pending_journal_recovery") is True
    assert marker.get("_journal_retry_kind") == "cancelled"
    assert marker.get("_journal_retry_stream_id") == stream_id
    assert not any(
        isinstance(row, dict) and row.get("_recovered_stream_id") == stream_id
        for row in cancelled.messages
    )

    # A same-session successor can be saved before the old process disappears.
    successor_user = {"role": "user", "content": "Successor prompt.", "timestamp": 20}
    successor_assistant = {"role": "assistant", "content": "Successor answer.", "timestamp": 21}
    cancelled.messages.extend([copy.deepcopy(successor_user), copy.deepcopy(successor_assistant)])
    cancelled.context_messages.extend([copy.deepcopy(successor_user), copy.deepcopy(successor_assistant)])
    cancelled.save()

    # The old stream publishes one last durable token, then the process dies.
    writer.append_sse_event("token", {"text": late})
    _simulate_restart()

    recovered = models.get_session(sid)
    exact_rows = [
        row
        for row in recovered.messages
        if isinstance(row, dict) and row.get("_recovered_stream_id") == stream_id
    ]
    assert [row.get("content") for row in exact_rows] == [early + late]

    marker_index, marker = _cancel_marker(recovered)
    recovered_index = recovered.messages.index(exact_rows[0])
    successor_user_index = next(
        index
        for index, row in enumerate(recovered.messages)
        if isinstance(row, dict) and row.get("content") == successor_user["content"]
    )
    successor_assistant_row = next(
        row
        for row in recovered.messages
        if isinstance(row, dict) and row.get("content") == successor_assistant["content"]
    )
    assert recovered_index < marker_index < successor_user_index
    assert successor_assistant_row == successor_assistant
    assert marker.get("_pending_journal_recovery") is None
    assert marker.get("_journal_retry_stream_id") is None

    context_contents = [
        row.get("content")
        for row in recovered.context_messages
        if isinstance(row, dict)
    ]
    assert context_contents == [
        "Do the cancellable task.",
        early + late,
        successor_user["content"],
        successor_assistant["content"],
    ]


def test_cancel_lazy_recovery_waits_for_old_worker_to_retire():
    sid = "cancel-restart-active-owner"
    stream_id = "stream-cancel-restart-active-owner"
    text = "Durable output while the cancelled worker is still unwinding."

    _start_cancelled_turn(sid, stream_id)
    writer = RunJournalWriter(sid, stream_id)
    writer.append_sse_event("token", {"text": text})

    assert cancel_stream(stream_id) is True
    cached = models.get_session(sid)
    _, marker = _cancel_marker(cached)
    assert marker.get("_pending_journal_recovery") is True
    assert config.ACTIVE_RUNS.get(stream_id, {}).get("phase") == "cancelling"
    assert not any(
        isinstance(row, dict) and row.get("_recovered_stream_id") == stream_id
        for row in cached.messages
    )
    assert marker.get("_pending_journal_recovery") is True

    # Registry reclamation inside the same interpreter is not proof the
    # worker is dead. A nonterminal journal must keep the durable hook armed.
    config.ACTIVE_RUNS.clear()
    models.SESSIONS.clear()
    same_process = models.get_session(sid)
    assert not any(
        isinstance(row, dict) and row.get("_recovered_stream_id") == stream_id
        for row in same_process.messages
    )
    _, same_process_marker = _cancel_marker(same_process)
    assert same_process_marker.get("_pending_journal_recovery") is True

    # A real process restart changes the persisted process token. Only then can
    # an ordinary cold read consume a nonterminal exact-stream journal tail.
    _simulate_restart()
    recovered = models.get_session(sid)
    exact_rows = [
        row
        for row in recovered.messages
        if isinstance(row, dict) and row.get("_recovered_stream_id") == stream_id
    ]
    assert [row.get("content") for row in exact_rows] == [text]
    _, marker = _cancel_marker(recovered)
    assert marker.get("_pending_journal_recovery") is None


def test_cancel_restart_keeps_same_text_from_other_turns_distinct():
    sid = "cancel-restart-same-text"
    stream_id = "stream-cancel-restart-same-text"
    repeated = "The same assistant prose appears in three different turns."

    session = _start_cancelled_turn(sid, stream_id)
    historical_user = {"role": "user", "content": "Historical prompt.", "timestamp": 1}
    historical_assistant = {"role": "assistant", "content": repeated, "timestamp": 2}
    session.messages[:] = [copy.deepcopy(historical_user), copy.deepcopy(historical_assistant)]
    session.context_messages[:] = [copy.deepcopy(historical_user), copy.deepcopy(historical_assistant)]
    session.save()

    RunJournalWriter(sid, stream_id).append_sse_event("token", {"text": repeated})
    assert cancel_stream(stream_id) is True

    cancelled = Session.load(sid)
    assert cancelled is not None
    successor_user = {"role": "user", "content": "Successor prompt.", "timestamp": 30}
    successor_assistant = {"role": "assistant", "content": repeated, "timestamp": 31}
    cancelled.messages.extend([copy.deepcopy(successor_user), copy.deepcopy(successor_assistant)])
    cancelled.context_messages.extend([copy.deepcopy(successor_user), copy.deepcopy(successor_assistant)])
    cancelled.save()

    _simulate_restart()
    recovered = models.get_session(sid)
    same_text_rows = [
        row
        for row in recovered.messages
        if isinstance(row, dict)
        and row.get("role") == "assistant"
        and row.get("content") == repeated
    ]
    assert len(same_text_rows) == 3
    exact = [
        row for row in same_text_rows
        if row.get("_recovered_stream_id") == stream_id
    ]
    assert len(exact) == 1
    marker_index, _ = _cancel_marker(recovered)
    exact_index = recovered.messages.index(exact[0])
    successor_index = recovered.messages.index(
        next(row for row in recovered.messages if row.get("content") == successor_user["content"])
    )
    assert exact_index < marker_index < successor_index

    context_same_text = [
        row
        for row in recovered.context_messages
        if isinstance(row, dict)
        and row.get("role") == "assistant"
        and row.get("content") == repeated
    ]
    assert len(context_same_text) == 3
    recovered_context = [
        row
        for row in context_same_text
        if row.get("_recovered_stream_id") == stream_id
    ]
    assert len(recovered_context) == 1
    recovered_context_index = recovered.context_messages.index(recovered_context[0])
    successor_context_index = recovered.context_messages.index(
        next(
            row
            for row in recovered.context_messages
            if isinstance(row, dict) and row.get("content") == successor_user["content"]
        )
    )
    assert recovered_context_index < successor_context_index


def test_cancel_restart_failed_recovery_save_keeps_hook_for_next_read(monkeypatch):
    sid = "cancel-restart-save-failure"
    stream_id = "stream-cancel-restart-save-failure"
    text = "Journal recovery must remain retryable after a failed sidecar save."

    _start_cancelled_turn(sid, stream_id)
    RunJournalWriter(sid, stream_id).append_sse_event("token", {"text": text})
    assert cancel_stream(stream_id) is True
    _simulate_restart()

    original_save = Session.save
    failed = {"value": False}

    def fail_first_recovered_save(session, *args, **kwargs):
        if (
            not failed["value"]
            and any(
                isinstance(row, dict)
                and row.get("_recovered_stream_id") == stream_id
                for row in getattr(session, "messages", [])
            )
        ):
            failed["value"] = True
            raise OSError("synthetic recovered sidecar save failure")
        return original_save(session, *args, **kwargs)

    monkeypatch.setattr(Session, "save", fail_first_recovered_save)
    first = models.get_session(sid)
    assert failed["value"] is True
    assert not any(
        isinstance(row, dict) and row.get("_recovered_stream_id") == stream_id
        for row in first.messages
    )
    _, first_marker = _cancel_marker(first)
    assert first_marker.get("_pending_journal_recovery") is True

    durable = Session.load(sid)
    assert durable is not None
    assert not any(
        isinstance(row, dict) and row.get("_recovered_stream_id") == stream_id
        for row in durable.messages
    )
    _, durable_marker = _cancel_marker(durable)
    assert durable_marker.get("_pending_journal_recovery") is True

    monkeypatch.setattr(Session, "save", original_save)
    models.SESSIONS.clear()
    recovered = models.get_session(sid)
    exact = [
        row for row in recovered.messages
        if isinstance(row, dict) and row.get("_recovered_stream_id") == stream_id
    ]
    assert [row.get("content") for row in exact] == [text]
    _, marker = _cancel_marker(recovered)
    assert marker.get("_pending_journal_recovery") is None



def test_cancel_restart_tool_completion_id_falls_back_only_to_idless_start():
    sid = "cancel-restart-tool-idless-start"
    stream_id = "stream-cancel-restart-tool-idless-start"

    _start_cancelled_turn(sid, stream_id)
    writer = RunJournalWriter(sid, stream_id)
    writer.append_sse_event(
        "tool",
        {"name": "terminal", "preview": "running", "args": {"command": "printf ok"}},
    )
    writer.append_sse_event(
        "tool_complete",
        {
            "name": "terminal",
            "tid": "gateway-completion-id",
            "preview": "done",
            "duration": 0.5,
            "is_error": False,
        },
    )
    assert cancel_stream(stream_id) is True

    _simulate_restart()
    recovered = models.get_session(sid)
    tools = [
        tool for tool in recovered.tool_calls
        if isinstance(tool, dict) and tool.get("_recovered_stream_id") == stream_id
    ]
    assert len(tools) == 1
    assert tools[0]["done"] is True
    assert tools[0]["preview"] == "done"
    assert tools[0]["duration"] == 0.5
    assert tools[0]["tid"].startswith("journal-")
    assert "_journal_synthetic_tid" not in tools[0]



def test_cancel_restart_tool_recovery_does_not_claim_successor_tool():
    sid = "cancel-restart-tool-owner"
    stream_id = "stream-cancel-restart-tool-owner"
    preview = "printf same-output"

    _start_cancelled_turn(sid, stream_id)
    writer = RunJournalWriter(sid, stream_id)
    writer.append_sse_event(
        "tool",
        {
            "name": "terminal",
            "preview": preview,
            "args": {"command": "printf old-first"},
            "tid": "old-tool-first",
        },
    )
    writer.append_sse_event(
        "tool",
        {
            "name": "terminal",
            "preview": preview,
            "args": {"command": "printf old-second"},
            "tid": "old-tool-second",
        },
    )
    writer.append_sse_event(
        "tool_complete",
        {
            "name": "terminal",
            "preview": "old-first-complete",
            "duration": 0.25,
            "is_error": False,
            "tid": "old-tool-first",
        },
    )
    assert cancel_stream(stream_id) is True

    cancelled = Session.load(sid)
    assert cancelled is not None
    successor_user = {"role": "user", "content": "Run the successor tool.", "timestamp": 40}
    successor_assistant = {"role": "assistant", "content": "Successor tool done.", "timestamp": 41}
    cancelled.messages.extend([copy.deepcopy(successor_user), copy.deepcopy(successor_assistant)])
    cancelled.context_messages.extend([copy.deepcopy(successor_user), copy.deepcopy(successor_assistant)])
    successor_tool = {
        "name": "terminal",
        "preview": preview,
        "snippet": preview,
        "assistant_msg_idx": len(cancelled.messages) - 1,
        "done": True,
    }
    cancelled.tool_calls = [copy.deepcopy(successor_tool)]
    cancelled.save()

    _simulate_restart()
    recovered = models.get_session(sid)
    recovered_tools = [
        tool
        for tool in recovered.tool_calls
        if isinstance(tool, dict) and tool.get("_recovered_stream_id") == stream_id
    ]
    assert len(recovered_tools) == 2
    by_tid = {tool["tid"]: tool for tool in recovered_tools}
    assert set(by_tid) == {"old-tool-first", "old-tool-second"}
    assert by_tid["old-tool-first"]["done"] is True
    assert by_tid["old-tool-first"]["preview"] == "old-first-complete"
    assert by_tid["old-tool-first"]["duration"] == 0.25
    assert by_tid["old-tool-second"]["done"] is False
    assert by_tid["old-tool-second"]["preview"] == preview

    successor_tools = [
        tool
        for tool in recovered.tool_calls
        if isinstance(tool, dict) and not tool.get("_recovered_stream_id")
    ]
    assert len(successor_tools) == 1
    assert {
        key: successor_tools[0][key]
        for key in ("name", "preview", "snippet", "done")
    } == {
        key: successor_tool[key]
        for key in ("name", "preview", "snippet", "done")
    }
    successor_owner_index = successor_tools[0]["assistant_msg_idx"]
    assert recovered.messages[successor_owner_index].get("content") == successor_assistant["content"]

    marker_index, _ = _cancel_marker(recovered)
    for tool in recovered_tools:
        owner_index = tool["assistant_msg_idx"]
        assert owner_index < marker_index
        assert recovered.messages[owner_index].get("_recovered_stream_id") == stream_id


@pytest.mark.parametrize("new_runtime_active", [True, False])
def test_newer_blocked_cancel_does_not_hide_older_recoverable_hook(new_runtime_active):
    sid = "cancel-restart-two-hooks"
    old_stream = "stream-cancel-old-ready"
    new_stream = "stream-cancel-new-blocked"
    old_text = "Older cancelled output is already durable."
    process_token = models._JOURNAL_RECOVERY_PROCESS_TOKEN

    old_owner_token = "old-cancel-owner-token"
    new_owner_token = "new-cancel-owner-token"
    old_user = {
        "role": "user",
        "content": "Older prompt.",
        "timestamp": 10,
        "_active_turn_token": old_owner_token,
    }
    old_marker = {
        "role": "assistant",
        "content": "Task cancelled.",
        "_error": True,
        "timestamp": 11,
        "_pending_journal_recovery": True,
        "_journal_retry_kind": "cancelled",
        "_journal_retry_stream_id": old_stream,
        "_journal_retry_attempts": 0,
        "_journal_retry_first_seen_ts": int(time.time()),
        "_journal_retry_process_token": process_token,
        "_journal_retry_owner_token": old_owner_token,
    }
    new_user = {
        "role": "user",
        "content": "Newer prompt.",
        "timestamp": 20,
        "_active_turn_token": new_owner_token,
    }
    new_marker = {
        "role": "assistant",
        "content": "Task cancelled.",
        "_error": True,
        "timestamp": 21,
        "_pending_journal_recovery": True,
        "_journal_retry_kind": "cancelled",
        "_journal_retry_stream_id": new_stream,
        "_journal_retry_attempts": 0,
        "_journal_retry_first_seen_ts": int(time.time()),
        "_journal_retry_process_token": process_token,
        "_journal_retry_owner_token": new_owner_token,
    }
    session = Session(
        session_id=sid,
        title="two cancel hooks",
        messages=[
            copy.deepcopy(old_user),
            copy.deepcopy(old_marker),
            copy.deepcopy(new_user),
            copy.deepcopy(new_marker),
        ],
        context_messages=[copy.deepcopy(old_user), copy.deepcopy(new_user)],
    )
    session.save()

    old_writer = RunJournalWriter(sid, old_stream)
    old_writer.append_sse_event("token", {"text": old_text})
    old_writer.append_sse_event("cancel", {"message": "Cancelled by user"})

    new_writer = RunJournalWriter(sid, new_stream)
    new_writer.append_sse_event(
        "token", {"text": "Newer output remains owned by a live worker."}
    )
    if new_runtime_active:
        config.ACTIVE_RUNS[new_stream] = {
            "session_id": sid,
            "backend": "legacy",
            "phase": "cancelling",
            "started_at": time.time(),
        }

    models.SESSIONS.clear()
    recovered = models.get_session(sid)

    old_rows = [
        row for row in recovered.messages
        if isinstance(row, dict) and row.get("_recovered_stream_id") == old_stream
    ]
    assert [row.get("content") for row in old_rows] == [old_text]

    pending_by_stream = {
        str(row.get("_journal_retry_stream_id")): row
        for row in recovered.messages
        if isinstance(row, dict) and row.get("_journal_retry_kind") == "cancelled"
    }
    assert old_stream not in pending_by_stream
    assert pending_by_stream[new_stream].get("_pending_journal_recovery") is True


@pytest.mark.parametrize(
    ("newer_state", "expected_new_attempts"),
    [
        ("live", 0),
        ("nonterminal", 0),
        ("terminal-empty", 1),
    ],
)
def test_newer_cancel_hook_does_not_block_older_interrupted_recovery(
    newer_state, expected_new_attempts
):
    sid = f"cancel-vs-interrupted-{newer_state}"
    old_stream = f"stream-interrupted-old-{newer_state}"
    new_stream = f"stream-cancel-new-{newer_state}"
    old_text = "Older interrupted output is already recoverable."
    new_owner_token = f"new-owner-{newer_state}"

    old_user = {
        "role": "user",
        "content": "Older interrupted prompt.",
        "timestamp": 10,
    }
    old_marker = models._build_recovery_marker_with_retry_hook(
        recovered_output=False,
        stream_id=old_stream,
        pending_started_at=10,
    )
    new_user = {
        "role": "user",
        "content": "Newer cancelled prompt.",
        "timestamp": 20,
        "_active_turn_token": new_owner_token,
    }
    new_marker = {
        "role": "assistant",
        "content": "Task cancelled.",
        "_error": True,
        "timestamp": 21,
        "_pending_journal_recovery": True,
        "_journal_retry_kind": "cancelled",
        "_journal_retry_stream_id": new_stream,
        "_journal_retry_attempts": 0,
        "_journal_retry_first_seen_ts": int(time.time()),
        "_journal_retry_process_token": models._JOURNAL_RECOVERY_PROCESS_TOKEN,
        "_journal_retry_owner_token": new_owner_token,
    }
    session = Session(
        session_id=sid,
        title="cancel must not mask interrupted recovery",
        messages=[
            copy.deepcopy(old_user),
            copy.deepcopy(old_marker),
            copy.deepcopy(new_user),
            copy.deepcopy(new_marker),
        ],
        context_messages=[copy.deepcopy(old_user), copy.deepcopy(new_user)],
    )
    session.save()

    RunJournalWriter(sid, old_stream).append_sse_event(
        "token", {"text": old_text}
    )

    new_writer = RunJournalWriter(sid, new_stream)
    if newer_state == "live":
        config.ACTIVE_RUNS[new_stream] = {
            "session_id": sid,
            "backend": "legacy",
            "phase": "cancelling",
            "started_at": time.time(),
        }
    elif newer_state == "nonterminal":
        new_writer.append_sse_event(
            "token", {"text": "Newer cancelled output is still arriving."}
        )
    else:
        new_writer.append_sse_event(
            "cancel", {"message": "Cancelled by user"}
        )

    models.SESSIONS.clear()
    recovered = models.get_session(sid)

    old_rows = [
        row
        for row in recovered.messages
        if isinstance(row, dict)
        and row.get("_recovered_stream_id") == old_stream
    ]
    assert [row.get("content") for row in old_rows] == [old_text]

    recovered_old_marker = next(
        row
        for row in recovered.messages
        if isinstance(row, dict) and row.get("type") == "interrupted"
    )
    assert recovered_old_marker.get("_pending_journal_recovery") is None
    assert recovered_old_marker.get("_journal_retry_stream_id") is None

    pending_new = next(
        row
        for row in recovered.messages
        if isinstance(row, dict)
        and row.get("_journal_retry_stream_id") == new_stream
    )
    assert pending_new.get("_pending_journal_recovery") is True
    assert pending_new.get("_journal_retry_attempts") == expected_new_attempts


def test_cancel_restart_context_fails_closed_for_ambiguous_duplicate_prompt_and_timestamp():
    sid = "cancel-restart-duplicate-user-owner"
    stream_id = "stream-cancel-restart-duplicate-user-owner"
    prompt = "Repeat exactly the same prompt."
    timestamp = 10
    recovered_text = "Recovered output belongs to the second identical prompt."

    session = _start_cancelled_turn(sid, stream_id)
    historical_user = {
        "role": "user",
        "content": prompt,
        "timestamp": timestamp,
        "_owner_probe": "historical-owner",
        "_active_turn_token": "historical-owner-token",
    }
    historical_assistant = {
        "role": "assistant",
        "content": "Historical answer.",
        "timestamp": timestamp,
    }
    cancelled_user = {
        "role": "user",
        "content": prompt,
        "timestamp": timestamp,
        "_owner_probe": "cancelled-owner",
    }
    session.pending_user_message = prompt
    session.pending_started_at = float(timestamp)
    session.messages[:] = [
        copy.deepcopy(historical_user),
        copy.deepcopy(historical_assistant),
        copy.deepcopy(cancelled_user),
    ]
    session.context_messages[:] = copy.deepcopy(session.messages)
    session.save()

    writer = RunJournalWriter(sid, stream_id)
    writer.append_sse_event("token", {"text": recovered_text})
    assert cancel_stream(stream_id) is True

    cancelled = Session.load(sid)
    assert cancelled is not None
    display_owner = next(
        row for row in cancelled.messages
        if isinstance(row, dict) and row.get("_owner_probe") == "cancelled-owner"
    )
    marker_index, marker = _cancel_marker(cancelled)
    owner_token = str(display_owner.get("_active_turn_token") or "")
    assert owner_token
    assert marker.get("_journal_retry_owner_token") == owner_token
    historical_context = next(
        row for row in cancelled.context_messages
        if isinstance(row, dict) and row.get("_owner_probe") == "historical-owner"
    )
    cancelled_context = next(
        row for row in cancelled.context_messages
        if isinstance(row, dict) and row.get("_owner_probe") == "cancelled-owner"
    )
    assert historical_context.get("_active_turn_token") == "historical-owner-token"
    assert not cancelled_context.get("_active_turn_token")

    successor_user = {"role": "user", "content": "Successor prompt.", "timestamp": 20}
    successor_assistant = {"role": "assistant", "content": "Successor answer.", "timestamp": 21}
    cancelled.messages.extend([copy.deepcopy(successor_user), copy.deepcopy(successor_assistant)])
    cancelled.context_messages.extend([copy.deepcopy(successor_user), copy.deepcopy(successor_assistant)])
    cancelled.save()

    _simulate_restart()
    recovered = models.get_session(sid)
    context = recovered.context_messages

    # The duplicate tokenless provider owner is ambiguous. Recovery remains
    # visible in the transcript, but provider context must not guess either
    # equal prompt as its owner or overwrite the historical token.
    assert not any(
        isinstance(row, dict)
        and row.get("_recovered_stream_id") == stream_id
        for row in context
    )
    historical_context = next(
        row for row in context
        if isinstance(row, dict) and row.get("_owner_probe") == "historical-owner"
    )
    assert historical_context.get("_active_turn_token") == "historical-owner-token"
    recovered_display = next(
        row for row in recovered.messages
        if isinstance(row, dict)
        and row.get("_recovered_stream_id") == stream_id
        and row.get("content") == recovered_text
    )
    display_index = recovered.messages.index(recovered_display)
    marker_index, _marker = _cancel_marker(recovered)
    successor_index = next(
        index for index, row in enumerate(recovered.messages)
        if isinstance(row, dict) and row.get("content") == successor_user["content"]
    )
    assert display_index < marker_index < successor_index



def test_cancel_restart_context_does_not_reassign_earlier_repeated_prompt_owner():
    sid = "cancel-restart-earlier-owner"
    stream_id = "stream-cancel-restart-earlier-owner"
    prompt = "Repeat prompt whose earlier owner must stay intact."
    timestamp = 10
    recovered_text = "Recovered output must not inherit the earlier prompt."

    session = _start_cancelled_turn(sid, stream_id)
    historical_user = {
        "role": "user",
        "content": prompt,
        "timestamp": timestamp,
        "_owner_probe": "historical-owner",
        "_active_turn_token": "historical-owner-token",
    }
    cancelled_user = {
        "role": "user",
        "content": prompt,
        "timestamp": timestamp,
        "_owner_probe": "cancelled-owner",
    }
    session.pending_user_message = prompt
    session.pending_started_at = float(timestamp)
    session.messages[:] = [
        copy.deepcopy(historical_user),
        {"role": "assistant", "content": "Historical answer.", "timestamp": timestamp},
        copy.deepcopy(cancelled_user),
    ]
    # The current pending owner has not reached provider context yet. The only
    # context user is an earlier equal prompt that already has a different token.
    session.context_messages[:] = [copy.deepcopy(historical_user)]
    session.save()

    writer = RunJournalWriter(sid, stream_id)
    writer.append_sse_event("token", {"text": recovered_text})
    assert cancel_stream(stream_id) is True

    cancelled = Session.load(sid)
    assert cancelled is not None
    display_owner = next(
        row for row in cancelled.messages
        if isinstance(row, dict) and row.get("_owner_probe") == "cancelled-owner"
    )
    owner_token = str(display_owner.get("_active_turn_token") or "")
    assert owner_token
    marker_index, marker = _cancel_marker(cancelled)
    assert marker.get("_journal_retry_owner_token") == owner_token

    historical_context = next(
        row for row in cancelled.context_messages
        if isinstance(row, dict) and row.get("_owner_probe") == "historical-owner"
    )
    assert historical_context.get("_active_turn_token") == "historical-owner-token"
    assert all(
        not (
            isinstance(row, dict)
            and row.get("role") == "user"
            and row.get("_active_turn_token") == owner_token
        )
        for row in cancelled.context_messages
    )

    successor_user = {"role": "user", "content": "Successor prompt.", "timestamp": 20}
    successor_assistant = {"role": "assistant", "content": "Successor answer.", "timestamp": 21}
    cancelled.messages.extend([copy.deepcopy(successor_user), copy.deepcopy(successor_assistant)])
    cancelled.context_messages.extend([copy.deepcopy(successor_user), copy.deepcopy(successor_assistant)])
    cancelled.save()

    _simulate_restart()
    recovered = models.get_session(sid)

    assert not any(
        isinstance(row, dict)
        and row.get("_recovered_stream_id") == stream_id
        for row in recovered.context_messages
    )
    historical_context = next(
        row for row in recovered.context_messages
        if isinstance(row, dict) and row.get("_owner_probe") == "historical-owner"
    )
    assert historical_context.get("_active_turn_token") == "historical-owner-token"

    recovered_display = next(
        row for row in recovered.messages
        if isinstance(row, dict)
        and row.get("_recovered_stream_id") == stream_id
        and row.get("content") == recovered_text
    )
    display_index = recovered.messages.index(recovered_display)
    marker_index, _marker = _cancel_marker(recovered)
    successor_index = next(
        index for index, row in enumerate(recovered.messages)
        if isinstance(row, dict) and row.get("content") == successor_user["content"]
    )
    assert display_index < marker_index < successor_index
def test_cancel_restart_context_fails_closed_when_compression_removed_exact_owner():
    sid = "cancel-restart-compressed-owner-missing"
    stream_id = "stream-cancel-restart-compressed-owner-missing"
    prompt = "Cancelled prompt after compressed history."
    recovered_text = "Recovered output must stay out of context without its exact owner."

    session = _start_cancelled_turn(sid, stream_id)
    history = []
    for index in range(3):
        history.extend([
            {"role": "user", "content": f"Historical prompt {index}.", "timestamp": index * 2 + 1},
            {"role": "assistant", "content": f"Historical answer {index}.", "timestamp": index * 2 + 2},
        ])
    cancelled_user = {
        "role": "user",
        "content": prompt,
        "timestamp": 10,
        "_owner_probe": "cancelled-owner",
    }
    compression_summary = {
        "role": "user",
        "content": "[Earlier conversation compressed into summary.]",
        "timestamp": 9,
        "_owner_probe": "compression-summary",
    }
    session.pending_user_message = prompt
    session.pending_started_at = 10.0
    session.messages[:] = history + [copy.deepcopy(cancelled_user)]
    session.context_messages[:] = [
        copy.deepcopy(compression_summary),
        copy.deepcopy(cancelled_user),
    ]
    session.save()

    writer = RunJournalWriter(sid, stream_id)
    writer.append_sse_event("token", {"text": recovered_text})
    assert cancel_stream(stream_id) is True

    cancelled = Session.load(sid)
    assert cancelled is not None
    successors = []
    for index in range(3):
        successors.extend([
            {"role": "user", "content": f"Successor prompt {index}.", "timestamp": 20 + index * 2},
            {"role": "assistant", "content": f"Successor answer {index}.", "timestamp": 21 + index * 2},
        ])
    cancelled.messages.extend(copy.deepcopy(successors))
    # Simulate a later compression that retained the summary and successors but
    # removed the cancelled turn's provider-context owner.
    cancelled.context_messages[:] = [copy.deepcopy(compression_summary)] + copy.deepcopy(successors)
    cancelled.save()

    _simulate_restart()
    recovered = models.get_session(sid)

    recovered_display = [
        row for row in recovered.messages
        if isinstance(row, dict)
        and row.get("_recovered_stream_id") == stream_id
        and row.get("content") == recovered_text
    ]
    assert len(recovered_display) == 1
    marker_index, _marker = _cancel_marker(recovered)
    display_index = recovered.messages.index(recovered_display[0])
    first_successor_index = next(
        index for index, row in enumerate(recovered.messages)
        if isinstance(row, dict) and row.get("content") == "Successor prompt 0."
    )
    assert display_index < marker_index < first_successor_index

    # Provider context has no exact token-bearing cancelled owner after
    # compression, so visible recovery must not be attached to any successor.
    assert not any(
        isinstance(row, dict)
        and row.get("_recovered_stream_id") == stream_id
        for row in recovered.context_messages
    )


def test_cancel_restart_context_uses_exact_owner_token_after_compression():
    sid = "cancel-restart-compressed-owner-token"
    stream_id = "stream-cancel-restart-compressed-owner-token"
    prompt = "Cancelled prompt whose exact owner survives compression."
    recovered_text = "Recovered output belongs immediately after the cancelled owner."

    session = _start_cancelled_turn(sid, stream_id)
    history = []
    for index in range(3):
        history.extend([
            {"role": "user", "content": f"Historical prompt {index}.", "timestamp": index * 2 + 1},
            {"role": "assistant", "content": f"Historical answer {index}.", "timestamp": index * 2 + 2},
        ])
    cancelled_user = {
        "role": "user",
        "content": prompt,
        "timestamp": 10,
        "_owner_probe": "cancelled-owner",
    }
    compression_summary = {
        "role": "user",
        "content": "[Earlier conversation compressed into summary.]",
        "timestamp": 9,
        "_owner_probe": "compression-summary",
    }
    session.pending_user_message = prompt
    session.pending_started_at = 10.0
    session.messages[:] = history + [copy.deepcopy(cancelled_user)]
    session.context_messages[:] = [
        copy.deepcopy(compression_summary),
        copy.deepcopy(cancelled_user),
    ]
    session.save()

    writer = RunJournalWriter(sid, stream_id)
    writer.append_sse_event("token", {"text": recovered_text})
    assert cancel_stream(stream_id) is True

    cancelled = Session.load(sid)
    assert cancelled is not None
    display_owner = next(
        row for row in cancelled.messages
        if isinstance(row, dict) and row.get("_owner_probe") == "cancelled-owner"
    )
    context_owner = next(
        row for row in cancelled.context_messages
        if isinstance(row, dict) and row.get("_owner_probe") == "cancelled-owner"
    )
    owner_token = str(display_owner.get("_active_turn_token") or "")
    assert owner_token
    assert context_owner.get("_active_turn_token") == owner_token

    successors = [
        {"role": "user", "content": "Successor prompt 0.", "timestamp": 20},
        {"role": "assistant", "content": "Successor answer 0.", "timestamp": 21},
        {"role": "user", "content": "Successor prompt 1.", "timestamp": 22},
        {"role": "assistant", "content": "Successor answer 1.", "timestamp": 23},
    ]
    cancelled.messages.extend(copy.deepcopy(successors))
    cancelled.context_messages.extend(copy.deepcopy(successors))
    cancelled.save()

    _simulate_restart()
    recovered = models.get_session(sid)
    context = recovered.context_messages

    owner_index = next(
        index for index, row in enumerate(context)
        if isinstance(row, dict) and row.get("_active_turn_token") == owner_token
    )
    recovered_index = next(
        index for index, row in enumerate(context)
        if isinstance(row, dict)
        and row.get("_recovered_stream_id") == stream_id
        and row.get("content") == recovered_text
    )
    successor_index = next(
        index for index, row in enumerate(context)
        if isinstance(row, dict) and row.get("content") == "Successor prompt 0."
    )
    assert owner_index < recovered_index < successor_index


@pytest.mark.parametrize("completion_key", ["tid", "tool_call_id"])
def test_overlapping_tool_completion_prefers_exact_id_before_idless_fallback(completion_key):
    sid = "cancel-overlap-exact-id"
    stream_id = "stream-cancel-overlap-exact-id"
    _start_cancelled_turn(sid, stream_id)
    writer = RunJournalWriter(sid, stream_id)
    writer.append_sse_event("tool", {"name": "terminal", "tid": "A", "preview": "start A"})
    writer.append_sse_event("tool", {"name": "terminal", "preview": "start B"})
    writer.append_sse_event("tool_complete", {"name": "terminal", completion_key: "A", "preview": "done A"})
    writer.append_sse_event("tool_complete", {"name": "terminal", completion_key: "B", "preview": "done B"})
    assert cancel_stream(stream_id)
    _simulate_restart()
    recovered = models.get_session(sid)
    tools = recovered.tool_calls
    assert [(tool["tid"], tool["preview"], tool["done"]) for tool in tools] == [
        ("A", "done A", True), ("journal-2", "done B", True),
    ]
    assert all("_journal_synthetic_tid" not in tool for tool in tools)


def test_recovered_equal_segments_and_tool_owners_survive_cold_load():
    sid = "cancel-equal-segments-cold-load"
    stream_id = "stream-cancel-equal-segments-cold-load"
    _start_cancelled_turn(sid, stream_id)
    writer = RunJournalWriter(sid, stream_id)
    for tid in ("A", "B"):
        writer.append_sse_event("token", {"text": "Checking…"})
        writer.append_sse_event("tool", {"name": "terminal", "tid": tid})
        writer.append_sse_event("tool_complete", {"name": "terminal", "tid": tid, "preview": f"done {tid}"})
    assert cancel_stream(stream_id)
    _simulate_restart()
    recovered = models.get_session(sid)
    before_messages = copy.deepcopy(recovered.messages)
    before_tools = copy.deepcopy(recovered.tool_calls)
    models.SESSIONS.clear()
    cold = models.get_session(sid)
    rows = [row for row in cold.messages if row.get("_recovered_stream_id") == stream_id]
    assert len(rows) == 2
    assert all(row["content"] == "Checking…" and not row.get("_partial") for row in rows)
    assert cold.messages == before_messages
    assert cold.tool_calls == before_tools
    assert models._sidecar_has_terminal_partial_error(cold.messages)
    assert not models._sidecar_has_terminal_partial_error(rows)
    owner_indexes = [tool["assistant_msg_idx"] for tool in cold.tool_calls]
    assert len(set(owner_indexes)) == 2
    for index in owner_indexes:
        assert cold.messages[index]["_recovered_stream_id"] == stream_id
        assert cold.messages[index]["content"] == "Checking…"
    assert not _cancel_marker(cold)[1].get("_pending_journal_recovery")



def _persist_multi_retry_turns(sid, kinds, outputs, *, defer_first=False):
    """Persist actual Stop hooks and production interrupted markers/journals."""
    session = Session(session_id=sid, title="multiple retry turns", messages=[], context_messages=[])
    session.save()
    models.SESSIONS[sid] = session
    streams = []
    for number, kind in enumerate(kinds):
        stream_id = f"{sid}-stream-{number}"
        streams.append(stream_id)
        started = 10 * (number + 1)
        owner = {"role": "user", "content": f"Prompt {number}", "timestamp": started}
        session.messages.append(copy.deepcopy(owner))
        session.context_messages.append(copy.deepcopy(owner))
        if kind == "ordinary":
            answer = {"role": "assistant", "content": f"Ordinary answer {number}", "timestamp": started + 1}
            session.messages.append(copy.deepcopy(answer))
            session.context_messages.append(copy.deepcopy(answer))
        elif kind == "interrupted":
            marker = models._build_recovery_marker_with_retry_hook(
                recovered_output=False, stream_id=stream_id, pending_started_at=started,
            )
            marker["timestamp"] = started + 1
            session.messages.append(marker)
        else:
            assert kind == "cancelled"
            session.pending_user_message = owner["content"]
            session.pending_started_at = started
            session.pending_user_source = "webui"
            session.active_stream_id = stream_id
            config.STREAMS[stream_id] = queue.Queue()
            config.CANCEL_FLAGS[stream_id] = threading.Event()
            agent = Mock()
            agent.session_id = sid
            config.AGENT_INSTANCES[stream_id] = agent
            config.ACTIVE_RUNS[stream_id] = {
                "session_id": sid, "phase": "running", "started_at": time.time(),
            }
            session.save()
            assert cancel_stream(stream_id) is True
        session.save()
    # Journals arrive after all markers, exactly the lazy-recovery condition.
    for number, events in enumerate(outputs):
        if defer_first and number == 0:
            continue
        writer = RunJournalWriter(sid, streams[number])
        for event, payload in events:
            writer.append_sse_event(event, payload)
        if kinds[number] != "ordinary" and (not events or events[-1][0] not in {"apperror", "error", "done", "cancel"}):
            writer.append_sse_event("cancel", {"message": "Terminal journal"})
    _simulate_restart()
    return streams


def _stream_output(session, stream_id):
    return [row for row in session.messages if row.get("_recovered_stream_id") == stream_id]


def _pending_stream_hook(session, stream_id):
    return next((row for row in session.messages if row.get("_journal_retry_stream_id") == stream_id), None)


def _assert_retry_turn_ownership(session, kinds, streams):
    user_positions = [i for i, row in enumerate(session.messages) if row.get("role") == "user"]
    assert len(user_positions) == len(kinds)
    for number, stream_id in enumerate(streams):
        end = user_positions[number + 1] if number + 1 < len(kinds) else len(session.messages)
        for index, row in enumerate(session.messages):
            if row.get("_recovered_stream_id") == stream_id:
                assert user_positions[number] < index < end
    for tool in session.tool_calls or []:
        owner = session.messages[tool["assistant_msg_idx"]]
        assert owner.get("_recovered_stream_id") == tool.get("_recovered_stream_id")


def test_newer_recovered_cancel_does_not_hide_older_interrupted_output():
    sid = "round4-older-interrupted-newer-cancel"
    kinds = ["interrupted", "cancelled"]
    streams = _persist_multi_retry_turns(sid, kinds, [
        [("token", {"text": "Older interrupted answer"})],
        [("token", {"text": "Newer cancelled answer"})],
    ])
    first = models.get_session(sid)
    assert [row["content"] for row in _stream_output(first, streams[1])] == ["Newer cancelled answer"]
    models.SESSIONS.clear()
    second = models.get_session(sid)
    assert [row["content"] for row in _stream_output(second, streams[0])] == ["Older interrupted answer"]
    assert _pending_stream_hook(second, streams[0]) is None
    _assert_retry_turn_ownership(second, kinds, streams)


def test_newer_empty_cancel_does_not_spend_older_ready_cancel_retry():
    sid = "round4-older-ready-newer-empty-cancel"
    streams = _persist_multi_retry_turns(sid, ["cancelled", "cancelled"], [
        [("token", {"text": "Older ready cancelled answer"})], [],
    ])
    first = models.get_session(sid)
    assert [row["content"] for row in _stream_output(first, streams[0])] == ["Older ready cancelled answer"]
    assert _pending_stream_hook(first, streams[0]) is None
    assert _pending_stream_hook(first, streams[1])["_journal_retry_attempts"] == 1


@pytest.mark.parametrize("cache_hits", [False, True], ids=["cold", "cached"])
@pytest.mark.parametrize("older_kind", ["interrupted", "cancelled"])
@pytest.mark.parametrize("ordinary_boundary", [False, True])
@pytest.mark.parametrize("newer_state", ["empty", "ready", "reasoning", "live", "nonterminal", "arriving", "expired"])
def test_mixed_multiple_retry_turns_keep_boundaries_and_independent_budgets(
    cache_hits, older_kind, ordinary_boundary, newer_state,
):
    sid = f"round4-mixed-{cache_hits}-{older_kind}-{ordinary_boundary}-{newer_state}"
    kinds = [older_kind, "cancelled"]
    outputs = [
        [("token", {"text": "Oldest output"}),
         ("tool", {"name": "read_file", "tid": "old-tool", "args": {"path": "old.txt"}}),
         ("tool_complete", {"name": "read_file", "tid": "old-tool", "preview": "Old full result"})],
        [("token", {"text": "Middle cancelled output"}),
         ("tool", {"name": "read_file", "tid": "middle-tool", "args": {"path": "middle.txt"}})],
    ]
    if ordinary_boundary:
        kinds.append("ordinary")
        outputs.append([])
    kinds.append("cancelled")
    newer_events = []
    if newer_state == "ready":
        newer_events = [("token", {"text": "Newest output"})]
    elif newer_state == "reasoning":
        newer_events = [("reasoning", {"text": "Newest private thought"})]
    outputs.append(newer_events)
    streams = _persist_multi_retry_turns(sid, kinds, outputs)
    newer_stream = streams[-1]
    if newer_state == "live":
        config.ACTIVE_RUNS[newer_stream] = {"session_id": sid, "phase": "cancelling", "started_at": time.time()}
    elif newer_state == "nonterminal":
        # A distinct same-process nonterminal journal is not permission to read
        # cancellation output, even when registry bookkeeping is absent.
        session = Session.load(sid)
        _pending_stream_hook(session, newer_stream)["_journal_retry_process_token"] = models._JOURNAL_RECOVERY_PROCESS_TOKEN
        session.save()
        from api.run_journal import _run_path
        _run_path(sid, newer_stream).unlink()
        RunJournalWriter(sid, newer_stream).append_sse_event("token", {"text": "Still owned output"})
    elif newer_state == "arriving":
        from api.run_journal import _run_path
        _run_path(sid, newer_stream).unlink()
    elif newer_state == "expired":
        session = Session.load(sid)
        _pending_stream_hook(session, newer_stream)["_journal_retry_attempts"] = models._JOURNAL_RETRY_MAX_ATTEMPTS
        session.save()

    for _ in range(4):
        if not cache_hits:
            models.SESSIONS.clear()
        recovered = models.get_session(sid)
    old_should_recover = older_kind == "cancelled" or not ordinary_boundary
    assert bool(_stream_output(recovered, streams[0])) is old_should_recover
    assert [row["content"] for row in _stream_output(recovered, streams[1]) if row.get("content")] == ["Middle cancelled output"]
    _assert_retry_turn_ownership(recovered, kinds, streams)
    if not old_should_recover:
        assert _pending_stream_hook(recovered, streams[0])["_journal_retry_attempts"] == 0
    if newer_state in {"empty", "live", "nonterminal", "arriving"}:
        expected = 4 if newer_state == "empty" else 0
        assert _pending_stream_hook(recovered, newer_stream)["_journal_retry_attempts"] == expected
    else:
        assert _pending_stream_hook(recovered, newer_stream) is None
    if newer_state == "reasoning":
        assert not any("Newest private thought" in str(row.get("content")) for row in recovered.context_messages)
    # Recovered cancellation output belongs to its exact user even when a
    # genuine ordinary assistant boundary prevents older interruption retry.
    context_middle = next(i for i, row in enumerate(recovered.context_messages) if row.get("content") == "Prompt 1")
    assert recovered.context_messages[context_middle + 1]["content"] == "Middle cancelled output"
    if old_should_recover:
        from api.streaming import _sanitize_messages_for_agent
        history = _sanitize_messages_for_agent(
            models.reconciled_state_db_messages_for_session(
                recovered, prefer_context=True, state_messages=[],
            )
        )
        assert any(row.get("content") == "Oldest output" for row in history)


@pytest.mark.parametrize("expiry", ["attempts", "age"])
def test_multiple_empty_cancel_hooks_have_separate_retry_budgets(expiry):
    sid = f"round4-independent-budgets-{expiry}"
    streams = _persist_multi_retry_turns(sid, ["cancelled"] * 3, [
        [("token", {"text": "Old ready output"})], [], [],
    ])
    session = Session.load(sid)
    _pending_stream_hook(session, streams[1])["_journal_retry_attempts"] = 3
    newest = _pending_stream_hook(session, streams[2])
    if expiry == "attempts":
        newest["_journal_retry_attempts"] = models._JOURNAL_RETRY_MAX_ATTEMPTS
    else:
        newest["_journal_retry_first_seen_ts"] = time.time() - models._JOURNAL_RETRY_GIVEUP_SECONDS - 1
    session.save()
    recovered = models.get_session(sid)
    assert [row["content"] for row in _stream_output(recovered, streams[0])] == ["Old ready output"]
    assert _pending_stream_hook(recovered, streams[2]) is None
    assert _pending_stream_hook(recovered, streams[1])["_journal_retry_attempts"] == 4
    assert _pending_stream_hook(recovered, streams[0]) is None
    models.SESSIONS.clear()
    again = models.get_session(sid)
    assert _pending_stream_hook(again, streams[1])["_journal_retry_attempts"] == 5
    assert [row["content"] for row in _stream_output(again, streams[0])] == ["Old ready output"]


def test_multiple_cancel_retry_counter_save_failure_aborts_pass(monkeypatch):
    sid = "round4-multiple-hooks-counter-rollback"
    streams = _persist_multi_retry_turns(sid, ["cancelled", "cancelled"], [
        [("token", {"text": "Older output waits for a durable retry transaction"})], [],
    ])
    original_save = Session.save
    failed = False

    def fail_first_budget_save(session, *args, **kwargs):
        nonlocal failed
        marker = _pending_stream_hook(session, streams[1])
        if not failed and marker and marker.get("_journal_retry_attempts") == 1:
            failed = True
            raise OSError("synthetic retry budget save failure")
        return original_save(session, *args, **kwargs)

    monkeypatch.setattr(Session, "save", fail_first_budget_save)
    first = models.get_session(sid)
    assert failed
    assert not _stream_output(first, streams[0])
    assert _pending_stream_hook(first, streams[1])["_journal_retry_attempts"] == 0
    durable = Session.load(sid)
    assert _pending_stream_hook(durable, streams[1])["_journal_retry_attempts"] == 0
    assert not _stream_output(durable, streams[0])
    monkeypatch.setattr(Session, "save", original_save)
    second = models.get_session(sid)
    assert len(_stream_output(second, streams[0])) == 1
    assert _pending_stream_hook(second, streams[1])["_journal_retry_attempts"] == 1


def test_older_interrupted_terminal_error_keeps_newer_cancel_tool_owner():
    sid = "round4-old-terminal-new-cancel-tool"
    kinds = ["interrupted", "cancelled"]
    streams = _persist_multi_retry_turns(sid, kinds, [
        [("token", {"text": "Old progress before terminal error"}),
         ("tool", {"name": "read_file", "tid": "old-error-tool", "args": {"path": "old.txt"}}),
         ("apperror", {"session_id": sid, "terminal_session_persisted": False,
                       "session": {"session_id": sid, "messages": [
                           {"role": "user", "content": "Prompt 0"},
                           {"role": "assistant", "content": "Old terminal failure", "_error": True},
                       ]}})],
        [("token", {"text": "New cancel output"}),
         ("tool", {"name": "read_file", "tid": "new-tool", "args": {"path": "new.txt"}})],
    ])
    models.get_session(sid)
    models.SESSIONS.clear()
    recovered = models.get_session(sid)
    assert any(row.get("content") == "Old terminal failure" for row in _stream_output(recovered, streams[0]))
    assert not any(row.get("type") == "interrupted" for row in recovered.messages)
    _assert_retry_turn_ownership(recovered, kinds, streams)
    assert {tool["tid"] for tool in recovered.tool_calls} == {"old-error-tool", "new-tool"}
    models.SESSIONS.clear()
    _assert_retry_turn_ownership(models.get_session(sid), kinds, streams)


@pytest.mark.parametrize("cache_hits", [False, True], ids=["cold", "cached"])
@pytest.mark.parametrize("later_cancel", [False, True])
@pytest.mark.parametrize("newer_output", ["prose", "reasoning", "tool"])
def test_two_interrupted_hooks_keep_newest_assistant_cutoff(cache_hits, later_cancel, newer_output):
    """A later interrupted recovery is an answer boundary, not a cancel recovery."""
    sid = f"round5-two-interrupts-{cache_hits}-{later_cancel}-{newer_output}"
    kinds = ["interrupted", "interrupted"]
    new_events = {
        "prose": [("token", {"text": "New interrupted answer"})],
        "reasoning": [("reasoning", {"text": "New interrupted thought"})],
        "tool": [("tool", {"name": "read_file", "tid": "new-interrupted-tool", "args": {"path": "new.txt"}})],
    }[newer_output]
    outputs = [[("token", {"text": "Old interrupted answer"})], new_events]
    if later_cancel:
        # A pending cancellation forces the retry selector to run even after
        # the scan-only fast path has a newer non-cancel assistant boundary.
        kinds.append("cancelled")
        outputs.append([])
    streams = _persist_multi_retry_turns(sid, kinds, outputs)
    first = models.get_session(sid)
    assert _stream_output(first, streams[1])
    initial_context = copy.deepcopy(first.context_messages)
    for _ in range(3):
        if not cache_hits:
            models.SESSIONS.clear()
        recovered = models.get_session(sid)
        assert not _stream_output(recovered, streams[0])
        assert _pending_stream_hook(recovered, streams[0])["_journal_retry_attempts"] == 0
        assert recovered.context_messages == initial_context
        _assert_retry_turn_ownership(recovered, kinds, streams)
    from api.streaming import _sanitize_messages_for_agent
    history = _sanitize_messages_for_agent(models.reconciled_state_db_messages_for_session(
        recovered, prefer_context=True, state_messages=[],
    ))
    assert not any(row.get("content") == "Old interrupted answer" for row in history)


@pytest.mark.parametrize("cache_hits", [False, True], ids=["cold", "cached"])
@pytest.mark.parametrize("context_shape", ["empty", "compressed"])
@pytest.mark.parametrize("equal_prose", [False, True])
def test_unproven_older_interrupted_recovery_stays_display_only(
    cache_hits, context_shape, equal_prose,
):
    sid = f"round5-display-only-{cache_hits}-{context_shape}-{equal_prose}"
    old_text = "Repeated answer" if equal_prose else "Old display-only answer"
    new_text = "Repeated answer" if equal_prose else "New cancelled answer"
    kinds = ["interrupted", "cancelled"]
    streams = _persist_multi_retry_turns(sid, kinds, [
        [("token", {"text": old_text}),
         ("tool", {"name": "read_file", "tid": "old-display-tool", "args": {"path": "old.txt"}})],
        [("token", {"text": new_text}),
         ("tool", {"name": "read_file", "tid": "new-display-tool", "args": {"path": "new.txt"}})],
    ])
    first = models.get_session(sid)
    assert _stream_output(first, streams[1])
    if context_shape == "empty":
        first.context_messages = []
    elif context_shape == "compressed":
        first.context_messages = [{"role": "system", "content": "Context compression: newer work only."}]
    first.save()
    if not cache_hits:
        models.SESSIONS.clear()
    recovered = models.get_session(sid)
    assert [row["content"] for row in _stream_output(recovered, streams[0]) if row.get("content")] == [old_text]
    assert _pending_stream_hook(recovered, streams[0]) is None
    _assert_retry_turn_ownership(recovered, kinds, streams)
    from api.streaming import _sanitize_messages_for_agent, _sanitize_messages_for_api, _api_safe_message_positions
    for _ in range(3):
        if not cache_hits:
            models.SESSIONS.clear()
        recovered = models.get_session(sid)
        _assert_retry_turn_ownership(recovered, kinds, streams)
        # Exercise the real next-send reconciliation, as well as raw display
        # fallbacks used by replay/compression and empty-context seeding.
        inputs = [models.reconciled_state_db_messages_for_session(
            recovered, prefer_context=True, state_messages=[],
        ), recovered.messages]
        for rows in inputs:
            histories = [_sanitize_messages_for_agent(rows), _sanitize_messages_for_api(rows),
                         [row for _, row in _api_safe_message_positions(rows)]]
            for history in histories:
                expected = int(equal_prose and (context_shape != "compressed" or rows is recovered.messages))
                assert sum(row.get("content") == old_text for row in history) == expected
        seeded = []
        models._seed_recovered_context_from_messages(recovered, seeded)
        assert sum(row.get("content") == old_text for row in seeded) == (1 if equal_prose else 0)


def test_latest_interrupted_recovery_still_feeds_provider_context():
    sid = "round5-latest-interrupted-context"
    streams = _persist_multi_retry_turns(sid, ["interrupted"], [[("token", {"text": "Current interrupted answer"})]])
    recovered = models.get_session(sid)
    assert [row["content"] for row in _stream_output(recovered, streams[0])] == ["Current interrupted answer"]
    from api.streaming import _sanitize_messages_for_agent
    history = _sanitize_messages_for_agent(models.reconciled_state_db_messages_for_session(
        recovered, prefer_context=True, state_messages=[],
    ))
    assert [(row["role"], row["content"]) for row in history] == [
        ("user", "Prompt 0"), ("assistant", "Current interrupted answer"),
    ]


@pytest.mark.parametrize("tag_value", [None, False, "true"])
def test_unproven_cancel_recovery_rows_keep_interrupted_scan_boundary(tag_value):
    sid = f"round5-unproven-cancel-{tag_value}"
    kinds = ["interrupted", "cancelled"]
    streams = _persist_multi_retry_turns(sid, kinds, [
        [("token", {"text": "Old interrupted answer"})],
        [("token", {"text": "New cancellation answer"})],
    ])
    recovered = models.get_session(sid)
    for row in _stream_output(recovered, streams[1]):
        if tag_value is None:
            row.pop("_recovered_from_cancel_journal", None)
        else:
            row["_recovered_from_cancel_journal"] = tag_value
    recovered.save()
    original_context = copy.deepcopy(recovered.context_messages)
    for _ in range(2):
        models.SESSIONS.clear()
        recovered = models.get_session(sid)
        assert not _stream_output(recovered, streams[0])
        assert recovered.context_messages == original_context
        assert _pending_stream_hook(recovered, streams[0])["_journal_retry_attempts"] == 0


def _next_send_history(session):
    from api.streaming import (
        _new_turn_context_from_messages, _dedupe_replayed_context_messages,
        _dedupe_replayed_active_context, _sanitize_messages_for_agent,
    )
    prompt = "Continue the next task"
    reconciled = models.reconciled_state_db_messages_for_session(
        session, prefer_context=True, state_messages=[],
    )
    history = _new_turn_context_from_messages(reconciled, prompt)
    history = _dedupe_replayed_context_messages(history, history, prompt)
    history = _dedupe_replayed_active_context(history, history, prompt)
    return _sanitize_messages_for_agent(history)


@pytest.mark.parametrize("cache_hits", [False, True], ids=["cold", "cached"])
@pytest.mark.parametrize("new_output", [False, True], ids=["empty-stop", "output-stop"])
@pytest.mark.parametrize("equal_prose", [False, True])
@pytest.mark.parametrize("native_pair", [False, True])
def test_proven_older_interrupted_answer_survives_real_stop_next_send(
    cache_hits, new_output, equal_prose, native_pair,
):
    sid = f"proven-old-context-{cache_hits}-{new_output}-{equal_prose}-{native_pair}"
    old_text = "Repeated answer" if equal_prose else "Old answer"
    new_text = "Repeated answer" if equal_prose else "New answer"
    kinds = ["interrupted", "cancelled"]
    streams = _persist_multi_retry_turns(sid, kinds, [
        [("token", {"text": old_text}),
         ("tool", {"name": "read_file", "tid": "old-owned-tool", "args": {"path": "old.txt"}})],
        [("token", {"text": new_text})] if new_output else [],
    ], defer_first=True)
    first = models.get_session(sid)
    assert not _stream_output(first, streams[0])
    pair = [
        {"role": "assistant", "content": "", "tool_calls": [{"id": "native-call", "type": "function", "function": {"name": "read_file", "arguments": '{"path":"prior.txt"}'}}]},
        {"role": "tool", "content": "Native tool result", "tool_call_id": "native-call"},
    ] if native_pair else []
    first.context_messages[1:1] = copy.deepcopy(pair)
    first.save()
    writer = RunJournalWriter(sid, streams[0])
    writer.append_sse_event("token", {"text": old_text})
    writer.append_sse_event("tool", {"name": "read_file", "tid": "old-owned-tool", "args": {"path": "old.txt"}})
    writer.append_sse_event("cancel", {"message": "Old terminal journal arrives late"})
    expected = [("user", "Prompt 0")] + [(row["role"], row["content"]) for row in pair] + [("assistant", old_text)]
    if new_output:
        expected += [("user", "Prompt 1"), ("assistant", new_text)]
    for _ in range(3):
        if not cache_hits:
            models.SESSIONS.clear()
        recovered = models.get_session(sid)
        history = _next_send_history(recovered)
        assert [(row["role"], row.get("content", "")) for row in history] == expected
        if native_pair:
            assert history[1]["tool_calls"] == pair[0]["tool_calls"]
            assert history[2]["tool_call_id"] == "native-call"
        assert _pending_stream_hook(recovered, streams[0]) is None
        assert not any(row.get("_recovered_display_only") for row in _stream_output(recovered, streams[0]))
        _assert_retry_turn_ownership(recovered, kinds, streams)


@pytest.mark.parametrize("failure", ["duplicate-owner", "source", "timestamp", "owner-token", "tokenless-successor", "foreign-successor-token", "duplicate-successor-token"])
def test_ambiguous_interrupted_context_proof_stays_display_only(failure):
    sid = f"ambiguous-old-context-{failure}"
    streams = _persist_multi_retry_turns(sid, ["interrupted", "cancelled"], [
        [("token", {"text": "Old ambiguous answer"})],
        [("token", {"text": "New answer"})],
    ])
    first = models.get_session(sid)
    assert not _stream_output(first, streams[0])
    owner = first.context_messages[0]
    successor = next(row for row in first.context_messages if row.get("role") == "user" and row.get("content") == "Prompt 1")
    if failure == "duplicate-owner":
        first.context_messages.insert(1, copy.deepcopy(owner))
    elif failure == "source":
        owner["_source"] = "telegram"
    elif failure == "timestamp":
        owner["timestamp"] += 0.25
    elif failure == "owner-token":
        owner["_active_turn_token"] = "foreign-owner"
    elif failure == "tokenless-successor":
        successor.pop("_active_turn_token", None)
    elif failure == "foreign-successor-token":
        successor["_active_turn_token"] = "foreign-successor"
    else:
        first.context_messages.append(copy.deepcopy(successor))
    first.save()
    before = copy.deepcopy(first.context_messages)
    for _ in range(3):
        models.SESSIONS.clear()
        recovered = models.get_session(sid)
        assert recovered.context_messages == before
        assert any(row.get("_recovered_display_only") is True for row in _stream_output(recovered, streams[0]))
        assert not any(row.get("content") == "Old ambiguous answer" for row in _next_send_history(recovered))
        from api.streaming import _sanitize_messages_for_agent
        assert not any(row.get("content") == "Old ambiguous answer" for row in _sanitize_messages_for_agent(recovered.messages))


def test_proven_interrupted_context_save_failure_retains_hook(monkeypatch):
    sid = "proven-old-context-save-failure"
    streams = _persist_multi_retry_turns(sid, ["interrupted", "cancelled"], [
        [("token", {"text": "Old answer"})], [("token", {"text": "New answer"})],
    ])
    session = models.get_session(sid)
    assert _pending_stream_hook(session, streams[0]) is not None
    before = copy.deepcopy((session.messages, session.context_messages, session.tool_calls, session.updated_at))
    original_save = Session.save
    monkeypatch.setattr(Session, "save", Mock(side_effect=OSError("fixture save failure")))
    assert models._retry_journal_recovery_in_place(session) is False
    assert session.save.called, session.messages
    assert (session.messages, session.context_messages, session.tool_calls, session.updated_at) == before
    monkeypatch.setattr(Session, "save", original_save)
    assert models._retry_journal_recovery_in_place(session) is True, session.messages
    models.SESSIONS.clear()
    recovered = models.get_session(sid)
    assert [(row["role"], row["content"]) for row in _next_send_history(recovered)] == [
        ("user", "Prompt 0"), ("assistant", "Old answer"), ("user", "Prompt 1"), ("assistant", "New answer"),
    ]
