"""start.ps1 must survive a real launch, not just a parse.

The static pwsh-block assertions elsewhere in this suite read start.ps1 as
text. They cannot see the launch blockers that only exist when PowerShell
actually runs the script:

- `Test-HermesWebuiState` declared `param([string]$Home)`. PowerShell variable
  names are case-insensitive, so that collided with the read-only automatic
  `$HOME`, and under `$ErrorActionPreference = 'Stop'` the first call aborted
  the script before any Agent discovery. Every native launch died, in every
  layout, with "Cannot overwrite variable Home because it is read-only".
- Gating the platform-default Agent candidate on the #2905 legacy WebUI-state
  preference dropped a real install: with WebUI state still at the legacy
  `%USERPROFILE%\\.hermes` and the only Agent at `%LOCALAPPDATA%\\hermes`, the
  LOCALAPPDATA path never entered the candidate list and startup failed with
  "hermes-agent not found".
- The source-first pass (mirroring api/config.py) can select a bare sibling
  checkout that has no venv and no hermes_bootstrap.py, which is an Agent that
  cannot supply its own dependencies.

So these tests execute the script under a real pwsh with disposable fixtures and
assert on the launcher output. They skip when pwsh is unavailable, and on
Windows, where .github/workflows/native-windows-startup.yml already runs
start.ps1 for real on windows-latest.

All state is confined to tmp_path: USERPROFILE, LOCALAPPDATA and HERMES_HOME
point inside the fixture, and HERMES_WEBUI_PYTHON is a stub that exits
immediately, so no server is started and no real install is touched.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
START_PS1 = REPO_ROOT / "start.ps1"

pytestmark = [
    pytest.mark.skipif(
        os.name == "nt",
        reason="native-windows-startup.yml executes start.ps1 on windows-latest",
    ),
    pytest.mark.skipif(
        shutil.which("pwsh") is None,
        reason="needs a real pwsh to execute start.ps1 (PowerShell 7 on Linux)",
    ),
]

# start.ps1 reports these two lines before it invokes the interpreter, so they
# are the observable result of discovery and Python selection.
AGENT_DIR_PREFIX = "[start.ps1] Agent dir:  "
PYTHON_PREFIX = "[start.ps1] Python:     "


def _write_stub_python(fixture: Path) -> Path:
    """An interpreter stand-in that exits 0 without starting server.py."""
    stub = fixture / "stub-python.sh"
    stub.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    stub.chmod(0o755)
    return stub


def _make_agent(root: Path, *, source: bool = False, venv: bool = False,
                bootstrap: bool = False) -> Path:
    """Build a hermes-agent-shaped directory.

    `source=True` means a bare source checkout: run_agent.py and no hermes_cli,
    which is what makes it eligible for the source-first pass. `venv` adds the
    Windows venv path start.ps1 looks for; `bootstrap` adds the file
    managed_agent_startup.activate_managed_agent() imports to supply deps.
    """
    root.mkdir(parents=True, exist_ok=True)
    (root / "run_agent.py").write_text("", encoding="utf-8")
    if not source:
        (root / "hermes_cli").mkdir(parents=True, exist_ok=True)
        (root / "hermes_cli" / "__init__.py").write_text("", encoding="utf-8")
    if venv:
        venv_python = root / "venv" / "Scripts" / "python.exe"
        venv_python.parent.mkdir(parents=True, exist_ok=True)
        venv_python.write_text("", encoding="utf-8")
    if bootstrap:
        (root / "hermes_bootstrap.py").write_text("", encoding="utf-8")
    return root


def _run_start_ps1(fixture: Path, extra_env: dict[str, str] | None = None) -> tuple[int, str]:
    """Execute start.ps1 against a disposable fixture tree."""
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(fixture / "user"),
        "USERPROFILE": str(fixture / "user"),
        "LOCALAPPDATA": str(fixture / "local"),
        "HERMES_WEBUI_PYTHON": str(_write_stub_python(fixture)),
    }
    env.update(extra_env or {})
    completed = subprocess.run(
        [shutil.which("pwsh"), "-NoProfile", "-File", str(START_PS1)],
        capture_output=True,
        text=True,
        env=env,
        timeout=180,
    )
    return completed.returncode, completed.stdout + completed.stderr


def _agent_dir(output: str) -> str:
    for line in output.splitlines():
        if line.startswith(AGENT_DIR_PREFIX):
            return line[len(AGENT_DIR_PREFIX):].strip()
    return ""


def _python(output: str) -> str:
    for line in output.splitlines():
        if line.startswith(PYTHON_PREFIX):
            return line[len(PYTHON_PREFIX):].strip()
    return ""


def test_standard_layout_reaches_agent_discovery(tmp_path):
    """The $Home/read-only-$HOME collision killed this before discovery."""
    fixture = tmp_path / "fx"
    agent = _make_agent(fixture / "local" / "hermes" / "hermes-agent")
    (fixture / "state").mkdir(parents=True)

    code, output = _run_start_ps1(fixture)

    assert "read-only or constant" not in output, (
        "start.ps1 aborted on the read-only automatic $HOME; the "
        "Test-HermesWebuiState parameter must not be named $Home"
    )
    assert _agent_dir(output) == str(agent), (
        "the standard layout must resolve the LOCALAPPDATA Agent; got:\n" + output
    )
    assert code == 0, output


def test_legacy_webui_state_does_not_hide_the_localappdata_agent(tmp_path):
    """WebUI state in the legacy home must not move where the Agent is searched.

    This is the upgrade shape from #2905: the session data has not moved yet
    but the Agent was installed at the new location. The #2905 preference is
    about where HERMES_HOME points; it must not decide where the Agent is
    looked for, or the only install on the machine becomes unreachable.
    """
    fixture = tmp_path / "fx"
    (fixture / "user" / ".hermes" / "webui").mkdir(parents=True)
    agent = _make_agent(fixture / "local" / "hermes" / "hermes-agent")

    code, output = _run_start_ps1(fixture)

    assert _agent_dir(output) == str(agent), (
        "a populated legacy WebUI home must not push the LOCALAPPDATA Agent out "
        "of the candidate list; got:\n" + output
    )
    assert code == 0, output


def test_legacy_only_agent_is_still_found(tmp_path):
    """The legacy Agent stays reachable now that candidate 5 is the new home."""
    fixture = tmp_path / "fx"
    (fixture / "user" / ".hermes" / "webui").mkdir(parents=True)
    agent = _make_agent(fixture / "user" / ".hermes" / "hermes-agent")

    code, output = _run_start_ps1(fixture)

    assert _agent_dir(output) == str(agent), (
        "an Agent that only exists at the legacy location is still a valid "
        "install; got:\n" + output
    )
    assert code == 0, output


def test_explicit_agent_dir_override_wins(tmp_path):
    fixture = tmp_path / "fx"
    agent = _make_agent(fixture / "elsewhere" / "hermes-agent")

    env_agent = str(agent)
    code, output = _run_start_ps1(fixture, extra_env={"HERMES_WEBUI_AGENT_DIR": env_agent})

    assert _agent_dir(output) == env_agent, (
        "HERMES_WEBUI_AGENT_DIR is the documented override and must not be "
        "second-guessed; got:\n" + output
    )
    assert code == 0, output


def test_bare_source_checkout_falls_back_to_a_candidate_venv(tmp_path):
    """A source checkout with no venv and no bootstrap needs the deps from elsewhere.

    The source-first pass reaches a sibling checkout before the platform
    default. Selecting it is right (it is what api/config.py does, and the
    export makes the server agree), but on its own it cannot import the Agent's
    dependencies: activate_managed_agent() no-ops without hermes_bootstrap.py.
    """
    fixture = tmp_path / "fx"
    (fixture / "user" / ".hermes" / "webui").mkdir(parents=True)
    installed = _make_agent(
        fixture / "local" / "hermes" / "hermes-agent", venv=True
    )
    sibling = _make_agent(REPO_ROOT.parent / "hermes-agent", source=True)

    try:
        code, output = _run_start_ps1(fixture)
    finally:
        shutil.rmtree(sibling, ignore_errors=True)

    expected = installed / "venv" / "Scripts" / "python.exe"
    assert _python(output) == str(expected), (
        "a bare source checkout has no dependencies of its own, so the launcher "
        "must fall back to a candidate venv instead of starting a Python that "
        "cannot import them; got:\n" + output
    )
    assert "hermes_bootstrap.py" in output, (
        "the fallback is a fallback: it should say which Agent it is covering\n"
        + output
    )
    assert _agent_dir(output) == str(sibling), (
        "the discovered Agent should stay the source checkout so the exported "
        "HERMES_WEBUI_AGENT_DIR matches api/config.py; got:\n" + output
    )
    assert code == 0, output


def test_source_checkout_with_bootstrap_keeps_its_own_interpreter(tmp_path):
    """The fallback must not fire when the checkout can supply its own deps."""
    fixture = tmp_path / "fx"
    (fixture / "user" / ".hermes" / "webui").mkdir(parents=True)
    _make_agent(fixture / "local" / "hermes" / "hermes-agent", venv=True)
    sibling = _make_agent(
        REPO_ROOT.parent / "hermes-agent", source=True, bootstrap=True
    )

    try:
        code, output = _run_start_ps1(fixture)
    finally:
        shutil.rmtree(sibling, ignore_errors=True)

    assert _agent_dir(output) == str(sibling), output
    assert _python(output) == str(_write_stub_python(fixture)), (
        "hermes_bootstrap.py activates the checkout's dependencies, so the venv "
        "fallback must stay out of the way; got:\n" + output
    )
    assert code == 0, output