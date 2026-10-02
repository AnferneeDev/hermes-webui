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
start.ps1 itself is copied into the fixture too, because its repo root and the
../hermes-agent sibling it searches are derived from its own location — running
the checked-out script would point that candidate at the real Agent checkout
next to the clone.
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
STATE_DIR_PREFIX = "[start.ps1] State dir:  "


def _write_stub_python(fixture: Path) -> Path:
    """An interpreter stand-in that exits 0 without starting server.py."""
    stub = fixture / "stub-python.sh"
    stub.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    stub.chmod(0o755)
    return stub


def _write_env_probe_python(fixture: Path) -> Path:
    """A stub that reports the environment start.ps1 exported to its child.

    The exported HERMES_HOME is not one of the lines start.ps1 prints, and it
    is the value that decides where api/config.py reads providers and models
    from, so it needs to be observable from the test.
    """
    probe = fixture / "probe-python.sh"
    probe.write_text(
        '#!/bin/sh\n'
        'echo "probe HERMES_HOME=$HERMES_HOME"\n'
        'echo "probe HERMES_WEBUI_STATE_DIR=$HERMES_WEBUI_STATE_DIR"\n'
        'exit 0\n',
        encoding="utf-8",
    )
    probe.chmod(0o755)
    return probe


def _make_agent(root: Path, *, source: bool = False, venv: bool = False,
                bootstrap: bool = False) -> Path:
    """Build a hermes-agent-shaped directory.

    `source=True` means a bare source checkout: run_agent.py and no hermes_cli,
    which is what makes it eligible for the source-first pass. The default is
    the pip-style shape instead — hermes_cli and no run_agent.py — because that
    is what an installed Agent looks like, and keeping the two shapes disjoint
    is what lets the source-first pass be exercised at all.
    `venv` adds the Windows venv path start.ps1 looks for; `bootstrap` adds the
    file managed_agent_startup.activate_managed_agent() imports to supply deps.
    """
    root.mkdir(parents=True, exist_ok=True)
    if source:
        (root / "run_agent.py").write_text("", encoding="utf-8")
    else:
        (root / "hermes_cli").mkdir(parents=True, exist_ok=True)
        (root / "hermes_cli" / "__init__.py").write_text("", encoding="utf-8")
    if venv:
        venv_python = root / "venv" / "Scripts" / "python.exe"
        venv_python.parent.mkdir(parents=True, exist_ok=True)
        venv_python.write_text("", encoding="utf-8")
    if bootstrap:
        (root / "hermes_bootstrap.py").write_text("", encoding="utf-8")
    return root


def _stage_repo(fixture: Path) -> Path:
    """Copy start.ps1 into an isolated repo root inside the fixture.

    start.ps1 derives $RepoRoot from its own path, and one of the layouts it
    searches is that root's sibling, ../hermes-agent. Running the checked-out
    script therefore points that candidate at the real checkout next to the
    clone — which, for anyone using the documented side-by-side layout
    (hermes-webui/ next to hermes-agent/), is their actual Agent working tree.
    Copying the script keeps the repo root, the sibling candidate and every
    Agent inside tmp_path, so a fixture can create and remove an Agent of its
    own without ever reaching the developer's tree.
    """
    repo = fixture / "repo"
    repo.mkdir(parents=True, exist_ok=True)
    shutil.copy2(START_PS1, repo / "start.ps1")
    # start.ps1 refuses to launch when server.py is absent from its own root.
    (repo / "server.py").write_text("", encoding="utf-8")
    return repo


def _run_start_ps1(fixture: Path, extra_env: dict[str, str] | None = None) -> tuple[int, str]:
    """Execute start.ps1 against a disposable fixture tree."""
    start_ps1 = _stage_repo(fixture) / "start.ps1"
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(fixture / "user"),
        "USERPROFILE": str(fixture / "user"),
        "LOCALAPPDATA": str(fixture / "local"),
        "HERMES_WEBUI_PYTHON": str(_write_stub_python(fixture)),
    }
    env.update(extra_env or {})
    completed = subprocess.run(
        [shutil.which("pwsh"), "-NoProfile", "-File", str(start_ps1)],
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


def _state_dir(output: str) -> str:
    for line in output.splitlines():
        if line.startswith(STATE_DIR_PREFIX):
            return line[len(STATE_DIR_PREFIX):].strip()
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
    # The sibling of the staged repo root, i.e. inside the fixture. Never
    # REPO_ROOT.parent: that is the real checkout for a side-by-side developer.
    sibling = _make_agent(fixture / "hermes-agent", source=True)

    code, output = _run_start_ps1(fixture)

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
    sibling = _make_agent(fixture / "hermes-agent", source=True, bootstrap=True)

    code, output = _run_start_ps1(fixture)

    assert _agent_dir(output) == str(sibling), output
    assert _python(output) == str(_write_stub_python(fixture)), (
        "hermes_bootstrap.py activates the checkout's dependencies, so the venv "
        "fallback must stay out of the way; got:\n" + output
    )
    assert code == 0, output


def test_explicit_state_dir_keeps_the_legacy_agent_ahead_of_home(tmp_path):
    """Candidate 5 must be the SERVER's default home, not HOME\\hermes-agent.

    With the WebUI state still at the legacy %USERPROFILE%\\.hermes, the server
    defaults its own home there too, so %USERPROFILE%\\.hermes\\hermes-agent is
    the install it would use. %USERPROFILE%\\hermes-agent is a flat checkout the
    server never searches, so when both exist the launcher has to reach the
    legacy one first or it exports a different Agent than the server defaults
    to.
    """
    fixture = tmp_path / "fx"
    (fixture / "user" / ".hermes" / "webui").mkdir(parents=True)
    legacy = _make_agent(fixture / "user" / ".hermes" / "hermes-agent")
    _make_agent(fixture / "user" / "hermes-agent")

    code, output = _run_start_ps1(
        fixture, extra_env={"HERMES_WEBUI_STATE_DIR": str(fixture / "state")}
    )

    assert _agent_dir(output) == str(legacy), (
        "the server's own default home must be searched before the flat "
        "HOME\\hermes-agent checkout; got:\n" + output
    )
    assert code == 0, output


def test_legacy_webui_state_redirects_only_the_state_dir(tmp_path):
    """A legacy state dir must not drag the working config home along with it.

    api/config.py reads providers and models from HERMES_HOME, so a user whose
    config.yaml sits in %LOCALAPPDATA%\\hermes has to keep reading it there.
    Only the webui/ state location is affected by the #2905 migration.
    """
    fixture = tmp_path / "fx"
    legacy_state = fixture / "user" / ".hermes" / "webui"
    legacy_state.mkdir(parents=True)
    new_home = fixture / "local" / "hermes"
    new_home.mkdir(parents=True)
    (new_home / "config.yaml").write_text("provider: local\n", encoding="utf-8")
    agent = _make_agent(new_home / "hermes-agent")
    probe = _write_env_probe_python(fixture)

    code, output = _run_start_ps1(
        fixture, extra_env={"HERMES_WEBUI_PYTHON": str(probe)}
    )

    assert _agent_dir(output) == str(agent), (
        "the new home's Agent must still be found in a legacy-state layout; "
        "got:\n" + output
    )
    assert _state_dir(output) == str(legacy_state), (
        "the sessions still live in the legacy webui directory, so the state "
        "default has to follow them there; got:\n" + output
    )
    assert f"probe HERMES_HOME={new_home}" in output, (
        "HERMES_HOME is where api/config.py reads providers and models from, so "
        "it must stay on the platform default that holds config.yaml; got:\n"
        + output
    )
    assert code == 0, output