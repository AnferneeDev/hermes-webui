"""start.ps1 must hand its discovered hermes-agent dir to the child process.

server.py calls activate_managed_agent() (managed_agent_startup.py) as its
first Agent-related import, and that hook reads HERMES_WEBUI_AGENT_DIR only.
start.ps1 bypasses bootstrap.py - the component that normally performs this
export - so when the launcher keeps the discovered directory in a PowerShell
variable, dependency activation silently no-ops and the next Agent import
crashes on a module the managed environment already provides.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
START_PS1 = REPO_ROOT / "start.ps1"


def _start_ps1_source() -> str:
    return START_PS1.read_text(encoding="utf-8")


def _export_statement_line(source: str) -> int:
    """1-based line of the HERMES_WEBUI_AGENT_DIR export, or -1 when absent."""
    pattern = re.compile(
        r"^\s*(?:\$env:HERMES_WEBUI_AGENT_DIR\s*=|"
        r"\[Environment\]::SetEnvironmentVariable\(\s*'HERMES_WEBUI_AGENT_DIR')",
        re.MULTILINE,
    )
    match = pattern.search(source)
    if match is None:
        return -1
    return source[: match.start()].count("\n") + 1


def test_start_ps1_exports_the_discovered_agent_dir():
    assert _export_statement_line(_start_ps1_source()) != -1, (
        "start.ps1 must export HERMES_WEBUI_AGENT_DIR before launching server.py; "
        "without it managed_agent_startup.activate_managed_agent() no-ops"
    )


def test_agent_dir_export_precedes_the_server_launch():
    source = _start_ps1_source()
    export_line = _export_statement_line(source)
    launch_line = source[: source.index("& $Python $serverPath")].count("\n") + 1
    assert export_line != -1
    assert export_line < launch_line


def test_agent_dir_export_precedes_the_agent_venv_python_override():
    """The exported value must be the discovered dir, not the venv-derived one.

    $Python is re-pointed at <agent>/venv/Scripts/python.exe further down; if
    the export trailed that block it could pick up the venv path instead of the
    Agent root that activate_managed_agent() validates.
    """
    source = _start_ps1_source()
    export_line = _export_statement_line(source)
    venv_line = source[: source.index("$agentVenvPython = Join-Path")].count("\n") + 1
    assert export_line != -1
    assert export_line < venv_line


def test_agent_dir_export_precedes_the_startup_banner():
    source = _start_ps1_source()
    export_line = _export_statement_line(source)
    banner_line = source[: source.index('Write-Host "[start.ps1] Hermes WebUI')].count(
        "\n"
    ) + 1
    assert export_line != -1
    assert export_line < banner_line


def test_bootstrap_owns_the_same_export_for_the_posix_launcher():
    """Guards the pairing this fix restores: bootstrap.py exports it for start.sh."""
    bootstrap = (REPO_ROOT / "bootstrap.py").read_text(encoding="utf-8")
    assert 'os.environ["HERMES_WEBUI_AGENT_DIR"]' in bootstrap



def test_hermes_home_default_precedes_agent_discovery():
    """HERMES_HOME must be defaulted before the candidate list is built.

    Exporting the discovery result makes start.ps1's order win over the
    server's _discover_agent_dir. Resolving HERMES_HOME first (matching
    api/config.py) keeps %LOCALAPPDATA%\\hermes ahead of a stale
    %USERPROFILE%\\.hermes install.
    """
    source = _start_ps1_source()
    home_default = source.index("if (-not $env:HERMES_HOME)")
    discovery = source.index("$AgentDir = $env:HERMES_WEBUI_AGENT_DIR")
    assert home_default < discovery, (
        "HERMES_HOME default must be resolved before agent discovery so the "
        "exported HERMES_WEBUI_AGENT_DIR matches api/config.py"
    )


def test_hermes_home_agent_candidate_is_listed_first():
    """First auto-discovery candidate must be $HERMES_HOME\\hermes-agent."""
    source = _start_ps1_source()
    # Narrow to the auto-discovery block (between empty-AgentDir check and export).
    block_start = source.index("if (-not $AgentDir)")
    block_end = source.index(
        "[Environment]::SetEnvironmentVariable('HERMES_WEBUI_AGENT_DIR'"
    )
    block = source[block_start:block_end]
    first_append = re.search(
        r"\$candidates\s*\+=\s*\(Join-Path\s+\$env:HERMES_HOME\s+'hermes-agent'\)",
        block,
    )
    assert first_append is not None, (
        "auto-discovery must Join-Path $env:HERMES_HOME 'hermes-agent' as a candidate"
    )
    earlier = re.search(r"\$candidates\s*\+=", block)
    assert earlier is not None
    assert earlier.start() == first_append.start(), (
        "Join-Path $env:HERMES_HOME 'hermes-agent' must be the first candidate append; "
        f"found earlier append at offset {earlier.start()} vs HERMES_HOME at "
        f"{first_append.start()}"
    )


def _discovery_block(source: str) -> str:
    """Auto-discovery block between empty-AgentDir check and the export."""
    block_start = source.index("if (-not $AgentDir)")
    block_end = source.index(
        "[Environment]::SetEnvironmentVariable('HERMES_WEBUI_AGENT_DIR'"
    )
    return source[block_start:block_end]


def test_discovery_candidate_order_matches_server():
    """Candidate appends must follow api/config.py through HOME/hermes-agent."""
    block = _discovery_block(_start_ps1_source())
    appends = [
        m.group(0)
        for m in re.finditer(r"\$candidates\s*\+=\s*[^\n]+", block)
    ]
    assert len(appends) >= 4, f"expected >=4 candidate appends, got {appends!r}"
    assert "Join-Path $env:HERMES_HOME 'hermes-agent'" in appends[0]
    assert "Join-Path $repoParent 'hermes-agent'" in appends[1]
    # After sibling/parent: platform-default home, then HOME\hermes-agent
    platform_idx = next(
        i
        for i, a in enumerate(appends)
        if "Join-Path $platformDefaultHermesHome 'hermes-agent'" in a
    )
    home_idx = next(
        i
        for i, a in enumerate(appends)
        if re.search(
            r"Join-Path\s+\$env:USERPROFILE\s+'hermes-agent'",
            a,
        )
    )
    sibling_idx = next(
        i for i, a in enumerate(appends) if "Join-Path $repoParent 'hermes-agent'" in a
    )
    assert sibling_idx < platform_idx < home_idx, (
        f"order must be sibling → platform-default → HOME/hermes-agent "
        f"(sibling@{sibling_idx}, platform@{platform_idx}, home@{home_idx})"
    )
    # Must not unconditionally prepend USERPROFILE\.hermes among fallbacks
    unconditional_legacy = [
        a
        for a in appends
        if "USERPROFILE" in a and ".hermes" in a and "hermes-agent" in a
    ]
    assert not unconditional_legacy, (
        "USERPROFILE/.hermes/hermes-agent must not be appended unconditionally; "
        "it belongs only inside the #2905 platform-default rule. "
        f"Found: {unconditional_legacy!r}"
    )


def test_discovery_uses_two_pass_run_agent_then_hermes_cli():
    """First pass prefers run_agent.py; only then accept hermes_cli (pip-style)."""
    block = _discovery_block(_start_ps1_source())
    run_agent = block.index("run_agent.py")
    hermes_cli_pass = block.index(
        "Test-Path (Join-Path $c 'hermes_cli') -PathType Container"
    )
    assert run_agent < hermes_cli_pass, (
        "run_agent.py pass must precede the hermes_cli pass so source checkouts win"
    )
    after_run = block[run_agent:]
    assert "if (-not $AgentDir)" in after_run, (
        "second pass must be gated on AgentDir still being empty after run_agent.py"
    )


def test_platform_default_home_uses_localappdata_when_established():
    """Case 1: custom HERMES_HOME empty + Agents in both legacy and LOCALAPPDATA.

    After sibling/parent, the platform-default candidate must prefer
    LOCALAPPDATA\\hermes (when established) per api.paths._platform_default_hermes_home,
    not an unconditional USERPROFILE\\.hermes first among fallbacks. Legacy is
    only chosen when it still holds WebUI state and the new location does not.
    """
    source = _start_ps1_source()
    assert "$platformDefaultHermesHome = Join-Path $env:LOCALAPPDATA 'hermes'" in source
    assert "$legacyHermesHome = Join-Path $env:USERPROFILE '.hermes'" in source
    assert "if (-not $newHasWebuiState -and $legacyHasWebuiState)" in source
    assert "$platformDefaultHermesHome = $legacyHermesHome" in source
    block = _discovery_block(source)
    assert "Join-Path $platformDefaultHermesHome 'hermes-agent'" in block
    # Unconditional legacy append must be gone from the discovery block
    assert "Join-Path $env:USERPROFILE '.hermes\\hermes-agent'" not in block


def test_home_hermes_agent_precedes_program_files_roots():
    """Case 2: HOME/hermes-agent must beat launcher-only Program Files roots.

    Server candidate #6 is HOME/hermes-agent; Program Files is launcher-only
    and must be searched last so it cannot preempt a HOME install the server
    would have used.
    """
    block = _discovery_block(_start_ps1_source())
    home_pos = block.index("Join-Path $env:USERPROFILE 'hermes-agent'")
    # ${env:ProgramFiles(x86)} nests parens, so match on ProgramW6432 marker.
    pf_pos = block.index("${env:ProgramW6432}")
    assert home_pos < pf_pos, (
        "HOME/hermes-agent must be appended before Program Files roots "
        f"(home@{home_pos}, pf@{pf_pos})"
    )
    # LOCALAPPDATA must not ride along in the Program Files loop anymore
    assert "@($env:LOCALAPPDATA," not in block
    assert "$env:LOCALAPPDATA, ${env:ProgramW6432}" not in block
