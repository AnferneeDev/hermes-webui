<#
.SYNOPSIS
    Native Windows launcher for Hermes WebUI - PowerShell equivalent
    of start.sh, bypassing bootstrap.py's platform refusal.

.DESCRIPTION
    Mirrors start.sh's discovery: load optional .env, find Python,
    locate the hermes-agent install, set sensible env defaults, then
    invoke server.py directly. The bootstrap.py path is skipped
    because it currently raises on platform.system() == 'Windows';
    server.py itself runs cleanly on native Windows.

    Assumes Python + hermes-agent + the WebUI Python deps are already
    installed natively on Windows - same assumption start.sh makes
    when invoked outside a fresh bootstrap. For first-time setup, the
    native Windows path is to install Python 3.11+, then create a
    Windows venv (`python -m venv venv`) and `pip install -r
    requirements.txt` from the hermes-agent root in PowerShell - this
    script then finds `venv\Scripts\python.exe` automatically. A venv
    created inside WSL2 is a Linux virtual environment (`venv/bin/python`)
    and cannot be used by native Windows Python, so the bootstrap.py-
    inside-WSL2 path produces a venv `start.ps1` can't invoke.

.PARAMETER Port
    TCP port the WebUI binds to. Overrides HERMES_WEBUI_PORT env.
    Default: 8787.

.PARAMETER BindHost
    Bind address. Overrides HERMES_WEBUI_HOST env.
    Default: 127.0.0.1.

.EXAMPLE
    .\start.ps1
    # Bind to 127.0.0.1:8787, foreground.

.EXAMPLE
    .\start.ps1 -Port 9000
    # Bind to 127.0.0.1:9000.

.EXAMPLE
    $env:HERMES_WEBUI_HOST = '0.0.0.0'
    .\start.ps1
    # Bind to all interfaces (set a password first via env or Settings).

.LINK
    https://github.com/nesquena/hermes-webui/issues/1952
#>

[CmdletBinding()]
param(
    [int]$Port = 0,
    [string]$BindHost = ''
)

$ErrorActionPreference = 'Stop'
$RepoRoot = Split-Path -Parent $PSCommandPath

# === Load .env (mirroring start.sh's filtering) ========================
$envFile = Join-Path $RepoRoot '.env'
if (Test-Path $envFile) {
    foreach ($line in Get-Content $envFile -Encoding UTF8) {
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith('#') -or -not $trimmed.Contains('=')) { continue }
        $kv = $trimmed -split '=', 2
        $key = ($kv[0].Trim() -replace '^export\s+', '')
        # Filter out shell-readonly vars (UID, GID, EUID, EGID, PPID) per start.sh
        if ($key -in @('UID', 'GID', 'EUID', 'EGID', 'PPID')) { continue }
        if ($key -notmatch '^[A-Za-z_][A-Za-z0-9_]*$') { continue }
        # Explicit $null check — an env var explicitly set to '' should still
        # be considered "set" and NOT overridden by .env (empty string is
        # falsey in PowerShell, so a plain truthy check would mis-skip).
        if ($null -ne [Environment]::GetEnvironmentVariable($key)) { continue }
        $val = $kv[1]
        if ($val -match '^"(.*)"$') { $val = $Matches[1] }
        elseif ($val -match "^'(.*)'$") { $val = $Matches[1] }
        [Environment]::SetEnvironmentVariable($key, $val)
    }
}

# === Find Python (matches start.sh order) ==============================
$Python = $env:HERMES_WEBUI_PYTHON
if (-not $Python) {
    foreach ($candidate in @('python3', 'python', 'py')) {
        $cmd = Get-Command $candidate -ErrorAction SilentlyContinue
        if ($cmd) { $Python = $cmd.Source; break }
    }
}
if (-not $Python) {
    Write-Error 'Python 3 is required to run server.py (set HERMES_WEBUI_PYTHON or add python to PATH).'
    exit 1
}

# === Resolve platform-default Hermes home before agent discovery =======
# api/config.py's _discover_agent_dir prefers $HERMES_HOME\hermes-agent
# after the explicit HERMES_WEBUI_AGENT_DIR override, then later falls back
# to `_DEFAULT_HERMES_HOME\hermes-agent` from
# api.paths._platform_default_hermes_home(). Mirror that #2905 rule here:
# %LOCALAPPDATA%\hermes once established; %USERPROFILE%\.hermes only when
# the legacy home still holds WebUI state and the new location does not.
# Resolving before the candidate list keeps the exported discovery result
# aligned with the server. Leave an already-set HERMES_HOME alone.
#
# Explicit HERMES_WEBUI_STATE_DIR: WebUI state is already located elsewhere,
# so a leftover %USERPROFILE%\.hermes\webui must NOT yank HERMES_HOME to the
# legacy path. Master keeps %LOCALAPPDATA%\hermes in that shape; switching
# would make api/profiles.py read provider settings from the wrong home.
if ($env:LOCALAPPDATA) {
    $platformDefaultHermesHome = Join-Path $env:LOCALAPPDATA 'hermes'
    $legacyHermesHome = Join-Path $env:USERPROFILE '.hermes'
    # Migration heuristic only when state dir is NOT explicitly overridden.
    if (-not $env:HERMES_WEBUI_STATE_DIR -and $legacyHermesHome -ne $platformDefaultHermesHome) {
        $newHasWebuiState = $false
        $legacyHasWebuiState = $false
        foreach ($rel in @('webui\sessions', 'webui\settings.json', 'webui')) {
            if (-not $newHasWebuiState -and (Test-Path (Join-Path $platformDefaultHermesHome $rel))) {
                $newHasWebuiState = $true
            }
            if (-not $legacyHasWebuiState -and (Test-Path (Join-Path $legacyHermesHome $rel))) {
                $legacyHasWebuiState = $true
            }
        }
        if (-not $newHasWebuiState -and $legacyHasWebuiState) {
            $platformDefaultHermesHome = $legacyHermesHome
        }
    }
} else {
    $platformDefaultHermesHome = Join-Path $env:USERPROFILE '.hermes'
}
if (-not $env:HERMES_HOME) {
    $env:HERMES_HOME = $platformDefaultHermesHome
}

# === Find Hermes Agent dir (server.py imports from it) =================
# When HERMES_WEBUI_AGENT_DIR is set we still validate it on disk —
# an explicit override pointing at a missing dir should fail FAST
# with a clear message, not silently progress into a python3 launch
# that's about to crash on missing imports. Smoke-test feedback on
# PR #2783: nesquena/hermes-webui requested this guard.
$AgentDir = $env:HERMES_WEBUI_AGENT_DIR
if ($AgentDir -and -not (Test-Path (Join-Path $AgentDir 'hermes_cli') -PathType Container)) {
    Write-Error "HERMES_WEBUI_AGENT_DIR is set to '$AgentDir' but no hermes_cli/ folder exists there. Unset the variable to fall back to auto-discovery, or fix the path."
    exit 1
}
if (-not $AgentDir) {
    # Mirror api/config.py `_discover_agent_dir` for server-equivalent candidates:
    # HERMES_HOME\hermes-agent → repo sibling → parent (when it looks like an
    # agent root) → _DEFAULT_HERMES_HOME\hermes-agent → HOME\hermes-agent.
    # Complete BOTH source (run_agent.py) and pip-style (hermes_cli) passes over
    # that list BEFORE trying launcher-only roots (legacy .hermes + Program Files).
    # Folding those into the same candidate list lets a stale Program Files source
    # outrank a working LOCALAPPDATA pip Agent on the all-source first pass — the
    # server never searches Program Files (or USERPROFILE\.hermes) at all. Build
    # Program Files incrementally — ${env:ProgramFiles(x86)} is null on 32-bit
    # Windows and in some constrained environments, and Join-Path throws on a
    # null Path. Skip any system-wide root that isn't set so the launcher stays
    # robust across Windows variants.
    $serverCandidates = @()
    $serverCandidates += (Join-Path $env:HERMES_HOME 'hermes-agent')
    $repoParent = Split-Path -Parent $RepoRoot
    $serverCandidates += (Join-Path $repoParent 'hermes-agent')
    # Parent-is-agent: repo cloned inside hermes-agent/ (same gate as
    # api/config.py's `_looks_like_agent_source_root` on REPO_ROOT.parent).
    if ((Test-Path (Join-Path $repoParent 'run_agent.py') -PathType Leaf) -or
        (Test-Path (Join-Path $repoParent 'hermes_cli') -PathType Container)) {
        $serverCandidates += $repoParent
    }
    # 5. Platform-default home\hermes-agent (api.paths._platform_default_hermes_home)
    $serverCandidates += (Join-Path $platformDefaultHermesHome 'hermes-agent')
    # 6. HOME\hermes-agent (Path.home() → %USERPROFILE% on Windows)
    $serverCandidates += (Join-Path $env:USERPROFILE 'hermes-agent')
    # De-dup server-equivalent list (HERMES_HOME may coincide with platform default).
    $serverCandidates = $serverCandidates | Select-Object -Unique
    # Two-pass over server-equivalent candidates first (matches api/config.py).
    foreach ($c in $serverCandidates) {
        if (Test-Path (Join-Path $c 'run_agent.py') -PathType Leaf) { $AgentDir = $c; break }
    }
    if (-not $AgentDir) {
        foreach ($c in $serverCandidates) {
            if (Test-Path (Join-Path $c 'hermes_cli') -PathType Container) { $AgentDir = $c; break }
        }
    }
    # Launcher-only fallbacks — only after both server-equivalent passes.
    # Master always searched %USERPROFILE%\.hermes\hermes-agent first; the server
    # never does (its HOME candidate is %USERPROFILE%\hermes-agent). Keep that
    # legacy path as a launcher-only rescue AFTER the server-equivalent passes so
    # a legacy-only Agent is still found, then Program Files (POSIX XDG/opt
    # equivalents have no Windows server twin). Legacy ahead of Program Files
    # matches master's precedence among these launcher-only roots.
    $launcherOnlyCandidates = @()
    $launcherOnlyCandidates += (Join-Path $env:USERPROFILE '.hermes\hermes-agent')
    foreach ($root in @(${env:ProgramW6432}, ${env:ProgramFiles}, ${env:ProgramFiles(x86)})) {
        if ($root) { $launcherOnlyCandidates += (Join-Path $root 'hermes\hermes-agent') }
    }
    # De-dup: WOW64 can make ProgramFiles == ProgramFiles(x86); legacy may equal
    # a prior server candidate when HERMES_HOME already points there.
    $launcherOnlyCandidates = $launcherOnlyCandidates | Select-Object -Unique
    if (-not $AgentDir) {
        foreach ($c in $launcherOnlyCandidates) {
            if (Test-Path (Join-Path $c 'run_agent.py') -PathType Leaf) { $AgentDir = $c; break }
        }
    }
    if (-not $AgentDir) {
        foreach ($c in $launcherOnlyCandidates) {
            if (Test-Path (Join-Path $c 'hermes_cli') -PathType Container) { $AgentDir = $c; break }
        }
    }
    # Combined list for the not-found error message.
    $candidates = @($serverCandidates) + @($launcherOnlyCandidates)
}
if (-not $AgentDir) {
    $searched = $candidates -join ', '
    Write-Error "hermes-agent not found. Searched: $searched. Set HERMES_WEBUI_AGENT_DIR explicitly to override."
    exit 1
}

# === Export the discovered agent dir to the child process ===============
# server.py calls activate_managed_agent() (managed_agent_startup.py) as its
# first Agent-related import, and that hook keys off HERMES_WEBUI_AGENT_DIR
# ONLY - it is what puts the Agent's dependency environment on sys.path.
# Without this export the value discovered above stays a PowerShell variable,
# activation silently no-ops, and the very next Agent import dies with
# ModuleNotFoundError on a dependency the managed environment already
# provides. api/config.py's own discovery fallback runs too late to help: it
# is imported after activate_managed_agent() has already returned.
# bootstrap.py does the equivalent at os.environ["HERMES_WEBUI_AGENT_DIR"];
# start.ps1 bypasses bootstrap.py, so it owns this export.
# The two-argument SetEnvironmentVariable form sets the variable for this
# process and its children, matching how this script already loads .env.
[Environment]::SetEnvironmentVariable('HERMES_WEBUI_AGENT_DIR', $AgentDir)

# === Prefer the agent's venv Python if available =======================
$agentVenvPython = Join-Path $AgentDir 'venv\Scripts\python.exe'
if (Test-Path $agentVenvPython) {
    $Python = $agentVenvPython
}

# === Resolve bind + state defaults =====================================
$BindHostFinal = if ($BindHost) { $BindHost } elseif ($env:HERMES_WEBUI_HOST) { $env:HERMES_WEBUI_HOST } else { '127.0.0.1' }
$PortFinal = if ($Port) {
    $Port
} elseif ($env:HERMES_WEBUI_PORT) {
    # TryParse + range guard on the env var. A plain [int] cast on the
    # env var throws InvalidCastException with no actionable context when
    # the env var is set to a non-integer (typo, accidental shell
    # expansion, etc.) — surface a targeted error message instead.
    $parsedPort = 0
    if (-not [int]::TryParse($env:HERMES_WEBUI_PORT, [ref]$parsedPort)) {
        Write-Error "HERMES_WEBUI_PORT='$($env:HERMES_WEBUI_PORT)' is not a valid integer port. Unset the variable to use the default (8787), or set it to a number 1-65535."
        exit 1
    }
    if ($parsedPort -lt 1 -or $parsedPort -gt 65535) {
        Write-Error "HERMES_WEBUI_PORT=$parsedPort is out of TCP-port range. Must be 1-65535."
        exit 1
    }
    $parsedPort
} else {
    8787
}
$env:HERMES_WEBUI_HOST = $BindHostFinal
$env:HERMES_WEBUI_PORT = "$PortFinal"
# HERMES_HOME default was resolved before agent discovery above so the
# exported HERMES_WEBUI_AGENT_DIR matches api/config.py's search order.
if (-not $env:HERMES_WEBUI_STATE_DIR) {
    $env:HERMES_WEBUI_STATE_DIR = Join-Path $env:HERMES_HOME 'webui'
}

# === Ensure dirs exist =================================================
New-Item -ItemType Directory -Force -Path $env:HERMES_HOME | Out-Null
New-Item -ItemType Directory -Force -Path $env:HERMES_WEBUI_STATE_DIR | Out-Null

# === Launch (foreground, matches start.sh) =============================
Write-Host "[start.ps1] Hermes WebUI native Windows launcher" -ForegroundColor Cyan
Write-Host "[start.ps1] Python:     $Python"
Write-Host "[start.ps1] Agent dir:  $AgentDir"
Write-Host "[start.ps1] State dir:  $env:HERMES_WEBUI_STATE_DIR"
Write-Host "[start.ps1] Binding:    ${BindHostFinal}:${PortFinal}"
Write-Host ""

$serverPath = Join-Path $RepoRoot 'server.py'
if (-not (Test-Path $serverPath)) {
    Write-Error "server.py not found at $serverPath - is this the hermes-webui repo root?"
    exit 1
}

# Capture exit code, let finally{} run Pop-Location, exit AFTER the try.
# Plain `exit $LASTEXITCODE` inside the try block can prevent the finally
# from running in some termination paths (especially when dot-sourced or
# in interactive sessions), leaving the caller's working directory stuck
# at $RepoRoot.
$script:serverExitCode = 0
Push-Location $RepoRoot
try {
    # @args was non-functional here — PowerShell does NOT populate $args when the
    # script declares [CmdletBinding()] with an explicit param() block (Copilot's
    # finding on PR #2807). Dropped rather than added a ValueFromRemainingArguments
    # parameter, because the existing tracked use case is the launcher running
    # server.py with the env-var-driven config — no pass-through args are needed.
    # If pass-through becomes a requirement later, add a [Parameter(ValueFromRemainingArguments=$true)] [string[]]$ServerArgs and splat that.
    & $Python $serverPath
    $script:serverExitCode = $LASTEXITCODE
} finally {
    Pop-Location
}
exit $script:serverExitCode
