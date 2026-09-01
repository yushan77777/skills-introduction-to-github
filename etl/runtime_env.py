"""Giving the ETL subprocess the Linux environment it already had.

The ETL does not run in a vacuum. On the server it needs ``SPARK_HOME`` and
``SPARK_CONF_DIR`` to find the Spark installation and its ``spark-defaults.conf``,
``JAVA_HOME`` for the JVM, ``PYSPARK_PYTHON`` so the executors use the same
interpreter as the driver, ``SPARK_LOCAL_IP`` on a multi-homed host so the
master can call the driver back, and ``LD_LIBRARY_PATH`` / ``TNS_ADMIN`` for the
Oracle Instant Client.

When the ETL is started by hand, all of that arrives from the operator's login
shell. When it is started by a web application under systemd it does **not**:
a service gets a near-empty environment, and inheriting that is how a run ends
up talking to no Spark master at all.

So the environment is rebuilt here, from two sources the server administrator
controls — never the browser:

``ETL_ENV_SCRIPT``
    A shell script sourced in a clean bash; every variable it exports is
    captured. This is the faithful option: point it at the same file the
    operators source before running the ETL by hand and the subprocess gets
    exactly what they get.

``ETL_ENV_FILE``
    Plain ``KEY=VALUE`` lines, in the format systemd's ``EnvironmentFile``
    uses. Simpler, and enough when only a handful of variables are needed.

Both are optional. With neither set the behaviour is what it was before: the
web process's own environment, which is right for a development machine and
wrong for a service.
"""

from __future__ import annotations

import os
import re
import subprocess
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

from .security import CREDENTIAL_NAME

#: Variables worth showing in a log header or a health check. Everything else
#: is still passed to the ETL — this list only decides what gets *reported*,
#: so a diagnosis does not require dumping the whole environment.
RUNTIME_ENV_KEYS = (
    "SPARK_HOME", "SPARK_CONF_DIR", "SPARK_LOCAL_IP", "SPARK_PUBLIC_DNS",
    "SPARK_MASTER", "PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON",
    "PYSPARK_SUBMIT_ARGS", "JAVA_HOME", "HADOOP_HOME", "HADOOP_CONF_DIR",
    "YARN_CONF_DIR", "LD_LIBRARY_PATH", "ORACLE_HOME", "TNS_ADMIN", "NLS_LANG",
    "PATH", "PYTHONPATH", "HOME", "USER", "TMPDIR",
)

#: Set by the shell itself; carrying them over is noise at best.
_SHELL_NOISE = {"_", "SHLVL", "PWD", "OLDPWD", "BASH_EXECUTION_STRING"}

_ENV_LINE = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$")

SCRIPT_TIMEOUT = 20


class EnvError(Exception):
    """The configured environment source could not be read."""


@dataclass
class RuntimeEnv:
    """The environment to hand the ETL, and where each part came from."""

    values: dict[str, str] = field(default_factory=dict)
    sources: list[str] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)


def _strip_quotes(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value


def parse_env_file(path: Path) -> dict[str, str]:
    """``KEY=VALUE`` lines, systemd ``EnvironmentFile`` style.

    Comments and blank lines are skipped, an optional ``export`` prefix is
    allowed, and surrounding quotes are removed. No shell expansion happens —
    a literal ``$HOME`` stays literal, exactly as systemd treats it.
    """
    try:
        text = path.read_text()
    except OSError as exc:
        raise EnvError(f"Could not read ETL_ENV_FILE {path}: {exc}") from exc

    values: dict[str, str] = {}
    for number, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = _ENV_LINE.match(line)
        if not match:
            raise EnvError(
                f"{path}: line {number} is not KEY=VALUE — {raw.strip()[:60]!r}")
        values[match.group(1)] = _strip_quotes(match.group(2))
    return values


def capture_script_env(script: Path, shell: str = "/bin/bash") -> dict[str, str]:
    """Source ``script`` in a clean shell and capture what it exports.

    ``env -0`` is used rather than ``env`` so a value containing a newline
    survives; the script's own stdout is discarded so a profile that prints a
    banner does not corrupt the capture.
    """
    if not script.exists():
        raise EnvError(f"ETL_ENV_SCRIPT does not exist: {script}")

    # `set -a` exports plain assignments too, so a script written without
    # `export` still works. stdout is redirected to stderr for the sourcing
    # part so only `env -0` writes to stdout.
    command = f'set -a; . "$1" >&2; set +a; exec env -0'
    try:
        result = subprocess.run(
            [shell, "-c", command, "_", str(script)],
            capture_output=True, timeout=SCRIPT_TIMEOUT,
        )
    except FileNotFoundError as exc:
        raise EnvError(f"Shell not found for ETL_ENV_SCRIPT: {shell}") from exc
    except subprocess.TimeoutExpired as exc:
        raise EnvError(
            f"ETL_ENV_SCRIPT {script} did not finish within "
            f"{SCRIPT_TIMEOUT}s — it may be waiting for input.") from exc

    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", "replace").strip().splitlines()
        tail = detail[-1] if detail else f"exit code {result.returncode}"
        raise EnvError(f"ETL_ENV_SCRIPT {script} failed: {tail}")

    values: dict[str, str] = {}
    for chunk in result.stdout.split(b"\0"):
        if not chunk:
            continue
        key, _, value = chunk.decode("utf-8", "replace").partition("=")
        if key and key not in _SHELL_NOISE and not key.startswith("BASH_FUNC_"):
            values[key] = value
    return values


@lru_cache(maxsize=8)
def _cached_script_env(path: str, mtime: float, size: int) -> dict[str, str]:
    return capture_script_env(Path(path))


def _script_env(script: Path) -> dict[str, str]:
    """Captured script environment, re-run when the script changes."""
    try:
        stat = script.stat()
    except OSError as exc:
        raise EnvError(f"ETL_ENV_SCRIPT does not exist: {script}") from exc
    return dict(_cached_script_env(str(script), stat.st_mtime, stat.st_size))


def build(settings) -> RuntimeEnv:
    """The full environment for an ETL subprocess.

    Layered, later winning: the web process's own environment, then the
    captured script, then the environment file. Nothing here is derived from a
    request — both sources are server-side configuration.
    """
    result = RuntimeEnv(values=dict(os.environ))
    result.sources.append("web application environment")

    if settings.env_script:
        script = Path(settings.env_script).expanduser()
        try:
            result.values.update(_script_env(script))
            result.sources.append(f"ETL_ENV_SCRIPT {script}")
        except EnvError as exc:
            result.problems.append(str(exc))

    if settings.env_file:
        path = Path(settings.env_file).expanduser()
        try:
            result.values.update(parse_env_file(path))
            result.sources.append(f"ETL_ENV_FILE {path}")
        except EnvError as exc:
            result.problems.append(str(exc))

    return result


def describe(values: dict[str, str]) -> list[tuple[str, str]]:
    """The runtime-relevant variables, for a log header or a health check.

    A variable whose *name* reads like a credential is reported as set rather
    than printed, so this is safe to write into a log file.
    """
    rows = []
    for key in RUNTIME_ENV_KEYS:
        if key not in values:
            continue
        value = values[key]
        if CREDENTIAL_NAME.search(key):
            value = f"(set, {len(value)} characters)"
        rows.append((key, value))
    return rows
