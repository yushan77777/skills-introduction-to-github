"""Where the ETL app finds the existing ETL project and its Linux runtime.

Everything is environment-driven so no path, host or credential is baked into
the Django source. Defaults point at the ``etl_project/`` directory shipped in
this repository; on the real server, point ``ETL_PROJECT_ROOT`` at the existing
ETL checkout and ``ETL_PYTHON`` at the interpreter that already has PySpark and
the Oracle client configured.

Nothing in this module imports the ETL, opens a connection or reads a
credential. It only answers "where is it and how is it run".
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

# The Django project root (the directory holding manage.py).
BASE_DIR = Path(__file__).resolve().parent.parent


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _int_env(name: str, default: int) -> int:
    raw = _env(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


@dataclass(frozen=True)
class EtlSettings:
    #: Root of the existing ETL project — the directory that contains
    #: ``etl_table_manual.py``, ``config/config.yaml`` and ``src/utils/``.
    project_root: Path
    #: Module holding the ETL entry point, importable from ``project_root``.
    etl_module: str
    #: Callable inside that module. ``run_etl`` in the supplied source.
    etl_entrypoint: str
    #: Interpreter used for ETL subprocesses. Must be the existing runtime:
    #: PySpark, oracledb, psycopg2, pandas, tqdm, cryptography, pyarrow.
    python_executable: str
    #: ``config.yaml`` location, relative to ``project_root`` unless absolute.
    config_yaml: str
    #: Directory the per-run log files are written to.
    log_dir: Path
    #: How many finished runs to keep in the in-memory history.
    history_size: int
    #: Maximum log lines held in memory per run (the full log is on disk).
    log_buffer: int
    #: Seconds to wait for a killed process group to actually disappear.
    stop_timeout: float

    @property
    def config_yaml_path(self) -> Path:
        path = Path(self.config_yaml)
        return path if path.is_absolute() else self.project_root / path

    @property
    def etl_module_path(self) -> Path:
        """File backing :attr:`etl_module`, used for job discovery."""
        return self.project_root / f"{self.etl_module}.py"

    @property
    def worker_path(self) -> Path:
        return Path(__file__).resolve().parent / "worker.py"


@lru_cache(maxsize=1)
def get_settings() -> EtlSettings:
    root = Path(_env("ETL_PROJECT_ROOT") or (BASE_DIR / "etl_project")).expanduser()
    # resolve() late: a missing directory is reported by the UI, not by an
    # exception at import time.
    root = root.resolve() if root.exists() else root.absolute()
    log_dir = Path(_env("ETL_LOG_DIR") or (root / "logs")).expanduser()
    return EtlSettings(
        project_root=root,
        etl_module=_env("ETL_MODULE", "etl_table_manual"),
        etl_entrypoint=_env("ETL_ENTRYPOINT", "run_etl"),
        python_executable=_env("ETL_PYTHON") or sys.executable,
        config_yaml=_env("ETL_CONFIG_YAML", "config/config.yaml"),
        log_dir=log_dir,
        history_size=_int_env("ETL_HISTORY_SIZE", 50),
        log_buffer=_int_env("ETL_LOG_BUFFER", 4000),
        stop_timeout=float(_int_env("ETL_STOP_TIMEOUT", 10)),
    )


SETTINGS = get_settings()
