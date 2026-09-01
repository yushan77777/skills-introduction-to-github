"""Subprocess entry point — the only place the ETL is ever executed.

This module imports the existing ETL module and calls its existing function.
It contains no ETL logic, no second connector implementation and no copy of
anything in ``etl_table_manual.py``: the Spark, Oracle, Greenplum and
encryption code all arrive with that import.

Why a subprocess instead of an in-process call:

* Spark takes over the JVM and process-wide state. A crashed, hung or
  memory-hungry run cannot take the web application down with it.
* ``run_etl`` reads ``config/config.yaml`` by relative path and the ETL module
  chdirs on import, so it needs the ETL project root as its working directory
  — not something to impose on a shared web process.
* Stopping becomes one signal to a process group rather than an
  uninterruptible thread.
* The ETL runs under the existing Linux runtime (``ETL_PYTHON``), which is
  usually a different interpreter from the one serving Django.

The job payload arrives as JSON on **stdin**, never on the command line, so
credentials never appear in ``ps``.

Two sentinel prefixes are the only structured channel back to the web process;
everything else on stdout is the ETL's own output, passed through untouched.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import traceback

RESULT_PREFIX = "__ETL_RESULT__ "
ERROR_PREFIX = "__ETL_ERROR__ "


def _fail(message: str, exc_type: str = "Error", code: int = 2) -> int:
    print(f"{ERROR_PREFIX}{json.dumps({'error': message, 'type': exc_type})}",
          flush=True)
    return code


def _configure_logging() -> logging.Logger:
    """A stdout logger for the ETL's own ``logger`` hook.

    ``etl_table_manual`` already routes its messages through a module-level
    ``logger`` when one is set, and falls back to ``print`` when it is not.
    Setting it here uses that existing mechanism rather than replacing it: the
    ETL's own ``__logger`` calls, and the ``greenplum``/``oracle`` helpers that
    take a ``logger`` argument, all keep working exactly as written.
    """
    logger = logging.getLogger("etl.run")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"))
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError) as exc:
        return _fail(f"Could not read the job payload: {exc}", "PayloadError")

    project_root = payload["project_root"]
    module_name = payload["module"]
    entrypoint = payload.get("entrypoint", "run_etl")
    params = payload["params"]

    logger = _configure_logging()

    if not os.path.isdir(project_root):
        return _fail(f"ETL project root does not exist: {project_root}",
                     "MissingProject")

    # run_etl resolves config/config.yaml relative to the working directory.
    os.chdir(project_root)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    logger.info("Working directory: %s", project_root)
    logger.info("Python runtime: %s", sys.executable)
    logger.info("Importing %s from the existing ETL project", module_name)

    try:
        import importlib
        etl_module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        traceback.print_exc()
        return _fail(
            f"Could not import {module_name}: {exc}. The ETL runtime "
            f"({sys.executable}) is missing a dependency the ETL needs — "
            f"check pyspark, pandas, tqdm, oracledb, psycopg2, pyarrow and "
            f"cryptography.",
            exc.__class__.__name__)
    except Exception as exc:
        traceback.print_exc()
        return _fail(f"Could not import {module_name}: {exc}",
                     exc.__class__.__name__)

    # Hand the ETL its own logger hook so its messages carry timestamps and
    # arrive on one stream. This is the ETL's existing mechanism, not a new one.
    if hasattr(etl_module, "logger"):
        etl_module.logger = logger

    try:
        run = getattr(etl_module, entrypoint)
    except AttributeError:
        return _fail(f"{module_name} has no {entrypoint}() function.",
                     "AttributeError")
    if not callable(run):
        return _fail(f"{module_name}.{entrypoint} is not callable.", "TypeError")

    safe_keys = ", ".join(
        sorted(k for k in params if "password" not in k.lower()))
    logger.info("Calling %s(%s)", entrypoint, safe_keys)

    try:
        result = run(**params)
    except TypeError as exc:
        # A signature mismatch is a configuration problem, not an ETL failure.
        traceback.print_exc()
        return _fail(f"{entrypoint}() rejected the parameters: {exc}",
                     "TypeError", code=1)
    except Exception as exc:
        traceback.print_exc()
        return _fail(str(exc) or exc.__class__.__name__, exc.__class__.__name__,
                     code=1)

    logger.info("ETL returned: %s", result)
    print(f"{RESULT_PREFIX}{json.dumps(result, default=str)}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
