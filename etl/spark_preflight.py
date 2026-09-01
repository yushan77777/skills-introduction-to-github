"""Standalone Spark connectivity probe, run under the ETL's own interpreter.

The question this answers is narrow and specific: **does a Spark session built
from `config/config.yaml` actually reach the master and get an executor?** It
is the ETL run minus the ETL — no Oracle, no Greenplum, no query, no output
path — so a failure here is unambiguously Spark's, and a success here means the
next failure is not.

It uses the project's own ``build_spark`` wherever it can, so what is tested is
the real code path with the real configuration, not a re-implementation of it.
Every Spark setting comes from ``spark_properties`` in the YAML; this script
sets no Spark environment variable and passes no override.

Like ``worker.py`` this is executed as a script by ``ETL_PYTHON``, never
imported as part of the Django app, so it uses no relative imports and depends
only on what the ETL runtime already has.

Structured findings are written as ``__PREFLIGHT__ {json}`` lines; everything
else on stdout is Spark's own output, passed through untouched.
"""

from __future__ import annotations

import json
import os
import re
import socket
import sys
import traceback

#: build_spark passes -Djava.io.tmpdir=... to both the driver and the
#: executors. A path the service user cannot write to breaks the driver in
#: ways that look nothing like a permissions problem.
_TMPDIR_OPT = re.compile(r"-Djava\.io\.tmpdir=(\S+)")

MARKER = "__PREFLIGHT__ "

#: The trivial job. Two partitions, so it needs a real executor rather than
#: being answered from the driver.
PROBE_PARTITIONS = 2
PROBE_VALUES = 10


def emit(stage: str, **fields) -> None:
    print(f"{MARKER}{json.dumps({'stage': stage, **fields}, default=str)}",
          flush=True)


def local_addresses() -> list[str]:
    host = socket.gethostname()
    try:
        return sorted({info[4][0] for info in
                       socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)})
    except OSError:
        return []


def describe_jars(raw: str) -> list[dict]:
    """Every jar in spark_properties.jars, with whether this user can read it."""
    out = []
    for path in [p.strip() for p in str(raw or "").split(",") if p.strip()]:
        out.append({
            "path": path,
            "exists": os.path.isfile(path),
            "readable": os.access(path, os.R_OK),
        })
    return out


def main() -> int:
    project_root = sys.argv[1]
    module_name = sys.argv[2] if len(sys.argv) > 2 else "etl_table_manual"

    os.chdir(project_root)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    emit("runtime",
         python=sys.executable,
         python_version=sys.version.split()[0],
         hostname=socket.gethostname(),
         local_addresses=local_addresses(),
         cwd=os.getcwd())

    try:
        import pyspark
        emit("pyspark", version=getattr(pyspark, "__version__", ""),
             location=getattr(pyspark, "__file__", ""),
             spark_home=os.environ.get("SPARK_HOME", ""))
    except Exception as exc:
        emit("pyspark", error=f"{exc.__class__.__name__}: {exc}")
        return 2

    # ---- configuration, from the YAML and nowhere else --------------------
    try:
        import yaml
        with open("config/config.yaml") as handle:
            cfg = yaml.safe_load(handle) or {}
    except Exception as exc:
        emit("config", error=f"Could not read config/config.yaml: {exc}")
        return 2

    sp = cfg.get("spark_properties")
    if not isinstance(sp, dict):
        emit("config", error="config.yaml has no spark_properties block.")
        return 2

    emit("config",
         master_url=sp.get("master_url", ""),
         session_name=sp.get("session_name", ""),
         executor_instances=sp.get("executor_instances", ""),
         executor_memory=sp.get("executor_memory", ""),
         executor_cores=sp.get("executor_cores", ""),
         cores_max=sp.get("cores_max", ""),
         driver_memory=sp.get("driver_memory", ""),
         jars=describe_jars(sp.get("jars", "")))

    # ---- build the session with the project's own builder ----------------
    build_spark = None
    try:
        import importlib
        module = importlib.import_module(module_name)
        build_spark = getattr(module, "build_spark", None)
        emit("builder", source=f"{module_name}.build_spark",
             available=build_spark is not None)
    except Exception as exc:
        emit("builder", source=f"{module_name}.build_spark", available=False,
             error=f"{exc.__class__.__name__}: {exc}")

    if build_spark is None:
        emit("session", error="build_spark() is not available, so the real "
                              "session builder could not be tested.")
        return 2

    emit("session", state="creating")
    try:
        spark = build_spark(cfg, app_name="ETL preflight")
    except Exception as exc:
        traceback.print_exc()
        emit("session", state="failed",
             error=f"{exc.__class__.__name__}: {exc}")
        return 1

    conf = spark.sparkContext.getConf()
    driver_host = conf.get("spark.driver.host", "")
    loopback = driver_host.startswith("127.") or driver_host == "localhost"
    emit("session", state="created",
         spark_version=spark.version,
         master=conf.get("spark.master", ""),
         app_id=spark.sparkContext.applicationId,
         app_name=conf.get("spark.app.name", ""),
         driver_host=driver_host,
         driver_port=conf.get("spark.driver.port", ""),
         driver_host_is_loopback=loopback,
         ui_url=conf.get("spark.ui.port", ""))

    for key in ("spark.driver.extraJavaOptions", "spark.executor.extraJavaOptions"):
        match = _TMPDIR_OPT.search(conf.get(key, "") or "")
        if not match:
            continue
        path = match.group(1).rstrip("/") or "/"
        # Only the driver's directory is on this host; the executors' is on
        # the worker nodes, so report but do not judge that one.
        local = key.startswith("spark.driver")
        emit("tmpdir", setting=key, path=path, local=local,
             exists=os.path.isdir(path) if local else None,
             writable=os.access(path, os.W_OK | os.X_OK) if local else None)

    # ---- prove an executor was actually allocated ------------------------
    emit("job", state="submitting")
    try:
        total = (spark.sparkContext
                 .parallelize(range(PROBE_VALUES), PROBE_PARTITIONS)
                 .sum())
    except Exception as exc:
        traceback.print_exc()
        emit("job", state="failed", error=f"{exc.__class__.__name__}: {exc}")
        _stop(spark)
        return 1

    expected = sum(range(PROBE_VALUES))
    emit("job", state="completed", result=total, expected=expected,
         correct=total == expected)

    try:
        executors = spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size()
        emit("executors", count=int(executors))
    except Exception:
        pass  # internal API; a failure here says nothing about the cluster

    _stop(spark)
    emit("done", ok=True)
    return 0


def _stop(spark) -> None:
    try:
        spark.stop()
    except Exception:
        pass


if __name__ == "__main__":
    sys.exit(main())
