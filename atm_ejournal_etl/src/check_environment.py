#!/usr/bin/env python3
"""
Environment preflight for the ATM E-Journal ETL.

    python3 src/check_environment.py            # fast, no cluster needed
    python3 src/check_environment.py --spark    # also starts a SparkSession and
                                                # runs a one-row distributed job

It answers the question that costs the most time to answer from a stack trace:
*is the Python/PySpark installation able to ship Python code to Spark at all?*

The failure it is built around
------------------------------
PySpark serialises every Python function it sends to the executors with the
cloudpickle version it bundles. A cloudpickle older than the interpreter cannot
read that interpreter's byte code, and the job dies on the driver with::

    _pickle.PicklingError: Could not serialize object:
        IndexError: tuple index out of range

Nothing in the ETL (or in any PySpark job) can work around it - even
``sc.parallelize([1, 2]).map(lambda x: x + 1)`` fails. The remedy is to install a
PySpark that supports the interpreter, ideally the version the cluster runs:

    Python 3.11  ->  pyspark >= 3.4
    Python 3.12  ->  pyspark >= 3.5
    Python 3.13  ->  pyspark >= 4.0

``pip install "pyspark==<the cluster's Spark version>"`` in the ETL virtualenv.
"""

from __future__ import annotations

import logging
import os
import platform
import subprocess
import sys
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("atm_ejournal.environment")

#: Minimum PySpark release that supports a given Python minor version.
MINIMUM_PYSPARK_FOR_PYTHON = {
    (3, 8): (3, 0),
    (3, 9): (3, 1),
    (3, 10): (3, 2),
    (3, 11): (3, 4),
    (3, 12): (3, 5),
    (3, 13): (4, 0),
}

SERIALIZATION_HINTS = ("could not serialize", "picklingerror", "tuple index out of range",
                       "unsupported pickle protocol", "code() argument")


class EnvironmentError_(Exception):
    """Raised when the interpreter cannot run distributed Python with this PySpark."""


# --------------------------------------------------------------------------- #
# Facts
# --------------------------------------------------------------------------- #


def describe_environment() -> Dict[str, Any]:
    """Versions that decide whether Python code can be shipped to Spark."""
    facts: Dict[str, Any] = {
        "python_version": platform.python_version(),
        "python_executable": sys.executable,
        "pyspark_version": None,
        "pyspark_path": None,
        "cloudpickle_version": None,
        "java_version": _java_version(),
        "spark_home": os.environ.get("SPARK_HOME", ""),
    }
    try:
        import pyspark                                 # noqa: PLC0415
        facts["pyspark_version"] = pyspark.__version__
        facts["pyspark_path"] = os.path.dirname(pyspark.__file__)
    except Exception as exc:                           # noqa: BLE001
        facts["pyspark_error"] = f"{type(exc).__name__}: {exc}"
    try:
        import pyspark.cloudpickle as cloudpickle      # noqa: PLC0415
        facts["cloudpickle_version"] = getattr(cloudpickle, "__version__", "unknown")
    except Exception:                                  # noqa: BLE001
        pass
    return facts


def _java_version() -> str:
    try:
        completed = subprocess.run(["java", "-version"], capture_output=True, text=True,
                                   timeout=15)
        output = [line.strip() for line in
                  (completed.stderr or completed.stdout).strip().splitlines()
                  # JAVA_TOOL_OPTIONS echoes a banner line before the version
                  if line.strip() and not line.startswith("Picked up")]
        return output[0] if output else "unknown"
    except Exception:                                  # noqa: BLE001 - java is optional here
        return "not found"


def pyspark_supports_python(pyspark_version: Optional[str] = None,
                            python_version: Optional[Tuple[int, int]] = None) -> Optional[bool]:
    """
    Is this PySpark new enough for this interpreter?

    ``None`` when it cannot be decided (unknown version, Python not in the table).
    """
    if pyspark_version is None:
        facts = describe_environment()
        pyspark_version = facts["pyspark_version"]
    if not pyspark_version:
        return None
    python_version = python_version or sys.version_info[:2]
    minimum = MINIMUM_PYSPARK_FOR_PYTHON.get(tuple(python_version))
    if minimum is None:
        return None
    try:
        parts = tuple(int(token) for token in str(pyspark_version).split(".")[:2])
    except ValueError:
        return None
    return parts >= minimum


# --------------------------------------------------------------------------- #
# The check that matters
# --------------------------------------------------------------------------- #


def _outer(value):
    """A closure, i.e. the shape of function PySpark ships internally."""
    def _inner():
        return value
    return _inner


def check_code_serialization() -> Tuple[bool, str]:
    """
    Serialise a closure with PySpark's own cloudpickle.

    This is the exact operation that fails on a mismatched install, and it needs
    neither a cluster nor a SparkSession - it takes milliseconds.
    """
    try:
        import pyspark.cloudpickle as cloudpickle      # noqa: PLC0415
    except Exception as exc:                           # noqa: BLE001
        return False, f"pyspark.cloudpickle could not be imported: {type(exc).__name__}: {exc}"
    try:
        cloudpickle.loads(cloudpickle.dumps(_outer(42)))
    except Exception as exc:                           # noqa: BLE001
        return False, f"{type(exc).__name__}: {exc}"
    return True, "ok"


def is_serialization_failure(exc: BaseException) -> bool:
    """True when an exception is (or wraps) a Python-code serialization failure."""
    seen = set()
    current: Optional[BaseException] = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        name = type(current).__name__.lower()
        message = str(current).lower()
        if "picklingerror" in name or any(hint in message for hint in SERIALIZATION_HINTS):
            return True
        current = current.__cause__ or current.__context__
    return False


def describe_serialization_failure(exc: Optional[BaseException] = None) -> str:
    """A message that names the cause and the remedy, for the log and the e-mail."""
    facts = describe_environment()
    supported = pyspark_supports_python(facts["pyspark_version"])
    minimum = MINIMUM_PYSPARK_FOR_PYTHON.get(sys.version_info[:2])
    lines = [
        "Spark could not serialise the Python code of this job.",
        f"  python      : {facts['python_version']} ({facts['python_executable']})",
        f"  pyspark     : {facts['pyspark_version']} ({facts['pyspark_path']})",
        f"  cloudpickle : {facts['cloudpickle_version']}",
    ]
    if exc is not None:
        lines.append(f"  error       : {type(exc).__name__}: {str(exc)[:200]}")
    if supported is False and minimum:
        lines += [
            "",
            f"This PySpark does not support Python {facts['python_version']}: its bundled "
            f"cloudpickle cannot read this interpreter's byte code, so every PySpark job that "
            "ships Python code fails the same way - including "
            "sc.parallelize([1, 2]).map(lambda x: x + 1).",
            "",
            "Remedy (in the ETL virtualenv), pick the PySpark that matches the cluster:",
            f"    pip install \"pyspark>={minimum[0]}.{minimum[1]}\"        "
            "# e.g. pyspark==4.0.0 for a Spark 4.0 cluster",
            "or run the ETL with an interpreter this PySpark supports (Python 3.10 or older "
            "for PySpark 3.3).",
        ]
    else:
        lines += [
            "",
            "Check that the driver and the executors run the same Python and PySpark "
            "versions (PYSPARK_PYTHON / PYSPARK_DRIVER_PYTHON), and run "
            "'python3 src/check_environment.py --spark' for a full report.",
        ]
    return "\n".join(lines)


def verify_python_serialization(raise_on_failure: bool = True) -> bool:
    """
    Preflight used by the ETL before the first batch, so a broken installation
    fails in a second with an explanation instead of mid-batch with a stack trace.
    """
    ok, detail = check_code_serialization()
    if ok:
        logger.debug("environment preflight: Python code serialization works")
        return True
    message = describe_serialization_failure(RuntimeError(detail))
    if raise_on_failure:
        raise EnvironmentError_(message)
    logger.error("%s", message)
    return False


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def _spark_round_trip() -> Tuple[bool, str]:
    """Start a local SparkSession and run one distributed Python task."""
    try:
        from pyspark.sql import SparkSession           # noqa: PLC0415

        spark = (SparkSession.builder.master("local[1]").appName("atm_etl_env_check")
                 .config("spark.ui.enabled", "false").getOrCreate())
        try:
            total = spark.sparkContext.parallelize([1, 2, 3]).map(lambda value: value * 2).sum()
            return True, f"distributed map returned {total}"
        finally:
            spark.stop()
    except Exception as exc:                           # noqa: BLE001
        return False, f"{type(exc).__name__}: {exc}"


def main(argv: Optional[List[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Check that this machine can run the ATM E-Journal ETL on Spark.")
    parser.add_argument("--spark", action="store_true",
                        help="also start a local SparkSession and run a distributed task")
    options = parser.parse_args(argv)

    facts = describe_environment()
    print("environment")
    print("-" * 70)
    for key in ("python_version", "python_executable", "pyspark_version", "pyspark_path",
                "cloudpickle_version", "java_version", "spark_home"):
        print(f"  {key:<20} {facts.get(key)}")
    if facts.get("pyspark_error"):
        print(f"  {'pyspark_error':<20} {facts['pyspark_error']}")

    print("\nchecks")
    print("-" * 70)
    failures = 0

    supported = pyspark_supports_python(facts["pyspark_version"])
    minimum = MINIMUM_PYSPARK_FOR_PYTHON.get(sys.version_info[:2])
    if supported is False and minimum:
        failures += 1
        print(f"  FAIL  pyspark {facts['pyspark_version']} does not support python "
              f"{facts['python_version']} (needs >= {minimum[0]}.{minimum[1]})")
    elif supported is True:
        print(f"  OK    pyspark {facts['pyspark_version']} supports python "
              f"{facts['python_version']}")
    else:
        print("  SKIP  python/pyspark support could not be determined")

    ok, detail = check_code_serialization()
    if ok:
        print("  OK    Python code can be serialised for the executors")
    else:
        failures += 1
        print(f"  FAIL  Python code cannot be serialised: {detail}")

    if options.spark:
        ok, detail = _spark_round_trip()
        print(f"  {'OK   ' if ok else 'FAIL '} local Spark round trip: {detail}")
        failures += 0 if ok else 1

    print()
    if failures:
        print(describe_serialization_failure())
        return 1
    print("environment looks fine for the ETL")
    return 0


if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    sys.exit(main())
