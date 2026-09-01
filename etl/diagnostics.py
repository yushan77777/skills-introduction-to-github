"""Naming the cause of a failed run instead of its last symptom.

A Spark failure reports itself twice, a minute apart and in the wrong order of
usefulness. The real event is a line like::

    ERROR StandaloneSchedulerBackend: All masters are unresponsive! Giving up.

which stops the SparkContext. Nothing appears to go wrong at that moment,
because the ETL is still setting up. The *next* operation that needs a live
context — usually the write — then fails with::

    An error occurred while calling o307.csv.
    : java.util.NoSuchElementException: None.get
      at org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker$.metrics

That method body is ``SparkContext.getActive.get``, so ``None.get`` means "no
active SparkContext", not "something is wrong with the CSV write". Reporting
the exception the ETL raised — which is all the process itself knows — sends
whoever reads it to the wrong end of the pipeline.

So the captured output is scanned for a small set of failures whose meaning is
unambiguous, and the first one found is offered as the likely cause alongside
the raw exception. Two rules keep this honest:

* Every pattern here is a verbatim message emitted by Spark, the JDBC layer or
  the Oracle client. Nothing is guessed at from a stack trace shape.
* It is presented as the *likely* cause with the evidence line attached, never
  as a verdict that replaces the real error. The raw exception is always still
  shown.

A pattern marked ``primary`` is a root cause and outranks one marked
``consequence``, whichever is seen first.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Cause:
    code: str
    summary: str
    hint: str
    #: True for a root cause, False for a downstream symptom of one.
    primary: bool = True


@dataclass(frozen=True)
class Finding:
    code: str
    summary: str
    hint: str
    primary: bool
    #: The captured line that matched, so the claim can be checked.
    evidence: str

    def as_dict(self) -> dict:
        return {"code": self.code, "summary": self.summary, "hint": self.hint,
                "primary": self.primary, "evidence": self.evidence}


#: (compiled pattern, cause). Order is only a tie-break; `primary` decides.
PATTERNS: list[tuple[re.Pattern, Cause]] = [
    (re.compile(r"All masters are unresponsive"), Cause(
        code="spark_master_unreachable",
        summary="The Spark driver never registered with the master, so the "
                "session had no cluster behind it.",
        hint="Three usual causes: the ETL's environment is not being passed "
             "(set ETL_ENV_SCRIPT — SPARK_HOME, SPARK_CONF_DIR, SPARK_LOCAL_IP); "
             "the master is not reachable from this host (run "
             "'manage.py etl_check', which probes the port); or the PySpark "
             "version does not match the master's Spark version (compare the "
             "'PySpark …' line at the top of this log with the Spark master UI).")),

    (re.compile(r"Cannot assign requested address"), Cause(
        code="spark_bind_address",
        summary="The driver could not bind to the address Spark chose for it.",
        hint="On a multi-homed host set SPARK_LOCAL_IP in ETL_ENV_SCRIPT to "
             "this machine's address on the Spark network.")),

    (re.compile(r"Initial job has not accepted any resources"), Cause(
        code="spark_no_resources",
        summary="The job registered with the master but no executor was ever "
                "given to it.",
        hint="The cluster has no free cores or memory for the request in "
             "config.yaml (executor_instances, executor_memory, cores_max), "
             "or another application is holding them. The Spark master UI "
             "shows what is available.")),

    (re.compile(r"Cannot call methods on a stopped SparkContext"
                r"|SparkContext (?:has been shut ?down|was shut ?down)"), Cause(
        code="spark_context_stopped",
        summary="The SparkContext was already stopped when this step ran.",
        hint="Something ended the session earlier in this log — look further "
             "up for the first ERROR line.")),

    (re.compile(r"NoSuchElementException: None\.get"), Cause(
        code="spark_context_not_active",
        summary="A write ran with no active SparkContext "
                "(BasicWriteJobStatsTracker.metrics calls "
                "SparkContext.getActive.get, and it was empty).",
        hint="This is a downstream symptom, not the fault. The session was "
             "stopped earlier — search this log for the first ERROR line, "
             "typically a Spark master registration failure a minute before.",
        primary=False)),

    (re.compile(r"DPI-1047"), Cause(
        code="oracle_client_missing",
        summary="The Oracle Instant Client could not be loaded.",
        hint="oracle_connect.py calls init_oracle_client with a hard-coded "
             "lib_dir. Install the client there, and set LD_LIBRARY_PATH in "
             "ETL_ENV_SCRIPT — a service does not inherit it from a shell.")),

    (re.compile(r"ORA-01017"), Cause(
        code="oracle_bad_credentials",
        summary="Oracle rejected the username or password for this connection "
                "profile.",
        hint="Open the Configuration panel: it reports, per profile, whether "
             "the encrypted password still decrypts with the key file it "
             "names. A profile that fails that check will fail here.")),

    (re.compile(r"ClassNotFoundException:\s*oracle\.jdbc"
                r"|No suitable driver"), Cause(
        code="jdbc_driver_missing",
        summary="The JDBC driver class was not on the Spark classpath.",
        hint="spark_properties.jars in config.yaml lists the jars, and "
             "build_spark passes them as spark.jars. Check every path there "
             "exists and is readable by the ETL user.")),

    (re.compile(r"java\.lang\.OutOfMemoryError"), Cause(
        code="out_of_memory",
        summary="The JVM ran out of memory.",
        hint="For a driver-side failure raise driver_memory in config.yaml; "
             "for an executor-side one raise executor_memory. Note that "
             "parquet_to_greenplum reads the whole file into pandas in the "
             "driver.")),
]


def scan(line: str) -> Finding | None:
    """The first known failure this line reports, if any."""
    for pattern, cause in PATTERNS:
        if pattern.search(line):
            return Finding(code=cause.code, summary=cause.summary,
                           hint=cause.hint, primary=cause.primary,
                           evidence=line.strip()[:500])
    return None


def better(new: Finding, current: Finding | None) -> bool:
    """Should ``new`` replace the finding already recorded?

    First one wins, except that a root cause displaces a symptom — the write
    failure is often seen before anyone reads far enough up the log to find
    what actually stopped the session.
    """
    if current is None:
        return True
    return new.primary and not current.primary
