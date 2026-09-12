"""
Test helpers: synthetic ATM e-journal files, a temporary project layout and a
fake Greenplum loader.

The journal text follows the grammar the existing parser was written against
(see ``docs/PARSER_README.md``), so the tests exercise the real parsing logic
rather than a simplified stand-in.
"""

from __future__ import annotations

import os
import textwrap
from datetime import datetime, timedelta
from typing import Dict, List, Optional

JOURNAL_HEADER = "[{date} {time} {ms:03d}][ATM][INF]> {message}"


def _stamp(moment: datetime, message: str, ms: int = 0) -> str:
    return JOURNAL_HEADER.format(date=moment.strftime("%d%m%Y"),
                                 time=moment.strftime("%H%M%S"),
                                 ms=ms, message=message)


def journal_text(transactions: int = 2,
                 start: Optional[datetime] = None,
                 card: str = "539157******0717",
                 amount: int = 50000,
                 status: str = "OK",
                 denomination: Optional[Dict[int, int]] = None) -> str:
    """Render a journal file holding ``transactions`` card sessions."""
    start = start or datetime(2026, 9, 10, 6, 42, 30)
    denomination = denomination or {5000: 10}
    lines: List[str] = []

    for index in range(transactions):
        moment = start + timedelta(minutes=index)
        lines += [
            _stamp(moment, "===================== Trx Started ====================="),
            _stamp(moment, f"-----Card Number : {card}"),
            _stamp(moment, "#SESSION-START#"),
            _stamp(moment, "-----Terminal ID : A0023011"),
            _stamp(moment, "-----APP NAME : VISA"),
            _stamp(moment, "#TRANSACTION-START#"),
            _stamp(moment, "-Amount Requsted --------------------"),
            _stamp(moment, f"---Amount        : {amount}"),
            _stamp(moment, "-Cash Withdraw Initiated -------------"),
            _stamp(moment, f"-----Amount : {amount}"),
            _stamp(moment, f"----AUX NO : {2868 + index} :xx:A0023011202609100642{index:02d}-02"),
            _stamp(moment, f"-----Withdraw Status : {status}"),
            _stamp(moment, "-----Account         : 539157XX..XX0717"),
            _stamp(moment, f"-----Response        : {'000' if status == 'OK' else '051'}"),
            _stamp(moment, f"-----Trace ID        : {559198 + index}"),
            _stamp(moment, "---Cash Withdraw Initiated "
                           + ("Completed" if status == "OK" else "Fail")),
        ]
        if status == "OK":
            lines += [
                _stamp(moment, "-Dispense Command Executed -----------"),
                _stamp(moment, "----Denomination"),
                _stamp(moment, "-----CU  TYP  VALUE   NUM"),
            ]
            for cassette, (value, count) in enumerate(sorted(denomination.items(), reverse=True),
                                                      start=1):
                lines.append(_stamp(moment, f"-----{cassette:02d}  RCY  {value:06d}  {count:03d}"))
            lines += [
                _stamp(moment, "---Dispense Succeeded"),
                _stamp(moment, "---Present Succeeded"),
                _stamp(moment, "---Cash Has Taken"),
            ]
        lines += [
            _stamp(moment, "#TRANSACTION-END#"),
            _stamp(moment, "============== Trx End:CardRemoved:9/10/2026 6:43:04 AM============="),
        ]
    return "\n".join(lines) + "\n"


def write_journal(path: str, **kwargs) -> str:
    """Write one synthetic journal file, creating parent folders."""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="latin-1") as handle:
        handle.write(journal_text(**kwargs))
    return path


def build_input_tree(root: str, atms: int = 2, files_per_atm: int = 2,
                     transactions: int = 2) -> List[str]:
    """``root/ATM00n/EJOURNAL_*.TXT`` - the layout the parser expects."""
    written: List[str] = []
    for atm in range(1, atms + 1):
        for index in range(files_per_atm):
            path = os.path.join(root, f"ATM{atm:03d}",
                                f"EJOURNAL_{10 + index:02d}092026_00.TXT")
            written.append(write_journal(
                path,
                transactions=transactions,
                start=datetime(2026, 9, 10 + index, 6, 42, 30),
                card=f"5391{atm:02d}******{1000 + index:04d}"))
    return written


CONFIG_TEMPLATE = """
project:
  BASE_DIR: "{base_dir}"
  ETL_NAME: "atm_ejournal"
  ENVIRONMENT: "TEST"

encryption:
  MODULE: "encryptor_does_not_exist"
  FACTORY: "Encryptor"
  DECRYPT_METHOD: "get_decrypt_data"
  ENCRYPT_METHOD: "get_encrypt_data"
  PICKLE_PATH: "config/pickle/encryption.pkl"
  ALLOW_BUILTIN_FALLBACK: true

defaults:
  input:
    INPUT_PATH: "ATM_EJOURNALS"
    FILE_PATTERN: "*.TXT,*.txt"
    BATCH_SIZE: {batch_size}
    MAX_BATCHES_PER_RUN: 0
    PRESCAN_ENABLED: true
    SNIFF_UNKNOWN_EXTENSIONS: false
    MIN_FILE_AGE_SECONDS: 0
    ATM_FOLDER_DEPTH: 1
    ENCODING: "latin-1"
  tracking:
    PROCESSED_FILES_CSV: "processed/processed_files.csv"
    PENDING_DIR: "processed/pending"
    RUN_SUMMARY_DIR: "processed/run_summary"
    FILE_KEY_MODE: "path"
  parquet:
    PARQUET_PATH: "parquet"
    PARQUET_COMPRESSION: "snappy"
    PARQUET_COALESCE_PARTITIONS: 1
    PARQUET_CLEANUP_ENABLED: true
    PARQUET_KEEP_FAILED_BATCHES: true
    PARQUET_FAILED_RETENTION_DAYS: 7
  logging:
    LOG_PATH: "logs"
    LOG_LEVEL: "INFO"
    CONSOLE_LOG_LEVEL: "ERROR"
    LOG_FILE_PREFIX: "atm_ejournal_etl"
    BATCH_LOG_FILE_PREFIX: "batch"
    LOG_FORMAT: "%(asctime)s %(levelname)-8s %(name)s [%(batch_id)s] %(message)s"
    LOG_RETENTION_DAYS: 365
    MAX_LOG_SIZE_GB: 1
    LOG_CLEANUP_ENABLED: true
  spark:
    SPARK_APP_NAME: "atm_ejournal_etl_test"
    SPARK_MASTER: "local[2]"
    SPARK_JARS: ""
    SPARK_EXECUTOR_INSTANCES: "1"
    SPARK_EXECUTOR_MEMORY: "1g"
    SPARK_EXECUTOR_CORES: "2"
    SPARK_CORES_MAX: "2"
    SPARK_DRIVER_MEMORY: "1g"
    SPARK_LOCAL_DIR: ""
    SPARK_EXECUTOR_JAVA_OPTIONS: ""
    SPARK_DRIVER_JAVA_OPTIONS: ""
    SPARK_LOG_LEVEL: "ERROR"
    SPARK_PARSE_PARTITIONS: 2
    SPARK_SHUFFLE_PARTITIONS: "2"
    EXTRA_CONF:
      spark.sql.session.timeZone: "UTC"
      spark.ui.enabled: "false"
  greenplum:
    GREENPLUM_HOST: "localhost"
    GREENPLUM_PORT: 5432
    GREENPLUM_DATABASE: "testdb"
    GREENPLUM_SCHEMA: "atm"
    GREENPLUM_TABLE: "atm_ejournal_withdrawals"
    GREENPLUM_STAGING_TABLE: "atm_ejournal_withdrawals_stg"
    GREENPLUM_CONTROL_TABLE: "atm_ejournal_batch_control"
    GREENPLUM_USER: "tester"
    GREENPLUM_PASSWORD: ""
    GREENPLUM_PICKLE: "config/pickle/encryption.pkl"
    GREENPLUM_DRIVER: "org.postgresql.Driver"
    GREENPLUM_WRITE_FORMAT: "jdbc"
    GREENPLUM_JDBC_BATCH_SIZE: 1000
    GREENPLUM_WRITE_PARTITIONS: 1
    GREENPLUM_LOAD_STRATEGY: "delete_insert_by_source_file"
    GREENPLUM_MERGE_KEYS: "ATM_NO,TRANSACTION_REF"
    GREENPLUM_TARGET_DISTRIBUTED_BY: ""
    GREENPLUM_CONTROL_DISTRIBUTED_BY: ""
    GREENPLUM_CREATE_OBJECTS: true
    GREENPLUM_QUERY_TIMEOUT_SECONDS: 60
  parser:
    KEEP_LAST_FAILURE: true
    LINK_FAILED_ACROSS_AMOUNTS: false
    RETRY_WINDOW_SECONDS: 180
    KEEP_UNPARSED_RECORDS: true
  mail:
    MAIL_ENABLED: false
    SMTP_HOST: "localhost"
    SMTP_PORT: 25
    SMTP_USE_TLS: false
    SMTP_USER: ""
    SMTP_PASSWORD: ""
    SMTP_PICKLE: "config/pickle/encryption.pkl"
    SMTP_TIMEOUT_SECONDS: 5
    MAIL_FROM: "etl@example.com"
    MAIL_TO: "ops@example.com"
    MAIL_CC: ""
    MAIL_SUBJECT_SUCCESS: "ATM E-Journal ETL SUCCESS"
    MAIL_SUBJECT_FAILURE: "ATM E-Journal ETL FAILED"
  airflow:
    AIRFLOW_DAG_ID: "atm_ejournal_etl"
    AIRFLOW_SCHEDULE: "0 2 * * *"
    AIRFLOW_START_DATE: "2026-01-01"
    AIRFLOW_CATCHUP: false
    AIRFLOW_RETRIES: 1
    AIRFLOW_RETRY_DELAY_MINUTES: 1
    AIRFLOW_EXECUTION_TIMEOUT_MINUTES: 30
    AIRFLOW_MAX_ACTIVE_RUNS: 1
    AIRFLOW_OWNER: "test"
    AIRFLOW_TAGS: "atm,test"
    AIRFLOW_PYTHON_BIN: "python3"
    AIRFLOW_SPARK_SUBMIT_BIN: ""
    AIRFLOW_ETL_HOME: "{base_dir}"

etls:
  atm_ejournal:
    input:
      BATCH_SIZE: {batch_size}
"""


def write_config(base_dir: str, batch_size: int = 2, extra_yaml: str = "") -> str:
    """
    Write a complete test configuration and return its path.

    ``extra_yaml`` is appended verbatim, which is how a test overrides a single
    section (for example a different ``PARQUET_CLEANUP_ENABLED``).
    """
    text = CONFIG_TEMPLATE.format(base_dir=base_dir, batch_size=batch_size) + extra_yaml
    path = os.path.join(base_dir, "config", "atm_ejournal.conf")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(textwrap.dedent(text))
    return path


class FakeGreenplumLoader:
    """
    Stand-in for :class:`greenplum_loader.GreenplumLoader`.

    Keeps the rows it was given in memory and mimics the batch control table, so
    the orchestration (ordering, tracking, recovery, cleanup) can be tested
    without a database. ``fail_on`` makes a given batch raise, ``crash_after``
    simulates the process dying *after* a successful commit.
    """

    def __init__(self, fail_on=None, crash_after=None):
        self.spark = None
        self.rows: List[dict] = []
        self.control: Dict[str, int] = {}
        self.fail_on = set(fail_on or [])
        self.crash_after = set(crash_after or [])
        self.load_calls: List[str] = []

    def load_batch(self, dataframe, batch_id, run_id, file_count=0, parquet_path="",
                   expected_rows=None):
        from greenplum_loader import GreenplumLoadError, LoadResult

        self.load_calls.append(batch_id)
        if batch_id in self.fail_on:
            raise GreenplumLoadError(f"simulated Greenplum failure for {batch_id}")

        collected = [row.asDict() for row in dataframe.collect()]
        # delete_insert_by_source_file semantics, so re-running a file is idempotent
        keys = {row.get("SOURCE_FILE_KEY") for row in collected}
        self.rows = [row for row in self.rows if row.get("SOURCE_FILE_KEY") not in keys]
        self.rows.extend(collected)
        self.control[f"{run_id}|{batch_id}"] = len(collected)

        if batch_id in self.crash_after:
            raise RuntimeError(f"simulated crash after the Greenplum commit for {batch_id}")

        return LoadResult(batch_id=batch_id, run_id=run_id, staged_rows=len(collected),
                          rows_loaded=len(collected), committed=True,
                          strategy="delete_insert_by_source_file")

    def is_batch_committed(self, run_id, batch_id):
        return self.control.get(f"{run_id}|{batch_id}")

    def count_batch_rows(self, run_id, batch_id):
        return self.control.get(f"{run_id}|{batch_id}", 0)
