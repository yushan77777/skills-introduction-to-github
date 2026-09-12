"""
Airflow DAG for the ATM E-Journal ETL.

    run_atm_ejournal_etl  ->  notify_success

* ``run_atm_ejournal_etl`` shells out to ``src/run_etl.py``, which does the batch
  processing (text -> Spark -> parquet -> Greenplum -> tracking CSV) and writes a
  run-summary JSON. The task's last stdout line - ``RUN_SUMMARY_PATH=...`` -
  arrives in XCom.
* ``notify_success`` reads that summary and sends the success e-mail with the
  metrics the ETL actually recorded (files discovered, processed, records
  loaded, per-batch table).
* Any task failure triggers ``send_failure_email`` through ``on_failure_callback``.

Every schedule/retry/ownership value comes from the ``airflow:`` section of
``config/atm_ejournal.conf``; nothing about the environment is hard-coded here.
Deploy by copying (or symlinking) this file into ``$AIRFLOW_HOME/dags`` and
setting ``ATM_ETL_HOME`` (project root) and ``ATM_ETL_CONFIG`` in the Airflow
environment.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# --------------------------------------------------------------------------- #
# Locate the project and load its configuration
# --------------------------------------------------------------------------- #

ETL_HOME = os.environ.get(
    "ATM_ETL_HOME",
    os.path.dirname(os.path.dirname(os.path.abspath(os.path.realpath(__file__)))))
SRC_DIR = os.path.join(ETL_HOME, "src")
CONFIG_PATH = os.environ.get("ATM_ETL_CONFIG", os.path.join(ETL_HOME, "config",
                                                            "atm_ejournal.conf"))
ETL_NAME = os.environ.get("ATM_ETL_NAME", "atm_ejournal")

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from config_loader import load_config                                  # noqa: E402
from mail_util import MailSender, format_failure_body                  # noqa: E402

CONFIG = load_config(CONFIG_PATH, ETL_NAME, validate=False)


def _start_date() -> datetime:
    raw = str(CONFIG.get("airflow.AIRFLOW_START_DATE", "2026-01-01"))
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return datetime(2026, 1, 1)


# --------------------------------------------------------------------------- #
# Callbacks
# --------------------------------------------------------------------------- #


def _summary_for(context) -> dict:
    """Best-effort read of the run summary produced by the ETL process."""
    path = ""
    task_instance = context.get("task_instance") or context.get("ti")
    if task_instance is not None:
        try:
            pushed = task_instance.xcom_pull(task_ids="run_atm_ejournal_etl")
            if pushed and "RUN_SUMMARY_PATH=" in str(pushed):
                path = str(pushed).strip().splitlines()[-1].split("RUN_SUMMARY_PATH=", 1)[1].strip()
        except Exception:                              # noqa: BLE001 - notification must not fail
            path = ""
    if not path:
        directory = CONFIG.path("tracking.RUN_SUMMARY_DIR", "processed/run_summary")
        path = os.path.join(directory, f"{CONFIG.etl_name}_latest.json")
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:                                  # noqa: BLE001
        return {}


def send_failure_email(context) -> None:
    """``on_failure_callback``: mail the details needed to start troubleshooting."""
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    summary = _summary_for(context)
    exception = context.get("exception")

    payload = {
        "dag_id": getattr(dag_run, "dag_id", CONFIG.get("airflow.AIRFLOW_DAG_ID")),
        "run_id": getattr(dag_run, "run_id", ""),
        "execution_date": str(context.get("logical_date") or context.get("execution_date") or ""),
        "task_id": getattr(task_instance, "task_id", ""),
        "try_number": getattr(task_instance, "try_number", ""),
        "etl_name": CONFIG.etl_name,
        "environment": CONFIG.environment,
        "start_time": summary.get("start_time") or str(getattr(task_instance, "start_date", "")),
        "failure_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "stage": summary.get("stage"),
        "batch_id": summary.get("batch_id"),
        "files_discovered": summary.get("files_discovered"),
        "files_processed": summary.get("files_processed"),
        "files_failed": summary.get("files_failed"),
        "batches_processed": summary.get("batches_processed"),
        "error": summary.get("error") or (str(exception) if exception else None),
        "stack_trace": summary.get("stack_trace"),
        "log_path": summary.get("log_path") or CONFIG.path("logging.LOG_PATH", "logs"),
        "airflow_log_url": getattr(task_instance, "log_url", ""),
    }
    MailSender(CONFIG).send(
        str(CONFIG.get("mail.MAIL_SUBJECT_FAILURE", "ATM E-Journal ETL FAILED"))
        + f" | {payload['dag_id']} | {payload['execution_date']}",
        format_failure_body(payload))


def send_success_email(**context) -> str:
    """Success mail built from the ETL's own run summary."""
    summary = _summary_for(context)
    dag_run = context.get("dag_run")
    summary.setdefault("etl_name", CONFIG.etl_name)
    summary.setdefault("environment", CONFIG.environment)
    summary["dag_id"] = getattr(dag_run, "dag_id", CONFIG.get("airflow.AIRFLOW_DAG_ID"))
    summary["execution_date"] = str(context.get("logical_date")
                                    or context.get("execution_date") or "")
    if not summary:
        summary = {"status": "SUCCESS", "dag_id": summary.get("dag_id")}

    MailSender(CONFIG).send(
        str(CONFIG.get("mail.MAIL_SUBJECT_SUCCESS", "ATM E-Journal ETL SUCCESS"))
        + f" | {summary.get('dag_id')} | {summary.get('execution_date')}",
        _success_body(summary))
    return summary.get("status", "SUCCESS")


def _success_body(summary: dict) -> str:
    from mail_util import format_success_body                          # noqa: PLC0415
    return format_success_body(summary)


# --------------------------------------------------------------------------- #
# DAG
# --------------------------------------------------------------------------- #

DEFAULT_ARGS = {
    "owner": str(CONFIG.get("airflow.AIRFLOW_OWNER", "data-engineering")),
    "depends_on_past": False,
    "retries": CONFIG.get_int("airflow.AIRFLOW_RETRIES", 2),
    "retry_delay": timedelta(minutes=CONFIG.get_int("airflow.AIRFLOW_RETRY_DELAY_MINUTES", 10)),
    "execution_timeout": timedelta(
        minutes=CONFIG.get_int("airflow.AIRFLOW_EXECUTION_TIMEOUT_MINUTES", 720)),
    "on_failure_callback": send_failure_email,
    # E-mail is sent by the callbacks above (they carry the ETL metrics), not by
    # Airflow's own email_on_failure.
    "email_on_failure": False,
    "email_on_retry": False,
}

PYTHON_BIN = str(CONFIG.get("airflow.AIRFLOW_PYTHON_BIN", "python3"))
SPARK_SUBMIT_BIN = str(CONFIG.get("airflow.AIRFLOW_SPARK_SUBMIT_BIN", "") or "")
RUN_COMMAND = (f"{SPARK_SUBMIT_BIN} {SRC_DIR}/run_etl.py" if SPARK_SUBMIT_BIN
               else f"{PYTHON_BIN} {SRC_DIR}/run_etl.py")

BASH_COMMAND = (
    f"cd {ETL_HOME} && "
    f"{RUN_COMMAND} --config {CONFIG_PATH} --etl {ETL_NAME}"
)

TASK_ENVIRONMENT = {
    "ATM_ETL_HOME": ETL_HOME,
    "ATM_ETL_CONFIG": CONFIG_PATH,
    "ATM_ETL_NAME": ETL_NAME,
    "ATM_ETL_ENV": CONFIG.environment,
    "ATM_ETL_RUN_ID": "{{ ts_nodash }}",
    "ATM_ETL_DAG_ID": str(CONFIG.get("airflow.AIRFLOW_DAG_ID", "atm_ejournal_etl")),
    "ATM_ETL_EXECUTION_DATE": "{{ ts }}",
    "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
    "JAVA_HOME": os.environ.get("JAVA_HOME", ""),
    "SPARK_HOME": os.environ.get("SPARK_HOME", ""),
    "PYTHONPATH": SRC_DIR,
}

_dag_kwargs = dict(
    dag_id=str(CONFIG.get("airflow.AIRFLOW_DAG_ID", "atm_ejournal_etl")),
    description="ATM E-Journal ETL: batched text -> Spark -> parquet -> Greenplum",
    default_args=DEFAULT_ARGS,
    start_date=_start_date(),
    catchup=CONFIG.get_bool("airflow.AIRFLOW_CATCHUP", False),
    max_active_runs=CONFIG.get_int("airflow.AIRFLOW_MAX_ACTIVE_RUNS", 1),
    tags=CONFIG.get_list("airflow.AIRFLOW_TAGS"),
)
_schedule = str(CONFIG.get("airflow.AIRFLOW_SCHEDULE", "0 2 * * *"))
try:                                                   # Airflow >= 2.4
    dag = DAG(schedule=_schedule, **_dag_kwargs)
except TypeError:                                      # pragma: no cover - Airflow 2.0-2.3
    dag = DAG(schedule_interval=_schedule, **_dag_kwargs)

with dag:
    run_etl_task = BashOperator(
        task_id="run_atm_ejournal_etl",
        bash_command=BASH_COMMAND,
        env=TASK_ENVIRONMENT,
        append_env=True,
        do_xcom_push=True,
    )

    notify_success_task = PythonOperator(
        task_id="notify_success",
        python_callable=send_success_email,
        trigger_rule="all_success",
    )

    run_etl_task >> notify_success_task
