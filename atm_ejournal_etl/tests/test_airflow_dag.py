"""The Airflow DAG: it loads, it is configured from the file, and both e-mails work."""

from __future__ import annotations

import importlib.util
import json
import os
import sys

import pytest

import airflow_stub
import fixtures


@pytest.fixture
def dag_module(etl_home, config_path, monkeypatch):
    """Import ``dags/atm_ejournal_etl_dag.py`` against the test configuration."""
    airflow_stub.install()
    monkeypatch.setenv("ATM_ETL_HOME", etl_home)
    monkeypatch.setenv("ATM_ETL_CONFIG", config_path)
    monkeypatch.setenv("ATM_ETL_NAME", "atm_ejournal")

    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dag_path = os.path.join(project_dir, "dags", "atm_ejournal_etl_dag.py")
    spec = importlib.util.spec_from_file_location("atm_ejournal_etl_dag_under_test", dag_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    yield module
    sys.modules.pop(spec.name, None)
    airflow_stub.uninstall()


# --------------------------------------------------------------------------- #
# DAG structure
# --------------------------------------------------------------------------- #


def test_dag_loads_with_configured_values(dag_module):
    dag = dag_module.dag
    assert dag.dag_id == "atm_ejournal_etl"
    assert dag.schedule == "0 2 * * *"
    assert dag.default_args["retries"] == 1
    assert dag.default_args["retry_delay"].total_seconds() == 60
    assert dag.default_args["owner"] == "test"
    assert dag.kwargs["catchup"] is False
    assert dag.kwargs["max_active_runs"] == 1
    assert dag.kwargs["tags"] == ["atm", "test"]
    assert dag.kwargs["start_date"].year == 2026


def test_tasks_and_dependencies(dag_module, etl_home):
    dag = dag_module.dag
    assert set(dag.tasks) == {"run_atm_ejournal_etl", "notify_success"}
    run_task = dag.tasks["run_atm_ejournal_etl"]
    assert "run_etl.py" in run_task.bash_command
    assert "--etl atm_ejournal" in run_task.bash_command
    assert run_task.do_xcom_push is True
    assert run_task.env["ATM_ETL_HOME"] == etl_home
    assert run_task.env["ATM_ETL_RUN_ID"] == "{{ ts_nodash }}"
    assert dag.tasks["notify_success"] in run_task.downstream


def test_failure_callback_is_registered(dag_module):
    assert dag_module.dag.default_args["on_failure_callback"] is dag_module.send_failure_email
    # Airflow's own mailer stays off - the callbacks carry the ETL metrics
    assert dag_module.dag.default_args["email_on_failure"] is False


# --------------------------------------------------------------------------- #
# Callbacks
# --------------------------------------------------------------------------- #


def _write_summary(etl_home, **overrides):
    directory = os.path.join(etl_home, "processed", "run_summary")
    os.makedirs(directory, exist_ok=True)
    summary = {
        "run_id": "20260912T020000", "etl_name": "atm_ejournal", "environment": "TEST",
        "status": "SUCCESS", "start_time": "2026-09-12 02:00:00",
        "end_time": "2026-09-12 02:14:00", "duration_human": "00:14:00",
        "files_discovered": 10000, "files_previously_processed": 9000,
        "files_processed": 1000, "files_failed": 0, "batches_processed": 2,
        "records_processed": 5000, "records_loaded": 5000,
        "greenplum_table": "atm.atm_ejournal_withdrawals",
        "log_path": os.path.join(etl_home, "logs", "atm_ejournal_etl_20260912T020000.log"),
        "batches": [{"batch_id": "BATCH_0001", "file_count": 500, "records": 2500,
                     "rows_loaded": 2500, "duration_seconds": 61.2, "status": "SUCCESS"}],
    }
    summary.update(overrides)
    path = os.path.join(directory, "atm_ejournal_latest.json")
    with open(path, "w") as handle:
        json.dump(summary, handle)
    return path


def test_success_email_uses_metrics_from_the_etl(dag_module, etl_home, monkeypatch):
    summary_path = _write_summary(etl_home)
    sent = {}

    def capture(self, subject, body, to=None, cc=None):
        sent["subject"], sent["body"] = subject, body
        return True
    monkeypatch.setattr(dag_module.MailSender, "send", capture)

    status = dag_module.send_success_email(
        task_instance=airflow_stub.StubTaskInstance(
            xcom_value=f"some output\nRUN_SUMMARY_PATH={summary_path}"),
        dag_run=airflow_stub.StubDagRun(),
        logical_date="2026-09-12T02:00:00+00:00")

    assert status == "SUCCESS"
    assert "SUCCESS" in sent["subject"]
    body = sent["body"]
    for label, value in (("Files discovered", "10000"), ("Previously processed", "9000"),
                         ("Files processed", "1000"), ("Number of batches", "2"),
                         ("Records loaded into Greenplum", "5000"),
                         ("Total Execution Time", "00:14:00")):
        line = next(row for row in body.splitlines() if row.startswith(label + ":"))
        assert line.split(":", 1)[1].strip() == value
    assert "BATCH_0001" in body
    assert "atm.atm_ejournal_withdrawals" in body


def test_failure_email_contains_stage_batch_and_trace(dag_module, etl_home, monkeypatch):
    _write_summary(etl_home, status="FAILED", stage="greenplum_load", batch_id="BATCH_0007",
                   error="GreenplumLoadError: connection refused",
                   stack_trace="Traceback (most recent call last): ...")
    sent = {}

    def capture(self, subject, body, to=None, cc=None):
        sent["subject"], sent["body"] = subject, body
        return True
    monkeypatch.setattr(dag_module.MailSender, "send", capture)

    dag_module.send_failure_email({
        "task_instance": airflow_stub.StubTaskInstance(xcom_value=None),
        "dag_run": airflow_stub.StubDagRun(),
        "logical_date": "2026-09-12T02:00:00+00:00",
        "exception": RuntimeError("boom"),
    })

    body = sent["body"]
    assert "FAILED" in sent["subject"]
    assert "ATM E-Journal ETL FAILED" in body
    assert "greenplum_load" in body
    assert "BATCH_0007" in body
    assert "connection refused" in body
    assert "Traceback" in body
    assert "Log location" in body
    assert "password" not in body.lower()


def test_callbacks_survive_a_missing_summary(dag_module, monkeypatch):
    sent = {}
    monkeypatch.setattr(dag_module.MailSender, "send",
                        lambda self, subject, body, to=None, cc=None: sent.update(
                            subject=subject, body=body) or True)
    dag_module.send_failure_email({
        "task_instance": airflow_stub.StubTaskInstance(xcom_value=None),
        "dag_run": airflow_stub.StubDagRun(),
        "exception": RuntimeError("no summary written"),
    })
    assert "no summary written" in sent["body"]
