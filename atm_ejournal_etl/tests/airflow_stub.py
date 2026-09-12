"""
Minimal Airflow test double.

Airflow is not a test dependency of this project, but the DAG file, its
configuration wiring and both e-mail callbacks must still be covered. This stub
provides just enough of ``airflow``, ``airflow.operators.bash`` and
``airflow.operators.python`` for the DAG module to import and be inspected.
"""

from __future__ import annotations

import sys
import types
from typing import Any, Dict, List


class StubBaseOperator:
    def __init__(self, task_id: str, **kwargs):
        self.task_id = task_id
        self.kwargs = kwargs
        self.upstream: List["StubBaseOperator"] = []
        self.downstream: List["StubBaseOperator"] = []
        dag = StubDag.current
        if dag is not None:
            dag.tasks[task_id] = self
            self.dag = dag

    def __rshift__(self, other):
        self.downstream.append(other)
        other.upstream.append(self)
        return other


class StubBashOperator(StubBaseOperator):
    def __init__(self, task_id, bash_command="", env=None, append_env=False,
                 do_xcom_push=False, **kwargs):
        super().__init__(task_id, **kwargs)
        self.bash_command = bash_command
        self.env = env or {}
        self.append_env = append_env
        self.do_xcom_push = do_xcom_push


class StubPythonOperator(StubBaseOperator):
    def __init__(self, task_id, python_callable=None, trigger_rule="all_success", **kwargs):
        super().__init__(task_id, **kwargs)
        self.python_callable = python_callable
        self.trigger_rule = trigger_rule


class StubDag:
    current = None

    def __init__(self, dag_id, schedule=None, default_args=None, **kwargs):
        if schedule is None and "schedule_interval" not in kwargs:
            raise TypeError("schedule is required")
        self.dag_id = dag_id
        self.schedule = schedule or kwargs.get("schedule_interval")
        self.default_args: Dict[str, Any] = default_args or {}
        self.kwargs = kwargs
        self.tasks: Dict[str, StubBaseOperator] = {}

    def __enter__(self):
        StubDag.current = self
        return self

    def __exit__(self, *exc_info):
        StubDag.current = None
        return False


def install() -> None:
    """Register the stub modules in ``sys.modules``."""
    airflow = types.ModuleType("airflow")
    airflow.DAG = StubDag

    operators = types.ModuleType("airflow.operators")
    bash_module = types.ModuleType("airflow.operators.bash")
    bash_module.BashOperator = StubBashOperator
    python_module = types.ModuleType("airflow.operators.python")
    python_module.PythonOperator = StubPythonOperator

    airflow.operators = operators
    operators.bash = bash_module
    operators.python = python_module

    sys.modules.update({
        "airflow": airflow,
        "airflow.operators": operators,
        "airflow.operators.bash": bash_module,
        "airflow.operators.python": python_module,
    })


def uninstall() -> None:
    for name in ("airflow.operators.python", "airflow.operators.bash",
                 "airflow.operators", "airflow"):
        sys.modules.pop(name, None)


class StubTaskInstance:
    def __init__(self, task_id="run_atm_ejournal_etl", xcom_value=None, try_number=1):
        self.task_id = task_id
        self.try_number = try_number
        self.log_url = "http://airflow/log"
        self.start_date = "2026-09-12 02:00:00"
        self._xcom_value = xcom_value

    def xcom_pull(self, task_ids=None):
        return self._xcom_value


class StubDagRun:
    def __init__(self, dag_id="atm_ejournal_etl", run_id="manual__2026-09-12T02:00:00"):
        self.dag_id = dag_id
        self.run_id = run_id
