"""Tests for the ETL app, plus a regression guard on the Monitoring pages.

The runner tests drive a **fixture ETL project** rather than the real one: the
supplied ETL needs PySpark, a Spark master, the Oracle Instant Client and live
databases, none of which belong in a test run. The fixture has the same shape
as ``etl_table_manual.py`` — a module-level ``logger`` hook, a ``STEP_MAP``
literal inside ``run_etl``, ``if etl_name == ...`` branches with the same
``ValueError`` guards, and a tqdm step bar — so discovery, process control,
step parsing, log capture, the single-run rule and stop are all exercised
against the real code paths.

Discovery, registry and validation are tested against the **real** ETL project
shipped in ``etl_project/``.
"""

from __future__ import annotations

import json
import os
import sys
import textwrap
import threading
import time
from pathlib import Path

from django.test import Client, TestCase, SimpleTestCase
from django.urls import reverse

from . import registry, validation
from .discovery import discover
from .runner import COMPLETED, FAILED, RUNNING, STOPPED, ETLBusy, ETLRunner
from .security import Redactor, mask_url_credentials, strip_sensitive
from .settings import BASE_DIR, EtlSettings

REAL_PROJECT = BASE_DIR / "etl_project"


def real_settings(**overrides) -> EtlSettings:
    base = dict(
        project_root=REAL_PROJECT,
        etl_module="etl_table_manual",
        etl_entrypoint="run_etl",
        python_executable=sys.executable,
        config_yaml="config/config.yaml",
        log_dir=REAL_PROJECT / "logs",
        history_size=50,
        log_buffer=4000,
        stop_timeout=10,
    )
    base.update(overrides)
    return EtlSettings(**base)


# ---------------------------------------------------------------------------
# Discovery — against the real ETL source
# ---------------------------------------------------------------------------

class DiscoveryTests(SimpleTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.discovery = discover(real_settings())

    def test_entry_point_is_found(self):
        self.assertTrue(self.discovery.ok, self.discovery.problems)
        self.assertEqual(self.discovery.entrypoint, "etl_table_manual.run_etl")
        self.assertEqual(self.discovery.problems, [])

    def test_all_seven_jobs_are_discovered(self):
        self.assertEqual(sorted(self.discovery.jobs), sorted([
            "csv_to_greenplum", "csv_to_oracle", "greenplum_read_to_parquet",
            "oracle_to_csv", "oracle_to_greenplum", "oracle_to_parquet",
            "parquet_to_greenplum",
        ]))
        for job in self.discovery.jobs.values():
            self.assertTrue(job.implemented, job.key)
            self.assertTrue(job.declared, job.key)

    def test_steps_come_from_the_step_map(self):
        self.assertEqual(
            self.discovery.jobs["oracle_to_csv"].steps,
            ["Build Spark", "Read Oracle", "Clean Columns", "Write CSV", "Finalize"])
        self.assertEqual(
            self.discovery.jobs["csv_to_greenplum"].steps,
            ["Build Spark", "Copy to Greenplum", "Finalize"])

    def test_required_parameters_come_from_the_value_error_guards(self):
        self.assertEqual(
            self.discovery.jobs["oracle_to_greenplum"].runtime_required,
            ["oracle_config_name", "query", "gp_url", "gp_table"])
        self.assertEqual(
            self.discovery.jobs["csv_to_oracle"].runtime_required,
            ["csv_input", "oracle_jdbc_url", "oracle_table", "oracle_user",
             "oracle_password"])

    def test_signature_defaults_are_read(self):
        params = self.discovery.parameters
        self.assertEqual(params["gp_user"], "gpadmin")
        self.assertEqual(params["gp_server_port"], "32768-42768")
        self.assertEqual(params["gp_mode"], "overwrite")
        self.assertIsNone(params["query"])

    def test_a_missing_project_reports_itself_rather_than_raising(self):
        result = discover(real_settings(project_root=Path("/nonexistent/etl")))
        self.assertFalse(result.ok)
        self.assertEqual(result.jobs, {})
        self.assertTrue(any("does not exist" in p for p in result.problems))


# ---------------------------------------------------------------------------
# Registry — form metadata layered on discovery
# ---------------------------------------------------------------------------

class RegistryTests(SimpleTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.discovery = discover(real_settings())

    def test_every_discovered_job_builds_a_form(self):
        for key in self.discovery.jobs:
            job = registry.build_job(key, self.discovery)
            self.assertTrue(job["available"], key)
            self.assertTrue(job["fields"], key)
            self.assertTrue(job["steps"], key)

    def test_required_flags_include_the_source_guards(self):
        job = registry.build_job("oracle_to_greenplum", self.discovery)
        required = {f["name"] for f in job["fields"] if f["required"]}
        self.assertTrue({"oracle_config_name", "query", "gp_url", "gp_table"}
                        <= required)

    def test_partition_column_is_required_even_though_the_source_omits_it(self):
        job = registry.build_job("oracle_to_csv", self.discovery)
        field = next(f for f in job["fields"] if f["name"] == "_column")
        self.assertTrue(field["required"])
        self.assertFalse(field["required_by_source"])
        self.assertTrue(job["notes"], "the exception must be documented in notes")

    def test_unpartitioned_greenplum_read_sends_an_empty_partition_column(self):
        kwargs = registry.to_run_etl_kwargs("greenplum_read_to_parquet", {
            "gp_url": "jdbc:postgresql://h:5432/d", "query": "select 1",
            "output_path": "/tmp/out", "parq_name": "a.parquet",
        })
        self.assertEqual(kwargs["p_column"], "")
        self.assertNotIn("lower_bound", kwargs)
        self.assertEqual(kwargs["etl_name"], "greenplum_read_to_parquet")

    def test_partitioned_greenplum_read_keeps_the_bounds(self):
        kwargs = registry.to_run_etl_kwargs("greenplum_read_to_parquet", {
            "gp_url": "jdbc:postgresql://h:5432/d", "query": "select 1",
            "output_path": "/tmp/out", "parq_name": "a.parquet",
            "use_partitioning": True, "p_column": "id",
            "lower_bound": "1", "upper_bound": "100",
        })
        self.assertEqual(kwargs["p_column"], "id")
        self.assertEqual(kwargs["upper_bound"], "100")
        self.assertNotIn("use_partitioning", kwargs, "UI-only field must not be sent")

    def test_signature_defaults_reach_the_kwargs(self):
        kwargs = registry.to_run_etl_kwargs("oracle_to_greenplum", {
            "oracle_config_name": "oracle_50", "query": "select 1",
            "_column": "ID", "gp_url": "jdbc:postgresql://h:5432/d",
            "gp_table": "public.t",
        })
        self.assertEqual(kwargs["gp_user"], "gpadmin")
        self.assertEqual(kwargs["gp_mode"], "overwrite")

    def test_password_fields_are_marked_sensitive(self):
        self.assertIn("gp_password", registry.sensitive_field_names())
        self.assertIn("oracle_password", registry.sensitive_field_names())
        self.assertIn("oracle_password",
                      registry.sensitive_field_names("csv_to_oracle"))


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

class ValidationTests(SimpleTestCase):
    def assert_error(self, key, values, field):
        with self.assertRaises(validation.ValidationError) as ctx:
            validation.validate(key, values)
        self.assertIn(field, ctx.exception.errors)

    def test_only_discovered_jobs_are_accepted(self):
        self.assert_error("rm -rf /", {}, "etl")
        self.assert_error("", {}, "etl")
        self.assert_error("oracle_to_csv;drop", {}, "etl")

    def test_missing_required_fields_are_reported_per_field(self):
        self.assert_error("oracle_to_csv", {}, "query")

    def test_unknown_parameter_names_are_rejected(self):
        self.assert_error("oracle_to_csv", {"__import__": "os"}, "__import__")

    def test_trailing_semicolon_is_rejected(self):
        self.assert_error("oracle_to_csv", {
            "oracle_config_name": "oracle_50", "query": "select 1;",
            "_column": "ID", "output_path": "/tmp/a.csv", "tmp_dir": "/tmp/t",
        }, "query")

    def test_url_patterns_are_enforced(self):
        self.assert_error("oracle_to_greenplum", {
            "oracle_config_name": "oracle_50", "query": "select 1",
            "_column": "ID", "gp_url": "ftp://nope", "gp_table": "t",
        }, "gp_url")

    def test_bounds_must_be_ordered(self):
        self.assert_error("greenplum_read_to_parquet", {
            "gp_url": "jdbc:postgresql://h:5432/d", "query": "select 1",
            "output_path": "/tmp/o", "parq_name": "a.parquet",
            "use_partitioning": True, "p_column": "id",
            "lower_bound": "100", "upper_bound": "1",
        }, "upper_bound")

    def test_a_complete_request_validates(self):
        job = validation.validate("oracle_to_csv", {
            "oracle_config_name": "oracle_50", "query": "select 1",
            "_column": "ID", "output_path": "/tmp/a.csv", "tmp_dir": "/tmp/t",
        })
        self.assertEqual(job["key"], "oracle_to_csv")


# ---------------------------------------------------------------------------
# Security
# ---------------------------------------------------------------------------

class SecurityTests(SimpleTestCase):
    def test_known_secrets_are_masked(self):
        self.assertNotIn("hunter22", Redactor(["hunter22"])("pw is hunter22"))

    def test_inline_credentials_are_masked_even_when_unknown(self):
        self.assertNotIn("s3cret", Redactor()("password=s3cret"))
        self.assertNotIn("s3cret", Redactor()("postgresql://u:s3cret@host/db"))

    def test_oracle_jdbc_urls_have_their_password_masked(self):
        masked = mask_url_credentials(
            "jdbc:oracle:thin:SCOTT/tiger@//host:1521/svc")
        self.assertNotIn("tiger", masked)
        self.assertIn("SCOTT", masked)
        self.assertIn("host:1521/svc", masked)

    def test_urls_without_credentials_are_untouched(self):
        url = "jdbc:postgresql://host:5432/db"
        self.assertEqual(mask_url_credentials(url), url)

    def test_credential_named_keys_are_stripped_even_if_unlisted(self):
        cleaned = strip_sensitive(
            {"gp_user": "u", "gp_password": "p", "some_token": "t"}, set())
        self.assertEqual(cleaned, {"gp_user": "u"})


# ---------------------------------------------------------------------------
# Step-bar parsing — real tqdm output lines
# ---------------------------------------------------------------------------

class ProgressParsingTests(SimpleTestCase):
    """The step strip must never invent, mislabel or skip a stage.

    Every line below is verbatim tqdm output produced by the ETL's own
    ``StepBar``, including the half-drawn states where the postfix has been
    set but the counter has not yet moved.
    """

    STEPS = ["Build Spark", "Read Oracle", "Clean Columns", "Write CSV",
             "Finalize"]

    def parse(self, *lines, steps=None):
        run = _bare_run(steps if steps is not None else self.STEPS)
        for line in lines:
            ETLRunner._apply_progress(run, _count(line), line)
        return run.step_index, run.step_name

    def test_the_opening_draw_is_not_read_as_a_step_name(self):
        index, name = self.parse(
            "oracle_to_csv:   0%|          | 0/5 [00:00<?, ?step/s]")
        self.assertEqual((index, name), (0, ""),
                         "'?step/s' is tqdm's rate, not a stage")

    def test_a_rate_is_never_reported_as_a_step_name(self):
        for rate in ("?step/s", "2.00step/s", " 1.5s/step"):
            _, name = self.parse(
                f"oracle_to_csv:  40%|####  | 2/5 [00:03<00:04, {rate}]")
            self.assertEqual(name, "Read Oracle", f"rate {rate!r} leaked")

    def test_the_stage_name_wins_over_a_lagging_counter(self):
        # StepBar sets the postfix before incrementing, so this half-drawn bar
        # says "1/5" while the ETL has actually moved on to Read Oracle.
        index, name = self.parse(
            "oracle_to_csv:  20%|##    | 1/5 [00:20<01:20,  1.0step/s, Read Oracle]")
        self.assertEqual((index, name), (2, "Read Oracle"))

    def test_the_counter_is_used_when_the_bar_carries_no_name(self):
        index, name = self.parse(
            "oracle_to_csv:  60%|######    | 3/5 [00:30<00:20,  1.0step/s]")
        self.assertEqual((index, name), (3, "Clean Columns"))

    def test_a_full_run_walks_every_declared_stage_in_order(self):
        seen = []
        run = _bare_run(self.STEPS)
        for i, step in enumerate(self.STEPS, start=1):
            for line in (
                f"oracle_to_csv: |{i - 1}/5 [00:00<00:00, 1.0step/s, {step}]",
                f"oracle_to_csv: |{i}/5 [00:00<00:00, 1.0step/s, {step}]",
            ):
                ETLRunner._apply_progress(run, _count(line), line)
            seen.append((run.step_index, run.step_name))
        self.assertEqual(seen, [(1, "Build Spark"), (2, "Read Oracle"),
                                (3, "Clean Columns"), (4, "Write CSV"),
                                (5, "Finalize")])

    def test_an_overrun_counter_is_clamped_to_the_declared_steps(self):
        # parquet_to_greenplum ticks four times against three declared stages.
        steps = ["Load CSV (Pandas)", "Copy to Greenplum", "Finalize"]
        index, name = self.parse(
            "parquet_to_greenplum: |4/3 [00:09<00:00,  2.0step/s]", steps=steps)
        self.assertEqual(index, 3)
        self.assertEqual(name, "Finalize")

    def test_an_unrecognised_name_does_not_move_the_strip_off_the_step_list(self):
        index, name = self.parse(
            "oracle_to_csv: |2/5 [00:03<00:04, 1.0step/s, Something Else]")
        self.assertEqual((index, name), (2, "Read Oracle"))


def _bare_run(steps):
    from .runner import Run
    return Run(id="t", etl_key="t", label="t", source="", destination="",
               steps=list(steps), params={}, triggered_by="t", log_path="")


def _count(line):
    import re
    match = re.search(r"\|\s*(\d+)\s*/\s*(\d+)\s*\[", line)
    return int(match.group(1)) if match else 0


# ---------------------------------------------------------------------------
# Fixture ETL project for the runner tests
# ---------------------------------------------------------------------------

FIXTURE_ETL = textwrap.dedent('''
    """Fixture with the same shape as etl_table_manual.py."""
    import sys
    import time
    from tqdm.auto import tqdm

    logger = None


    class StepBar:
        def __init__(self, etl_name, steps):
            self.steps = steps
            self.pbar = tqdm(total=len(steps), desc=etl_name, unit="step")

        def next(self, step_name):
            self.pbar.set_postfix_str(step_name)
            self.pbar.update(1)

        def close(self):
            self.pbar.close()


    def run_etl(etl_name, base_dir=None, source=None, target=None,
                secret=None, hold=0.05, gp_user="gpadmin"):
        etl_name = etl_name.lower().strip()

        STEP_MAP = {
            "fast_job": ["Alpha", "Beta", "Gamma"],
            "slow_job": ["Alpha", "Beta", "Gamma"],
            "broken_job": ["Alpha", "Beta"],
        }
        if etl_name not in STEP_MAP:
            raise ValueError(f"Unknown etl_name={etl_name}")

        bar = StepBar(etl_name, STEP_MAP[etl_name])
        try:
            if etl_name == "fast_job":
                if not (source and target):
                    raise ValueError("fast_job requires: source, target")
                for step in STEP_MAP[etl_name]:
                    bar.next(step)
                    print(f"working on {step} with secret={secret}")
                    time.sleep(float(hold))
                return {"status": "ok", "etl": etl_name, "target": target}

            if etl_name == "slow_job":
                if not (source and target):
                    raise ValueError("slow_job requires: source, target")
                bar.next("Alpha")
                print("slow job started", flush=True)
                time.sleep(600)
                return {"status": "ok"}

            if etl_name == "broken_job":
                if not source:
                    raise ValueError("broken_job requires: source")
                bar.next("Alpha")
                raise RuntimeError("the warehouse refused the connection")
        finally:
            bar.close()
''')


def fixture_job(key: str, steps: list[str]) -> dict:
    """The job description shape ``ETLRunner.submit`` expects."""
    return {"key": key, "label": key.replace("_", " ").title(),
            "source": "Fixture", "destination": "Fixture", "steps": steps}


class RunnerTestCase(TestCase):
    """Base class giving each test its own fixture project and runner."""

    steps = ["Alpha", "Beta", "Gamma"]

    def setUp(self):
        super().setUp()
        import tempfile
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        (root / "etl_table_manual.py").write_text(FIXTURE_ETL)
        self.settings_obj = real_settings(
            project_root=root, log_dir=root / "logs",
            python_executable=sys.executable, stop_timeout=15)
        self.runner = ETLRunner(self.settings_obj)
        self.addCleanup(self._stop_everything)

    def _stop_everything(self):
        try:
            self.runner.stop()
        except Exception:
            pass

    def wait_for(self, run, timeout=60):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if run.status != RUNNING:
                return run
            time.sleep(0.05)
        raise AssertionError(f"run {run.id} did not finish within {timeout}s")


class RunLifecycleTests(RunnerTestCase):
    def test_a_successful_run_completes_with_the_etl_result(self):
        run = self.runner.submit(
            fixture_job("fast_job", self.steps),
            {"etl_name": "fast_job", "source": "s", "target": "t"},
            "tester")
        self.wait_for(run)

        self.assertEqual(run.status, COMPLETED)
        self.assertEqual(run.exit_code, 0)
        self.assertEqual(run.result, {"status": "ok", "etl": "fast_job",
                                      "target": "t"})
        self.assertEqual(run.step_index, len(self.steps))
        self.assertIsNotNone(run.finished_at)
        self.assertIsNotNone(run.duration_seconds())
        self.assertEqual(run.triggered_by, "tester")

    def test_the_step_bar_of_the_etl_drives_the_step_state(self):
        run = self.runner.submit(
            fixture_job("fast_job", self.steps),
            {"etl_name": "fast_job", "source": "s", "target": "t", "hold": 0.4},
            "tester")
        seen = set()
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and run.status == RUNNING:
            if run.step_name:
                seen.add(run.step_name)
            time.sleep(0.05)
        self.wait_for(run)
        self.assertTrue(seen & set(self.steps),
                        f"no ETL step name was observed, saw {seen}")

    def test_the_run_is_written_to_a_log_file(self):
        run = self.runner.submit(
            fixture_job("fast_job", self.steps),
            {"etl_name": "fast_job", "source": "s", "target": "t"},
            "tester")
        self.wait_for(run)
        contents = Path(run.log_path).read_text()
        self.assertIn("working on Alpha", contents)
        self.assertIn(run.id, contents)

    def test_secrets_never_reach_the_log_or_the_history(self):
        run = self.runner.submit(
            fixture_job("fast_job", self.steps),
            {"etl_name": "fast_job", "source": "s", "target": "t",
             "secret": "topsecret123"},
            "tester", secret_names={"secret"}, secret_values=["topsecret123"])
        self.wait_for(run)

        self.assertNotIn("secret", run.params)
        self.assertNotIn("topsecret123", json.dumps(run.snapshot()))
        self.assertNotIn("topsecret123", Path(run.log_path).read_text())

    def test_history_records_the_finished_run(self):
        run = self.runner.submit(
            fixture_job("fast_job", self.steps),
            {"etl_name": "fast_job", "source": "s", "target": "t"}, "tester")
        self.wait_for(run)
        history = self.runner.history()
        self.assertEqual(len(history), 1)
        row = history[0]
        self.assertEqual(row["status"], COMPLETED)
        self.assertEqual(row["triggered_by"], "tester")
        self.assertEqual(row["log_path"], run.log_path)
        self.assertIsNotNone(row["finished_at"])


class FailureTests(RunnerTestCase):
    def test_an_etl_exception_is_reported_as_failed_with_its_message(self):
        run = self.runner.submit(
            fixture_job("broken_job", ["Alpha", "Beta"]),
            {"etl_name": "broken_job", "source": "s"}, "tester")
        self.wait_for(run)

        self.assertEqual(run.status, FAILED)
        self.assertEqual(run.error["type"], "RuntimeError")
        self.assertIn("warehouse refused", run.error["message"])
        self.assertNotEqual(run.exit_code, 0)

    def test_a_missing_etl_script_fails_before_anything_starts(self):
        runner = ETLRunner(real_settings(
            project_root=Path(self.tmp.name), log_dir=Path(self.tmp.name) / "logs",
            etl_module="not_a_real_module"))
        from .runner import ETLStartError
        with self.assertRaises(ETLStartError):
            runner.submit(fixture_job("fast_job", self.steps),
                          {"etl_name": "fast_job"}, "tester")
        # The slot must be free again after a failed start.
        self.assertIsNone(runner.active_run())
        self.assertIsNone(runner.foreign_lock())

    def test_a_missing_runtime_fails_before_anything_starts(self):
        runner = ETLRunner(real_settings(
            project_root=Path(self.tmp.name), log_dir=Path(self.tmp.name) / "logs",
            python_executable="/nonexistent/python"))
        from .runner import ETLStartError
        with self.assertRaises(ETLStartError) as ctx:
            runner.submit(fixture_job("fast_job", self.steps),
                          {"etl_name": "fast_job"}, "tester")
        self.assertIn("ETL_PYTHON", str(ctx.exception))

    def test_bad_parameters_are_reported_as_a_signature_mismatch(self):
        run = self.runner.submit(
            fixture_job("fast_job", self.steps),
            {"etl_name": "fast_job", "source": "s", "target": "t",
             "no_such_parameter": 1}, "tester")
        self.wait_for(run)
        self.assertEqual(run.status, FAILED)
        self.assertEqual(run.error["type"], "TypeError")


class SingleRunTests(RunnerTestCase):
    def test_a_second_run_is_refused_while_one_is_executing(self):
        first = self.runner.submit(
            fixture_job("slow_job", self.steps),
            {"etl_name": "slow_job", "source": "s", "target": "t"}, "tester")
        self._await_running(first)

        with self.assertRaises(ETLBusy) as ctx:
            self.runner.submit(
                fixture_job("fast_job", self.steps),
                {"etl_name": "fast_job", "source": "s", "target": "t"}, "other")
        self.assertIn(first.label, str(ctx.exception))
        self.assertEqual(ctx.exception.active["id"], first.id)
        self.assertEqual(len(self.runner.history()), 1, "no second run recorded")

    def test_the_lock_file_blocks_a_second_web_process_too(self):
        first = self.runner.submit(
            fixture_job("slow_job", self.steps),
            {"etl_name": "slow_job", "source": "s", "target": "t"}, "tester")
        self._await_running(first)

        # A different ETLRunner instance stands in for another gunicorn worker:
        # it shares nothing in memory, only the lock file on disk.
        other_worker = ETLRunner(self.settings_obj)
        self.assertIsNone(other_worker.active_run())
        self.assertIsNotNone(other_worker.foreign_lock())
        with self.assertRaises(ETLBusy):
            other_worker.submit(
                fixture_job("fast_job", self.steps),
                {"etl_name": "fast_job", "source": "s", "target": "t"}, "other")

    def test_concurrent_submissions_leave_exactly_one_winner(self):
        results: list = []
        barrier = threading.Barrier(4)

        def attempt():
            barrier.wait()
            try:
                results.append(self.runner.submit(
                    fixture_job("fast_job", self.steps),
                    {"etl_name": "fast_job", "source": "s", "target": "t",
                     "hold": 0.3}, "racer"))
            except ETLBusy:
                results.append(None)

        threads = [threading.Thread(target=attempt) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)

        started = [r for r in results if r is not None]
        self.assertEqual(len(started), 1, "exactly one run may start")
        self.wait_for(started[0])

    def test_the_slot_is_free_again_once_a_run_finishes(self):
        first = self.runner.submit(
            fixture_job("fast_job", self.steps),
            {"etl_name": "fast_job", "source": "s", "target": "t"}, "tester")
        self.wait_for(first)
        self.assertIsNone(self.runner.active_run())
        self.assertIsNone(self.runner.foreign_lock())
        self.assertFalse(self.runner._lockfile.path.exists())

        second = self.runner.submit(
            fixture_job("fast_job", self.steps),
            {"etl_name": "fast_job", "source": "s", "target": "t"}, "tester")
        self.wait_for(second)
        self.assertEqual(second.status, COMPLETED)

    def _await_running(self, run, timeout=30):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if run.pid and run.status == RUNNING:
                return
            time.sleep(0.05)
        raise AssertionError("the run never reached the running state")


class StopTests(RunnerTestCase):
    def test_stop_terminates_the_process_and_marks_the_run_stopped(self):
        run = self.runner.submit(
            fixture_job("slow_job", self.steps),
            {"etl_name": "slow_job", "source": "s", "target": "t"}, "tester")
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and not run.pid:
            time.sleep(0.05)
        pid = run.pid
        self.assertIsNotNone(pid)

        report = self.runner.stop()
        self.assertTrue(report["ok"], report)
        self.wait_for(run, timeout=30)

        self.assertEqual(run.status, STOPPED)
        self.assertIsNone(run.error)
        self.assertIsNotNone(run.finished_at)
        self.assertFalse(_pid_alive(pid), "the ETL process is still running")
        self.assertIsNone(self.runner.active_run())
        self.assertFalse(self.runner._lockfile.path.exists())

    def test_stopping_when_nothing_runs_is_reported_not_crashed(self):
        report = self.runner.stop()
        self.assertFalse(report["ok"])
        self.assertIn("No ETL run", report["error"])

    def test_a_stopped_run_appears_in_history_as_stopped(self):
        run = self.runner.submit(
            fixture_job("slow_job", self.steps),
            {"etl_name": "slow_job", "source": "s", "target": "t"}, "tester")
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and not run.pid:
            time.sleep(0.05)
        self.runner.stop()
        self.wait_for(run, timeout=30)
        self.assertEqual(self.runner.history()[0]["status"], STOPPED)


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------

class EtlViewTests(TestCase):
    def setUp(self):
        self.client = Client()

    def test_the_console_page_renders(self):
        response = self.client.get(reverse("etl:index"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Current execution")
        self.assertContains(response, "Execution history")
        self.assertContains(response, "csrfmiddlewaretoken")

    def test_the_jobs_api_lists_the_discovered_jobs(self):
        data = self.client.get(reverse("etl:api_jobs")).json()
        self.assertTrue(data["ok"])
        self.assertEqual(len(data["jobs"]), 7)
        self.assertEqual(data["entrypoint"], "etl_table_manual.run_etl")

    def test_the_config_api_never_returns_a_credential(self):
        """Nothing secret in config.yaml may reach the browser.

        The values are read out of the live configuration rather than written
        down here — a test that hard-codes a password publishes it.
        """
        payload = self.client.get(reverse("etl:api_config")).json()
        self.assert_no_credentials(payload)

    def test_the_jobs_api_never_returns_a_credential(self):
        self.assert_no_credentials(self.client.get(reverse("etl:api_jobs")).json())

    def assert_no_credentials(self, payload):
        # Structural: no key that names a credential, at any depth. Holds even
        # when no configuration file is present.
        for key in _walk_keys(payload):
            self.assertNotRegex(
                key, r"(?i)(password|passwd|pwd|secret|token|api[_-]?key)",
                f"the response carries a {key!r} field")
        # Value-level: nothing that is actually a credential in this config.
        # assertNotIn would print the secret in its failure message, so the
        # check is spelled out with assertFalse instead.
        body = json.dumps(payload)
        for secret in _configured_secrets():
            self.assertFalse(
                secret in body,
                "a credential from config.yaml reached the response")

    def test_running_an_unknown_job_is_rejected(self):
        response = self.client.post(
            reverse("etl:api_run"),
            data=json.dumps({"etl": "; rm -rf /", "values": {}}),
            content_type="application/json")
        self.assertEqual(response.status_code, 400)
        self.assertIn("etl", response.json()["errors"])

    def test_running_with_missing_parameters_is_rejected(self):
        response = self.client.post(
            reverse("etl:api_run"),
            data=json.dumps({"etl": "oracle_to_csv", "values": {}}),
            content_type="application/json")
        self.assertEqual(response.status_code, 400)
        self.assertIn("query", response.json()["errors"])

    def test_a_malformed_body_is_rejected(self):
        response = self.client.post(reverse("etl:api_run"), data="not json",
                                    content_type="application/json")
        self.assertEqual(response.status_code, 400)

    def test_get_is_not_accepted_for_state_changing_endpoints(self):
        for name in ("etl:api_run", "etl:api_stop", "etl:api_validate"):
            self.assertEqual(self.client.get(reverse(name)).status_code, 405,
                             name)

    def test_posts_without_a_csrf_token_are_refused(self):
        enforcing = Client(enforce_csrf_checks=True)
        response = enforcing.post(
            reverse("etl:api_run"),
            data=json.dumps({"etl": "oracle_to_csv", "values": {}}),
            content_type="application/json")
        self.assertEqual(response.status_code, 403)

    def test_the_status_endpoint_reports_an_idle_backend(self):
        data = self.client.get(reverse("etl:api_status")).json()
        self.assertIsNone(data["run"])
        self.assertFalse(data["state"]["busy"])

    def test_an_unknown_run_id_is_a_404_not_a_crash(self):
        response = self.client.get(reverse("etl:api_status"), {"run": "deadbeef"})
        self.assertEqual(response.status_code, 404)

    def test_a_log_cannot_be_fetched_for_an_unknown_run(self):
        response = self.client.get(reverse("etl:api_log"), {"run": "../../etc/passwd"})
        self.assertEqual(response.status_code, 404)

    def test_stopping_nothing_is_a_conflict_not_an_error(self):
        response = self.client.post(reverse("etl:api_stop"), data="{}",
                                    content_type="application/json")
        self.assertEqual(response.status_code, 409)


def _walk_keys(node):
    """Every mapping key anywhere in a JSON-shaped structure."""
    if isinstance(node, dict):
        for key, value in node.items():
            yield str(key)
            yield from _walk_keys(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk_keys(item)


def _configured_secrets() -> list[str]:
    """Every credential the live config.yaml can yield: the stored ciphertexts,
    the Fernet key itself, the passwords embedded in the Oracle JDBC URLs, and
    — the one that matters most — the **decrypted** plaintext of each stored
    password, since that is what a leak would actually expose.

    Reading them at run time rather than listing them here is the point: a
    test that hard-codes a password publishes it to everyone who can read the
    repository. Returns an empty list when no configuration is present, so a
    fresh clone still runs the structural half of the check.
    """
    import re

    path = real_settings().config_yaml_path
    if not path.exists():
        return []
    try:
        import yaml
        data = yaml.safe_load(path.read_text()) or {}
    except Exception:
        return []

    secrets: list[str] = []
    for block in (data or {}).values():
        if not isinstance(block, dict):
            continue
        for key in ("password", "passwd"):
            value = block.get(key)
            if isinstance(value, str) and len(value) >= 6:
                secrets.append(value)
        url = block.get("url")
        if isinstance(url, str):
            match = re.search(r":([^/\s:@]+)/([^@\s/]+)@", url)
            if match and len(match.group(2)) >= 4:
                secrets.append(match.group(2))
        key_file = block.get("pickle")
        if key_file:
            key_path = Path(key_file)
            if not key_path.is_absolute():
                key_path = real_settings().project_root / key_path
            if key_path.exists():
                secrets.append(key_path.read_bytes().decode("ascii", "ignore"))
                plain = _decrypt(key_path, block.get("password"))
                if plain:
                    secrets.append(plain)
    return [s for s in secrets if s]


def _decrypt(key_path: Path, ciphertext) -> str:
    """Plaintext of one stored password, or "" when it cannot be recovered."""
    if not isinstance(ciphertext, str) or not ciphertext:
        return ""
    try:
        from cryptography.fernet import Fernet
        return Fernet(key_path.read_bytes()).decrypt(ciphertext.encode()).decode()
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Regression guard: the Monitoring application must keep working
# ---------------------------------------------------------------------------

class PlatformIntegrationTests(TestCase):
    def setUp(self):
        self.client = Client()

    def test_the_landing_page_offers_monitoring_and_etl(self):
        response = self.client.get(reverse("portal:index"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Monitoring")
        self.assertContains(response, "ETL")
        self.assertContains(response, reverse("home:index"))
        self.assertContains(response, reverse("etl:index"))

    def test_the_monitoring_launcher_still_renders(self):
        response = self.client.get(reverse("home:index"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Disk Usage")

    def test_the_disk_usage_page_still_renders(self):
        response = self.client.get(reverse("disk_usage:index"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Share of level")

    def test_the_disk_usage_urls_are_unchanged(self):
        self.assertEqual(reverse("disk_usage:index"), "/disk-usage/")
        self.assertEqual(reverse("disk_usage:api_directories"),
                         "/disk-usage/api/directories/")
        self.assertEqual(reverse("disk_usage:api_history"),
                         "/disk-usage/api/history/")

    def test_every_page_shares_the_platform_navigation(self):
        for name in ("portal:index", "home:index", "disk_usage:index",
                     "etl:index"):
            response = self.client.get(reverse(name))
            self.assertContains(response, 'class="topnav"', msg_prefix=name)
            self.assertContains(response, reverse("etl:index"), msg_prefix=name)
