"""``manage.py etl_check`` — verify the ETL integration on this server.

Run it after deploying, or whenever the ETL console reports something
unexpected. It exercises the same code the web application uses — discovery,
the configuration reader, the runtime paths and the run lock — and prints what
it found, without starting an ETL and without printing a credential.

Exit status is 0 when everything checks out and 1 when something is wrong, so
it can be used as a deployment gate.
"""

from __future__ import annotations

import os
from pathlib import Path

from django.core.management.base import BaseCommand

from etl import etl_config, runtime_env
from etl.discovery import discover
from etl.registry import build_job
from etl.runner import RUNNER
from etl.settings import SETTINGS

OK, WARN, BAD = "ok", "warning", "error"
MARK = {OK: "  ok  ", WARN: " warn ", BAD: " FAIL "}


def _parse_spark_master(url: str) -> list[tuple[str, int]]:
    """``spark://h1:7077,h2:7077`` -> ``[(h1, 7077), (h2, 7077)]``."""
    if not url.startswith("spark://"):
        return []
    out = []
    for authority in url[len("spark://"):].split("/")[0].split(","):
        authority = authority.strip()
        if not authority:
            continue
        host, _, port = authority.partition(":")
        try:
            out.append((host, int(port or 7077)))
        except ValueError:
            continue
    return out


def _probe(host: str, port: int, timeout: float = 4.0) -> tuple[bool, str]:
    import socket
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, ""
    except OSError as exc:
        return False, f"{exc.__class__.__name__}: {exc}"


class Command(BaseCommand):
    help = "Check the ETL project, runtime, configuration and run lock."

    def add_arguments(self, parser):
        parser.add_argument(
            "--verbose-jobs", action="store_true",
            help="List every parameter of every discovered job.")

    def handle(self, *args, **options):
        self.failures = 0
        self.warnings = 0

        self._section("Settings")
        self._line(OK, "Project root", str(SETTINGS.project_root))
        self._line(OK, "ETL module", f"{SETTINGS.etl_module}.{SETTINGS.etl_entrypoint}")
        self._line(OK, "ETL_PYTHON", SETTINGS.python_executable)
        self._line(OK, "Config YAML", str(SETTINGS.config_yaml_path))
        self._line(OK, "Log directory", str(SETTINGS.log_dir))

        self._section("Paths")
        self._check_path("ETL project root", SETTINGS.project_root, "dir")
        self._check_path("ETL entry point", SETTINGS.etl_module_path, "file")
        self._check_path("ETL runtime", Path(SETTINGS.python_executable), "exe")
        self._check_path("Configuration file", SETTINGS.config_yaml_path, "file")
        self._check_writable("Log directory", SETTINGS.log_dir)

        self._section("ETL runtime environment")
        self._check_environment()

        self._section("Job discovery")
        discovery = discover(SETTINGS)
        for problem in discovery.problems:
            self._line(WARN, "discovery", problem)
        if not discovery.ok:
            self._line(BAD, "result", "No runnable ETL job was found.")
        else:
            for key in discovery.jobs:
                job = build_job(key, discovery)
                status = OK if job["available"] else WARN
                self._line(status, job["key"],
                           f"{len(job['steps'])} steps, "
                           f"requires {', '.join(job['runtime_required']) or 'nothing'}")
                if options["verbose_jobs"]:
                    for field in job["fields"]:
                        flag = "*" if field["required"] else " "
                        self.stdout.write(
                            f"              {flag} {field['name']:22s} "
                            f"{field['type']}")

        self._section("Configuration")
        summary = etl_config.build_summary(SETTINGS).as_dict()
        if not summary["available"]:
            self._line(BAD, "config.yaml", summary["message"])
        else:
            self._line(OK, "spark_properties",
                       f"{len(summary['spark'])} settings")
            # A profile whose credentials do not work is a warning, not a
            # failure: the console still runs, and every other profile is
            # still usable. Only the jobs that name this profile would fail.
            for profile in summary["profiles"]:
                level = OK if profile["credential_level"] == "ok" else WARN
                self._line(level, profile["name"],
                           f"{profile['kind']}, {profile['endpoint']} — "
                           f"{profile['credential']}")

        self._section("Spark master")
        self._check_spark_master()

        self._section("Run slot")
        active = RUNNER.active_run()
        foreign = RUNNER.foreign_lock()
        if active is not None:
            self._line(WARN, "busy",
                       f"{active.label} is running (pid {active.pid}).")
        elif foreign is not None:
            self._line(WARN, "busy",
                       f"A run started by process {foreign.get('web_pid')} "
                       f"still holds the lock ({foreign.get('etl')}).")
        else:
            self._line(OK, "free", "No ETL run is in progress.")

        self.stdout.write("")
        if self.failures:
            self.stdout.write(self.style.ERROR(
                f"{self.failures} blocking problem(s), {self.warnings} warning(s). "
                f"The ETL console cannot run a job until these are fixed."))
            raise SystemExit(1)
        if self.warnings:
            self.stdout.write(self.style.WARNING(
                f"The ETL console is usable. {self.warnings} warning(s) — a "
                f"warning on a connection profile means jobs that name that "
                f"profile will fail when they try to connect."))
        else:
            self.stdout.write(self.style.SUCCESS("Everything checks out."))

    # -- checks -----------------------------------------------------------
    def _check_environment(self) -> None:
        """What the ETL subprocess will see.

        Spark is configured from ``config/config.yaml`` alone, so nothing here
        is expected to be set and nothing is reported as missing. The values
        are listed because they are still worth seeing when diagnosing a run —
        an unset SPARK_HOME simply means PySpark uses its own bundled Spark.
        Use ``manage.py spark_check`` to test the Spark configuration itself.
        """
        runtime = runtime_env.build(SETTINGS)
        for problem in runtime.problems:
            self._line(BAD, "environment source", problem)
        self._line(OK, "sources", ", ".join(runtime.sources))
        self._line(OK, "Spark configuration",
                   "read from config/config.yaml only — no environment "
                   "variable configures Spark")

        rows = dict(runtime_env.describe(runtime.values))
        for key in ("SPARK_HOME", "JAVA_HOME", "PYSPARK_PYTHON",
                    "SPARK_CONF_DIR", "SPARK_LOCAL_IP", "LD_LIBRARY_PATH"):
            self._line(OK, key, rows.get(key, "not set"))
        for key, value in rows.items():
            if key not in ("SPARK_HOME", "JAVA_HOME", "PYSPARK_PYTHON",
                           "SPARK_CONF_DIR", "SPARK_LOCAL_IP", "LD_LIBRARY_PATH"):
                self._line(OK, key, value if len(value) < 160
                           else value[:157] + "...")

    def _check_spark_master(self) -> None:
        """Can this host open a TCP connection to the configured master?

        "All masters are unresponsive" in a run log means registration never
        completed. This separates "cannot reach the port at all" (network,
        wrong URL, master down) from "reached it but registration was refused"
        (usually a Spark version mismatch between the client and the master).
        """
        url = etl_config.spark_master_url(SETTINGS)
        if not url:
            self._line(WARN, "master_url",
                       "not found in config.yaml spark_properties")
            return
        self._line(OK, "master_url", url)

        endpoints = _parse_spark_master(url)
        if not endpoints:
            self._line(OK, "reachability",
                       f"not a spark:// standalone URL — nothing to probe")
            return

        for host, port in endpoints:
            reachable, detail = _probe(host, port)
            if reachable:
                self._line(OK, f"{host}:{port}", "accepted a connection")
            else:
                self._line(BAD, f"{host}:{port}",
                           f"{detail}. The ETL will fail with 'All masters "
                           f"are unresponsive'.")

    # -- output helpers ---------------------------------------------------
    def _section(self, title: str) -> None:
        self.stdout.write("")
        self.stdout.write(self.style.MIGRATE_HEADING(title))

    def _line(self, level: str, label: str, detail: str) -> None:
        style = {OK: self.style.SUCCESS, WARN: self.style.WARNING,
                 BAD: self.style.ERROR}[level]
        if level == BAD:
            self.failures += 1
        elif level == WARN:
            self.warnings += 1
        self.stdout.write(f"  [{style(MARK[level])}] {label:22s} {detail}")

    def _check_path(self, label: str, path: Path, kind: str) -> None:
        if kind == "dir" and path.is_dir():
            return self._line(OK, label, "present")
        if kind == "file" and path.is_file():
            return self._line(OK, label, "present")
        if kind == "exe" and path.is_file() and os.access(path, os.X_OK):
            return self._line(OK, label, "present and executable")
        if kind == "exe" and path.is_file():
            return self._line(BAD, label, f"not executable: {path}")
        self._line(BAD, label, f"missing: {path}")

    def _check_writable(self, label: str, path: Path) -> None:
        target = path if path.exists() else path.parent
        if not target.exists():
            return self._line(BAD, label, f"no existing parent for {path}")
        if os.access(target, os.W_OK | os.X_OK):
            state = "writable" if path.exists() else f"will be created in {target}"
            return self._line(OK, label, state)
        self._line(BAD, label,
                   f"not writable by {os.environ.get('USER', 'this user')}: {target}")
