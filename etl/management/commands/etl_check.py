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

from etl import etl_config
from etl.discovery import discover
from etl.registry import build_job
from etl.runner import RUNNER
from etl.settings import SETTINGS

OK, WARN, BAD = "ok", "warning", "error"
MARK = {OK: "  ok  ", WARN: " warn ", BAD: " FAIL "}


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
