"""``manage.py spark_check`` — can the ETL reach the Spark master?

Runs the ETL run minus the ETL: a session built by the project's own
``build_spark`` from ``config/config.yaml``, then one trivial job that needs a
real executor. Everything Spark uses comes from ``spark_properties`` in the
YAML — this command sets no Spark environment variable and passes no override.

Written for the case where a run fails with "All masters are unresponsive" and
nothing appears in the Spark master UI, which has three distinguishable
causes:

* the master's port is not reachable from this host at all;
* it is reachable, but the driver advertises an address the master cannot call
  back on, so registration never completes;
* it is reachable and routable, but the client's Spark version does not match
  the master's, so the registration message is rejected.

The checks below separate them.
"""

from __future__ import annotations

import json
import os
import re
import signal
import socket
import subprocess
import sys
import threading
import urllib.error
import urllib.request
from pathlib import Path

from django.core.management.base import BaseCommand

from etl import diagnostics, etl_config
from etl.settings import SETTINGS
from etl.spark_preflight import MARKER

OK, WARN, BAD = "ok", "warning", "error"
MARK = {OK: "  ok  ", WARN: " warn ", BAD: " FAIL "}

#: Spark's own default for the standalone master's web UI.
DEFAULT_UI_PORT = 8080
#: Spark renders the version in a span whose class list contains "version"
#: alongside layout classes, so the class attribute cannot be matched exactly.
_UI_VERSION = re.compile(r'class="[^"]*\bversion\b[^"]*"[^>]*>\s*([0-9][\w.\-]*)')

#: The name the preflight gives its application, used to find it on the master.
PROBE_APP_NAME = "ETL preflight"


class Command(BaseCommand):
    help = ("Check that a Spark session built from config.yaml reaches the "
            "master and gets an executor.")

    def add_arguments(self, parser):
        parser.add_argument(
            "--timeout", type=int, default=180,
            help="Seconds to allow. Spark gives up on registration after "
                 "about 60, so leave room for that plus JVM start-up "
                 "(default: 180).")
        parser.add_argument(
            "--ui-port", type=int, default=DEFAULT_UI_PORT,
            help=f"Master web UI port, used only to read the cluster's Spark "
                 f"version (default: {DEFAULT_UI_PORT}, Spark's own default).")
        parser.add_argument(
            "--no-ui", action="store_true",
            help="Skip the master web UI probe.")
        parser.add_argument(
            "--verbose-spark", action="store_true",
            help="Show Spark's own output as well as the findings.")

    # -- entry point -------------------------------------------------------
    def handle(self, *args, **options):
        self.failures = 0
        self.warnings = 0
        self.client_version = ""
        self.master_version = ""
        self.apps_before: set = set()

        master_url = etl_config.spark_master_url(SETTINGS)

        self._section("Configuration (config/config.yaml)")
        if not master_url:
            self._line(BAD, "master_url",
                       "spark_properties.master_url is missing — there is "
                       "nothing to connect to.")
            return self._verdict()
        self._line(OK, "master_url", master_url)

        self._section("Master reachability")
        endpoints = _parse_master(master_url)
        if not endpoints:
            self._line(WARN, "endpoint",
                       "not a spark:// standalone URL; skipping the TCP probe")
        for host, port in endpoints:
            ok, detail = _probe(host, port)
            self._line(OK if ok else BAD, f"{host}:{port}",
                       "accepted a connection" if ok else
                       f"{detail} — the driver cannot register, and no "
                       f"application will appear in the master UI.")

        ui_host = endpoints[0][0] if endpoints else ""
        if not options["no_ui"] and ui_host:
            self._master_ui(ui_host, options["ui_port"])

        self._section("Live session (build_spark from config.yaml)")
        self._run_preflight(options)

        if not options["no_ui"] and ui_host:
            self._section("Did the master see it?")
            self._master_saw_it(ui_host, options["ui_port"])

        self._section("Verdict")
        return self._verdict()

    # -- master web UI -----------------------------------------------------
    def _fetch(self, url: str, limit: int = 400_000) -> str | None:
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                return response.read(limit).decode("utf-8", "replace")
        except (urllib.error.URLError, OSError, ValueError):
            return None

    def _master_json(self, host: str, port: int) -> dict | None:
        body = self._fetch(f"http://{host}:{port}/json/")
        if not body:
            return None
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return None

    def _master_ui(self, host: str, port: int) -> None:
        page = self._fetch(f"http://{host}:{port}/")
        if page is None:
            self._line(WARN, "master UI",
                       f"http://{host}:{port}/ not readable. Pass --ui-port if "
                       f"the UI is not on {port}; without it the cluster's "
                       f"version and its view of the application cannot be "
                       f"checked.")
            return

        match = _UI_VERSION.search(page)
        if match:
            self.master_version = match.group(1)
            self._line(OK, "cluster Spark version", self.master_version)
        else:
            self._line(WARN, "cluster Spark version", "not found on the UI page")

        data = self._master_json(host, port)
        if data is None:
            return
        self.apps_before = {a.get("id") for a in data.get("activeapps", [])} | \
                           {a.get("id") for a in data.get("completedapps", [])}
        alive = data.get("aliveworkers", 0)
        self._line(OK if alive else BAD, "alive workers",
                   str(alive) if alive else
                   "0 — the master has no workers, so no executor can ever be "
                   "allocated and every application will sit waiting.")
        free = data.get("cores", 0) - data.get("coresused", 0)
        self._line(OK if free > 0 else WARN, "free cores", str(free))

    def _master_saw_it(self, host: str, port: int) -> None:
        """The question the symptom asks: did the application reach the master?

        Nothing in the master UI means the registration never arrived — a
        network or version problem. An application that appears and then fails
        means it did arrive, and the fault is downstream of registration.
        """
        data = self._master_json(host, port)
        if data is None:
            self._line(WARN, "master view", "the master UI was not readable")
            return

        apps = list(data.get("activeapps", [])) + list(data.get("completedapps", []))
        mine = [a for a in apps if a.get("name") == PROBE_APP_NAME
                and a.get("id") not in getattr(self, "apps_before", set())]
        if not mine:
            self._line(BAD, "master view",
                       f"the master never listed an application called "
                       f"'{PROBE_APP_NAME}'. The registration did not arrive, "
                       f"so this is not a driver call-back problem: it is the "
                       f"network route to the master, or a client/cluster "
                       f"Spark version mismatch.")
            return

        app = mine[-1]
        state = app.get("state", "?")
        self._line(OK if state in ("RUNNING", "FINISHED") else BAD,
                   "master view",
                   f"the master listed it as {app.get('id')} in state {state}. "
                   f"The registration DID arrive, so look downstream of it — "
                   f"the master UI shows its own reason.")

    # -- the probe itself --------------------------------------------------
    def _run_preflight(self, options) -> None:
        script = Path(__file__).resolve().parents[2] / "spark_preflight.py"
        command = [SETTINGS.python_executable, "-u", str(script),
                   str(SETTINGS.project_root), SETTINGS.etl_module]

        # Deliberately the same launch the runner uses, minus anything that
        # would configure Spark: the point is to test what a real run does.
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        root = str(SETTINGS.project_root)
        existing = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = f"{root}{os.pathsep}{existing}" if existing else root

        try:
            proc = subprocess.Popen(
                command, cwd=root, env=env, text=True,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                start_new_session=True)
        except OSError as exc:
            self._line(BAD, "probe", f"could not start: {exc}")
            return

        findings: list[dict] = []
        cause = None

        # The read loop below blocks until the pipe closes, which only happens
        # when the process exits — so the deadline has to be enforced from
        # outside it. A Spark session that neither registers nor gives up will
        # otherwise hang this command forever.
        expired = threading.Event()

        def on_timeout() -> None:
            expired.set()
            self._kill(proc)

        watchdog = threading.Timer(options["timeout"], on_timeout)
        watchdog.daemon = True
        watchdog.start()

        try:
            for raw in proc.stdout:
                line = raw.rstrip()
                if line.startswith(MARKER):
                    try:
                        findings.append(json.loads(line[len(MARKER):]))
                    except json.JSONDecodeError:
                        pass
                    continue
                found = diagnostics.scan(line)
                if found and diagnostics.better(found, cause):
                    cause = found
                if options["verbose_spark"] and line.strip():
                    self.stdout.write(f"        {line}")
            proc.wait()
        except KeyboardInterrupt:
            self._kill(proc)
            raise
        finally:
            watchdog.cancel()

        if expired.is_set():
            self._line(BAD, "probe",
                       f"still running after {options['timeout']}s — killed. "
                       f"A session that neither connects nor gives up usually "
                       f"means the master's port is filtered rather than "
                       f"closed, so the connection attempt never returns.")

        self._report(findings, cause, proc.returncode if not expired.is_set() else 0)

    def _kill(self, proc) -> None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except OSError:
            proc.kill()

    # -- reporting ---------------------------------------------------------
    def _report(self, findings: list[dict], cause, returncode) -> None:
        by_stage: dict[str, list[dict]] = {}
        for item in findings:
            by_stage.setdefault(item.get("stage", "?"), []).append(item)

        for item in by_stage.get("pyspark", []):
            if item.get("error"):
                self._line(BAD, "PySpark", item["error"])
                continue
            self.client_version = item.get("version", "")
            self._line(OK, "client PySpark", f"{self.client_version} "
                                             f"({item.get('location', '')})")
            if not item.get("spark_home"):
                self._line(OK, "SPARK_HOME",
                           "not set — PySpark uses the Spark bundled with the "
                           "package above. That is fine as long as its version "
                           "matches the cluster's.")

        for item in by_stage.get("config", []):
            if item.get("error"):
                self._line(BAD, "config.yaml", item["error"])
                continue
            for jar in item.get("jars", []):
                if not jar["exists"]:
                    self._line(BAD, "jar", f"missing: {jar['path']}")
                elif not jar["readable"]:
                    self._line(BAD, "jar",
                               f"not readable by this user: {jar['path']}")
                else:
                    self._line(OK, "jar", jar["path"])

        for item in by_stage.get("runtime", []):
            self._line(OK, "hostname", f"{item.get('hostname')} -> "
                                       f"{', '.join(item.get('local_addresses') or []) or 'no address'}")

        for item in by_stage.get("session", []):
            if item.get("error"):
                self._line(BAD, "session", item["error"])
            elif item.get("state") == "created":
                self._line(OK, "session", f"created, app {item.get('app_id')}")
                self._line(OK, "cluster reported version",
                           item.get("spark_version", "?"))
                host = item.get("driver_host", "")
                if item.get("driver_host_is_loopback"):
                    self._line(BAD, "spark.driver.host",
                               f"{host} — a loopback address. The master "
                               f"cannot call back to it, so registration never "
                               f"completes and no application appears in the "
                               f"UI. Fix this host's name resolution, or set "
                               f"spark.driver.host / spark.driver.bindAddress.")
                else:
                    self._line(OK, "spark.driver.host",
                               f"{host}:{item.get('driver_port', '?')}")

        for item in by_stage.get("tmpdir", []):
            path, setting = item.get("path"), item.get("setting")
            if not item.get("local"):
                self._line(OK, "executor java.io.tmpdir",
                           f"{path} (on the worker nodes — not checked here)")
            elif not item.get("exists"):
                self._line(BAD, "driver java.io.tmpdir",
                           f"{path} does not exist on this host, but "
                           f"{setting} points the driver JVM at it.")
            elif not item.get("writable"):
                self._line(BAD, "driver java.io.tmpdir",
                           f"{path} is not writable by this user.")
            else:
                self._line(OK, "driver java.io.tmpdir", path)

        for item in by_stage.get("job", []):
            if item.get("state") == "completed":
                self._line(OK if item.get("correct") else BAD, "probe job",
                           f"returned {item.get('result')} "
                           f"(expected {item.get('expected')})")
            elif item.get("state") == "failed":
                self._line(BAD, "probe job", item.get("error", "failed"))

        for item in by_stage.get("executors", []):
            count = item.get("count", 0)
            self._line(OK if count else WARN, "executors",
                       f"{count} registered")

        if self.client_version and self.master_version:
            if self.client_version.split(".")[:2] != self.master_version.split(".")[:2]:
                self._line(BAD, "version match",
                           f"client PySpark {self.client_version} against "
                           f"cluster Spark {self.master_version}. A standalone "
                           f"master refuses a registration from a different "
                           f"major/minor version, which reads as 'All masters "
                           f"are unresponsive' and shows nothing in the UI. "
                           f"Install pyspark=={self.master_version} into the "
                           f"ETL runtime.")
            else:
                self._line(OK, "version match",
                           f"client {self.client_version} / cluster "
                           f"{self.master_version}")

        if cause is not None:
            self._line(BAD, "likely cause", f"{cause.summary} {cause.hint}")

        if returncode not in (0, None) and not self.failures:
            self._line(BAD, "probe", f"exited with code {returncode}")

    def _verdict(self):
        self.stdout.write("")
        if self.failures:
            self.stdout.write(self.style.ERROR(
                f"{self.failures} problem(s), {self.warnings} warning(s). "
                f"Spark is not usable from this host yet — the ETL will fail "
                f"the same way."))
            raise SystemExit(1)
        if self.warnings:
            self.stdout.write(self.style.WARNING(
                f"Spark works from this host. {self.warnings} warning(s)."))
        else:
            self.stdout.write(self.style.SUCCESS(
                "Spark works from this host: the session registered with the "
                "master and a job ran on an executor."))

    # -- output helpers ----------------------------------------------------
    def _section(self, title: str) -> None:
        self.stdout.write("")
        self.stdout.write(self.style.MIGRATE_HEADING(title))

    def _line(self, level: str, label: str, detail: str) -> None:
        # A Py4J message carries its whole Java stack; the findings list stays
        # readable, and --verbose-spark still shows the full text.
        detail = (detail or "").strip().splitlines()[0] if detail else ""
        style = {OK: self.style.SUCCESS, WARN: self.style.WARNING,
                 BAD: self.style.ERROR}[level]
        if level == BAD:
            self.failures += 1
        elif level == WARN:
            self.warnings += 1
        self.stdout.write(f"  [{style(MARK[level])}] {label:24s} {detail}")


def _parse_master(url: str) -> list[tuple[str, int]]:
    if not url.startswith("spark://"):
        return []
    out = []
    for authority in url[len("spark://"):].split("/")[0].split(","):
        host, _, port = authority.strip().partition(":")
        if host:
            try:
                out.append((host, int(port or 7077)))
            except ValueError:
                continue
    return out


def _probe(host: str, port: int, timeout: float = 5.0) -> tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, ""
    except OSError as exc:
        return False, f"{exc.__class__.__name__}: {exc}"
