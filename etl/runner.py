"""Process control for the ETL: start one run, watch it, stop it hard.

Design rules, each one traceable to a requirement:

**One run at a time, enforced in the backend.**
    Two guards, not one. A :class:`threading.Lock` covers threads inside a
    worker process, and an exclusive lock *file* covers everything else — a
    second gunicorn worker, a second web process, or a run that outlived a
    restart of the web application. The lock file records the process group,
    so a stale lock is detected by asking the operating system whether that
    group still exists, never by trusting a flag.

**The backend is authoritative.**
    Status comes from the process: its output stream while it runs, its exit
    code when it ends. Nothing in the browser can set a job to running or
    finished.

**Stop means stop.**
    ``SIGKILL`` to the whole process group, immediately — not a graceful
    request the ETL may decline. The child is started with
    ``start_new_session=True`` precisely so that one signal reaches Spark's
    driver and every process it spawned, leaving no orphans behind.

**Progress is the ETL's own.**
    Step state is parsed out of the ``StepBar`` (tqdm) the ETL already writes
    and the log lines it already emits. No percentage is invented here.
"""

from __future__ import annotations

import errno
import json
import os
import re
import signal
import subprocess
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from . import diagnostics, runtime_env
from .security import Redactor, strip_sensitive
from .settings import SETTINGS, EtlSettings
from .worker import ERROR_PREFIX, RESULT_PREFIX

#: tqdm writes e.g.
#: ``oracle_to_csv:  40%|████  | 2/5 [00:03<00:04, 1.5step/s, Read Oracle]``
_TQDM_COUNT = re.compile(r"\|\s*(\d+)\s*/\s*(\d+)\s*\[")
#: The whole bar — description, percentage, bar, count and bracket — so that
#: anything the ETL printed after it can be separated out and kept. tqdm
#: redraws with \r and no trailing newline, so an ordinary print lands on the
#: same physical line as the bar; treating that line as "just progress" would
#: throw the message away.
#:
#: Anchored, because each captured segment begins where tqdm's \r put it: the
#: bar is always at the start, and only trailing text can follow it.
_TQDM_BAR = re.compile(
    r"^(?:.*?\d+%\s*\|[^|]*\|)?\s*\d+\s*/\s*\d+\s*\[[^\]]*\]")
_TQDM_TAIL = re.compile(r",\s*([^,\]]+)\]\s*$")
#: The trailing segment is tqdm's rate, not a step name, when it looks like
#: ``?step/s``, ``2.00step/s`` or ``1.5s/step``.
_TQDM_RATE = re.compile(r"^[\d.?]+\s*[a-zA-Z]*/[a-zA-Z]+$")
_ERROR_WORDS = re.compile(r"(?i)\b(error|exception|traceback|failed|fatal)\b")

RUNNING = "running"
COMPLETED = "completed"
FAILED = "failed"
STOPPED = "stopped"

TERMINAL_STATUSES = (COMPLETED, FAILED, STOPPED)


class ETLBusy(Exception):
    """Raised when a run is requested while another is still executing."""

    def __init__(self, message: str, active: dict | None = None):
        super().__init__(message)
        self.active = active or {}


class ETLStartError(Exception):
    """Raised when the ETL process could not be started at all."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _pgid_alive(pgid: int) -> bool:
    """Does this process group still exist? Signal 0 only checks, never kills."""
    try:
        os.killpg(pgid, 0)
        return True
    except OSError as exc:
        # EPERM means it exists but belongs to another user — still alive.
        return exc.errno == errno.EPERM


# --------------------------------------------------------------- lock file ---

class RunLock:
    """Exclusive, crash-safe "an ETL is running" marker on disk."""

    def __init__(self, path: Path):
        self.path = path
        self._held = False

    def read(self) -> dict | None:
        try:
            return json.loads(self.path.read_text())
        except (OSError, json.JSONDecodeError):
            return None

    def _clear_if_stale(self) -> dict | None:
        """Remove the lock when its process group is gone. Returns a live owner."""
        info = self.read()
        if info is None:
            # Unreadable or absent: if a file is there but unparseable it is
            # not protecting anything, so clear it.
            if self.path.exists():
                self.path.unlink(missing_ok=True)
            return None
        pgid = info.get("pgid")
        if isinstance(pgid, int) and _pgid_alive(pgid):
            return info
        self.path.unlink(missing_ok=True)
        return None

    def acquire(self, info: dict) -> None:
        """Create the lock exclusively, or raise :class:`ETLBusy`."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        for _ in range(2):
            try:
                handle = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except FileExistsError:
                owner = self._clear_if_stale()
                if owner is not None:
                    raise ETLBusy(
                        "Another ETL run is already in progress"
                        + (f" ({owner.get('etl')}, started {owner.get('started_at')})."
                           if owner.get("etl") else "."),
                        active=owner,
                    )
                continue  # the lock was stale; try once more
            with os.fdopen(handle, "w") as fh:
                json.dump(info, fh)
            self._held = True
            return
        raise ETLBusy("Could not take the ETL run lock; try again in a moment.")

    def update(self, **fields) -> None:
        if not self._held:
            return
        info = self.read() or {}
        info.update(fields)
        try:
            self.path.write_text(json.dumps(info))
        except OSError:
            pass

    def release(self) -> None:
        if self._held:
            self.path.unlink(missing_ok=True)
            self._held = False


# -------------------------------------------------------------------- job ---

@dataclass
class LogLine:
    seq: int
    ts: str
    text: str
    stream: str = "out"


@dataclass
class Run:
    """One ETL execution: what it is, how it is going, and how it ended."""

    id: str
    etl_key: str
    label: str
    source: str
    destination: str
    steps: list[str]
    params: dict                       # never contains a credential
    triggered_by: str
    log_path: str
    status: str = RUNNING
    started_at: str = ""
    finished_at: str | None = None
    step_index: int = 0
    step_name: str = ""
    result: dict | None = None
    error: dict | None = None
    #: The likely cause, recognised in the ETL's own output. Never replaces
    #: `error` — it explains it.
    root_cause: diagnostics.Finding | None = None
    exit_code: int | None = None
    pid: int | None = None
    logs: deque = field(default_factory=lambda: deque(maxlen=4000))
    _seq: int = 0
    _proc: subprocess.Popen | None = None
    _stop_requested: bool = False
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    # -- log capture ------------------------------------------------------
    def append(self, text: str, stream: str = "out") -> None:
        with self._lock:
            self._seq += 1
            self.logs.append(LogLine(
                seq=self._seq,
                ts=datetime.now(timezone.utc).strftime("%H:%M:%S"),
                text=text,
                stream=stream,
            ))

    # -- public views -----------------------------------------------------
    def duration_seconds(self) -> float | None:
        if not self.started_at:
            return None
        end = self.finished_at or _now()
        try:
            return round((datetime.fromisoformat(end)
                          - datetime.fromisoformat(self.started_at)).total_seconds(), 1)
        except ValueError:
            return None

    def summary(self) -> dict:
        """History row: everything the execution history table shows."""
        return {
            "id": self.id,
            "etl": self.etl_key,
            "label": self.label,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": self.duration_seconds(),
            "triggered_by": self.triggered_by,
            # The history column shows the cause when one was recognised: the
            # raw exception is usually the last symptom, not the fault.
            "error": (self.root_cause.summary if self.root_cause
                      else (self.error or {}).get("message", "")),
            "root_cause": self.root_cause.as_dict() if self.root_cause else None,
            "log_path": self.log_path,
            "step_index": self.step_index,
            "step_name": self.step_name,
            "step_count": len(self.steps),
        }

    def snapshot(self, since: int = 0) -> dict:
        with self._lock:
            lines = [vars(line) for line in self.logs if line.seq > since]
            cursor = self._seq
        data = self.summary()
        data.update({
            "source": self.source,
            "destination": self.destination,
            "steps": self.steps,
            "params": self.params,
            "result": self.result,
            "error_detail": self.error,
            "raw_error": (self.error or {}).get("message", ""),
            "exit_code": self.exit_code,
            "pid": self.pid,
            "logs": lines,
            "cursor": cursor,
            "active": self.status == RUNNING,
        })
        return data


# ---------------------------------------------------------------- manager ---

class ETLRunner:
    """Owns the single ETL slot and the in-memory execution history."""

    def __init__(self, settings: EtlSettings = SETTINGS):
        self.settings = settings
        self._runs: dict[str, Run] = {}
        self._order: deque[str] = deque(maxlen=max(settings.history_size, 5))
        self._active_id: str | None = None
        self._guard = threading.Lock()
        self._lockfile = RunLock(settings.log_dir / "etl-run.lock")

    # -- state ------------------------------------------------------------
    def active_run(self) -> Run | None:
        run = self._runs.get(self._active_id or "")
        return run if run and run.status == RUNNING else None

    def foreign_lock(self) -> dict | None:
        """A run held by another process (or a previous web process), if any.

        Returns ``None`` when the slot is free — including when the lock file
        is stale, which this call cleans up.
        """
        if self.active_run() is not None:
            return None
        return self._lockfile._clear_if_stale()

    def get(self, run_id: str) -> Run | None:
        return self._runs.get(run_id)

    def history(self, limit: int = 25) -> list[dict]:
        with self._guard:
            ids = list(self._order)[-limit:][::-1]
        return [self._runs[i].summary() for i in ids if i in self._runs]

    # -- lifecycle --------------------------------------------------------
    def submit(
        self,
        job: dict,
        kwargs: dict,
        triggered_by: str,
        secret_names: Iterable[str] = (),
        secret_values: Iterable[str] = (),
    ) -> Run:
        """Start one ETL run, or raise :class:`ETLBusy` / :class:`ETLStartError`.

        The runner is deliberately ignorant of forms and field metadata. It is
        handed a validated job description, the exact keyword arguments to pass
        to the entry point, and the credential values to keep out of the logs.

        :param job: ``{key, label, source, destination, steps}`` — as produced
            by :func:`etl.registry.build_job` and already validated.
        :param kwargs: the exact ``run_etl`` keyword arguments, including
            ``etl_name``.
        :param triggered_by: who asked for this run, for the history table.
        :param secret_names: keys of *kwargs* to omit from the recorded
            parameters.
        :param secret_values: values to mask everywhere in the captured output.
        """
        job_key = job["key"]
        secrets = set(secret_names)

        with self._guard:
            active = self.active_run()
            if active is not None:
                raise ETLBusy(
                    f"'{active.label}' has been running since {active.started_at} "
                    f"and must finish or be stopped first.",
                    active=active.summary(),
                )

            run_id = uuid.uuid4().hex[:12]
            stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
            self.settings.log_dir.mkdir(parents=True, exist_ok=True)
            log_path = self.settings.log_dir / f"{stamp}-{job_key}-{run_id}.log"

            run = Run(
                id=run_id,
                etl_key=job_key,
                label=job["label"],
                source=job["source"],
                destination=job["destination"],
                steps=list(job["steps"]),
                params=strip_sensitive(
                    {k: v for k, v in kwargs.items() if k != "etl_name"}, secrets),
                triggered_by=triggered_by,
                log_path=str(log_path),
                started_at=_now(),
            )
            run.logs = deque(maxlen=self.settings.log_buffer)

            # Raises ETLBusy if another process holds the slot.
            self._lockfile.acquire({
                "run_id": run_id, "etl": job_key, "label": job["label"],
                "started_at": run.started_at, "triggered_by": triggered_by,
                "web_pid": os.getpid(), "pgid": os.getpgid(0),
                "log_path": str(log_path),
            })

            self._runs[run_id] = run
            self._order.append(run_id)
            self._active_id = run_id

        redactor = Redactor([str(v) for v in secret_values if v])
        payload = {
            "project_root": str(self.settings.project_root),
            "module": self.settings.etl_module,
            "entrypoint": self.settings.etl_entrypoint,
            "params": kwargs,
            # Reported by the worker at the top of every run log, so a
            # missing SPARK_HOME or a wrong PYSPARK_PYTHON is visible without
            # having to reproduce the failure.
            "env_report_keys": list(runtime_env.RUNTIME_ENV_KEYS),
        }

        try:
            self._start_process(run, payload)
        except ETLStartError:
            self._finish(run, FAILED)
            raise

        threading.Thread(target=self._watch, args=(run, redactor),
                         name=f"etl-{run.id}", daemon=True).start()
        return run

    def _start_process(self, run: Run, payload: dict) -> None:
        settings = self.settings
        if not settings.project_root.exists():
            run.error = {"type": "MissingProject", "message":
                         f"ETL project root not found: {settings.project_root}"}
            raise ETLStartError(run.error["message"])
        if not settings.etl_module_path.exists():
            run.error = {"type": "MissingScript", "message":
                         f"ETL entry point not found: {settings.etl_module_path}"}
            raise ETLStartError(run.error["message"])
        if not Path(settings.python_executable).exists():
            run.error = {"type": "MissingRuntime", "message":
                         f"ETL Python runtime not found: {settings.python_executable}. "
                         f"Set ETL_PYTHON to the interpreter that has PySpark."}
            raise ETLStartError(run.error["message"])

        # The ETL's own Linux environment — SPARK_HOME, JAVA_HOME,
        # PYSPARK_PYTHON, LD_LIBRARY_PATH for the Oracle client, and anything
        # else it needs. A service inherits almost none of this, so it is
        # rebuilt from the sources the administrator configured.
        runtime = runtime_env.build(settings)
        if runtime.problems:
            run.error = {"type": "EnvironmentError",
                         "message": runtime.problems[0]}
            for problem in runtime.problems:
                run.append(problem, stream="err")
            raise ETLStartError(run.error["message"])

        env = runtime.values
        env["PYTHONUNBUFFERED"] = "1"
        # tqdm suppresses a redraw that lands within `mininterval` (0.1s by
        # default) of the previous one, which silently drops the step bar's
        # first transitions. Turning the throttle off makes every stage the
        # ETL announces reach the UI. It changes only how often the existing
        # progress bar repaints — never what the ETL does.
        env.setdefault("TQDM_MININTERVAL", "0")
        # Prepend rather than replace: an environment script may legitimately
        # put its own directories on PYTHONPATH.
        existing = env.get("PYTHONPATH", "")
        root = str(settings.project_root)
        env["PYTHONPATH"] = f"{root}{os.pathsep}{existing}" if existing else root

        run.append(f"Starting {run.label} ({run.etl_key})")
        run.append(f"Log file: {run.log_path}")
        run.append(f"Environment from: {', '.join(runtime.sources)}")

        try:
            proc = subprocess.Popen(
                [settings.python_executable, "-u", str(settings.worker_path)],
                cwd=root,
                env=env,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                # Its own session and process group, so a stop reaches Spark
                # and every process the ETL started.
                start_new_session=True,
            )
        except OSError as exc:
            run.error = {"type": exc.__class__.__name__, "message":
                         f"Could not start the ETL process: {exc}"}
            run.append(run.error["message"], stream="err")
            raise ETLStartError(run.error["message"]) from exc

        run._proc = proc
        run.pid = proc.pid
        try:
            self._lockfile.update(pid=proc.pid, pgid=os.getpgid(proc.pid))
        except OSError:
            pass

        try:
            # Credentials travel on stdin, so they never appear in `ps`.
            proc.stdin.write(json.dumps(payload))
            proc.stdin.close()
        except (BrokenPipeError, OSError) as exc:
            run.append(f"Could not send the job payload to the ETL process: {exc}",
                       stream="err")

    def _watch(self, run: Run, redactor: Redactor) -> None:
        """Stream the ETL's own output to memory and to the run log file."""
        proc = run._proc
        log_file = None
        try:
            log_file = open(run.log_path, "w", encoding="utf-8", buffering=1)
            log_file.write(f"# {run.label} ({run.etl_key})\n")
            log_file.write(f"# run id {run.id}, started {run.started_at}, "
                           f"triggered by {run.triggered_by}\n")
        except OSError as exc:
            run.append(f"Could not open the run log file: {exc}", stream="err")

        try:
            for raw in proc.stdout:
                # tqdm redraws the step bar with \r; split so each redraw is
                # its own line.
                for piece in raw.replace("\r", "\n").split("\n"):
                    line = piece.rstrip()
                    if not line:
                        continue
                    safe = redactor(line)
                    if log_file:
                        try:
                            log_file.write(safe + "\n")
                        except OSError:
                            log_file = None
                    self._handle_line(run, safe)
        except Exception as exc:  # a broken pipe must not lose the run
            run.append(f"Log stream ended unexpectedly: {exc}", stream="err")
        finally:
            if log_file:
                try:
                    log_file.close()
                except OSError:
                    pass

        proc.wait()
        run.exit_code = proc.returncode
        self._finalise(run)

    def _handle_line(self, run: Run, line: str) -> None:
        if line.startswith(RESULT_PREFIX):
            try:
                run.result = json.loads(line[len(RESULT_PREFIX):])
            except json.JSONDecodeError:
                run.result = {"raw": line[len(RESULT_PREFIX):]}
            return

        if line.startswith(ERROR_PREFIX):
            try:
                payload = json.loads(line[len(ERROR_PREFIX):])
            except json.JSONDecodeError:
                payload = {"type": "Error", "message": line[len(ERROR_PREFIX):]}
            run.error = {
                "type": payload.get("type", "Error"),
                "message": payload.get("error") or payload.get("message", ""),
            }
            finding = diagnostics.scan(run.error["message"])
            if finding and diagnostics.better(finding, run.root_cause):
                run.root_cause = finding
            run.append(f"{run.error['type']}: {run.error['message']}", stream="err")
            return

        bar = _TQDM_BAR.match(line)
        if bar:
            # The redrawn bar is noise in the log pane; it drives the step
            # strip. Anything printed around it is a real message and is
            # handled below as if the bar had not been there.
            count = _TQDM_COUNT.search(bar.group(0))
            if count:
                self._apply_progress(run, int(count.group(1)), bar.group(0))
            residual = f"{line[:bar.start()]} {line[bar.end():]}".strip()
            # A bar is padded with spaces to erase the previous, longer draw.
            if not residual:
                return
            line = residual

        finding = diagnostics.scan(line)
        if finding and diagnostics.better(finding, run.root_cause):
            run.root_cause = finding

        run.append(line, stream="err" if _ERROR_WORDS.search(line) else "out")

    @staticmethod
    def _apply_progress(run: Run, count: int, line: str) -> None:
        """Reconcile one redrawn step bar into the run's step state.

        The ETL's ``StepBar.next`` sets the postfix *then* increments, so a
        half-drawn bar can carry a count and a name that disagree by one. The
        name is the more reliable of the two — it is the step the ETL said it
        was entering — so when it matches a step declared in ``STEP_MAP`` it
        decides the position and the count is ignored. Only when the bar
        carries no usable name does the count stand on its own.
        """
        tail = _TQDM_TAIL.search(line)
        name = tail.group(1).strip() if tail else ""
        if name and _TQDM_RATE.match(name):
            name = ""                      # that was the rate, not a step

        if name and name in run.steps:
            run.step_index = run.steps.index(name) + 1
            run.step_name = name
            return

        run.step_index = min(count, len(run.steps))
        if 0 < run.step_index <= len(run.steps):
            run.step_name = run.steps[run.step_index - 1]
        elif name:
            run.step_name = name

    def _finalise(self, run: Run) -> None:
        code = run.exit_code

        if run._stop_requested:
            status = STOPPED
            run.error = None
            run.append("Run stopped on request; the ETL process group was killed.",
                       stream="err")
        elif code == 0 and run.error is None:
            status = COMPLETED
            run.step_index = len(run.steps)
            run.step_name = run.steps[-1] if run.steps else ""
            run.append("ETL completed successfully.")
        else:
            status = FAILED
            if run.root_cause is not None:
                run.append(f"Likely cause: {run.root_cause.summary}", stream="err")
                run.append(f"Suggested next step: {run.root_cause.hint}",
                           stream="err")
            if run.error is None:
                if code is not None and code < 0:
                    name = signal.Signals(-code).name if -code in \
                        {s.value for s in signal.Signals} else f"signal {-code}"
                    run.error = {
                        "type": "Terminated",
                        "message": f"The ETL process was terminated by {name} "
                                   f"from outside the application.",
                    }
                else:
                    run.error = {
                        "type": "ProcessExit",
                        "message": f"The ETL process exited with code {code}. "
                                   f"See the log for the failing step.",
                    }
                run.append(run.error["message"], stream="err")

        self._finish(run, status)

    def _finish(self, run: Run, status: str) -> None:
        run.status = status
        run.finished_at = _now()
        run._proc = None
        with self._guard:
            if self._active_id == run.id:
                self._active_id = None
        self._lockfile.release()

    # -- stop -------------------------------------------------------------
    def stop(self, run_id: str | None = None) -> dict:
        """Kill the active run immediately. Returns a small report."""
        run = self._runs.get(run_id) if run_id else self.active_run()
        if run is None or run.status != RUNNING:
            foreign = self.foreign_lock()
            if foreign:
                return self._stop_foreign(foreign)
            return {"ok": False, "error": "No ETL run is currently executing."}

        proc = run._proc
        if proc is None or proc.poll() is not None:
            return {"ok": False,
                    "error": "That run has already finished; refresh the page."}

        run._stop_requested = True
        run.append("Stop requested — killing the ETL process group.", stream="err")

        killed = self._kill_group(proc.pid)
        if not killed["ok"]:
            run._stop_requested = False
            run.append(f"Stop failed: {killed['error']}", stream="err")
            return {"ok": False, "error": killed["error"], "run_id": run.id}

        # The watcher thread finalises the run when the pipe closes; give it a
        # moment so the caller's next poll already shows "stopped".
        deadline = time.monotonic() + self.settings.stop_timeout
        while time.monotonic() < deadline and run.status == RUNNING:
            time.sleep(0.1)

        if run.status == RUNNING:
            return {"ok": True, "run_id": run.id, "status": run.status,
                    "message": "Kill signal delivered; waiting for the process "
                               "to disappear."}
        return {"ok": True, "run_id": run.id, "status": run.status,
                "message": "ETL process terminated."}

    def _kill_group(self, pid: int) -> dict:
        """SIGKILL the child's whole process group. No graceful phase."""
        try:
            pgid = os.getpgid(pid)
        except ProcessLookupError:
            return {"ok": True}
        except OSError as exc:
            return {"ok": False, "error": f"Could not resolve the process group: {exc}"}

        try:
            os.killpg(pgid, signal.SIGKILL)
            return {"ok": True}
        except ProcessLookupError:
            return {"ok": True}          # already gone
        except PermissionError:
            return {"ok": False, "error":
                    "The application user is not permitted to signal the ETL "
                    "process group. Run the web application as the same user "
                    "that owns the ETL runtime."}
        except OSError as exc:
            return {"ok": False, "error": f"Could not stop the ETL process: {exc}"}

    def _stop_foreign(self, owner: dict) -> dict:
        """Stop a run started by another web process, using the lock file."""
        pgid = owner.get("pgid")
        if not isinstance(pgid, int):
            return {"ok": False,
                    "error": "The run lock does not record a process group; "
                             "stop the ETL on the server."}
        killed = self._kill_group_by_pgid(pgid)
        if killed["ok"]:
            self._lockfile.path.unlink(missing_ok=True)
            return {"ok": True, "status": STOPPED,
                    "message": f"Stopped the ETL run started by process "
                               f"{owner.get('web_pid')}."}
        return killed

    def _kill_group_by_pgid(self, pgid: int) -> dict:
        try:
            os.killpg(pgid, signal.SIGKILL)
            return {"ok": True}
        except ProcessLookupError:
            return {"ok": True}
        except PermissionError:
            return {"ok": False, "error":
                    "Not permitted to signal that ETL process group."}
        except OSError as exc:
            return {"ok": False, "error": f"Could not stop the ETL process: {exc}"}


#: Module-level singleton. One per web worker process; the lock file keeps the
#: "one run at a time" rule across workers and across restarts.
RUNNER = ETLRunner()
