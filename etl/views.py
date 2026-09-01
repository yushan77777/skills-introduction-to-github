"""Page and JSON API for the ETL app.

The browser never talks to the ETL directly: every action goes through one of
these views, which validate the request against the jobs discovered in the ETL
source before anything is executed. There is no endpoint that runs a command,
evaluates Python, reads an arbitrary file or writes configuration — the only
executable surface is ``run_etl`` with a discovered ``etl_name`` and validated
keyword arguments.
"""

from __future__ import annotations

import functools
import json

from django.http import FileResponse, Http404, JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET, require_POST

from . import etl_config
from .discovery import cached_discovery
from .registry import list_jobs, sensitive_field_names, to_run_etl_kwargs
from .runner import RUNNER, ETLBusy, ETLStartError
from .settings import SETTINGS
from .validation import ValidationError, check_environment, validate

MAX_BODY_BYTES = 512 * 1024


# ------------------------------------------------------------------ page ---

def index(request):
    discovery = cached_discovery()
    return render(request, "etl/index.html", {
        "entrypoint": discovery.entrypoint,
        "project_root": str(SETTINGS.project_root),
        "job_count": sum(1 for j in discovery.jobs.values()
                         if j.implemented and j.declared),
    })


# --------------------------------------------------------------- helpers ---

def json_api(view):
    """Turn unexpected errors into JSON so the UI shows a banner, not a 500."""

    @functools.wraps(view)
    def wrapper(request, *args, **kwargs):
        try:
            return view(request, *args, **kwargs)
        except ValidationError as exc:
            return JsonResponse({"ok": False, "errors": exc.errors}, status=400)
        except Exception as exc:
            # The class name and message are safe; no traceback is exposed.
            return JsonResponse(
                {"ok": False,
                 "error": f"{exc.__class__.__name__}: {exc}"}, status=500)

    return wrapper


def _body(request) -> dict:
    if len(request.body) > MAX_BODY_BYTES:
        raise ValidationError({"_": "Request payload is too large."})
    try:
        data = json.loads(request.body or b"{}")
    except json.JSONDecodeError:
        raise ValidationError({"_": "Expected a JSON request body."})
    if not isinstance(data, dict):
        raise ValidationError({"_": "Expected a JSON object."})
    return data


def _request_parts(request) -> tuple[str, dict]:
    data = _body(request)
    values = data.get("values") or {}
    if not isinstance(values, dict):
        raise ValidationError({"values": "Malformed request payload."})
    return str(data.get("etl") or ""), values


def _triggered_by(request) -> str:
    """Who asked for this run.

    The ETL app adds no login of its own, so this identifies the requester the
    best way the deployment allows: the Django user when the Monitoring site is
    running with authentication, otherwise the client address.
    """
    user = getattr(request, "user", None)
    if user is not None and getattr(user, "is_authenticated", False):
        return user.get_username()
    forwarded = request.META.get("HTTP_X_FORWARDED_FOR", "")
    client = forwarded.split(",")[0].strip() or request.META.get("REMOTE_ADDR", "")
    return f"web ({client})" if client else "web"


def _active_payload() -> dict:
    """The current run, or whatever holds the slot, or nothing."""
    run = RUNNER.active_run()
    if run is not None:
        return {"busy": True, "run": run.snapshot()}
    foreign = RUNNER.foreign_lock()
    if foreign:
        return {"busy": True, "run": None, "lock": foreign}
    return {"busy": False, "run": None}


# -------------------------------------------------------------------- API ---

@require_GET
@json_api
def api_jobs(request):
    """Discovered ETL jobs and everything the form needs to render them."""
    discovery = cached_discovery()
    return JsonResponse({
        "ok": discovery.ok,
        "entrypoint": discovery.entrypoint,
        "module_path": discovery.module_path,
        "project_root": str(SETTINGS.project_root),
        "jobs": list_jobs(discovery),
        "config_keys": etl_config.config_keys(),
        "defaults": {"base_dir": str(SETTINGS.project_root)},
        "problems": discovery.problems,
        "state": _active_payload(),
    })


@require_GET
@json_api
def api_config(request):
    """Read-only configuration summary. There is no write counterpart."""
    return JsonResponse(etl_config.summary())


@require_POST
@json_api
def api_validate(request):
    job_key, values = _request_parts(request)
    job = validate(job_key, values)
    findings = check_environment(job, values)
    blocking = [f for f in findings if f["level"] == "error"]
    return JsonResponse({
        "ok": not blocking,
        "errors": {},
        "findings": findings,
        "message": ("Configuration looks good." if not findings else
                    "Checked the configuration — see the notes below."),
    })


@require_POST
@json_api
def api_run(request):
    job_key, values = _request_parts(request)
    job = validate(job_key, values)
    secrets = sensitive_field_names(job_key)
    try:
        run = RUNNER.submit(
            job,
            to_run_etl_kwargs(job_key, values),
            _triggered_by(request),
            secret_names=secrets,
            secret_values=[values[k] for k in secrets if values.get(k)],
        )
    except ETLBusy as exc:
        # 409 Conflict: the backend refused, whatever the browser believed.
        return JsonResponse({"ok": False, "busy": True, "error": str(exc),
                             "active": exc.active}, status=409)
    except ETLStartError as exc:
        return JsonResponse({"ok": False, "error": str(exc)}, status=502)
    return JsonResponse({"ok": True, "run": run.snapshot()}, status=202)


@require_GET
@json_api
def api_status(request):
    """Snapshot of one run, or of the active run when no id is given."""
    run_id = (request.GET.get("run") or "").strip()
    try:
        since = int(request.GET.get("since") or 0)
    except ValueError:
        since = 0

    run = RUNNER.get(run_id) if run_id else RUNNER.active_run()
    if run is None:
        if run_id:
            return JsonResponse(
                {"ok": False,
                 "error": "No run with that id — it may have aged out of the "
                          "in-memory history. The log file is still on disk."},
                status=404)
        return JsonResponse({"ok": True, "run": None, "state": _active_payload()})
    return JsonResponse({"ok": True, "run": run.snapshot(since=since),
                         "state": _active_payload()})


@require_POST
@json_api
def api_stop(request):
    data = _body(request)
    run_id = str(data.get("run") or "").strip() or None
    report = RUNNER.stop(run_id)
    return JsonResponse(report, status=200 if report.get("ok") else 409)


@require_GET
@json_api
def api_history(request):
    try:
        limit = min(int(request.GET.get("limit") or 25), 200)
    except ValueError:
        limit = 25
    return JsonResponse({"ok": True, "runs": RUNNER.history(limit),
                         "state": _active_payload(),
                         "log_dir": str(SETTINGS.log_dir)})


@require_GET
def api_log(request):
    """Serve one run's log file.

    The path is looked up from the in-memory run record, never taken from the
    request, so this cannot be turned into an arbitrary file read.
    """
    run = RUNNER.get((request.GET.get("run") or "").strip())
    if run is None:
        raise Http404("Unknown run id.")
    try:
        handle = open(run.log_path, "rb")
    except OSError:
        raise Http404("The log file for that run is no longer available.")
    response = FileResponse(handle, content_type="text/plain; charset=utf-8")
    response["Content-Disposition"] = (
        f'inline; filename="{run.etl_key}-{run.id}.log"')
    return response
