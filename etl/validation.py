"""Server-side validation of a run request.

The browser applies the same rules for fast feedback, but nothing reaches
``run_etl`` on the strength of that — every request is re-checked here.

Three layers, cheapest first:

1. **Schema** — required, type, options, pattern and dependency rules built
   from the discovered signature plus the field specs.
2. **Runtime** — the same tuples ``run_etl`` guards itself with, discovered
   from the source, so a request that passes here cannot trip the ETL's own
   ``ValueError``.
3. **Environment** — filesystem and TCP checks. Run only by *Validate*, and
   reported as findings rather than hard failures: a path that exists only on
   the Spark executors must not block a legitimate run.
"""

from __future__ import annotations

import os
import re
import socket
from pathlib import Path

from .discovery import cached_discovery
from .registry import active_fields, build_job

MAX_QUERY_CHARS = 200_000
MAX_FIELD_CHARS = 4_000
MAX_VALUE_KEYS = 60


class ValidationError(Exception):
    def __init__(self, errors: dict[str, str]):
        self.errors = errors
        super().__init__("; ".join(f"{k}: {v}" for k, v in errors.items()))


def resolve_job(job_key: str) -> dict:
    """Look up a job, refusing anything not discovered in the ETL source.

    This is the allow-list that keeps ``etl_name`` from becoming an arbitrary
    string: only identifiers the parser found in ``run_etl`` get through.
    """
    key = str(job_key or "").strip()
    if not key:
        raise ValidationError({"etl": "Choose an ETL job first."})
    discovery = cached_discovery()
    if key not in discovery.jobs:
        available = ", ".join(sorted(discovery.jobs)) or "none"
        raise ValidationError(
            {"etl": f"Unknown ETL job '{key}'. Discovered jobs: {available}."})
    job = build_job(key, discovery)
    if not job["available"]:
        raise ValidationError(
            {"etl": f"'{key}' is declared in the ETL source but not runnable "
                    f"(no implementation branch)."})
    return job


# ----------------------------------------------------------------- schema ---

def validate_schema(job: dict, values: dict) -> dict[str, str]:
    errors: dict[str, str] = {}
    key = job["key"]
    known = {f["name"] for f in job["fields"]}

    for unexpected in sorted(set(values) - known):
        errors[unexpected] = "Not a parameter of this ETL job."

    for f in active_fields(key, values):
        raw = values.get(f["name"])
        if isinstance(raw, str):
            raw = raw.strip()

        if raw in (None, "", []):
            if f["required"] and f.get("default") in (None, ""):
                errors[f["name"]] = f"{f['label']} is required."
            continue

        if f["type"] == "checkbox":
            if not isinstance(raw, bool):
                errors[f["name"]] = f"{f['label']} must be true or false."
            continue

        if f["type"] == "number":
            try:
                float(raw)
            except (TypeError, ValueError):
                errors[f["name"]] = f"{f['label']} must be a number."
                continue

        if f.get("options") and str(raw) not in {o["value"] for o in f["options"]}:
            allowed = ", ".join(o["value"] for o in f["options"])
            errors[f["name"]] = f"{f['label']} must be one of: {allowed}."
            continue

        if f.get("pattern") and isinstance(raw, str):
            if not re.match(f["pattern"], raw):
                errors[f["name"]] = (
                    f.get("pattern_hint")
                    or f"{f['label']} has an unexpected format.")
                continue

        limit = MAX_QUERY_CHARS if f["type"] == "textarea" else MAX_FIELD_CHARS
        if isinstance(raw, str) and len(raw) > limit:
            errors[f["name"]] = f"{f['label']} exceeds {limit:,} characters."

    _validate_query(values, errors)
    _validate_bounds(values, errors)
    return errors


def _validate_query(values: dict, errors: dict) -> None:
    query = str(values.get("query") or "").strip()
    if query.endswith(";") and "query" not in errors:
        errors["query"] = (
            "Remove the trailing semicolon — the ETL wraps the query as "
            "( … ) subq and a semicolon breaks it.")


def _validate_bounds(values: dict, errors: dict) -> None:
    lo, hi = values.get("lower_bound"), values.get("upper_bound")
    if lo in (None, "") or hi in (None, ""):
        return
    try:
        if float(lo) >= float(hi):
            errors["upper_bound"] = "Upper bound must be greater than lower bound."
    except (TypeError, ValueError):
        pass


# ---------------------------------------------------------------- runtime ---

def validate_runtime(job: dict, values: dict) -> dict[str, str]:
    """Mirror of the ValueError guards discovered inside ``run_etl``."""
    errors: dict[str, str] = {}
    required = job["runtime_required"]
    missing = [n for n in required if not str(values.get(n) or "").strip()]
    for name in missing:
        errors[name] = (
            f"{job['key']} requires: {', '.join(required)} — this is the ETL's "
            f"own check, not an extra rule.")
    return errors


def validate(job_key: str, values: dict) -> dict:
    """Validate a request and return the resolved job. Raises on any error."""
    if not isinstance(values, dict):
        raise ValidationError({"values": "Malformed request payload."})
    if len(values) > MAX_VALUE_KEYS:
        raise ValidationError({"values": "Too many parameters in the request."})

    job = resolve_job(job_key)
    errors = validate_schema(job, values)
    errors.update({k: v for k, v in validate_runtime(job, values).items()
                   if k not in errors})
    if errors:
        raise ValidationError(errors)
    return job


# ------------------------------------------------------------ environment ---

def check_environment(job: dict, values: dict) -> list[dict]:
    """Filesystem and reachability findings. Never raises."""
    findings: list[dict] = []
    for f in active_fields(job["key"], values):
        raw = str(values.get(f["name"]) or "").strip()
        if raw and f.get("path_kind"):
            findings.extend(_check_path(f["label"], raw, f["path_kind"]))

    for url_field in ("gp_url", "oracle_jdbc_url"):
        url = str(values.get(url_field) or "").strip()
        if url:
            finding = _check_reachable(url_field, url)
            if finding:
                findings.append(finding)
    return findings


def _check_path(label: str, raw: str, kind: str) -> list[dict]:
    path = Path(raw)
    out: list[dict] = []
    if not path.is_absolute():
        out.append({"level": "warning", "message":
                    f"{label} is a relative path. It resolves against the ETL "
                    f"project root on the server, not your own machine."})
    if kind == "input_file":
        if not path.exists():
            out.append({"level": "error",
                        "message": f"{label} does not exist: {raw}"})
    elif kind == "input_dir":
        if not path.is_dir():
            out.append({"level": "error",
                        "message": f"{label} is not a directory: {raw}"})
    elif kind in ("output_file", "output_dir"):
        parent = path.parent if kind == "output_file" else path
        target = parent if parent.exists() else _first_existing_parent(parent)
        if target is None:
            out.append({"level": "error", "message":
                        f"No existing parent directory for {label}: {raw}"})
        elif not os.access(target, os.W_OK | os.X_OK):
            out.append({"level": "error", "message":
                        f"{label} is not writable by the application user: {target}"})
        if kind == "output_file" and path.exists():
            out.append({"level": "warning", "message":
                        f"{label} already exists and will be replaced: {raw}"})
        elif kind == "output_dir" and path.exists() and any(path.iterdir()):
            out.append({"level": "warning", "message":
                        f"{label} is not empty and will be overwritten: {raw}"})
    return out


def _first_existing_parent(path: Path) -> Path | None:
    for candidate in [path, *path.parents]:
        if candidate.exists():
            return candidate
    return None


_JDBC_PG = re.compile(r"^jdbc:postgresql://([^:/\s]+)(?::(\d+))?/")
_JDBC_ORA = re.compile(r"^jdbc:oracle:thin:@(?://)?([^:/\s]+):(\d+)")


def _check_reachable(field_name: str, url: str) -> dict | None:
    match = _JDBC_PG.match(url) or _JDBC_ORA.match(url)
    if not match:
        return {"level": "warning", "message":
                f"Could not read a host and port out of {field_name}; the "
                f"reachability check was skipped."}
    host = match.group(1)
    port = int(match.group(2) or 5432)
    try:
        with socket.create_connection((host, port), timeout=3):
            return {"level": "ok",
                    "message": f"{host}:{port} accepted a connection."}
    except OSError as exc:
        return {"level": "error", "message":
                f"{host}:{port} refused a connection ({exc.__class__.__name__}). "
                f"Only the network route is checked here, not the credentials."}
