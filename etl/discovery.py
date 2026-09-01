"""Automatic ETL job discovery.

The jobs offered by the UI are *read out of the ETL source*, never hard-coded
here. ``etl_table_manual.py`` is parsed with :mod:`ast` — it is never imported
and never executed — and three things are lifted from the entry point:

``STEP_MAP``
    The dict literal at the top of ``run_etl``. Its keys are the ETL job names
    and its values are the real stage lists the ETL's own ``StepBar`` ticks
    through, so the UI's step display is the ETL's own step display.

``if etl_name == "...":`` branches
    Which jobs actually have an implementation.

``if not (a and b and c): raise ValueError`` guards
    The exact parameters each job refuses to start without — the same tuple the
    ETL checks itself, so the UI's "required" set can never drift from it.

Parsing rather than importing matters: importing would pull in PySpark, the
Oracle client and the encryption module into the web process, and would run the
module-level ``os.chdir`` in the ETL file.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any

from .settings import SETTINGS, EtlSettings


@dataclass
class DiscoveredJob:
    """One ETL job as found in the source."""

    key: str
    steps: list[str] = field(default_factory=list)
    runtime_required: list[str] = field(default_factory=list)
    #: True when the source has an ``if etl_name == key:`` branch for it.
    implemented: bool = False
    #: True when the key appears in the entry point's STEP_MAP.
    declared: bool = False


@dataclass
class Discovery:
    """Everything the parser could establish about the ETL entry point."""

    ok: bool = False
    module_path: str = ""
    entrypoint: str = ""
    jobs: dict[str, DiscoveredJob] = field(default_factory=dict)
    #: Keyword parameters of the entry point -> default value (or ``None``).
    parameters: dict[str, Any] = field(default_factory=dict)
    #: Human-readable reasons discovery failed or is incomplete.
    problems: list[str] = field(default_factory=list)

    def job_keys(self) -> list[str]:
        return list(self.jobs)


def _literal(node: ast.AST) -> Any:
    """Best-effort literal evaluation; returns ``None`` for anything dynamic."""
    try:
        return ast.literal_eval(node)
    except (ValueError, SyntaxError, TypeError):
        return None


def _find_function(tree: ast.Module, name: str) -> ast.FunctionDef | None:
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    return None


def _step_map(func: ast.FunctionDef) -> dict[str, list[str]]:
    """The STEP_MAP dict literal assigned inside the entry point."""
    for node in ast.walk(func):
        if not isinstance(node, ast.Assign):
            continue
        targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
        if "STEP_MAP" not in targets:
            continue
        value = _literal(node.value)
        if isinstance(value, dict):
            return {
                str(k): [str(s) for s in v]
                for k, v in value.items()
                if isinstance(v, (list, tuple))
            }
    return {}


def _guard_names(branch: ast.If) -> list[str]:
    """Names in the ``if not (a and b): raise ValueError`` guard of a branch.

    Only the guard at the top of the branch is considered, and only when it
    raises — that is exactly the shape the ETL uses to declare what it needs.
    """
    for node in branch.body:
        if not isinstance(node, ast.If):
            continue
        if not any(isinstance(s, ast.Raise) for s in node.body):
            continue
        test = node.test
        if not (isinstance(test, ast.UnaryOp) and isinstance(test.op, ast.Not)):
            continue
        operand = test.operand
        if isinstance(operand, ast.BoolOp) and isinstance(operand.op, ast.And):
            names = [v.id for v in operand.values if isinstance(v, ast.Name)]
            if names:
                return names
        elif isinstance(operand, ast.Name):
            return [operand.id]
    return []


def _implemented_branches(func: ast.FunctionDef) -> dict[str, list[str]]:
    """``{etl_name: [required parameter names]}`` from the dispatch branches."""
    found: dict[str, list[str]] = {}
    for node in ast.walk(func):
        if not isinstance(node, ast.If) or not isinstance(node.test, ast.Compare):
            continue
        test = node.test
        if not (isinstance(test.left, ast.Name) and test.left.id == "etl_name"):
            continue
        if len(test.ops) != 1 or not isinstance(test.ops[0], ast.Eq):
            continue
        key = _literal(test.comparators[0])
        if isinstance(key, str):
            found[key] = _guard_names(node)
    return found


def _parameters(func: ast.FunctionDef) -> dict[str, Any]:
    """Entry point signature: parameter name -> default (``None`` if required)."""
    args = func.args
    positional = list(args.posonlyargs) + list(args.args)
    defaults: list[Any] = [None] * (len(positional) - len(args.defaults))
    defaults += [_literal(d) for d in args.defaults]
    params = {a.arg: d for a, d in zip(positional, defaults)}
    for arg, default in zip(args.kwonlyargs, args.kw_defaults):
        params[arg.arg] = _literal(default) if default is not None else None
    return params


def discover(settings: EtlSettings = SETTINGS) -> Discovery:
    """Parse the ETL entry point and report the jobs it implements."""
    result = Discovery(
        module_path=str(settings.etl_module_path),
        entrypoint=f"{settings.etl_module}.{settings.etl_entrypoint}",
    )
    path = settings.etl_module_path

    if not settings.project_root.exists():
        result.problems.append(
            f"ETL project root does not exist: {settings.project_root}. "
            f"Set ETL_PROJECT_ROOT to the existing ETL checkout."
        )
        return result
    if not path.exists():
        result.problems.append(
            f"ETL module not found: {path}. Set ETL_MODULE if the entry point "
            f"file is named differently."
        )
        return result

    try:
        tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"), str(path))
    except SyntaxError as exc:
        result.problems.append(f"{path.name} could not be parsed: {exc}")
        return result
    except OSError as exc:
        result.problems.append(f"{path.name} could not be read: {exc}")
        return result

    func = _find_function(tree, settings.etl_entrypoint)
    if func is None:
        result.problems.append(
            f"{path.name} has no top-level {settings.etl_entrypoint}() function. "
            f"Set ETL_ENTRYPOINT if it was renamed."
        )
        return result

    result.parameters = _parameters(func)
    steps = _step_map(func)
    branches = _implemented_branches(func)

    if not steps:
        result.problems.append(
            f"No STEP_MAP literal found inside {settings.etl_entrypoint}(); "
            f"falling back to the dispatch branches for the job list."
        )

    for key in list(steps) + [k for k in branches if k not in steps]:
        job = result.jobs.get(key) or DiscoveredJob(key=key)
        job.declared = key in steps
        job.steps = steps.get(key, [])
        job.implemented = key in branches
        job.runtime_required = branches.get(key, [])
        result.jobs[key] = job

    for key, job in result.jobs.items():
        if job.declared and not job.implemented:
            result.problems.append(
                f"'{key}' is listed in STEP_MAP but has no implementation branch; "
                f"it is shown as unavailable."
            )
        elif job.implemented and not job.declared:
            result.problems.append(
                f"'{key}' is implemented but missing from STEP_MAP; the ETL "
                f"itself rejects it with 'Unknown etl_name'."
            )

    result.ok = any(j.implemented and j.declared for j in result.jobs.values())
    if not result.ok and not result.problems:
        result.problems.append(f"No runnable ETL jobs found in {path.name}.")
    return result


@lru_cache(maxsize=1)
def _cached(mtime: float, size: int) -> Discovery:
    return discover()


def cached_discovery() -> Discovery:
    """Discovery result, re-parsed automatically when the ETL file changes."""
    path = SETTINGS.etl_module_path
    try:
        stat = path.stat()
        return _cached(stat.st_mtime, stat.st_size)
    except OSError:
        return discover()


def runnable_keys() -> set[str]:
    """Job identifiers the backend is willing to execute."""
    return {
        key for key, job in cached_discovery().jobs.items()
        if job.implemented and job.declared
    }
