"""Read-only summary of the ETL's own YAML configuration.

The configuration stays backend-controlled: this module reads
``config/config.yaml``, reports the values an operator needs in order to
understand what a run will do, and offers **no way to change any of them**.
There is no editor, no write path and no raw-YAML dump — a password, an
encrypted blob or a credential embedded in a JDBC URL never reaches the
browser.

What it reports per connection profile is deliberately narrow: the key name,
the driver, the user, the host/port/database reached, and whether the
credential material is actually usable. That last check is the useful one — it
catches a ``pass1.pkl`` that no longer matches the encrypted passwords in the
file, which otherwise only shows up as a failed run.
"""

from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .security import mask_url_credentials
from .settings import SETTINGS, EtlSettings

#: Config blocks that are not connection profiles.
_NON_PROFILE_KEYS = {"spark_properties"}

#: Spark settings worth surfacing, in display order.
_SPARK_FIELDS = [
    ("master_url", "Master URL"),
    ("session_name", "Session name"),
    ("executor_instances", "Executor instances"),
    ("executor_memory", "Executor memory"),
    ("executor_cores", "Executor cores"),
    ("cores_max", "Max cores"),
    ("driver_memory", "Driver memory"),
    ("jars", "Connector jars"),
]


@dataclass
class ConfigSummary:
    available: bool = False
    source: str = ""
    message: str = ""
    spark: list[dict] = None
    profiles: list[dict] = None
    runtime: list[dict] = None
    problems: list[str] = None

    def as_dict(self) -> dict:
        return {
            "available": self.available,
            "source": self.source,
            "message": self.message,
            "spark": self.spark or [],
            "profiles": self.profiles or [],
            "runtime": self.runtime or [],
            "problems": self.problems or [],
        }


def _load_encrypter(settings: EtlSettings):
    """The ETL project's own ``Encrypter``, loaded without importing ``src``.

    Loading the file directly keeps a generic package name out of the web
    process's ``sys.modules`` while still using the ETL's own implementation
    rather than a second copy of it.
    """
    path = settings.project_root / "src" / "utils" / "encrypt_module.py"
    if not path.exists():
        return None, f"encrypt_module.py not found at {path}"
    try:
        spec = importlib.util.spec_from_file_location("_etl_encrypt_module", path)
        module = importlib.util.module_from_spec(spec)
        sys.modules.setdefault("_etl_encrypt_module", module)
        spec.loader.exec_module(module)
        return module.Encrypter, ""
    except Exception as exc:  # a missing cryptography package lands here
        return None, f"{exc.__class__.__name__}: {exc}"


def _oracle_endpoint(url: str) -> str:
    """``jdbc:oracle:thin:user/pw@//host:port/service`` -> ``host:port/service``."""
    tail = url.split("@", 1)[-1].lstrip("/")
    return tail or "—"


def _postgres_endpoint(url: str) -> str:
    """``jdbc:postgresql://host:port/db`` -> ``host:port/db``."""
    parsed = urlparse(url.replace("jdbc:", "", 1))
    if parsed.hostname:
        port = f":{parsed.port}" if parsed.port else ""
        return f"{parsed.hostname}{port}{parsed.path}"
    return url.split("//", 1)[-1] or "—"


def _profile(name: str, block: dict, encrypter, settings: EtlSettings) -> dict:
    url = str(block.get("url") or "")
    driver = str(block.get("driver") or "")
    kind = "oracle" if "oracle" in (driver + url).lower() else (
        "greenplum" if "postgres" in (driver + url).lower() else "other")

    entry = {
        "name": name,
        "kind": kind,
        "user": str(block.get("user") or block.get("username") or "—"),
        "driver": driver or "—",
        "endpoint": _oracle_endpoint(url) if kind == "oracle" else
                    _postgres_endpoint(url) if kind == "greenplum" else
                    mask_url_credentials(url) or "—",
        "schema": str(block.get("schema") or ""),
        "credential": "—",
        "credential_level": "muted",
    }

    pickle_rel = block.get("pickle")
    password = block.get("password")

    if not pickle_rel:
        if password in (None, ""):
            entry["credential"] = "No password set in config"
            entry["credential_level"] = "muted"
        else:
            entry["credential"] = "Plain value in config.yaml"
            entry["credential_level"] = "warning"
        return entry

    key_path = Path(pickle_rel)
    if not key_path.is_absolute():
        key_path = settings.project_root / key_path
    entry["key_file"] = str(key_path)

    if not key_path.exists():
        entry["credential"] = f"Key file missing: {key_path.name}"
        entry["credential_level"] = "error"
        return entry
    if encrypter is None:
        entry["credential"] = "Encrypted (not verified)"
        entry["credential_level"] = "muted"
        return entry
    if password in (None, ""):
        entry["credential"] = "Encrypted password missing from config"
        entry["credential_level"] = "error"
        return entry

    try:
        encrypter(str(key_path)).get_decrypt_data(password)
        entry["credential"] = f"Encrypted, decrypts with {key_path.name}"
        entry["credential_level"] = "ok"
    except Exception:
        entry["credential"] = (
            f"Encrypted, but {key_path.name} does not decrypt it")
        entry["credential_level"] = "error"
    return entry


def _runtime_rows(settings: EtlSettings) -> list[dict]:
    root = settings.project_root
    module = settings.etl_module_path
    return [
        {"label": "ETL project root", "value": str(root),
         "level": "ok" if root.exists() else "error"},
        {"label": "Entry point",
         "value": f"{settings.etl_module}.{settings.etl_entrypoint}()",
         "level": "ok" if module.exists() else "error"},
        {"label": "Python runtime", "value": settings.python_executable,
         "level": "ok" if Path(settings.python_executable).exists() else "error"},
        {"label": "Configuration file", "value": str(settings.config_yaml_path),
         "level": "ok" if settings.config_yaml_path.exists() else "error"},
        {"label": "Run log directory", "value": str(settings.log_dir),
         "level": "ok" if settings.log_dir.exists() else "warning"},
    ]


def build_summary(settings: EtlSettings = SETTINGS) -> ConfigSummary:
    summary = ConfigSummary(
        source=str(settings.config_yaml_path),
        spark=[], profiles=[], runtime=_runtime_rows(settings), problems=[],
    )
    path = settings.config_yaml_path

    if not path.exists():
        summary.message = (
            f"config.yaml not found at {path}. Set ETL_PROJECT_ROOT or "
            f"ETL_CONFIG_YAML to the existing ETL configuration.")
        summary.problems.append(summary.message)
        return summary

    try:
        import yaml
        with open(path, "r") as handle:
            data = yaml.safe_load(handle) or {}
    except ImportError:
        summary.message = "PyYAML is not installed in the web application's environment."
        summary.problems.append(summary.message)
        return summary
    except Exception as exc:
        summary.message = f"Could not read {path.name}: {exc.__class__.__name__}"
        summary.problems.append(f"{path.name} could not be parsed: {exc}")
        return summary

    if not isinstance(data, dict):
        summary.message = f"{path.name} does not contain a mapping at the top level."
        summary.problems.append(summary.message)
        return summary

    summary.available = True

    spark = data.get("spark_properties") or {}
    if isinstance(spark, dict):
        for key, label in _SPARK_FIELDS:
            if key in spark:
                summary.spark.append({"label": label, "value": str(spark[key])})
    if not summary.spark:
        summary.problems.append(
            "No spark_properties block in the configuration; build_spark() will "
            "raise a KeyError when a Spark-based job starts.")

    encrypter, enc_problem = _load_encrypter(settings)
    if enc_problem:
        summary.problems.append(
            f"Encrypted passwords were not verified — {enc_problem}")

    for name, block in data.items():
        if name in _NON_PROFILE_KEYS or not isinstance(block, dict):
            continue
        summary.profiles.append(_profile(str(name), block, encrypter, settings))

    summary.profiles.sort(key=lambda p: (p["kind"], p["name"]))
    for profile in summary.profiles:
        if profile["credential_level"] == "error":
            summary.problems.append(
                f"Connection profile '{profile['name']}': {profile['credential']}.")

    summary.message = (
        f"{len(summary.profiles)} connection profile(s) and the Spark cluster "
        f"settings, read from {path.name}. Values are shown read-only — change "
        f"them in the file on the server, not here.")
    return summary


@lru_cache(maxsize=1)
def _cached(mtime: float, size: int) -> dict:
    return build_summary().as_dict()


def summary() -> dict:
    """Config summary, re-read automatically when the YAML changes."""
    try:
        stat = SETTINGS.config_yaml_path.stat()
        return _cached(stat.st_mtime, stat.st_size)
    except OSError:
        return build_summary().as_dict()


def spark_master_url(settings: EtlSettings = SETTINGS) -> str:
    """``spark_properties.master_url`` from config.yaml, or "" if unreadable."""
    path = settings.config_yaml_path
    if not path.exists():
        return ""
    try:
        import yaml
        data = yaml.safe_load(path.read_text()) or {}
    except Exception:
        return ""
    spark = data.get("spark_properties")
    if isinstance(spark, dict):
        return str(spark.get("master_url") or "")
    return ""


def config_keys() -> dict:
    """Connection profile names, grouped, for the form's dropdowns.

    Only key *names* leave this function — never a host, user or password.
    """
    data = summary()
    groups: dict[str, list[str]] = {"oracle": [], "greenplum": [], "other": []}
    for profile in data["profiles"]:
        groups.setdefault(profile["kind"], []).append(profile["name"])
    return {
        "oracle": sorted(groups.get("oracle", [])),
        "greenplum": sorted(groups.get("greenplum", [])),
        "other": sorted(groups.get("other", [])),
        "available": data["available"],
        "source": data["source"],
        "message": "" if data["available"] else data["message"],
    }
