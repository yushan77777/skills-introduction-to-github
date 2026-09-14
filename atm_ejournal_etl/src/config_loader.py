"""
Configuration loading for the ATM E-Journal ETL.

The configuration file (``config/atm_ejournal.conf``) is YAML in the same style
as the existing spark/table configuration supplied with the project. This module
turns it into an :class:`EtlConfig` that the rest of the ETL reads from, so no
environment specific value is ever hard-coded in Python.

Resolution order for one ETL profile::

    defaults.<section>  ->  etls.<etl_name>.<section>  ->  ${ENV_VAR} expansion

Usage
-----
    from config_loader import load_config

    cfg = load_config("config/atm_ejournal.conf", etl_name="atm_ejournal")
    cfg.get_int("input.BATCH_SIZE")
    cfg.path("tracking.PROCESSED_FILES_CSV")     # absolute, anchored on BASE_DIR
"""

from __future__ import annotations

import copy
import logging
import os
import re
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger("atm_ejournal.config")

#: ``${VAR}`` and ``${VAR:-default}``
ENV_VAR_RE = re.compile(r"\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?::-(?P<default>[^}]*))?\}")

#: Sections merged from ``defaults`` into every ETL profile.
MERGED_SECTIONS = ("input", "tracking", "parquet", "logging", "spark",
                   "greenplum", "parser", "mail", "airflow")

#: Keys that must be present and non-empty for a run to be attempted.
REQUIRED_KEYS = (
    "input.INPUT_PATH",
    "input.BATCH_SIZE",
    "tracking.PROCESSED_FILES_CSV",
    "parquet.PARQUET_PATH",
    "logging.LOG_PATH",
    "spark.SPARK_APP_NAME",
    "spark.SPARK_MASTER",
    "greenplum.GREENPLUM_URL",
    "greenplum.GREENPLUM_SCHEMA",
    "greenplum.GREENPLUM_TABLE",
    "greenplum.GREENPLUM_DRIVER",
)

#: Never written to a log line, an exception or the run summary.
SECRET_KEY_TOKENS = ("PASSWORD", "SECRET", "TOKEN", "PASSPHRASE", "PWD")

TRUE_TOKENS = {"1", "true", "yes", "y", "on", "t"}
FALSE_TOKENS = {"0", "false", "no", "n", "off", "f"}


class ConfigError(Exception):
    """Raised for a missing, unreadable or invalid configuration."""


def is_secret_key(key: str) -> bool:
    """True when a configuration key holds a credential."""
    upper = key.rsplit(".", 1)[-1].upper()
    return any(token in upper for token in SECRET_KEY_TOKENS)


def _expand(value: Any) -> Any:
    """Expand ``${VAR}`` / ``${VAR:-default}`` inside strings, recursively."""
    if isinstance(value, str):
        def replace(match: "re.Match") -> str:
            return os.environ.get(match.group("name"), match.group("default") or "")
        return ENV_VAR_RE.sub(replace, value)
    if isinstance(value, dict):
        return {k: _expand(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand(v) for v in value]
    return value


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Merge ``override`` onto a copy of ``base`` (dicts merged, scalars replaced)."""
    merged = copy.deepcopy(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


class EtlConfig:
    """
    One resolved ETL profile.

    Values are addressed with dotted keys (``"input.BATCH_SIZE"``). Plain
    ``UPPER_CASE`` keys also work when they are unique across sections, so both
    ``cfg.get("BATCH_SIZE")`` and ``cfg.get("input.BATCH_SIZE")`` resolve.
    """

    def __init__(self, data: Dict[str, Any], etl_name: str, config_path: str):
        self._data = data
        self.etl_name = etl_name
        self.config_path = os.path.abspath(config_path)
        self.base_dir = os.path.abspath(
            self.get("project.BASE_DIR", os.path.dirname(os.path.dirname(self.config_path)))
        )
        self.environment = str(self.get("project.ENVIRONMENT", "UNKNOWN"))

    # -- lookup ------------------------------------------------------------ #

    def get(self, key: str, default: Any = None) -> Any:
        if "." in key:
            node: Any = self._data
            for part in key.split("."):
                if not isinstance(node, dict) or part not in node:
                    return default
                node = node[part]
            return node
        for section in self._data.values():
            if isinstance(section, dict) and key in section:
                return section[key]
        return self._data.get(key, default)

    def require(self, key: str) -> Any:
        value = self.get(key)
        if value is None or (isinstance(value, str) and not value.strip()):
            raise ConfigError(f"[{self.etl_name}] missing configuration value: {key}")
        return value

    def section(self, name: str) -> Dict[str, Any]:
        value = self._data.get(name, {})
        return copy.deepcopy(value) if isinstance(value, dict) else {}

    # -- typed accessors --------------------------------------------------- #

    def get_int(self, key: str, default: Optional[int] = None) -> int:
        value = self.get(key, default)
        if value is None:
            raise ConfigError(f"[{self.etl_name}] missing integer configuration value: {key}")
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            raise ConfigError(f"[{self.etl_name}] {key} must be an integer, got {value!r}")

    def get_float(self, key: str, default: Optional[float] = None) -> float:
        value = self.get(key, default)
        if value is None:
            raise ConfigError(f"[{self.etl_name}] missing numeric configuration value: {key}")
        try:
            return float(str(value).strip())
        except (TypeError, ValueError):
            raise ConfigError(f"[{self.etl_name}] {key} must be numeric, got {value!r}")

    def get_bool(self, key: str, default: Optional[bool] = None) -> bool:
        value = self.get(key, default)
        if isinstance(value, bool):
            return value
        if value is None:
            raise ConfigError(f"[{self.etl_name}] missing boolean configuration value: {key}")
        token = str(value).strip().lower()
        if token in TRUE_TOKENS:
            return True
        if token in FALSE_TOKENS:
            return False
        raise ConfigError(f"[{self.etl_name}] {key} must be a boolean, got {value!r}")

    def get_list(self, key: str, default: Optional[List[str]] = None) -> List[str]:
        """Read a comma separated string (or a YAML list) as a list of tokens."""
        value = self.get(key, default)
        if value is None:
            return []
        if isinstance(value, (list, tuple)):
            return [str(v).strip() for v in value if str(v).strip()]
        return [token.strip() for token in str(value).split(",") if token.strip()]

    def path(self, key: str, default: Optional[str] = None) -> str:
        """Resolve a configured path against ``BASE_DIR`` when it is relative."""
        value = self.get(key, default)
        if value is None:
            raise ConfigError(f"[{self.etl_name}] missing path configuration value: {key}")
        value = os.path.expanduser(str(value))
        return value if os.path.isabs(value) else os.path.normpath(os.path.join(self.base_dir, value))

    # -- validation / reporting -------------------------------------------- #

    def validate(self) -> None:
        """Fail fast on a configuration that cannot produce a correct run."""
        missing = [key for key in REQUIRED_KEYS
                   if self.get(key) is None or str(self.get(key)).strip() == ""]
        if missing:
            raise ConfigError(f"[{self.etl_name}] missing configuration value(s): "
                              + ", ".join(missing))

        batch_size = self.get_int("input.BATCH_SIZE")
        if batch_size < 1:
            raise ConfigError(f"[{self.etl_name}] input.BATCH_SIZE must be >= 1, got {batch_size}")
        if self.get_int("input.MAX_BATCHES_PER_RUN", 0) < 0:
            raise ConfigError(f"[{self.etl_name}] input.MAX_BATCHES_PER_RUN must be >= 0")
        if self.get_int("logging.LOG_RETENTION_DAYS", 365) < 1:
            raise ConfigError(f"[{self.etl_name}] logging.LOG_RETENTION_DAYS must be >= 1")
        if self.get_float("logging.MAX_LOG_SIZE_GB", 1) <= 0:
            raise ConfigError(f"[{self.etl_name}] logging.MAX_LOG_SIZE_GB must be > 0")

        strategy = str(self.get("greenplum.GREENPLUM_LOAD_STRATEGY", "")).lower()
        allowed = {"append", "overwrite", "delete_insert_by_source_file",
                   "merge_by_key", "truncate_load"}
        if strategy not in allowed:
            raise ConfigError(f"[{self.etl_name}] greenplum.GREENPLUM_LOAD_STRATEGY must be one of "
                              + ", ".join(sorted(allowed)) + f", got {strategy!r}")
        if strategy == "merge_by_key" and not self.get_list("greenplum.GREENPLUM_MERGE_KEYS"):
            raise ConfigError(f"[{self.etl_name}] greenplum.GREENPLUM_MERGE_KEYS is required for "
                              "the merge_by_key load strategy")

        engine = str(self.get("parser.PARSE_ENGINE", "auto")).lower()
        if engine not in {"auto", "local", "spark"}:
            raise ConfigError(f"[{self.etl_name}] parser.PARSE_ENGINE must be auto, local or "
                              f"spark, got {engine!r}")
        if self.get_int("parser.PARSE_WRITE_CHUNK_RECORDS", 50000) < 1:
            raise ConfigError(f"[{self.etl_name}] parser.PARSE_WRITE_CHUNK_RECORDS must be >= 1")

        key_mode = str(self.get("tracking.FILE_KEY_MODE", "path")).lower()
        if key_mode not in {"path", "path_size", "path_mtime"}:
            raise ConfigError(f"[{self.etl_name}] tracking.FILE_KEY_MODE must be one of "
                              "path, path_size, path_mtime")

        write_mode = str(self.get("greenplum.GREENPLUM_WRITE_MODE", "append")).lower()
        if write_mode not in {"append", "overwrite"}:
            raise ConfigError(f"[{self.etl_name}] greenplum.GREENPLUM_WRITE_MODE must be "
                              "'append' or 'overwrite'")

        url = str(self.get("greenplum.GREENPLUM_URL", ""))
        if not url.lower().startswith("jdbc:"):
            raise ConfigError(f"[{self.etl_name}] greenplum.GREENPLUM_URL must be a JDBC URL, "
                              "e.g. jdbc:postgresql://<host>:5432/<database>")

    def safe_dump(self) -> Dict[str, Any]:
        """The whole configuration with every credential masked - safe to log."""
        def scrub(node: Any, prefix: str = "") -> Any:
            if isinstance(node, dict):
                return {k: ("********" if is_secret_key(k) and node[k] else scrub(v, k))
                        for k, v in node.items()}
            if isinstance(node, list):
                return [scrub(v, prefix) for v in node]
            return node
        return scrub(self._data)

    def as_dict(self) -> Dict[str, Any]:
        return copy.deepcopy(self._data)

    def __repr__(self) -> str:            # pragma: no cover - debugging aid
        return f"<EtlConfig {self.etl_name} from {self.config_path}>"


def list_etls(config_path: str) -> List[str]:
    """Names of every ETL profile defined in the configuration file."""
    raw = _read_yaml(config_path)
    return sorted((raw.get("etls") or {}).keys())


def _read_yaml(config_path: str) -> Dict[str, Any]:
    if not os.path.isfile(config_path):
        raise ConfigError(f"configuration file not found: {config_path}")
    try:
        with open(config_path, "r", encoding="utf-8") as handle:
            raw = yaml.safe_load(handle)
    except yaml.YAMLError as exc:
        raise ConfigError(f"configuration file is not valid YAML ({config_path}): {exc}") from exc
    except OSError as exc:
        raise ConfigError(f"configuration file could not be read ({config_path}): {exc}") from exc
    if not isinstance(raw, dict):
        raise ConfigError(f"configuration file must contain a YAML mapping: {config_path}")
    return raw


def load_config(config_path: Optional[str] = None,
                etl_name: Optional[str] = None,
                overrides: Optional[Dict[str, Any]] = None,
                validate: bool = True) -> EtlConfig:
    """
    Load one ETL profile.

    Parameters
    ----------
    config_path:
        Path to the configuration file. Defaults to ``$ATM_ETL_CONFIG`` and then
        to ``config/atm_ejournal.conf`` next to the project root.
    etl_name:
        Profile under ``etls:``. Defaults to ``$ATM_ETL_NAME``, then
        ``project.ETL_NAME``, then the only profile defined.
    overrides:
        Dotted-key overrides applied last, e.g. ``{"input.BATCH_SIZE": 10}``.
        Used by the CLI (``--batch-size``) and by tests.
    """
    config_path = config_path or os.environ.get("ATM_ETL_CONFIG") or _default_config_path()
    raw = _read_yaml(config_path)

    profiles = raw.get("etls") or {}
    if not isinstance(profiles, dict) or not profiles:
        raise ConfigError(f"no ETL profiles defined under 'etls:' in {config_path}")

    name = (etl_name or os.environ.get("ATM_ETL_NAME")
            or (raw.get("project") or {}).get("ETL_NAME"))
    if name not in profiles:
        if name:
            raise ConfigError(f"ETL '{name}' is not defined in {config_path}. "
                              f"Available: {', '.join(sorted(profiles))}")
        if len(profiles) > 1:
            raise ConfigError(f"several ETL profiles defined in {config_path}; "
                              f"select one with --etl: {', '.join(sorted(profiles))}")
        name = next(iter(profiles))

    defaults = raw.get("defaults") or {}
    profile = profiles.get(name) or {}
    resolved: Dict[str, Any] = {"project": copy.deepcopy(raw.get("project") or {})}
    for section in MERGED_SECTIONS:
        merged = _deep_merge(defaults.get(section) or {}, profile.get(section) or {})
        if merged:
            resolved[section] = merged
    # Sections only present in the profile (future extensions) are kept as-is.
    for section, value in profile.items():
        if section not in resolved:
            resolved[section] = copy.deepcopy(value)

    resolved = _expand(resolved)

    for key, value in (overrides or {}).items():
        _set_dotted(resolved, key, value)

    cfg = EtlConfig(resolved, etl_name=name, config_path=config_path)
    if validate:
        cfg.validate()
    logger.debug("configuration loaded for ETL '%s' from %s", name, config_path)
    return cfg


def _set_dotted(data: Dict[str, Any], key: str, value: Any) -> None:
    parts = key.split(".")
    if len(parts) == 1:                      # bare key -> find its section
        for section in data.values():
            if isinstance(section, dict) and key in section:
                section[key] = value
                return
        data[key] = value
        return
    node = data
    for part in parts[:-1]:
        node = node.setdefault(part, {})
        if not isinstance(node, dict):
            raise ConfigError(f"cannot override {key}: {part} is not a section")
    node[parts[-1]] = value


def _default_config_path() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.normpath(os.path.join(here, "..", "config", "atm_ejournal.conf"))
