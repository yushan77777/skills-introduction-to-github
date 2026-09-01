"""Greenplum / database access layer for the disk usage app.

Connection settings come from config.ini (see project root). Greenplum is
wire-compatible with PostgreSQL, so psycopg2 is used as the driver. A sqlite
driver is also supported for local development / demo purposes.
"""

import configparser
import os
import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

_config = None

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")


class ConfigError(Exception):
    pass


def get_config():
    """Load and cache config.ini (path overridable via MONITORING_CONFIG)."""
    global _config
    if _config is None:
        path = os.environ.get("MONITORING_CONFIG", str(BASE_DIR / "config.ini"))
        parser = configparser.ConfigParser()
        read = parser.read(path)
        if not read:
            raise ConfigError(f"Config file not found: {path}")
        _config = parser
    return _config


def table_name(key):
    """Return a table name from the [tables] section, validated as a safe
    SQL identifier (table names cannot be bound as query parameters)."""
    cfg = get_config()
    name = cfg.get("tables", key)
    if not _IDENTIFIER_RE.match(name):
        raise ConfigError(f"Unsafe table name in config: {name!r}")
    return name


def _connect():
    """Open a new DB connection. Returns (connection, driver)."""
    cfg = get_config()["greenplum"]
    driver = cfg.get("driver", "postgresql").strip().lower()

    if driver in ("postgresql", "postgres", "greenplum", "psycopg2"):
        import psycopg2

        url = cfg.get("url", fallback="")
        if url:
            conn = psycopg2.connect(url)
        else:
            conn = psycopg2.connect(
                host=cfg.get("host"),
                port=cfg.getint("port", fallback=5432),
                dbname=cfg.get("database"),
                user=cfg.get("username"),
                password=cfg.get("password"),
                connect_timeout=cfg.getint("connect_timeout", fallback=10),
            )
        return conn, "postgresql"

    if driver == "sqlite":
        import sqlite3

        path = cfg.get("sqlite_path", fallback="demo_data/demo.sqlite")
        full = Path(path)
        if not full.is_absolute():
            full = BASE_DIR / full
        return sqlite3.connect(str(full)), "sqlite"

    raise ConfigError(f"Unknown driver in config: {driver!r}")


def query(sql, params=()):
    """Run a read-only query and return a list of dicts.

    SQL is written with %s placeholders (psycopg2 style); they are
    translated to ? automatically for the sqlite driver.
    """
    conn, driver = _connect()
    try:
        if driver == "sqlite":
            sql = sql.replace("%s", "?")
        cur = conn.cursor()
        cur.execute(sql, params)
        columns = [d[0] for d in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def escape_like(value):
    """Escape LIKE wildcards in a user-supplied path fragment."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
