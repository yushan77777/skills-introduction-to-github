#!/usr/bin/env python3
"""
Greenplum preflight for the ATM E-Journal ETL.

    python3 src/check_greenplum.py

Answers, in one screen, everything a failing load can be blamed on:

* can this account connect at all, and as whom;
* does the configured schema exist, and is the target table in it;
* does the table have the columns the batch writes - and if not, the exact
  ``ALTER TABLE`` statements that add them;
* may this account INSERT into it, and does it own it (that is what deciding
  whether the ETL can add the columns itself comes down to);
* is the batch control table there;
* how many rows are in the table already, per run and batch.

It is read-only: nothing is created, altered or written.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("atm_ejournal.check_greenplum")

#: The schema kinds (from ``spark_parser.schema_fields``) as SQL column types.
SQL_TYPE_BY_KIND = {
    "string": "text",
    "boolean": "boolean",
    "int": "integer",
    "long": "bigint",
    "double": "numeric(20,2)",
    "date": "date",
    "timestamp": "timestamp",
}


def expected_columns(cfg) -> List[Tuple[str, str]]:
    """The columns a batch writes, as ``[(name, sql type)]`` - no Spark needed."""
    from spark_parser import normalise_note_values, schema_fields                # noqa: PLC0415

    note_values = normalise_note_values(cfg.get_list("parser.NOTE_DENOMINATIONS") or None)
    return [(name, SQL_TYPE_BY_KIND.get(kind, "text"))
            for name, kind in schema_fields(note_values)]


def inspect(cfg, spark=None) -> Dict[str, Any]:
    """Collect the facts. Returns a dict; :func:`main` prints it."""
    from greenplum_loader import GreenplumLoader, sql_literal                    # noqa: PLC0415

    loader = GreenplumLoader(cfg, spark=spark)
    facts: Dict[str, Any] = {
        "url": loader.url,
        "user": loader.user,
        "driver": loader.driver,
        "schema": loader.schema,
        "table": loader.table,
        "control_table": loader.control,
        "write_format": loader.write_format,
        "load_strategy": loader.strategy,
        "create_objects": loader.create_objects,
        "connected": False,
    }

    connection = loader.connect()                      # raises with a clear message
    facts["connected"] = True
    try:
        facts["database_user"] = connection.scalar("SELECT current_user")
        facts["database"] = connection.scalar("SELECT current_database()")
        facts["search_path"] = connection.scalar("SHOW search_path")
        facts["server_version"] = str(connection.scalar("SELECT version()"))[:80]

        facts["schema_exists"] = bool(connection.scalar(
            "SELECT COUNT(*) FROM information_schema.schemata "
            f"WHERE schema_name = {sql_literal(loader.schema)}"))
        facts["table_exists"] = loader.table_exists(connection, loader.table)
        facts["control_table_exists"] = loader.table_exists(connection, loader.control)

        expected = expected_columns(cfg)
        facts["expected_column_count"] = len(expected)
        if facts["table_exists"]:
            present = loader.existing_columns(connection, loader.table)
            facts["table_column_count"] = len(present)
            known = set(present)
            missing = [(name, kind) for name, kind in expected if name not in known]
            facts["missing_columns"] = missing
            facts["alter_statements"] = loader.alter_statements(missing)
            facts["extra_columns"] = [name for name in present
                                      if name not in {n for n, _ in expected}]
            facts["rows"] = connection.scalar(
                f"SELECT COUNT(*) FROM {loader.target_table}")
            facts["rows_by_batch"] = _rows_by_batch(connection, loader)
            facts["can_insert"] = _privilege(connection, loader, "INSERT")
            facts["owner"] = connection.scalar(
                "SELECT pg_get_userbyid(c.relowner) FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                f"WHERE n.nspname = {sql_literal(loader.schema)} "
                f"AND c.relname = {sql_literal(loader.table)}")
        else:
            facts["missing_columns"] = expected
            facts["alter_statements"] = []
    finally:
        connection.close()
    return facts


def _privilege(connection, loader, privilege: str) -> Optional[bool]:
    from greenplum_loader import sql_literal                                     # noqa: PLC0415

    try:
        return bool(connection.scalar(
            f"SELECT has_table_privilege({sql_literal(loader.dbtable(loader.table))}, "
            f"{sql_literal(privilege)})"))
    except Exception:                                  # noqa: BLE001 - informational only
        return None


def _rows_by_batch(connection, loader, limit: int = 10) -> List[Tuple[str, str, int]]:
    from greenplum_loader import sql_literal                                     # noqa: PLC0415

    try:
        statement = (f'SELECT "ETL_RUN_ID", "BATCH_ID", COUNT(*) FROM {loader.target_table} '
                     'GROUP BY 1, 2 ORDER BY 1 DESC, 2 DESC')
        if connection.flavour == "psycopg2":
            with connection._connection.cursor() as cursor:                      # noqa: SLF001
                cursor.execute(statement)
                return [(row[0], row[1], int(row[2])) for row in cursor.fetchmany(limit)]
        sql_statement = connection._connection.createStatement()                 # noqa: SLF001
        try:
            results = sql_statement.executeQuery(statement)
            rows = []
            while results.next() and len(rows) < limit:
                rows.append((results.getString(1), results.getString(2), int(results.getLong(3))))
            results.close()
            return rows
        finally:
            sql_statement.close()
    except Exception:                                  # noqa: BLE001 - the table may be empty
        return []


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def report(facts: Dict[str, Any]) -> int:
    """Print the facts; returns the process exit code (0 = ready to load)."""
    print("connection")
    print("-" * 70)
    for key in ("url", "user", "driver", "database", "database_user", "server_version",
                "search_path"):
        if key in facts:
            print(f"  {key:<22} {facts[key]}")

    print("\ntarget")
    print("-" * 70)
    print(f"  {'schema':<22} {facts['schema']}  "
          f"{'(exists)' if facts.get('schema_exists') else 'MISSING'}")
    print(f"  {'table':<22} {facts['table']}  "
          f"{'(exists)' if facts.get('table_exists') else 'does not exist yet'}")
    print(f"  {'control table':<22} {facts['control_table']}  "
          f"{'(exists)' if facts.get('control_table_exists') else 'does not exist yet'}")
    print(f"  {'write format':<22} {facts['write_format']}")
    print(f"  {'load strategy':<22} {facts['load_strategy']}")
    print(f"  {'create objects':<22} {facts['create_objects']}")
    if facts.get("table_exists"):
        print(f"  {'owner':<22} {facts.get('owner')}")
        print(f"  {'insert allowed':<22} {facts.get('can_insert')}")
        print(f"  {'columns in table':<22} {facts.get('table_column_count')} "
              f"(the batch writes {facts.get('expected_column_count')})")
        print(f"  {'rows':<22} {facts.get('rows')}")
        for run_id, batch_id, count in facts.get("rows_by_batch", []):
            print(f"      {run_id} / {batch_id}: {count}")

    problems = 0
    print("\nchecks")
    print("-" * 70)
    if not facts.get("schema_exists"):
        problems += 1
        print(f"  FAIL  schema {facts['schema']} does not exist (or is not visible to "
              f"{facts.get('database_user')}) - create it, or point "
              "greenplum.GREENPLUM_SCHEMA at the schema the table is really in")
    else:
        print(f"  OK    schema {facts['schema']} exists")

    if facts.get("table_exists"):
        missing = facts.get("missing_columns") or []
        if missing:
            problems += 1
            print(f"  FAIL  {len(missing)} column(s) the batch writes are not in the table: "
                  + ", ".join(name for name, _ in missing[:8])
                  + (" ..." if len(missing) > 8 else ""))
            if facts.get("create_objects"):
                print("        the ETL adds them itself when the account may ALTER the table; "
                      "otherwise run:")
            for statement in facts.get("alter_statements", []):
                print(f"          {statement}")
        else:
            print("  OK    the table has every column the batch writes")
        if facts.get("can_insert") is False:
            problems += 1
            print(f"  FAIL  {facts.get('database_user')} may not INSERT into "
                  f"{facts['schema']}.{facts['table']}")
        elif facts.get("can_insert"):
            print("  OK    insert is allowed")
    elif facts.get("create_objects"):
        print("  OK    the table does not exist yet - the first batch creates it")
    else:
        problems += 1
        print("  FAIL  the table does not exist and GREENPLUM_CREATE_OBJECTS is false")

    print()
    print("ready to load" if not problems else f"{problems} problem(s) to fix before loading")
    return 0 if not problems else 1


def main(argv: Optional[List[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Check the Greenplum side of the ETL.")
    parser.add_argument("--config", default=None)
    parser.add_argument("--etl", default=None)
    options = parser.parse_args(argv)

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
    from config_loader import load_config                                        # noqa: PLC0415
    from spark_session import build_spark_session, stop_spark_session            # noqa: PLC0415

    cfg = load_config(options.config, options.etl)
    spark = None
    try:
        try:
            import psycopg2                            # noqa: PLC0415, F401
        except ImportError:
            # No psycopg2: the control connection borrows the JDBC driver from
            # the Spark JVM, so a session is needed for the check as well.
            spark = build_spark_session(cfg)
        return report(inspect(cfg, spark=spark))
    except Exception as exc:                           # noqa: BLE001
        print(f"\nGreenplum check failed: {type(exc).__name__}: {exc}")
        return 2
    finally:
        if spark is not None:
            stop_spark_session(spark)


if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    sys.exit(main())
