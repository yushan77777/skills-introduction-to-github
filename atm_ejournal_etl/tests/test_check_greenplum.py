"""
The Greenplum preflight: one screen that says why a load would fail.

The check is read-only, so these tests answer the SQL from a double instead of
a database - what matters is that every fact the report acts on is collected,
and that the exit code tells a script whether the load can go ahead.
"""

from __future__ import annotations

import pytest

from check_greenplum import expected_columns, inspect, main, report

pytest.importorskip("pyspark", reason="pyspark is not installed")


class FakeConnection:
    """Answers the handful of questions :func:`inspect` asks."""

    def __init__(self, schema_exists=True, tables=("atm_ejournal_withdrawals",),
                 rows=2956, owner="etl_user", can_insert=True):
        self.flavour = "psycopg2"
        self.schema_exists = schema_exists
        self.tables = list(tables)
        self.rows = rows
        self.owner = owner
        self.can_insert = can_insert
        self.closed = False

    def scalar(self, sql):
        if "current_user" in sql:
            return "etl_user"
        if "current_database" in sql:
            return "gpdb"
        if "search_path" in sql.lower():
            return "atm, public"
        if "version()" in sql:
            return "PostgreSQL 16.4 (Greenplum Database 7)"
        if "information_schema.schemata" in sql:
            return 1 if self.schema_exists else 0
        if "information_schema.tables" in sql:
            return 1 if any(f"'{table}'" in sql for table in self.tables) else 0
        if "has_table_privilege" in sql:
            return self.can_insert
        if "pg_get_userbyid" in sql:
            return self.owner
        if "COUNT(*)" in sql:
            return self.rows
        return 0

    def close(self):
        self.closed = True


@pytest.fixture
def connect(monkeypatch):
    """Point the loader at a FakeConnection and control the table's columns."""
    from greenplum_loader import GreenplumLoader

    state = {"connection": FakeConnection(), "columns": None}

    def columns(self, connection, table):
        if state["columns"] is not None:
            return list(state["columns"])
        return [name for name, _kind in expected_columns_for(self)]

    def expected_columns_for(loader):
        from spark_parser import schema_fields
        return schema_fields()

    monkeypatch.setattr(GreenplumLoader, "connect", lambda self: state["connection"])
    monkeypatch.setattr(GreenplumLoader, "existing_columns", columns)
    return state


# --------------------------------------------------------------------------- #
# What the batch writes
# --------------------------------------------------------------------------- #


def test_expected_columns_are_the_parquet_columns(cfg):
    from spark_parser import COLUMN_ORDER

    expected = expected_columns(cfg)

    assert [name for name, _type in expected] == COLUMN_ORDER
    types = dict(expected)
    assert types["ATM_NO"] == "text"
    assert types["NOTES_5000"] == "integer"
    assert types["AMOUNT"] == "numeric(20,2)"
    assert types["LOAD_TS"] == "timestamp"


def test_the_configured_note_values_are_expected(cfg):
    cfg._data["parser"]["NOTE_DENOMINATIONS"] = "5000,1000"             # noqa: SLF001

    names = [name for name, _type in expected_columns(cfg)]

    assert "NOTES_5000" in names and "NOTES_1000" in names
    assert "NOTES_2000" not in names


# --------------------------------------------------------------------------- #
# The facts, and the verdict drawn from them
# --------------------------------------------------------------------------- #


def test_a_ready_table_is_reported_ready(cfg, connect, capsys):
    facts = inspect(cfg)

    assert facts["connected"] is True
    assert facts["database_user"] == "etl_user"
    assert facts["schema_exists"] is True
    assert facts["table_exists"] is True
    assert facts["missing_columns"] == []
    assert facts["rows"] == 2956
    assert report(facts) == 0
    assert "ready to load" in capsys.readouterr().out


def test_missing_columns_come_with_the_alter_statements(cfg, connect, capsys):
    connect["columns"] = ["ATM_NO", "AMOUNT", "STATUS"]

    facts = inspect(cfg)
    exit_code = report(facts)
    printed = capsys.readouterr().out

    missing = [name for name, _type in facts["missing_columns"]]
    assert "NOTES_5000" in missing and "BATCH_ID" in missing
    assert exit_code == 1
    assert 'ALTER TABLE "atm"."atm_ejournal_withdrawals" ADD COLUMN "NOTES_5000" integer;' in printed
    assert "column(s) the batch writes are not in the table" in printed


def test_a_table_that_does_not_exist_yet_is_not_a_problem(cfg, connect, capsys):
    connect["connection"] = FakeConnection(tables=[])

    exit_code = report(inspect(cfg))

    assert exit_code == 0
    assert "the first batch creates it" in capsys.readouterr().out


def test_a_missing_table_is_a_problem_when_the_etl_may_not_create_it(cfg, connect, capsys):
    connect["connection"] = FakeConnection(tables=[])
    cfg._data["greenplum"]["GREENPLUM_CREATE_OBJECTS"] = False         # noqa: SLF001

    exit_code = report(inspect(cfg))

    assert exit_code == 1
    assert "GREENPLUM_CREATE_OBJECTS is false" in capsys.readouterr().out


def test_a_missing_schema_names_the_setting_to_change(cfg, connect, capsys):
    connect["connection"] = FakeConnection(schema_exists=False, tables=[])

    exit_code = report(inspect(cfg))

    assert exit_code == 1
    assert "GREENPLUM_SCHEMA" in capsys.readouterr().out


def test_an_account_that_may_not_insert_fails_the_check(cfg, connect, capsys):
    connect["connection"] = FakeConnection(can_insert=False)

    facts = inspect(cfg)
    exit_code = report(facts)

    assert facts["can_insert"] is False
    assert exit_code == 1
    assert "may not INSERT into" in capsys.readouterr().out


def test_the_owner_is_reported_because_it_decides_who_may_alter(cfg, connect, capsys):
    connect["connection"] = FakeConnection(owner="dba")
    connect["columns"] = ["ATM_NO"]

    report(inspect(cfg))

    assert "dba" in capsys.readouterr().out


def test_the_connection_is_always_closed(cfg, connect):
    inspect(cfg)

    assert connect["connection"].closed is True


def test_no_ddl_is_ever_run(cfg, connect):
    """The check must not create or alter anything - it only reads."""
    executed = []
    connect["connection"].execute = executed.append                    # type: ignore[attr-defined]
    connect["columns"] = ["ATM_NO"]

    inspect(cfg)

    assert executed == []


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def test_a_connection_failure_is_reported_not_raised(cfg, config_path, monkeypatch, capsys):
    from greenplum_loader import GreenplumLoadError, GreenplumLoader

    def refuse(self):
        raise GreenplumLoadError("could not connect to Greenplum gp:5432/gpdb as etl_user: "
                                 "FATAL: password authentication failed")

    monkeypatch.setattr(GreenplumLoader, "connect", refuse)

    exit_code = main(["--config", config_path, "--etl", "atm_ejournal"])
    printed = capsys.readouterr().out

    assert exit_code == 2
    assert "Greenplum check failed" in printed
    assert "password authentication failed" in printed


def test_the_cli_exit_code_is_the_verdict(config_path, connect, capsys):
    connect["columns"] = ["ATM_NO"]

    assert main(["--config", config_path, "--etl", "atm_ejournal"]) == 1
    assert "problem(s) to fix before loading" in capsys.readouterr().out
