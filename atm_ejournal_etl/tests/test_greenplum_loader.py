"""Greenplum loading: SQL built per strategy, transaction handling, idempotency."""

from __future__ import annotations

import logging

import pytest

from greenplum_loader import (GreenplumLoadError, GreenplumLoader, quote_identifier,
                              sql_literal)


class RecordingConnection:
    """A ControlConnection double that records the SQL it is given."""

    def __init__(self, scalars=None, fail_on=None):
        self.flavour = "psycopg2"
        self.statements = []
        self.scalars = list(scalars or [])
        self.fail_on = fail_on
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def execute(self, sql):
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("simulated database error")
        self.statements.append(sql)
        return 1

    def scalar(self, sql):
        self.statements.append(sql)
        return self.scalars.pop(0) if self.scalars else 0

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


class FakeWriter:
    def __init__(self, sink):
        self.sink = sink
        self.options = {}

    def mode(self, mode):
        self.sink["mode"] = mode
        return self

    def format(self, fmt):
        self.sink["format"] = fmt
        return self

    def option(self, key, value):
        self.options[key] = value
        self.sink["options"] = self.options
        return self

    def save(self):
        self.sink["saved"] = True


class FakeDataFrame:
    """Enough of a DataFrame for the loader: a schema and a writer."""

    def __init__(self, columns=("ATM_NO", "AMOUNT", "SOURCE_FILE_KEY", "BATCH_ID", "ETL_RUN_ID")):
        from pyspark.sql.types import StringType, StructField, StructType

        self.schema = StructType([StructField(name, StringType(), True) for name in columns])
        self.sink = {}

    @property
    def write(self):
        return FakeWriter(self.sink)

    @property
    def rdd(self):
        raise AssertionError("the loader must not touch the RDD")


pytest.importorskip("pyspark", reason="pyspark is not installed")


@pytest.fixture
def loader(cfg, monkeypatch):
    instance = GreenplumLoader(cfg)
    monkeypatch.setattr(type(instance), "password",
                        property(lambda self: "never-logged"))
    return instance


# --------------------------------------------------------------------------- #
# SQL building
# --------------------------------------------------------------------------- #


def test_identifier_validation():
    assert quote_identifier("atm_table") == '"atm_table"'
    with pytest.raises(GreenplumLoadError, match="invalid SQL identifier"):
        quote_identifier("bad; drop table x")
    assert sql_literal("O'Brien") == "'O''Brien'"
    assert sql_literal(None) == "NULL"


def test_delete_insert_by_source_file_statements(loader):
    statements = loader._delete_statements("RUN1", "BATCH_0001")       # noqa: SLF001
    assert any('"ETL_RUN_ID" = \'RUN1\'' in sql and '"BATCH_ID" = \'BATCH_0001\'' in sql
               for sql in statements)
    assert any('"SOURCE_FILE_KEY" IN' in sql for sql in statements)


def test_merge_by_key_statements(cfg, monkeypatch):
    cfg._data["greenplum"]["GREENPLUM_LOAD_STRATEGY"] = "merge_by_key"    # noqa: SLF001
    instance = GreenplumLoader(cfg)
    statements = instance._delete_statements("RUN1", "BATCH_0001")        # noqa: SLF001
    merge_sql = statements[-1]
    assert "USING" in merge_sql
    assert 't."ATM_NO" IS NOT DISTINCT FROM s."ATM_NO"' in merge_sql


def test_truncate_load_statements(cfg):
    cfg._data["greenplum"]["GREENPLUM_LOAD_STRATEGY"] = "truncate_load"   # noqa: SLF001
    statements = GreenplumLoader(cfg)._delete_statements("R", "B")        # noqa: SLF001
    assert statements == ['TRUNCATE TABLE "atm"."atm_ejournal_withdrawals"']


def test_insert_only_has_no_source_file_delete(cfg):
    cfg._data["greenplum"]["GREENPLUM_LOAD_STRATEGY"] = "insert_only"     # noqa: SLF001
    statements = GreenplumLoader(cfg)._delete_statements("R", "B")        # noqa: SLF001
    assert len(statements) == 1 and "SOURCE_FILE_KEY" not in statements[0]


def test_control_table_ddl_uses_configured_distribution(cfg):
    cfg._data["greenplum"]["GREENPLUM_CONTROL_DISTRIBUTED_BY"] = "batch_id"   # noqa: SLF001
    ddl = GreenplumLoader(cfg)._control_table_ddl()                           # noqa: SLF001
    assert ddl.startswith('CREATE TABLE IF NOT EXISTS "atm"."atm_ejournal_batch_control"')
    assert ddl.endswith('DISTRIBUTED BY ("batch_id")')


def test_target_table_ddl_from_schema(loader):
    dataframe = FakeDataFrame()
    ddl = loader._target_table_ddl(dataframe.schema)                      # noqa: SLF001
    assert '"ATM_NO" text' in ddl and '"BATCH_ID" text' in ddl


# --------------------------------------------------------------------------- #
# Staging write
# --------------------------------------------------------------------------- #


def test_staging_write_uses_jdbc_options(loader, caplog):
    dataframe = FakeDataFrame()
    with caplog.at_level(logging.INFO):
        loader.write_staging(dataframe, "BATCH_0001")
    assert dataframe.sink["mode"] == "overwrite"
    assert dataframe.sink["format"] == "jdbc"
    assert dataframe.sink["options"]["dbtable"] == "atm.atm_ejournal_withdrawals_stg"
    assert "never-logged" not in caplog.text               # the password is never logged


def test_staging_write_failure_is_wrapped(loader, monkeypatch):
    class Exploding(FakeDataFrame):
        @property
        def write(self):
            raise RuntimeError("connection refused")

    with pytest.raises(GreenplumLoadError, match="staging write failed"):
        loader.write_staging(Exploding(), "BATCH_0001")


# --------------------------------------------------------------------------- #
# load_batch
# --------------------------------------------------------------------------- #


def test_load_batch_commits_and_records_control_row(loader, monkeypatch):
    connection = RecordingConnection(scalars=[5])          # staging count
    monkeypatch.setattr(loader, "connect", lambda: connection)
    monkeypatch.setattr(loader, "count_batch_rows", lambda run, batch: 5)
    monkeypatch.setattr(loader, "ensure_objects", lambda **kwargs: None)

    result = loader.load_batch(FakeDataFrame(), "BATCH_0001", "RUN1",
                               file_count=2, parquet_path="/parquet/batch_0001",
                               expected_rows=5)

    assert result.committed is True
    assert result.rows_loaded == 5
    assert connection.committed and not connection.rolled_back
    joined = " | ".join(connection.statements)
    assert "INSERT INTO \"atm\".\"atm_ejournal_withdrawals\"" in joined
    assert "INSERT INTO \"atm\".\"atm_ejournal_batch_control\"" in joined
    assert connection.closed


def test_row_count_mismatch_rolls_back(loader, monkeypatch):
    connection = RecordingConnection(scalars=[3])          # staging has 3, parquet had 5
    monkeypatch.setattr(loader, "connect", lambda: connection)
    monkeypatch.setattr(loader, "ensure_objects", lambda **kwargs: None)

    with pytest.raises(GreenplumLoadError, match="staging row count mismatch"):
        loader.load_batch(FakeDataFrame(), "BATCH_0001", "RUN1", expected_rows=5)
    assert connection.rolled_back and not connection.committed


def test_insert_failure_rolls_back(loader, monkeypatch):
    connection = RecordingConnection(scalars=[5], fail_on="INSERT INTO")
    monkeypatch.setattr(loader, "connect", lambda: connection)
    monkeypatch.setattr(loader, "ensure_objects", lambda **kwargs: None)

    with pytest.raises(GreenplumLoadError, match="Greenplum load failed"):
        loader.load_batch(FakeDataFrame(), "BATCH_0001", "RUN1", expected_rows=5)
    assert connection.rolled_back and not connection.committed


def test_is_batch_committed_reads_the_control_table(loader, monkeypatch):
    monkeypatch.setattr(loader, "connect", lambda: RecordingConnection(scalars=[7]))
    assert loader.is_batch_committed("RUN1", "BATCH_0001") == 7

    monkeypatch.setattr(loader, "connect", lambda: RecordingConnection(scalars=[None]))
    assert loader.is_batch_committed("RUN1", "BATCH_0002") is None


def test_is_batch_committed_refuses_to_guess_when_unreadable(loader, monkeypatch):
    class Broken(RecordingConnection):
        def scalar(self, sql):
            raise RuntimeError("control table missing")

    monkeypatch.setattr(loader, "connect", lambda: Broken())
    with pytest.raises(GreenplumLoadError, match="refusing to guess"):
        loader.is_batch_committed("RUN1", "BATCH_0001")


def test_password_is_never_in_a_connection_error(cfg, monkeypatch):
    instance = GreenplumLoader(cfg)
    monkeypatch.setattr(type(instance), "password", property(lambda self: "TOP-SECRET"))
    from greenplum_loader import ControlConnection

    with pytest.raises(GreenplumLoadError) as error:
        ControlConnection.open("127.0.0.1", 1, "db", "user", "TOP-SECRET", spark=None)
    assert "TOP-SECRET" not in str(error.value)
