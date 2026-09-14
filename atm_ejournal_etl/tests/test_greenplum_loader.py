"""Greenplum loading: the JDBC write, the load strategies and batch bookkeeping."""

from __future__ import annotations

import logging

import pytest

from greenplum_loader import (GreenplumLoadError, GreenplumLoader, parse_jdbc_url,
                              quote_identifier, sql_literal)

pytest.importorskip("pyspark", reason="pyspark is not installed")


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
    """Records the exact chain the loader calls: format/option/mode/save."""

    def __init__(self, sink):
        self.sink = sink
        sink.setdefault("options", {})

    def format(self, fmt):
        self.sink["format"] = fmt
        return self

    def option(self, key, value):
        self.sink["options"][key] = value
        return self

    def mode(self, mode):
        self.sink["mode"] = mode
        return self

    def save(self):
        self.sink["saved"] = True


class FakeDataFrame:
    def __init__(self, columns=("ATM_NO", "AMOUNT", "SOURCE_FILE_KEY", "BATCH_ID", "ETL_RUN_ID"),
                 partitions=1):
        from pyspark.sql.types import StringType, StructField, StructType

        self.schema = StructType([StructField(name, StringType(), True) for name in columns])
        self.sink = {}
        self._partitions = partitions

    @property
    def write(self):
        return FakeWriter(self.sink)

    @property
    def rdd(self):
        dataframe = self

        class _Rdd:
            @staticmethod
            def getNumPartitions():
                return dataframe._partitions
        return _Rdd()

    def coalesce(self, n):
        self._partitions = n
        return self

    def repartition(self, n):
        self._partitions = n
        return self


@pytest.fixture
def loader(cfg):
    return GreenplumLoader(cfg)


# --------------------------------------------------------------------------- #
# Connection details taken straight from the configuration
# --------------------------------------------------------------------------- #


def test_connection_values_come_from_the_configuration(loader):
    assert loader.url == "jdbc:postgresql://localhost:5432/testdb"
    assert loader.user == "tester"
    assert loader.password == "test-password"
    assert loader.driver == "org.postgresql.Driver"
    assert loader.dbtable(loader.table) == "atm.atm_ejournal_withdrawals"
    assert loader.jdbc_properties == {"user": "tester", "password": "test-password",
                                      "driver": "org.postgresql.Driver"}


def test_parse_jdbc_url():
    assert parse_jdbc_url("jdbc:postgresql://gp.example:5433/bidb") == ("gp.example", 5433, "bidb")
    assert parse_jdbc_url("jdbc:postgresql://gp.example/bidb") == ("gp.example", 5432, "bidb")
    with pytest.raises(GreenplumLoadError, match="must look like"):
        parse_jdbc_url("postgres://gp.example/bidb")


def test_identifier_validation():
    assert quote_identifier("atm_table") == '"atm_table"'
    with pytest.raises(GreenplumLoadError, match="invalid SQL identifier"):
        quote_identifier("bad; drop table x")
    assert sql_literal("O'Brien") == "'O''Brien'"
    assert sql_literal(None) == "NULL"


# --------------------------------------------------------------------------- #
# The write itself
# --------------------------------------------------------------------------- #


def test_write_dataframe_uses_the_jdbc_writer(loader, caplog):
    dataframe = FakeDataFrame()
    with caplog.at_level(logging.INFO):
        loader.write_dataframe(dataframe, loader.table, mode="append")

    assert dataframe.sink["format"] == "jdbc"
    assert dataframe.sink["mode"] == "append"
    assert dataframe.sink["saved"] is True
    options = dataframe.sink["options"]
    assert options["url"] == "jdbc:postgresql://localhost:5432/testdb"
    assert options["dbtable"] == "atm.atm_ejournal_withdrawals"
    assert options["user"] == "tester"
    assert options["password"] == "test-password"
    assert options["driver"] == "org.postgresql.Driver"
    assert options["batchsize"] == 1000
    assert "test-password" not in caplog.text          # credentials stay out of the log


def test_write_partitions_are_applied(cfg):
    cfg._data["greenplum"]["GREENPLUM_WRITE_PARTITIONS"] = 4       # noqa: SLF001
    dataframe = FakeDataFrame(partitions=16)
    GreenplumLoader(cfg).write_dataframe(dataframe, "atm_ejournal_withdrawals")
    assert dataframe._partitions == 4                              # noqa: SLF001


def test_write_failure_is_wrapped(loader):
    class Exploding(FakeDataFrame):
        @property
        def write(self):
            raise RuntimeError("connection refused")

    with pytest.raises(GreenplumLoadError, match="jdbc write into atm.atm_ejournal_withdrawals"):
        loader.write_dataframe(Exploding(), loader.table)


# --------------------------------------------------------------------------- #
# append / overwrite (the default path)
# --------------------------------------------------------------------------- #


def test_append_writes_straight_into_the_target_table(loader, monkeypatch):
    connection = RecordingConnection(scalars=[5])
    monkeypatch.setattr(loader, "connect", lambda: connection)
    monkeypatch.setattr(loader, "ensure_objects", lambda **kwargs: None)

    dataframe = FakeDataFrame()
    result = loader.load_batch(dataframe, "BATCH_0001", "RUN1", file_count=2,
                               parquet_path="/parquet/batch_0001", expected_rows=5)

    assert dataframe.sink["options"]["dbtable"] == "atm.atm_ejournal_withdrawals"
    assert dataframe.sink["mode"] == "append"
    assert result.committed is True
    assert result.strategy == "append"
    joined = " | ".join(connection.statements)
    assert "atm_ejournal_withdrawals_stg" not in joined            # no staging table involved
    assert 'INSERT INTO "atm"."atm_ejournal_batch_control"' in joined


def test_append_removes_rows_of_an_earlier_attempt_of_the_same_batch(loader, monkeypatch):
    connection = RecordingConnection(scalars=[5])
    monkeypatch.setattr(loader, "connect", lambda: connection)
    monkeypatch.setattr(loader, "ensure_objects", lambda **kwargs: None)

    loader.load_batch(FakeDataFrame(), "BATCH_0001", "RUN1", expected_rows=5)

    delete = [sql for sql in connection.statements if sql.startswith("DELETE FROM \"atm\".\"atm_e")]
    assert delete and "'RUN1'" in delete[0] and "'BATCH_0001'" in delete[0]


def test_overwrite_strategy(cfg, monkeypatch):
    cfg._data["greenplum"]["GREENPLUM_LOAD_STRATEGY"] = "overwrite"    # noqa: SLF001
    instance = GreenplumLoader(cfg)
    monkeypatch.setattr(instance, "connect", lambda: RecordingConnection(scalars=[3]))
    monkeypatch.setattr(instance, "ensure_objects", lambda **kwargs: None)

    dataframe = FakeDataFrame()
    instance.load_batch(dataframe, "BATCH_0001", "RUN1", expected_rows=3)

    assert dataframe.sink["mode"] == "overwrite"
    assert dataframe.sink["options"]["dbtable"] == "atm.atm_ejournal_withdrawals"


def test_unknown_strategy_is_rejected(cfg):
    cfg._data["greenplum"]["GREENPLUM_LOAD_STRATEGY"] = "sideways"     # noqa: SLF001
    with pytest.raises(GreenplumLoadError, match="unknown load strategy"):
        GreenplumLoader(cfg).load_batch(FakeDataFrame(), "B", "R")


def test_control_table_can_be_switched_off(cfg, monkeypatch):
    cfg._data["greenplum"]["GREENPLUM_USE_CONTROL_TABLE"] = False      # noqa: SLF001
    instance = GreenplumLoader(cfg)
    connection = RecordingConnection(scalars=[4])
    monkeypatch.setattr(instance, "connect", lambda: connection)
    monkeypatch.setattr(instance, "ensure_objects", lambda **kwargs: None)

    instance.load_batch(FakeDataFrame(), "BATCH_0001", "RUN1", expected_rows=4)

    assert not any("batch_control" in sql for sql in connection.statements)


# --------------------------------------------------------------------------- #
# Staged strategies
# --------------------------------------------------------------------------- #


@pytest.fixture
def staged_loader(cfg):
    cfg._data["greenplum"]["GREENPLUM_LOAD_STRATEGY"] = (               # noqa: SLF001
        "delete_insert_by_source_file")
    return GreenplumLoader(cfg)


def test_staged_load_writes_staging_then_promotes(staged_loader, monkeypatch):
    connection = RecordingConnection(scalars=[5, 5])
    monkeypatch.setattr(staged_loader, "connect", lambda: connection)
    monkeypatch.setattr(staged_loader, "ensure_objects", lambda **kwargs: None)

    dataframe = FakeDataFrame()
    result = staged_loader.load_batch(dataframe, "BATCH_0001", "RUN1", expected_rows=5)

    assert dataframe.sink["options"]["dbtable"] == "atm.atm_ejournal_withdrawals_stg"
    assert dataframe.sink["mode"] == "overwrite"
    joined = " | ".join(connection.statements)
    assert '"SOURCE_FILE_KEY" IN' in joined
    assert 'INSERT INTO "atm"."atm_ejournal_withdrawals"' in joined
    assert connection.committed and result.committed


def test_staged_row_count_mismatch_rolls_back(staged_loader, monkeypatch):
    connection = RecordingConnection(scalars=[3])       # staging 3, parquet 5
    monkeypatch.setattr(staged_loader, "connect", lambda: connection)
    monkeypatch.setattr(staged_loader, "ensure_objects", lambda **kwargs: None)

    with pytest.raises(GreenplumLoadError, match="staging row count mismatch"):
        staged_loader.load_batch(FakeDataFrame(), "BATCH_0001", "RUN1", expected_rows=5)
    assert connection.rolled_back and not connection.committed


def test_staged_insert_failure_rolls_back(staged_loader, monkeypatch):
    connection = RecordingConnection(scalars=[5], fail_on="INSERT INTO")
    monkeypatch.setattr(staged_loader, "connect", lambda: connection)
    monkeypatch.setattr(staged_loader, "ensure_objects", lambda **kwargs: None)

    with pytest.raises(GreenplumLoadError, match="Greenplum promote failed"):
        staged_loader.load_batch(FakeDataFrame(), "BATCH_0001", "RUN1", expected_rows=5)
    assert connection.rolled_back and not connection.committed


def test_merge_by_key_statements(cfg):
    cfg._data["greenplum"]["GREENPLUM_LOAD_STRATEGY"] = "merge_by_key"  # noqa: SLF001
    statements = GreenplumLoader(cfg)._delete_statements("RUN1", "BATCH_0001")   # noqa: SLF001
    assert "USING" in statements[-1]
    assert 't."ATM_NO" IS NOT DISTINCT FROM s."ATM_NO"' in statements[-1]


def test_truncate_load_statements(cfg):
    cfg._data["greenplum"]["GREENPLUM_LOAD_STRATEGY"] = "truncate_load"  # noqa: SLF001
    statements = GreenplumLoader(cfg)._delete_statements("R", "B")       # noqa: SLF001
    assert statements == ['TRUNCATE TABLE "atm"."atm_ejournal_withdrawals"']


# --------------------------------------------------------------------------- #
# DDL and recovery
# --------------------------------------------------------------------------- #


def test_control_table_ddl_uses_configured_distribution(cfg):
    cfg._data["greenplum"]["GREENPLUM_CONTROL_DISTRIBUTED_BY"] = "batch_id"   # noqa: SLF001
    ddl = GreenplumLoader(cfg)._control_table_ddl()                           # noqa: SLF001
    assert ddl.startswith('CREATE TABLE IF NOT EXISTS "atm"."atm_ejournal_batch_control"')
    assert ddl.endswith('DISTRIBUTED BY ("batch_id")')


def test_target_table_ddl_from_the_batch_schema(loader):
    ddl = loader._target_table_ddl(FakeDataFrame().schema)                    # noqa: SLF001
    assert '"ATM_NO" text' in ddl and '"BATCH_ID" text' in ddl


def test_is_batch_committed_reads_the_control_table(loader, monkeypatch):
    monkeypatch.setattr(loader, "connect", lambda: RecordingConnection(scalars=[7]))
    assert loader.is_batch_committed("RUN1", "BATCH_0001") == 7


def test_is_batch_committed_falls_back_to_the_target_table(loader, monkeypatch):
    # no control row -> count the batch's rows in the target table
    monkeypatch.setattr(loader, "connect", lambda: RecordingConnection(scalars=[None, 4]))
    assert loader.is_batch_committed("RUN1", "BATCH_0002") == 4

    monkeypatch.setattr(loader, "connect", lambda: RecordingConnection(scalars=[None, 0]))
    assert loader.is_batch_committed("RUN1", "BATCH_0003") is None


def test_is_batch_committed_refuses_to_guess_when_unreachable(loader, monkeypatch):
    class Broken(RecordingConnection):
        def scalar(self, sql):
            raise RuntimeError("database unreachable")

    monkeypatch.setattr(loader, "connect", lambda: Broken())
    with pytest.raises(GreenplumLoadError, match="refusing to guess"):
        loader.is_batch_committed("RUN1", "BATCH_0001")


def test_password_is_not_echoed_in_a_connection_error(cfg):
    from greenplum_loader import ControlConnection

    with pytest.raises(GreenplumLoadError) as error:
        ControlConnection.open("jdbc:postgresql://127.0.0.1:1/db", "user", "TOP-SECRET",
                               spark=None)
    assert "TOP-SECRET" not in str(error.value)


# --------------------------------------------------------------------------- #
# The greenplum-spark connector writer
# --------------------------------------------------------------------------- #


def test_connector_writer_uses_dbschema_and_its_options(cfg):
    cfg._data["greenplum"]["GREENPLUM_WRITE_FORMAT"] = "greenplum"      # noqa: SLF001
    cfg._data["greenplum"]["GREENPLUM_CONNECTOR_OPTIONS"] = {           # noqa: SLF001
        "server.port": "32768-42768", "segment.num": "16",
        "numWriteTasks": "32", "gpfdist.sessions": "32", "compression": "gzip",
    }
    instance = GreenplumLoader(cfg)
    dataframe = FakeDataFrame()

    instance.write_dataframe(dataframe, instance.table, mode="append")

    assert dataframe.sink["format"] == "greenplum"
    options = dataframe.sink["options"]
    assert options["dbschema"] == "atm"
    assert options["dbtable"] == "atm_ejournal_withdrawals"     # schema passed separately
    assert options["url"] == "jdbc:postgresql://localhost:5432/testdb"
    assert options["server.port"] == "32768-42768"
    assert options["segment.num"] == "16"
    assert options["numWriteTasks"] == "32"
    assert options["gpfdist.sessions"] == "32"
    assert options["compression"] == "gzip"
    assert "batchsize" not in options                           # jdbc-only option
    assert dataframe.sink["mode"] == "append"


def test_connector_failures_name_the_format(cfg):
    cfg._data["greenplum"]["GREENPLUM_WRITE_FORMAT"] = "greenplum"      # noqa: SLF001
    instance = GreenplumLoader(cfg)

    class Exploding(FakeDataFrame):
        @property
        def write(self):
            raise RuntimeError("gpfdist port range unavailable")

    with pytest.raises(GreenplumLoadError, match="greenplum write into"):
        instance.write_dataframe(Exploding(), instance.table)
