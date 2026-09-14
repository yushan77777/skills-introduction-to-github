"""
End-to-end load against a real database (opt-in).

Greenplum speaks the PostgreSQL protocol, so this suite runs against either a
Greenplum cluster or a plain PostgreSQL instance. It is skipped unless the
connection is provided in the environment::

    export ATM_ETL_TEST_JDBC_URL="jdbc:postgresql://127.0.0.1:5432/bidb"
    export ATM_ETL_TEST_DB_USER="etl_user"
    export ATM_ETL_TEST_DB_PASSWORD="etl_password"
    export ATM_ETL_TEST_JDBC_JAR="/opt/jars/postgresql-42.7.4.jar"
    python3 -m pytest tests/test_greenplum_integration.py -q

What it proves, with no mocks in the Greenplum path: the ETL creates its tables,
the Spark JDBC write lands the parsed rows, the batch control table is filled in,
a second run loads nothing, and the staged strategy promotes through the staging
table.
"""

from __future__ import annotations

import os

import pytest
import yaml

import fixtures
from config_loader import load_config
from etl_runner import AtmEjournalEtl

JDBC_URL = os.environ.get("ATM_ETL_TEST_JDBC_URL")
DB_USER = os.environ.get("ATM_ETL_TEST_DB_USER", "")
DB_PASSWORD = os.environ.get("ATM_ETL_TEST_DB_PASSWORD", "")
JDBC_JAR = os.environ.get("ATM_ETL_TEST_JDBC_JAR", "")
DB_SCHEMA = os.environ.get("ATM_ETL_TEST_DB_SCHEMA", "atm")

pytestmark = [
    pytest.mark.spark,
    pytest.mark.skipif(not JDBC_URL,
                       reason="set ATM_ETL_TEST_JDBC_URL to run the database integration tests"),
]


def _configure(etl_home: str, batch_size: int, strategy: str, table: str,
               parse_engine: str = "spark") -> str:
    """Write the test configuration, pointed at the real database."""
    config_path = fixtures.write_config(etl_home, batch_size=batch_size,
                                        parse_engine=parse_engine)
    with open(config_path) as handle:
        data = yaml.safe_load(handle)

    data["defaults"]["greenplum"].update({
        "GREENPLUM_URL": JDBC_URL,
        "GREENPLUM_USER": DB_USER,
        "GREENPLUM_PASSWORD": DB_PASSWORD,
        "GREENPLUM_DRIVER": "org.postgresql.Driver",
        "GREENPLUM_SCHEMA": DB_SCHEMA,
        "GREENPLUM_TABLE": table,
        "GREENPLUM_STAGING_TABLE": f"{table}_stg",
        "GREENPLUM_CONTROL_TABLE": f"{table}_control",
        "GREENPLUM_LOAD_STRATEGY": strategy,
    })
    if JDBC_JAR:
        data["defaults"]["spark"]["SPARK_JARS"] = JDBC_JAR
        data["defaults"]["spark"]["EXTRA_CONF"]["spark.driver.extraClassPath"] = JDBC_JAR

    with open(config_path, "w") as handle:
        yaml.safe_dump(data, handle)
    return config_path


def _sql(loader, statement):
    connection = loader.connect()
    try:
        return connection.scalar(statement)
    finally:
        connection.close()


def _drop(loader, table):
    connection = loader.connect()
    try:
        for name in (table, f"{table}_stg", f"{table}_control"):
            connection.execute(f'DROP TABLE IF EXISTS "{DB_SCHEMA}"."{name}"')
        connection.commit()
    finally:
        connection.close()


@pytest.fixture
def real_table(etl_home, request, spark):
    """
    A throw-away target table, dropped before and after the test.

    The loader is given the SparkSession because the control connection falls
    back to the JDBC driver on the Spark classpath when psycopg2 is not
    installed - which is exactly how it runs on an edge node without psycopg2.
    """
    from greenplum_loader import GreenplumLoader

    table = f"it_{request.node.name[:40]}".replace("[", "_").replace("]", "_")
    config_path = _configure(etl_home, batch_size=2, strategy="append", table=table)
    loader = GreenplumLoader(load_config(config_path, "atm_ejournal"), spark=spark)
    _drop(loader, table)
    yield table, config_path, loader
    _drop(loader, table)


def test_batches_are_written_into_the_target_table(etl_home, real_table, spark):
    table, config_path, loader = real_table
    fixtures.build_input_tree(os.path.join(etl_home, "ATM_EJOURNALS"),
                              atms=2, files_per_atm=2, transactions=2)

    summary = AtmEjournalEtl(load_config(config_path, "atm_ejournal"), run_id="IT1").run()

    assert summary.status == "SUCCESS"
    assert summary.files_processed == 4
    assert summary.records_processed == 8
    assert summary.records_loaded == 8
    assert _sql(loader, f'SELECT COUNT(*) FROM "{DB_SCHEMA}"."{table}"') == 8
    assert _sql(loader, f'SELECT COUNT(*) FROM "{DB_SCHEMA}"."{table}" '
                        "WHERE \"STATUS\" = 'SUCCESS'") == 8
    assert _sql(loader, f'SELECT SUM("AMOUNT") FROM "{DB_SCHEMA}"."{table}"') == 400000

    # the batch control table records both batches
    assert _sql(loader, f'SELECT COUNT(*) FROM "{DB_SCHEMA}"."{table}_control" '
                        "WHERE \"status\" = 'SUCCESS'") == 2
    assert _sql(loader, f'SELECT SUM("rows_loaded") FROM "{DB_SCHEMA}"."{table}_control"') == 8


def test_second_run_loads_nothing(etl_home, real_table, spark):
    table, config_path, loader = real_table
    fixtures.build_input_tree(os.path.join(etl_home, "ATM_EJOURNALS"),
                              atms=1, files_per_atm=2, transactions=1)

    AtmEjournalEtl(load_config(config_path, "atm_ejournal"), run_id="IT1").run()
    rows_after_first = _sql(loader, f'SELECT COUNT(*) FROM "{DB_SCHEMA}"."{table}"')

    summary = AtmEjournalEtl(load_config(config_path, "atm_ejournal"), run_id="IT2").run()

    assert summary.files_previously_processed == 2
    assert summary.batches_processed == 0
    assert _sql(loader, f'SELECT COUNT(*) FROM "{DB_SCHEMA}"."{table}"') == rows_after_first


def test_staged_strategy_promotes_through_the_staging_table(etl_home, real_table, spark):
    table, _config_path, loader = real_table
    config_path = _configure(etl_home, batch_size=2, strategy="delete_insert_by_source_file",
                             table=table)
    fixtures.build_input_tree(os.path.join(etl_home, "ATM_EJOURNALS"),
                              atms=1, files_per_atm=2, transactions=2)

    summary = AtmEjournalEtl(load_config(config_path, "atm_ejournal"), run_id="IT1").run()

    assert summary.status == "SUCCESS"
    assert _sql(loader, f'SELECT COUNT(*) FROM "{DB_SCHEMA}"."{table}"') == 4
    assert _sql(loader, f'SELECT COUNT(*) FROM "{DB_SCHEMA}"."{table}_stg"') == 4

    # reload the same files: the staged strategy replaces their rows
    os.remove(os.path.join(etl_home, "processed", "processed_files.csv"))
    AtmEjournalEtl(load_config(config_path, "atm_ejournal"), run_id="IT2").run()

    assert _sql(loader, f'SELECT COUNT(*) FROM "{DB_SCHEMA}"."{table}"') == 4


def test_is_batch_committed_reflects_a_real_load(etl_home, real_table, spark):
    table, config_path, loader = real_table
    fixtures.build_input_tree(os.path.join(etl_home, "ATM_EJOURNALS"),
                              atms=1, files_per_atm=1, transactions=2)

    AtmEjournalEtl(load_config(config_path, "atm_ejournal"), run_id="IT1").run()

    assert loader.is_batch_committed("IT1", "BATCH_0001") == 2
    assert loader.is_batch_committed("IT1", "BATCH_0099") is None


def test_local_engine_loads_into_the_database(etl_home, real_table, spark):
    """
    The path for a PySpark that cannot ship Python code: parsed here, written to
    parquet with PyArrow, loaded by Spark over JDBC.
    """
    pytest.importorskip("pyarrow", reason="pyarrow is not installed")
    table, _config_path, loader = real_table
    config_path = _configure(etl_home, batch_size=2, strategy="append", table=table,
                             parse_engine="local")
    fixtures.build_input_tree(os.path.join(etl_home, "ATM_EJOURNALS"),
                              atms=2, files_per_atm=1, transactions=2)

    etl = AtmEjournalEtl(load_config(config_path, "atm_ejournal"), run_id="ITL1")
    summary = etl.run()

    assert etl.parse_engine == "local"
    assert summary.status == "SUCCESS"
    assert summary.records_loaded == 4
    assert _sql(loader, f'SELECT COUNT(*) FROM "{DB_SCHEMA}"."{table}"') == 4
    assert _sql(loader, f'SELECT SUM("AMOUNT") FROM "{DB_SCHEMA}"."{table}"') == 200000
    assert _sql(loader, f'SELECT COUNT(DISTINCT "ATM_NO") FROM "{DB_SCHEMA}"."{table}"') == 2
    # the parquet PyArrow wrote is readable by Spark with the expected types
    assert _sql(loader, f'SELECT COUNT(*) FROM "{DB_SCHEMA}"."{table}" '
                        'WHERE "TRANSACTION_DATETIME" IS NOT NULL AND "DATE" IS NOT NULL') == 4
