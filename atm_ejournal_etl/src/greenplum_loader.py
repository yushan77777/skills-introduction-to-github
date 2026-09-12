"""
Greenplum loading for the ATM E-Journal ETL.

The batch parquet is loaded through a **staging table**, and promoted into the
target table inside a single transaction that also records the batch in a
control table::

    parquet/batch_0001  --(Spark write)-->  atm.atm_ejournal_withdrawals_stg
                                                     |
                              BEGIN                   |
                                DELETE target rows for this batch's files
                                INSERT INTO target SELECT * FROM staging
                                INSERT INTO control (run_id, batch_id, ...)
                              COMMIT
                                                     |
                                            target + control row

Why this shape
--------------
* The commit is atomic: the target rows and the "this batch is loaded" marker
  become visible together, so a crash can never leave one without the other.
* The control row is the authority used on restart. Files are marked SUCCESS in
  ``processed_files.csv`` only after this commit; if the ETL dies in between,
  the next run finds the pending marker, sees the committed control row and
  completes the bookkeeping instead of re-loading the data.
* ``delete_insert_by_source_file`` (default) makes a re-run of the same files
  idempotent: whatever those files loaded before is removed before the insert,
  so a retried file cannot duplicate rows in Greenplum.

Load strategies (``GREENPLUM_LOAD_STRATEGY``)
---------------------------------------------
``delete_insert_by_source_file``
    Delete every target row whose ``SOURCE_FILE_KEY`` appears in the staging
    batch, then insert. Recommended: the journal file is the natural unit of
    reprocessing.
``merge_by_key``
    Delete target rows matching ``GREENPLUM_MERGE_KEYS`` and insert - for a
    table whose grain is a business key rather than the source file.
``insert_only``
    Plain append. No idempotency; only for a feed that is de-duplicated
    downstream.
``truncate_load``
    Truncate the target, then insert - for a full reload of a small table.

Passwords are decrypted through the project encryptor and are never logged or
included in an exception message.
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger("atm_ejournal.greenplum")

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")

#: Spark type name -> Greenplum/PostgreSQL column type, for generated DDL.
TYPE_MAP = {
    "StringType": "text",
    "BooleanType": "boolean",
    "IntegerType": "integer",
    "LongType": "bigint",
    "DoubleType": "numeric(20,2)",
    "FloatType": "numeric(20,2)",
    "DateType": "date",
    "TimestampType": "timestamp",
}


class GreenplumLoadError(Exception):
    """Raised when a batch could not be loaded into Greenplum."""


@dataclass
class LoadResult:
    """Outcome of one batch load - recorded in the run summary and the logs."""

    batch_id: str
    run_id: str
    staged_rows: int = 0
    rows_loaded: int = 0
    rows_deleted: int = 0
    duration_seconds: float = 0.0
    strategy: str = ""
    committed: bool = False
    target_table: str = ""
    warnings: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return self.__dict__.copy()


def quote_identifier(name: str) -> str:
    """Validate and quote a configured table/schema/column name."""
    name = str(name).strip()
    if not IDENTIFIER_RE.match(name):
        raise GreenplumLoadError(f"invalid SQL identifier in configuration: {name!r}")
    return f'"{name}"'


def sql_literal(value: Optional[str]) -> str:
    """Single-quoted literal for the internally generated values used in SQL."""
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


# --------------------------------------------------------------------------- #
# Control connection (psycopg2 when available, JDBC through the JVM otherwise)
# --------------------------------------------------------------------------- #


class ControlConnection:
    """
    A transactional connection used for the DDL/DML around the bulk load.

    Uses ``psycopg2`` when it is installed; otherwise it borrows the JDBC driver
    already on the Spark classpath (``postgresql-42.x.jar``) through the JVM, so
    no extra Python dependency is required on the edge node.
    """

    def __init__(self, connection, flavour: str):
        self._connection = connection
        self.flavour = flavour

    # -- construction ------------------------------------------------------ #

    @classmethod
    def open(cls, host: str, port: int, database: str, user: str, password: str,
             spark=None, timeout_seconds: int = 3600) -> "ControlConnection":
        try:
            import psycopg2                            # noqa: PLC0415 - optional dependency
        except ImportError:
            psycopg2 = None                            # type: ignore

        if psycopg2 is not None:
            try:
                connection = psycopg2.connect(host=host, port=port, dbname=database,
                                              user=user, password=password,
                                              connect_timeout=30)
                connection.autocommit = False
                logger.debug("control connection established (psycopg2)")
                return cls(connection, "psycopg2")
            except Exception as exc:                   # noqa: BLE001 - password never echoed
                raise GreenplumLoadError(
                    f"could not connect to Greenplum {host}:{port}/{database} as {user} "
                    f"({type(exc).__name__})") from None

        if spark is None:
            raise GreenplumLoadError("psycopg2 is not installed and no SparkSession is "
                                     "available for a JDBC control connection")
        try:
            jvm = spark._jvm                                          # noqa: SLF001
            properties = jvm.java.util.Properties()
            properties.setProperty("user", user)
            properties.setProperty("password", password)
            url = f"jdbc:postgresql://{host}:{port}/{database}"
            connection = jvm.java.sql.DriverManager.getConnection(url, properties)
            connection.setAutoCommit(False)
            logger.debug("control connection established (JDBC through the JVM)")
            return cls(connection, "jdbc")
        except Exception as exc:                       # noqa: BLE001
            raise GreenplumLoadError(
                f"could not open a JDBC control connection to {host}:{port}/{database} "
                f"({type(exc).__name__})") from None

    # -- statements -------------------------------------------------------- #

    def execute(self, sql: str) -> int:
        """Run a statement and return the affected row count (-1 when unknown)."""
        logger.debug("SQL: %s", sql)
        if self.flavour == "psycopg2":
            with self._connection.cursor() as cursor:
                cursor.execute(sql)
                return cursor.rowcount if cursor.rowcount is not None else -1
        statement = self._connection.createStatement()
        try:
            statement.execute(sql)
            return statement.getUpdateCount()
        finally:
            statement.close()

    def scalar(self, sql: str) -> Any:
        logger.debug("SQL: %s", sql)
        if self.flavour == "psycopg2":
            with self._connection.cursor() as cursor:
                cursor.execute(sql)
                row = cursor.fetchone()
                return row[0] if row else None
        statement = self._connection.createStatement()
        try:
            results = statement.executeQuery(sql)
            try:
                return results.getObject(1) if results.next() else None
            finally:
                results.close()
        finally:
            statement.close()

    def commit(self) -> None:
        self._connection.commit()

    def rollback(self) -> None:
        try:
            self._connection.rollback()
        except Exception as exc:                       # noqa: BLE001
            logger.error("rollback failed: %s", type(exc).__name__)

    def close(self) -> None:
        try:
            self._connection.close()
        except Exception:                              # noqa: BLE001
            pass


# --------------------------------------------------------------------------- #
# Loader
# --------------------------------------------------------------------------- #


class GreenplumLoader:
    """Loads batch parquet into Greenplum and owns the batch control table."""

    def __init__(self, cfg, spark=None, secret_resolver=None):
        self.cfg = cfg
        self.spark = spark
        self.host = str(cfg.require("greenplum.GREENPLUM_HOST"))
        self.port = cfg.get_int("greenplum.GREENPLUM_PORT", 5432)
        self.database = str(cfg.require("greenplum.GREENPLUM_DATABASE"))
        self.schema = str(cfg.require("greenplum.GREENPLUM_SCHEMA"))
        self.table = str(cfg.require("greenplum.GREENPLUM_TABLE"))
        self.staging = str(cfg.get("greenplum.GREENPLUM_STAGING_TABLE", f"{self.table}_stg"))
        self.control = str(cfg.get("greenplum.GREENPLUM_CONTROL_TABLE",
                                   "atm_ejournal_batch_control"))
        self.user = str(cfg.require("greenplum.GREENPLUM_USER"))
        self.driver = str(cfg.get("greenplum.GREENPLUM_DRIVER", "org.postgresql.Driver"))
        self.write_format = str(cfg.get("greenplum.GREENPLUM_WRITE_FORMAT", "jdbc")).lower()
        self.strategy = str(cfg.get("greenplum.GREENPLUM_LOAD_STRATEGY",
                                    "delete_insert_by_source_file")).lower()
        self.merge_keys = cfg.get_list("greenplum.GREENPLUM_MERGE_KEYS")
        self.batch_size = cfg.get_int("greenplum.GREENPLUM_JDBC_BATCH_SIZE", 20000)
        self.write_partitions = cfg.get_int("greenplum.GREENPLUM_WRITE_PARTITIONS", 0)
        self.create_objects = cfg.get_bool("greenplum.GREENPLUM_CREATE_OBJECTS", True)
        self.query_timeout = cfg.get_int("greenplum.GREENPLUM_QUERY_TIMEOUT_SECONDS", 3600)
        self.target_distributed_by = str(cfg.get("greenplum.GREENPLUM_TARGET_DISTRIBUTED_BY", ""))
        self.control_distributed_by = str(cfg.get("greenplum.GREENPLUM_CONTROL_DISTRIBUTED_BY", ""))

        self._secret_resolver = secret_resolver
        self._password: Optional[str] = None
        self._objects_ready = False

    # -- connection details ------------------------------------------------- #

    @property
    def password(self) -> str:
        """Decrypted Greenplum password. Cached in memory, never logged."""
        if self._password is None:
            if self._secret_resolver is None:
                from encryption_util import SecretResolver     # noqa: PLC0415
                self._secret_resolver = SecretResolver.from_config(self.cfg)
            self._password = self._secret_resolver.resolve_config_secret(
                self.cfg, "greenplum.GREENPLUM_PASSWORD", "greenplum.GREENPLUM_PICKLE")
        return self._password

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    def qualified(self, table: str) -> str:
        return f"{quote_identifier(self.schema)}.{quote_identifier(table)}"

    @property
    def target_table(self) -> str:
        return self.qualified(self.table)

    @property
    def staging_table(self) -> str:
        return self.qualified(self.staging)

    @property
    def control_table(self) -> str:
        return self.qualified(self.control)

    def connect(self) -> ControlConnection:
        return ControlConnection.open(self.host, self.port, self.database, self.user,
                                      self.password, spark=self.spark,
                                      timeout_seconds=self.query_timeout)

    # -- DDL ---------------------------------------------------------------- #

    def ensure_objects(self, schema=None, connection: Optional[ControlConnection] = None) -> None:
        """
        Create the control table (and the target table, from the parquet schema)
        when they do not exist. Disabled with ``GREENPLUM_CREATE_OBJECTS: false``
        for sites where DDL is applied by a DBA.
        """
        if self._objects_ready or not self.create_objects:
            return
        owned = connection is None
        connection = connection or self.connect()
        try:
            connection.execute(self._control_table_ddl())
            if schema is not None:
                connection.execute(self._target_table_ddl(schema))
            connection.commit()
            self._objects_ready = True
            logger.info("Greenplum objects verified | target=%s | staging=%s | control=%s",
                        self.target_table, self.staging_table, self.control_table)
        except Exception as exc:                       # noqa: BLE001
            connection.rollback()
            raise GreenplumLoadError(f"could not create/verify Greenplum objects: {exc}") from exc
        finally:
            if owned:
                connection.close()

    def _control_table_ddl(self) -> str:
        distribution = (f" DISTRIBUTED BY ({quote_identifier(self.control_distributed_by)})"
                        if self.control_distributed_by else "")
        return (
            f"CREATE TABLE IF NOT EXISTS {self.control_table} ("
            ' "etl_name" text,'
            ' "run_id" text,'
            ' "batch_id" text,'
            ' "file_count" integer,'
            ' "record_count" bigint,'
            ' "rows_loaded" bigint,'
            ' "parquet_path" text,'
            ' "load_started" timestamp,'
            ' "load_completed" timestamp,'
            ' "status" text'
            f"){distribution}"
        )

    def _target_table_ddl(self, schema) -> str:
        columns = []
        for field_ in schema.fields:
            type_name = type(field_.dataType).__name__
            columns.append(f"{quote_identifier(field_.name)} "
                           f"{TYPE_MAP.get(type_name, 'text')}")
        distribution = (f" DISTRIBUTED BY ({quote_identifier(self.target_distributed_by)})"
                        if self.target_distributed_by else "")
        return (f"CREATE TABLE IF NOT EXISTS {self.target_table} ("
                + ", ".join(columns) + f"){distribution}")

    # -- staging ------------------------------------------------------------ #

    def write_staging(self, dataframe, batch_id: str) -> int:
        """
        Bulk-write the batch parquet into the staging table (mode ``overwrite``,
        so the staging table always holds exactly one batch).
        """
        to_write = dataframe
        if self.write_partitions > 0:
            try:
                if dataframe.rdd.getNumPartitions() > self.write_partitions:
                    to_write = dataframe.coalesce(self.write_partitions)
                else:
                    to_write = dataframe.repartition(self.write_partitions)
            except Exception:                          # noqa: BLE001 - partitioning is advisory
                to_write = dataframe

        logger.info("Greenplum staging write started | batch=%s | table=%s | format=%s",
                    batch_id, self.staging_table, self.write_format)
        started = time.time()
        try:
            writer = to_write.write.mode("overwrite")
            if self.write_format == "greenplum":
                (writer.format("greenplum")
                 .option("url", self.jdbc_url)
                 .option("user", self.user)
                 .option("password", self.password)
                 .option("dbschema", self.schema)
                 .option("dbtable", self.staging)
                 .save())
            else:
                (writer.format("jdbc")
                 .option("url", self.jdbc_url)
                 .option("dbtable", f"{self.schema}.{self.staging}")
                 .option("user", self.user)
                 .option("password", self.password)
                 .option("driver", self.driver)
                 .option("batchsize", self.batch_size)
                 .option("truncate", "false")
                 .save())
        except Exception as exc:                       # noqa: BLE001 - message may hold the URL only
            raise GreenplumLoadError(f"staging write failed for {batch_id} into "
                                     f"{self.staging_table}: {exc}") from exc
        logger.info("Greenplum staging write completed | batch=%s | %.1fs",
                    batch_id, time.time() - started)
        return time.time() - started

    # -- promotion ---------------------------------------------------------- #

    def _delete_statements(self, run_id: str, batch_id: str) -> List[str]:
        """SQL that removes anything this batch is about to (re)insert."""
        statements = [
            # A retry of this exact batch: remove what the previous attempt left.
            f"DELETE FROM {self.target_table} WHERE "
            f'"ETL_RUN_ID" = {sql_literal(run_id)} AND "BATCH_ID" = {sql_literal(batch_id)}'
        ]
        if self.strategy == "delete_insert_by_source_file":
            statements.append(
                f"DELETE FROM {self.target_table} t WHERE t.\"SOURCE_FILE_KEY\" IN "
                f"(SELECT DISTINCT s.\"SOURCE_FILE_KEY\" FROM {self.staging_table} s)")
        elif self.strategy == "merge_by_key":
            conditions = " AND ".join(
                f't.{quote_identifier(key)} IS NOT DISTINCT FROM s.{quote_identifier(key)}'
                for key in self.merge_keys)
            statements.append(
                f"DELETE FROM {self.target_table} t USING {self.staging_table} s "
                f"WHERE {conditions}")
        elif self.strategy == "truncate_load":
            statements = [f"TRUNCATE TABLE {self.target_table}"]
        elif self.strategy != "insert_only":
            raise GreenplumLoadError(f"unknown load strategy: {self.strategy}")
        return statements

    def load_batch(self, dataframe, batch_id: str, run_id: str,
                   file_count: int = 0, parquet_path: str = "",
                   expected_rows: Optional[int] = None) -> LoadResult:
        """
        Load one batch: staging write, then an atomic promote + control row.

        Returns a :class:`LoadResult`. Raises :class:`GreenplumLoadError` with
        the transaction rolled back if anything fails - in that case nothing is
        visible in the target table and no control row exists, so the batch is
        simply re-run next time.
        """
        result = LoadResult(batch_id=batch_id, run_id=run_id, strategy=self.strategy,
                            target_table=f"{self.schema}.{self.table}")
        started = time.time()
        load_started_at = datetime.now()

        self.ensure_objects(schema=dataframe.schema)
        self.write_staging(dataframe, batch_id)

        connection = self.connect()
        try:
            staged = connection.scalar(f"SELECT COUNT(*) FROM {self.staging_table}")
            result.staged_rows = int(staged or 0)
            if expected_rows is not None and result.staged_rows != expected_rows:
                raise GreenplumLoadError(
                    f"staging row count mismatch for {batch_id}: parquet has {expected_rows} "
                    f"row(s), staging table has {result.staged_rows}")

            logger.info("Greenplum load started | batch=%s | strategy=%s | staged rows=%d",
                        batch_id, self.strategy, result.staged_rows)

            deleted = 0
            for statement in self._delete_statements(run_id, batch_id):
                affected = connection.execute(statement)
                deleted += max(affected, 0)
            result.rows_deleted = deleted
            if deleted:
                logger.info("Greenplum load | batch=%s | %d existing row(s) removed before "
                            "insert (idempotency: %s)", batch_id, deleted, self.strategy)

            columns = ", ".join(quote_identifier(field_.name) for field_ in dataframe.schema.fields)
            connection.execute(
                f"INSERT INTO {self.target_table} ({columns}) "
                f"SELECT {columns} FROM {self.staging_table}")

            connection.execute(
                f"DELETE FROM {self.control_table} WHERE "
                f'"etl_name" = {sql_literal(self.cfg.etl_name)} AND '
                f'"run_id" = {sql_literal(run_id)} AND "batch_id" = {sql_literal(batch_id)}')
            connection.execute(
                f"INSERT INTO {self.control_table} "
                '("etl_name", "run_id", "batch_id", "file_count", "record_count", '
                '"rows_loaded", "parquet_path", "load_started", "load_completed", "status") '
                f"VALUES ({sql_literal(self.cfg.etl_name)}, {sql_literal(run_id)}, "
                f"{sql_literal(batch_id)}, {int(file_count)}, {int(result.staged_rows)}, "
                f"{int(result.staged_rows)}, {sql_literal(parquet_path)}, "
                f"{sql_literal(load_started_at.strftime('%Y-%m-%d %H:%M:%S'))}, "
                f"{sql_literal(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}, 'SUCCESS')")

            connection.commit()
            result.committed = True
        except GreenplumLoadError:
            connection.rollback()
            raise
        except Exception as exc:                       # noqa: BLE001
            connection.rollback()
            raise GreenplumLoadError(f"Greenplum load failed for {batch_id}: {exc}") from exc
        finally:
            if not result.committed:
                logger.error("Greenplum transaction rolled back for batch %s - no rows and no "
                             "control record were committed", batch_id)
            connection.close()

        # Verified outside the write transaction, so it reflects what is visible.
        result.rows_loaded = self.count_batch_rows(run_id, batch_id)
        result.duration_seconds = time.time() - started
        if expected_rows is not None and result.rows_loaded != expected_rows:
            warning = (f"target row count for {batch_id} is {result.rows_loaded}, expected "
                       f"{expected_rows}")
            result.warnings.append(warning)
            logger.warning("Greenplum load | %s", warning)
        logger.info("Greenplum load completed | batch=%s | rows loaded=%d | %.1fs",
                    batch_id, result.rows_loaded, result.duration_seconds)
        return result

    # -- verification / recovery -------------------------------------------- #

    def count_batch_rows(self, run_id: str, batch_id: str) -> int:
        connection = self.connect()
        try:
            value = connection.scalar(
                f"SELECT COUNT(*) FROM {self.target_table} WHERE "
                f'"ETL_RUN_ID" = {sql_literal(run_id)} AND "BATCH_ID" = {sql_literal(batch_id)}')
            return int(value or 0)
        except Exception as exc:                       # noqa: BLE001 - verification only
            logger.warning("could not verify loaded rows for %s: %s", batch_id, exc)
            return -1
        finally:
            connection.close()

    def is_batch_committed(self, run_id: str, batch_id: str) -> Optional[int]:
        """
        Ask Greenplum whether a batch committed - the question a restart has to
        answer for a pending marker. Returns the loaded row count, or ``None``
        when the batch never committed.
        """
        connection = self.connect()
        try:
            value = connection.scalar(
                f"SELECT SUM(\"rows_loaded\") FROM {self.control_table} WHERE "
                f'"etl_name" = {sql_literal(self.cfg.etl_name)} AND '
                f'"run_id" = {sql_literal(run_id)} AND "batch_id" = {sql_literal(batch_id)} '
                "AND \"status\" = 'SUCCESS'")
            return None if value is None else int(value)
        except Exception as exc:                       # noqa: BLE001
            logger.error("could not read the batch control table (%s): %s",
                         self.control_table, exc)
            raise GreenplumLoadError("batch control table could not be read; refusing to "
                                     "guess whether the batch committed") from exc
        finally:
            connection.close()

    def loaded_file_keys(self, file_keys: Iterable[str]) -> List[str]:
        """
        Which of these source files already have rows in the target table.
        Used by the recovery step when a control row is missing.
        """
        keys = [key for key in file_keys if key]
        if not keys:
            return []
        in_list = ", ".join(sql_literal(key) for key in keys)
        connection = self.connect()
        try:
            if connection.flavour == "psycopg2":
                with connection._connection.cursor() as cursor:       # noqa: SLF001
                    cursor.execute(f'SELECT DISTINCT "SOURCE_FILE_KEY" FROM {self.target_table} '
                                   f'WHERE "SOURCE_FILE_KEY" IN ({in_list})')
                    return [row[0] for row in cursor.fetchall()]
            statement = connection._connection.createStatement()      # noqa: SLF001
            try:
                results = statement.executeQuery(
                    f'SELECT DISTINCT "SOURCE_FILE_KEY" FROM {self.target_table} '
                    f'WHERE "SOURCE_FILE_KEY" IN ({in_list})')
                found = []
                while results.next():
                    found.append(results.getString(1))
                results.close()
                return found
            finally:
                statement.close()
        except Exception as exc:                       # noqa: BLE001
            logger.warning("could not check loaded source files: %s", exc)
            return []
        finally:
            connection.close()
