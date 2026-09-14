"""
Greenplum loading for the ATM E-Journal ETL.

The write itself is the plain Spark JDBC write::

    df.write \\
        .format("jdbc") \\
        .option("url", "jdbc:postgresql://<host>:5432/<database>") \\
        .option("dbtable", "schema.table_name") \\
        .option("user", "username") \\
        .option("password", "password") \\
        .option("driver", "org.postgresql.Driver") \\
        .mode("append") \\
        .save()

``url``, ``user``, ``password`` and ``driver`` come straight from the
``greenplum:`` section of the configuration file - no encryption layer.

Load strategies (``GREENPLUM_LOAD_STRATEGY``)
---------------------------------------------
``append`` (default)
    The write above, straight into the target table.
``overwrite``
    The same write with ``mode("overwrite")`` - a full reload of the table.
``delete_insert_by_source_file``
    Writes the batch into a staging table first, then, in one transaction,
    deletes the target rows belonging to this batch's source files and inserts
    the staged rows. Use it when a file may be delivered or reprocessed twice
    and duplicates in the target are not acceptable.
``merge_by_key``
    Same, but the delete matches ``GREENPLUM_MERGE_KEYS`` instead of the source
    file.
``truncate_load``
    Staging, then truncate the target and insert.

Batch bookkeeping
-----------------
When ``GREENPLUM_USE_CONTROL_TABLE`` is on (default), every loaded batch is
recorded in the control table. That row is what a restart consults: if the ETL
dies between the Greenplum write and the update of ``processed_files.csv``, the
next run sees the control row and completes the bookkeeping instead of loading
the batch a second time. With it off, the same question is answered by counting
the batch's rows in the target table (every row carries ``ETL_RUN_ID`` and
``BATCH_ID``).
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger("atm_ejournal.greenplum")

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")

#: jdbc:postgresql://host:port/database?params
JDBC_URL_RE = re.compile(
    r"^jdbc:(?P<flavour>postgresql|pivotal:greenplum)://(?P<host>[^:/?]+)"
    r"(?::(?P<port>\d+))?/(?P<database>[^?;]+)", re.I)

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

STAGED_STRATEGIES = ("delete_insert_by_source_file", "merge_by_key", "truncate_load")
DIRECT_STRATEGIES = ("append", "overwrite")


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


def parse_jdbc_url(url: str) -> Tuple[str, int, str]:
    """``jdbc:postgresql://host:5432/db`` -> ``("host", 5432, "db")``."""
    match = JDBC_URL_RE.match(str(url).strip())
    if not match:
        raise GreenplumLoadError(
            "greenplum.GREENPLUM_URL must look like "
            "jdbc:postgresql://<host>:<port>/<database>")
    return (match.group("host"),
            int(match.group("port") or 5432),
            match.group("database"))


# --------------------------------------------------------------------------- #
# Control connection (psycopg2 when available, JDBC through the JVM otherwise)
# --------------------------------------------------------------------------- #


class ControlConnection:
    """
    A transactional connection for the SQL around the bulk write (staging
    promote, control row, row counts).

    Uses ``psycopg2`` when it is installed; otherwise it borrows the JDBC driver
    already on the Spark classpath through the JVM, so no extra Python
    dependency is needed on the edge node.
    """

    def __init__(self, connection, flavour: str):
        self._connection = connection
        self.flavour = flavour

    @classmethod
    def open(cls, url: str, user: str, password: str, spark=None) -> "ControlConnection":
        host, port, database = parse_jdbc_url(url)
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
            connection = jvm.java.sql.DriverManager.getConnection(url, properties)
            connection.setAutoCommit(False)
            logger.debug("control connection established (JDBC through the JVM)")
            return cls(connection, "jdbc")
        except Exception as exc:                       # noqa: BLE001
            raise GreenplumLoadError(
                f"could not open a JDBC control connection to {host}:{port}/{database} "
                f"({type(exc).__name__})") from None

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
    """Writes batch parquet into Greenplum with the Spark JDBC writer."""

    def __init__(self, cfg, spark=None):
        self.cfg = cfg
        self.spark = spark

        self.url = str(cfg.require("greenplum.GREENPLUM_URL"))
        self.user = str(cfg.get("greenplum.GREENPLUM_USER", "") or "")
        self.password = str(cfg.get("greenplum.GREENPLUM_PASSWORD", "") or "")
        self.driver = str(cfg.get("greenplum.GREENPLUM_DRIVER", "org.postgresql.Driver"))

        self.schema = str(cfg.require("greenplum.GREENPLUM_SCHEMA"))
        self.table = str(cfg.require("greenplum.GREENPLUM_TABLE"))
        self.staging = str(cfg.get("greenplum.GREENPLUM_STAGING_TABLE", f"{self.table}_stg"))
        self.control = str(cfg.get("greenplum.GREENPLUM_CONTROL_TABLE",
                                   "atm_ejournal_batch_control"))

        self.write_mode = str(cfg.get("greenplum.GREENPLUM_WRITE_MODE", "append")).lower()
        self.strategy = str(cfg.get("greenplum.GREENPLUM_LOAD_STRATEGY", "append")).lower()
        self.merge_keys = cfg.get_list("greenplum.GREENPLUM_MERGE_KEYS")
        self.batch_size = cfg.get_int("greenplum.GREENPLUM_JDBC_BATCH_SIZE", 20000)
        self.write_partitions = cfg.get_int("greenplum.GREENPLUM_WRITE_PARTITIONS", 0)
        self.use_control_table = cfg.get_bool("greenplum.GREENPLUM_USE_CONTROL_TABLE", True)
        self.create_objects = cfg.get_bool("greenplum.GREENPLUM_CREATE_OBJECTS", True)
        self.target_distributed_by = str(cfg.get("greenplum.GREENPLUM_TARGET_DISTRIBUTED_BY", ""))
        self.control_distributed_by = str(cfg.get("greenplum.GREENPLUM_CONTROL_DISTRIBUTED_BY", ""))

        self._objects_ready = False

    # -- naming ------------------------------------------------------------- #

    def qualified(self, table: str) -> str:
        """Quoted ``"schema"."table"`` for SQL statements."""
        return f"{quote_identifier(self.schema)}.{quote_identifier(table)}"

    def dbtable(self, table: str) -> str:
        """Plain ``schema.table`` for the JDBC ``dbtable`` option."""
        return f"{self.schema}.{table}"

    @property
    def target_table(self) -> str:
        return self.qualified(self.table)

    @property
    def staging_table(self) -> str:
        return self.qualified(self.staging)

    @property
    def control_table(self) -> str:
        return self.qualified(self.control)

    @property
    def jdbc_properties(self) -> Dict[str, str]:
        """The connection properties, as used by ``df.write.jdbc(...)``."""
        return {"user": self.user, "password": self.password, "driver": self.driver}

    def connect(self) -> ControlConnection:
        return ControlConnection.open(self.url, self.user, self.password, spark=self.spark)

    # -- the write ---------------------------------------------------------- #

    def write_dataframe(self, dataframe, table: str, mode: str = "append") -> float:
        """
        The Spark JDBC write:

            df.write.format("jdbc").option("url", ...).option("dbtable", ...)
              .option("user", ...).option("password", ...).option("driver", ...)
              .mode(mode).save()

        Returns the elapsed seconds. Raises :class:`GreenplumLoadError` with the
        credentials kept out of the message.
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

        logger.info("Greenplum write | table=%s | mode=%s | url=%s | user=%s",
                    self.dbtable(table), mode, self.url, self.user)
        started = time.time()
        try:
            (to_write.write
             .format("jdbc")
             .option("url", self.url)
             .option("dbtable", self.dbtable(table))
             .option("user", self.user)
             .option("password", self.password)
             .option("driver", self.driver)
             .option("batchsize", self.batch_size)
             .mode(mode)
             .save())
        except Exception as exc:                       # noqa: BLE001
            raise GreenplumLoadError(f"JDBC write into {self.dbtable(table)} failed: "
                                     f"{exc}") from exc
        elapsed = time.time() - started
        logger.info("Greenplum write completed | table=%s | %.1fs", self.dbtable(table), elapsed)
        return elapsed

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
            if self.use_control_table:
                connection.execute(self._control_table_ddl())
            if schema is not None:
                connection.execute(self._target_table_ddl())
            connection.commit()
            self._objects_ready = True
            logger.info("Greenplum objects verified | target=%s | control=%s",
                        self.target_table,
                        self.control_table if self.use_control_table else "(disabled)")
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

    def _target_table_ddl(self, schema=None) -> str:
        from spark_parser import build_schema                          # noqa: PLC0415

        schema = schema if schema is not None else build_schema()
        columns = [f"{quote_identifier(field_.name)} "
                   f"{TYPE_MAP.get(type(field_.dataType).__name__, 'text')}"
                   for field_ in schema.fields]
        distribution = (f" DISTRIBUTED BY ({quote_identifier(self.target_distributed_by)})"
                        if self.target_distributed_by else "")
        return (f"CREATE TABLE IF NOT EXISTS {self.target_table} ("
                + ", ".join(columns) + f"){distribution}")

    # -- staged strategies --------------------------------------------------- #

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
        return statements

    # -- load --------------------------------------------------------------- #

    def load_batch(self, dataframe, batch_id: str, run_id: str,
                   file_count: int = 0, parquet_path: str = "",
                   expected_rows: Optional[int] = None) -> LoadResult:
        """
        Load one batch (read back from its parquet) into Greenplum.

        ``append``/``overwrite`` write straight into the target table;
        the staged strategies write into the staging table and promote it in one
        transaction. Returns a :class:`LoadResult`.
        """
        if self.strategy not in DIRECT_STRATEGIES + STAGED_STRATEGIES:
            raise GreenplumLoadError(f"unknown load strategy: {self.strategy}")

        result = LoadResult(batch_id=batch_id, run_id=run_id, strategy=self.strategy,
                            target_table=self.dbtable(self.table))
        started = time.time()
        load_started_at = datetime.now()

        self.ensure_objects(schema=dataframe.schema)

        if self.strategy in DIRECT_STRATEGIES:
            self._load_direct(dataframe, batch_id, run_id, result)
        else:
            self._load_staged(dataframe, batch_id, run_id, result, expected_rows)

        result.committed = True
        result.rows_loaded = self.count_batch_rows(run_id, batch_id)
        if result.rows_loaded < 0 and expected_rows is not None:
            result.rows_loaded = expected_rows          # counting is best effort
        result.duration_seconds = time.time() - started

        self._record_control_row(batch_id, run_id, file_count, parquet_path,
                                 expected_rows if expected_rows is not None else result.rows_loaded,
                                 result.rows_loaded, load_started_at)

        if expected_rows is not None and result.rows_loaded not in (-1, expected_rows):
            warning = (f"target row count for {batch_id} is {result.rows_loaded}, expected "
                       f"{expected_rows}")
            result.warnings.append(warning)
            logger.warning("Greenplum load | %s", warning)
        logger.info("Greenplum load completed | batch=%s | strategy=%s | rows loaded=%d | %.1fs",
                    batch_id, self.strategy, result.rows_loaded, result.duration_seconds)
        return result

    def _load_direct(self, dataframe, batch_id: str, run_id: str, result: LoadResult) -> None:
        """``append`` / ``overwrite``: the JDBC write straight into the target."""
        mode = "overwrite" if self.strategy == "overwrite" else self.write_mode
        if mode == "append":
            # A retried batch must not leave the previous attempt's rows behind.
            result.rows_deleted = self._delete_previous_attempt(run_id, batch_id)
        self.write_dataframe(dataframe, self.table, mode=mode)

    def _load_staged(self, dataframe, batch_id: str, run_id: str, result: LoadResult,
                     expected_rows: Optional[int]) -> None:
        """Staging table, then delete + insert inside one transaction."""
        self.write_dataframe(dataframe, self.staging, mode="overwrite")

        connection = self.connect()
        promoted = False
        try:
            staged = connection.scalar(f"SELECT COUNT(*) FROM {self.staging_table}")
            result.staged_rows = int(staged or 0)
            if expected_rows is not None and result.staged_rows != expected_rows:
                raise GreenplumLoadError(
                    f"staging row count mismatch for {batch_id}: parquet has {expected_rows} "
                    f"row(s), staging table has {result.staged_rows}")

            logger.info("Greenplum promote started | batch=%s | strategy=%s | staged rows=%d",
                        batch_id, self.strategy, result.staged_rows)

            deleted = 0
            for statement in self._delete_statements(run_id, batch_id):
                deleted += max(connection.execute(statement), 0)
            result.rows_deleted = deleted
            if deleted:
                logger.info("Greenplum promote | batch=%s | %d existing row(s) removed before "
                            "insert (idempotency: %s)", batch_id, deleted, self.strategy)

            columns = ", ".join(quote_identifier(field_.name) for field_ in dataframe.schema.fields)
            connection.execute(f"INSERT INTO {self.target_table} ({columns}) "
                               f"SELECT {columns} FROM {self.staging_table}")
            connection.commit()
            promoted = True
        except GreenplumLoadError:
            connection.rollback()
            raise
        except Exception as exc:                       # noqa: BLE001
            connection.rollback()
            raise GreenplumLoadError(f"Greenplum promote failed for {batch_id}: {exc}") from exc
        finally:
            if not promoted:
                logger.error("Greenplum transaction rolled back for batch %s - no rows were "
                             "committed", batch_id)
            connection.close()

    def _delete_previous_attempt(self, run_id: str, batch_id: str) -> int:
        """Remove rows a previous attempt of this exact batch already wrote."""
        connection = None
        try:
            connection = self.connect()
            deleted = connection.execute(
                f"DELETE FROM {self.target_table} WHERE "
                f'"ETL_RUN_ID" = {sql_literal(run_id)} AND "BATCH_ID" = {sql_literal(batch_id)}')
            connection.commit()
            if deleted > 0:
                logger.warning("Greenplum load | batch=%s | %d row(s) from an earlier attempt of "
                               "this batch removed before the append", batch_id, deleted)
            return max(deleted, 0)
        except GreenplumLoadError as exc:
            logger.warning("could not clean an earlier attempt of %s before appending: %s",
                           batch_id, exc)
            return 0
        finally:
            if connection is not None:
                connection.close()

    def _record_control_row(self, batch_id: str, run_id: str, file_count: int,
                            parquet_path: str, record_count: int, rows_loaded: int,
                            load_started_at: datetime) -> None:
        if not self.use_control_table:
            return
        connection = None
        try:
            connection = self.connect()
            connection.execute(
                f"DELETE FROM {self.control_table} WHERE "
                f'"etl_name" = {sql_literal(self.cfg.etl_name)} AND '
                f'"run_id" = {sql_literal(run_id)} AND "batch_id" = {sql_literal(batch_id)}')
            connection.execute(
                f"INSERT INTO {self.control_table} "
                '("etl_name", "run_id", "batch_id", "file_count", "record_count", '
                '"rows_loaded", "parquet_path", "load_started", "load_completed", "status") '
                f"VALUES ({sql_literal(self.cfg.etl_name)}, {sql_literal(run_id)}, "
                f"{sql_literal(batch_id)}, {int(file_count)}, {int(max(record_count, 0))}, "
                f"{int(max(rows_loaded, 0))}, {sql_literal(parquet_path)}, "
                f"{sql_literal(load_started_at.strftime('%Y-%m-%d %H:%M:%S'))}, "
                f"{sql_literal(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}, 'SUCCESS')")
            connection.commit()
            logger.debug("batch control row written for %s", batch_id)
        except Exception as exc:                       # noqa: BLE001
            if connection is not None:
                connection.rollback()
            # The data is in the target table; the batch is still recoverable
            # from the ETL_RUN_ID/BATCH_ID columns, so this is a warning.
            logger.warning("batch control row could not be written for %s (%s) - recovery will "
                           "fall back to counting rows in the target table", batch_id, exc)
        finally:
            if connection is not None:
                connection.close()

    # -- verification / recovery -------------------------------------------- #

    def count_batch_rows(self, run_id: str, batch_id: str) -> int:
        """Rows visible in the target table for this batch (-1 when unknown)."""
        connection = None
        try:
            connection = self.connect()
            value = connection.scalar(
                f"SELECT COUNT(*) FROM {self.target_table} WHERE "
                f'"ETL_RUN_ID" = {sql_literal(run_id)} AND "BATCH_ID" = {sql_literal(batch_id)}')
            return int(value or 0)
        except Exception as exc:                       # noqa: BLE001 - verification only
            logger.warning("could not verify loaded rows for %s: %s", batch_id, exc)
            return -1
        finally:
            if connection is not None:
                connection.close()

    def is_batch_committed(self, run_id: str, batch_id: str) -> Optional[int]:
        """
        Ask Greenplum whether a batch reached the target table - the question a
        restart has to answer for a pending marker. Returns the loaded row count,
        or ``None`` when the batch is not there.
        """
        connection = self.connect()
        try:
            if self.use_control_table:
                value = connection.scalar(
                    f'SELECT SUM("rows_loaded") FROM {self.control_table} WHERE '
                    f'"etl_name" = {sql_literal(self.cfg.etl_name)} AND '
                    f'"run_id" = {sql_literal(run_id)} AND "batch_id" = {sql_literal(batch_id)} '
                    "AND \"status\" = 'SUCCESS'")
                if value is not None:
                    return int(value)
            # No control table (or no control row): ask the target table itself.
            rows = connection.scalar(
                f"SELECT COUNT(*) FROM {self.target_table} WHERE "
                f'"ETL_RUN_ID" = {sql_literal(run_id)} AND "BATCH_ID" = {sql_literal(batch_id)}')
            return int(rows) if rows else None
        except Exception as exc:                       # noqa: BLE001
            logger.error("could not check whether batch %s committed: %s", batch_id, exc)
            raise GreenplumLoadError("Greenplum could not be queried; refusing to guess whether "
                                     "the batch committed") from exc
        finally:
            connection.close()

    def loaded_file_keys(self, file_keys: Iterable[str]) -> List[str]:
        """Which of these source files already have rows in the target table."""
        keys = [key for key in file_keys if key]
        if not keys:
            return []
        in_list = ", ".join(sql_literal(key) for key in keys)
        connection = None
        try:
            connection = self.connect()
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
            if connection is not None:
                connection.close()
