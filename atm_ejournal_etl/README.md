# ATM E-Journal ETL

Batched, restartable ETL around the existing ATM e-journal parser:

```
Input files
    -> processed-file check          processed/processed_files.csv
    -> unprocessed files
    -> batch selection               input.BATCH_SIZE
    -> parse                         the existing parser (local or on the executors)
    -> Parquet                       parquet/batch_nnnn
    -> Greenplum                     staging -> target, one transaction
    -> processed-file tracking       append-only, after the commit
    -> Parquet cleanup
    -> next batch
```

The parsing logic is unchanged: `src/atm_ejournal_parser.py` is the module that was
already in use (grammar, retry collapsing, denominations, audit counters - see
[`docs/PARSER_README.md`](docs/PARSER_README.md)). What is new is everything around it:
batching, tracking, parquet staging, Greenplum loading, configuration, logging with
retention, Airflow orchestration and notifications.

---

## Contents

| Path | Purpose |
| --- | --- |
| `config/atm_ejournal.conf` | Central configuration (YAML). One section per concern, one profile per ETL |
| `config/atm_ejournal.conf.example` | The same file with dummy values only |
| `src/atm_ejournal_parser.py` | **Existing** parser, unchanged |
| `src/config_loader.py` | Loads/validates a configuration profile |
| `src/check_environment.py` | Preflight: can this Python/PySpark ship code to the executors? |
| `src/log_manager.py` | Run log, per-batch logs, retention / size sweep |
| `src/file_registry.py` | Discovery, batching, `processed_files.csv`, pending markers |
| `src/spark_session.py` | SparkSession from the configuration |
| `src/spark_parser.py` | Spark engine: runs the existing parser on the executors; owns the output schema |
| `src/local_parser.py` | Local engine: runs the existing parser here and writes the parquet with PyArrow |
| `src/parquet_stage.py` | Batch parquet write, validation, read-back, cleanup |
| `src/greenplum_loader.py` | The Spark JDBC write + load strategies + batch control table |
| `src/mail_util.py` | Success/failure e-mails |
| `src/etl_runner.py` | The orchestrator (batch loop, recovery, run summary) |
| `src/manual_load.py` | Manual runners: batch-wise process+insert, and "load this path" |
| `src/run_etl.py` | Command line entry point |
| `dags/atm_ejournal_etl_dag.py` | Airflow DAG with failure/success e-mails |
| `notebooks/manual_batch_load.ipynb` | **Manual run**: process+insert batch by batch, and load a parquet path |
| `notebooks/run_atm_ejournal_etl.ipynb` | Manual run through the full orchestrator: dry run, batch limit, inspection |
| `notebooks/run_atm_ejournal_parser.ipynb` | **Existing** parser notebook, unchanged. It imports the parser from the folder it runs in, so set `MODULE_DIR` in its first cell to `../src` |
| `tests/` | Unit tests (SMTP and Airflow mocked) and an opt-in real-database suite |
| `logs/`, `parquet/`, `processed/` | Runtime directories (created if missing) |

---

## Quick start

```bash
pip install -r requirements.txt

cp config/atm_ejournal.conf.example config/atm_ejournal.conf
chmod 600 config/atm_ejournal.conf        # it holds the Greenplum/SMTP passwords
export ATM_ETL_HOME=/etl/atm_ejournal

# edit config/atm_ejournal.conf: GREENPLUM_URL / USER / PASSWORD / DRIVER,
# the schema + table, INPUT_PATH and the Spark settings

# confirm this Python/PySpark installation can ship code to the executors
python3 src/check_environment.py

# see what a run would do, without loading anything
python3 src/run_etl.py --dry-run --max-batches 1

# the real thing
python3 src/run_etl.py --config config/atm_ejournal.conf --etl atm_ejournal
```

Exit code `0` = every batch succeeded, `1` = at least one batch failed, `2` = bad
configuration. The last stdout line is `RUN_SUMMARY_PATH=<file>`, which the DAG picks up
from XCom.

Useful options: `--batch-size`, `--input-path`, `--max-batches`, `--run-id`, `--dry-run`,
`--list-etls`.

---

## Configuration

Everything environment specific lives in `config/atm_ejournal.conf`. It is YAML in the
style of the existing spark/table configuration, resolved as

```
defaults.<section>  ->  etls.<etl_name>.<section>  ->  ${ENV_VAR} expansion  ->  CLI overrides
```

`${VAR}` and `${VAR:-fallback}` are expanded from the environment, so one file serves
DEV/UAT/PROD. Relative paths are resolved against `project.BASE_DIR` (`$ATM_ETL_HOME`).

### `input`

| Key | Meaning |
| --- | --- |
| `INPUT_PATH` | Root directory holding the journal files (`<INPUT_PATH>/<ATM_NO>/<file>`) |
| `FILE_PATTERN` | Comma separated file-name globs, e.g. `*.TXT,*.txt` |
| `BATCH_SIZE` | Files per batch. The driver never holds more than this many paths |
| `MAX_BATCHES_PER_RUN` | Stop after N batches (`0` = until everything is processed) |
| `PRESCAN_ENABLED` | Count the directory before processing so the log/e-mail can report discovered / previously processed / remaining. Metadata only; turn off for extreme directory sizes |
| `SNIFF_UNKNOWN_EXTENSIONS` | Open non-matching files and look for journal headers (reuses the parser's sniffing). Off by default - it opens files |
| `MIN_FILE_AGE_SECONDS` | Skip files younger than this, so a file still being written is not parsed |
| `ATM_FOLDER_DEPTH` | Folder level that carries `ATM_NO` (1 = directly under `INPUT_PATH`) |
| `ENCODING` | Journal file encoding (`latin-1`, as the parser uses) |

### `tracking`

| Key | Meaning |
| --- | --- |
| `PROCESSED_FILES_CSV` | Append-only record of successfully loaded files |
| `PENDING_DIR` | In-flight batch markers, written before the Greenplum load |
| `RUN_SUMMARY_DIR` | Run metrics as JSON (`<etl>_<run_id>.json` + `<etl>_latest.json`) |
| `FILE_KEY_MODE` | Identity of a file: `path` (default), `path_size`, `path_mtime` |

### `parquet`

| Key | Meaning |
| --- | --- |
| `PARQUET_PATH` | Root of the intermediate parquet (`<root>/batch_0001`, ...) |
| `PARQUET_COMPRESSION` | `snappy` (default), `gzip`, `zstd`, ... |
| `PARQUET_COALESCE_PARTITIONS` | Output files per batch (`0` = leave Spark's partitioning) |
| `PARQUET_CLEANUP_ENABLED` | Delete a batch directory after the load is confirmed |
| `PARQUET_KEEP_FAILED_BATCHES` | Keep the parquet of a failed batch for troubleshooting |
| `PARQUET_FAILED_RETENTION_DAYS` | Age at which kept parquet is swept (`0` = never) |

### `logging`

| Key | Meaning |
| --- | --- |
| `LOG_PATH` | Log directory |
| `LOG_LEVEL` / `CONSOLE_LOG_LEVEL` | File and console levels (`DEBUG`/`INFO`/`WARNING`/`ERROR`) |
| `LOG_FILE_PREFIX` / `BATCH_LOG_FILE_PREFIX` | `atm_ejournal_etl_<run>.log`, `batch_<id>_<ts>.log` |
| `LOG_FORMAT` | Format string; `%(batch_id)s` is always available |
| `LOG_RETENTION_DAYS` | Logs older than this are deleted (365 = one year) |
| `MAX_LOG_SIZE_GB` | Total size limit for the directory (1 GB) |
| `LOG_CLEANUP_ENABLED` | Switch the sweep off |

### `spark`

`SPARK_APP_NAME`, `SPARK_MASTER`, `SPARK_JARS`, `SPARK_EXECUTOR_INSTANCES`,
`SPARK_EXECUTOR_MEMORY`, `SPARK_EXECUTOR_CORES`, `SPARK_CORES_MAX`, `SPARK_DRIVER_MEMORY`,
`SPARK_LOCAL_DIR`, `SPARK_EXECUTOR_JAVA_OPTIONS`, `SPARK_DRIVER_JAVA_OPTIONS`,
`SPARK_LOG_LEVEL`, `SPARK_SHUFFLE_PARTITIONS`, plus:

| Key | Meaning |
| --- | --- |
| `SPARK_PARSE_PARTITIONS` | Parse tasks per batch (`0` = one per executor core slot, capped at the file count) |
| `EXTRA_CONF` | Any additional `spark.*` settings as a mapping |

The shipped defaults are the cluster settings already in use (master URL, connector jars,
`java.io.tmpdir`, `user.timezone=UTC`, `oracle.jdbc.timezoneAsRegion=false`).

### `greenplum`

| Key | Meaning |
| --- | --- |
| `GREENPLUM_URL` | JDBC URL, e.g. `jdbc:postgresql://<host>:5432/<database>` |
| `GREENPLUM_USER` | Database user |
| `GREENPLUM_PASSWORD` | Password, in clear text - keep the file `chmod 600` |
| `GREENPLUM_DRIVER` | JDBC driver class, `org.postgresql.Driver` |
| `GREENPLUM_SCHEMA` / `GREENPLUM_TABLE` | Target table; together they form the `dbtable` option |
| `GREENPLUM_LOAD_STRATEGY` | `append` (default), `overwrite`, `delete_insert_by_source_file`, `merge_by_key`, `truncate_load` |
| `GREENPLUM_WRITE_MODE` | `append` or `overwrite` - the `.mode(...)` of the write |
| `GREENPLUM_WRITE_FORMAT` | `jdbc` (default) or `greenplum` (the greenplum-spark connector) |
| `GREENPLUM_CONNECTOR_OPTIONS` | Connector-only options: `server.port`, `segment.num`, `numWriteTasks`, `gpfdist.sessions`, `compression` |
| `GREENPLUM_STAGING_TABLE` | Staging table, used by the staged strategies only |
| `GREENPLUM_CONTROL_TABLE` | Batch control table - what a restart reads |
| `GREENPLUM_USE_CONTROL_TABLE` | Record each loaded batch there (default true) |
| `GREENPLUM_MERGE_KEYS` | Key columns for `merge_by_key` |
| `GREENPLUM_JDBC_BATCH_SIZE` | JDBC `batchsize` option |
| `GREENPLUM_WRITE_PARTITIONS` | Parallel writers (`0` = leave the DataFrame as it is) |
| `GREENPLUM_CREATE_OBJECTS` | Create the target/control tables when missing |
| `GREENPLUM_TARGET_DISTRIBUTED_BY` / `..._CONTROL_DISTRIBUTED_BY` | `DISTRIBUTED BY` used when the ETL creates the tables (leave empty on plain PostgreSQL) |

### `parser`

| Key | Meaning |
| --- | --- |
| `PARSE_ENGINE` | `local`, `spark` or `auto` (default) - see [Parse engines](#parse-engines) |
| `PARSE_WORKERS` | Local engine: parser processes (`0` = one per CPU core) |
| `PARSE_WRITE_CHUNK_RECORDS` | Local engine: records buffered before a parquet row group is written |
| `KEEP_LAST_FAILURE`, `LINK_FAILED_ACROSS_AMOUNTS`, `RETRY_WINDOW_SECONDS` | Passed straight to the existing `deduplicate_attempts()` |
| `KEEP_UNPARSED_RECORDS` | Load low-confidence blocks (`PARSE_CONFIDENT = false`) or drop them |

### `mail`

`MAIL_ENABLED`, `SMTP_HOST`, `SMTP_PORT`, `SMTP_USE_TLS`, `SMTP_USER`, `SMTP_PASSWORD`,
`SMTP_TIMEOUT_SECONDS`, `MAIL_FROM`, `MAIL_TO`, `MAIL_CC`, `MAIL_SUBJECT_SUCCESS`,
`MAIL_SUBJECT_FAILURE`.

### `airflow`

`AIRFLOW_DAG_ID`, `AIRFLOW_SCHEDULE`, `AIRFLOW_START_DATE`, `AIRFLOW_CATCHUP`,
`AIRFLOW_RETRIES`, `AIRFLOW_RETRY_DELAY_MINUTES`, `AIRFLOW_EXECUTION_TIMEOUT_MINUTES`,
`AIRFLOW_MAX_ACTIVE_RUNS`, `AIRFLOW_OWNER`, `AIRFLOW_TAGS`, `AIRFLOW_PYTHON_BIN`,
`AIRFLOW_SPARK_SUBMIT_BIN` (set it to use `spark-submit` instead of `python3`),
`AIRFLOW_ETL_HOME`.

### More than one ETL

Add another key under `etls:` - it inherits everything from `defaults:` and overrides only
what differs:

```yaml
etls:
  atm_ejournal:
    input: {INPUT_PATH: "/etl/atm_ejournal/input", BATCH_SIZE: 500}
  atm_ejournal_archive:
    input: {INPUT_PATH: "/etl/atm_ejournal/archive", BATCH_SIZE: 2000}
    tracking: {PROCESSED_FILES_CSV: "processed/processed_files_archive.csv"}
    parquet: {PARQUET_PATH: "parquet_archive"}
    greenplum: {GREENPLUM_TABLE: "atm_ejournal_withdrawals_archive"}
```

Run it with `--etl atm_ejournal_archive`, or deploy a second DAG with `ATM_ETL_NAME` set.
No code changes: no path, table or batch size is hard-coded in Python.

---

## Directory structure

```
atm_ejournal_etl/
    config/
        atm_ejournal.conf              <- url/user/password/driver, chmod 600
        atm_ejournal.conf.example
    src/                               <- ETL modules + the existing parser
    dags/atm_ejournal_etl_dag.py
    notebooks/
    tests/
    logs/                              <- run + batch logs
    parquet/batch_0001/                <- one directory per in-flight batch
    processed/
        processed_files.csv            <- append-only tracking
        pending/BATCH_0007.json        <- in-flight batch markers
        run_summary/atm_ejournal_*.json
```

---

## How batch processing works

1. The input directory is walked **lazily** (`os.scandir` through `os.walk`), one
   directory listing at a time - a directory with millions of files is never listed into
   a Python list.
2. `processed_files.csv` is read once into a set of file keys; discovered files already in
   that set are skipped.
3. The remaining stream is cut into batches of `BATCH_SIZE` (`iter_batches`), so the driver
   holds at most one batch of paths.
4. Each batch is processed end to end before the next one starts. Batch ids are
   `BATCH_0001`, `BATCH_0002`, ... within a run; the run id makes them unique over time.

Only the *paths* are distributed to Spark. Each executor opens the file it was given and
runs the existing parser on it, so neither the driver nor any single executor ever holds
the whole batch of journal text.

De-duplication of retries stays identical to the pandas version: `extract_transaction_blocks`
namespaces every session id with its file (`<file>#S00001`), so a retry chain can never
span two files, and collapsing per file gives exactly the same rows as the folder-level
call - this is pinned by `tests/test_spark_parser.py`.

---

## Running it by hand

`notebooks/manual_batch_load.ipynb` drives the same functions the scheduled ETL uses, for
an initial load or a catch-up you want to watch. Both entry points live in
`src/manual_load.py`, so they can also be called from a script:

```python
from config_loader import load_config
from manual_load import run_batches, load_parquet_path, process_path, print_reports

cfg = load_config("config/atm_ejournal.conf", "atm_ejournal")
```

**Batch-wise process and insert.** For each batch: parse -> one parquet -> insert into
Greenplum -> record the files in `processed_files.csv` -> delete the parquet -> next batch.

```python
reports = run_batches(cfg, batch_size=500, max_batches=1,
                      on_batch=lambda r: print(r.batch_id, r.status, r.rows_loaded))
print_reports(reports)
```

| Argument | Meaning |
| --- | --- |
| `batch_size` | Files per batch (default `input.BATCH_SIZE`) |
| `max_batches` | Stop after N batches; `None` processes everything still pending |
| `input_path` | Read another directory instead of `input.INPUT_PATH` |
| `keep_parquet` | Keep each batch's parquet (filed under the run id) instead of deleting it |
| `on_batch` | Called with each `BatchReport` as it finishes, for live progress |

Files are marked processed only after Greenplum confirms the insert, and a batch that fails
is reported while the run continues - its files stay pending for the next call.

**Load a path that was not inserted yet.** Point it at a `.parquet` file, a Spark parquet
directory, or a folder holding several of them, and it writes the whole thing to Greenplum:

```python
load_parquet_path(cfg, "/analyticsShare/.../output/")            # only what is new
load_parquet_path(cfg, "/analyticsShare/.../output/", only_new=False)   # everything again
```

`only_new=True` (default) skips the targets recorded in `processed/loaded_parquet.csv`, so
running it again on the same folder inserts only the parquet that has not been inserted
yet. `ETL_RUN_ID` and `BATCH_ID` are stamped with this load's ids before the write (a
JVM-side `withColumn(lit(...))`), which is what the control row, the row-count check and
the retry guard key on; pass `stamp_ids=False` to load the rows exactly as the file has
them.

**A whole folder of journals**: `process_path(cfg, "/path/to/journals", batch_size=500)` -
`run_batches` pointed at that directory with no batch limit.

---

## Parse engines

The parsing logic is the same module either way - what differs is *where* it runs.

| | `local` | `spark` |
| --- | --- | --- |
| Journals parsed by | this host, one process pool (`PARSE_WORKERS`) | the executors, one task per file |
| Parquet written by | PyArrow, streamed in chunks | Spark |
| Python code sent to Spark | **none** | the parser modules (`addPyFile`) |
| Spark is used for | `spark.read.parquet` + the JDBC write (JVM only) | everything |
| Works on | any PySpark/Python combination | a PySpark that supports the interpreter |
| Scales with | the ETL host's cores | the cluster |

`auto` (the default) picks `spark` when the installation can serialise Python code for the
executors and `local` when it cannot, logging which and why.

**When `local` is the answer.** PySpark serialises every Python function it ships with the
cloudpickle version it bundles. On Spark 3.1.x (cloudpickle 1.6) under Python 3.11 that
cannot work at all - `rdd.mapPartitions`, `createDataFrame` and Python UDFs all fail with
`PicklingError: ... IndexError: tuple index out of range`. The local engine avoids the
problem instead of fighting it: no Python crosses into Spark, so the combination stops
mattering.

**Memory.** Rows are streamed into the parquet file `PARSE_WRITE_CHUNK_RECORDS` at a time,
so a batch never has to fit in memory - the footprint is one chunk plus the single journal
file being parsed, whatever `BATCH_SIZE` is.

**Throughput.** `PARSE_WORKERS` parser processes run in parallel (the pool uses the
standard library's pickle on a module level function, never cloudpickle). On a big initial
load with a cluster available, `spark` still spreads the work wider - so upgrade PySpark
when you can and switch back with `PARSE_ENGINE: spark` or `auto`.

**The parquet is identical.** Both engines build it from one schema description
(`spark_parser.SCHEMA_FIELDS`), PyArrow writes it with `flavor="spark"`, and a test asserts
the two engines produce the same rows for the same input.

---

## How processed files are tracked

`processed/processed_files.csv` is **append-only**:

```csv
file_name,file_path,file_key,atm_no,file_size,file_mtime,processed_date,batch_id,run_id,records,status
EJOURNAL_10092026_00.TXT,/etl/.../ATM001/EJOURNAL_10092026_00.TXT,ATM001/EJOURNAL_10092026_00.TXT,ATM001,3176,2026-09-12 06:00:00,2026-09-12 20:00:01,BATCH_0001,20260912_200000,4,SUCCESS
```

* rows are appended under an exclusive lock, flushed and `fsync`-ed - existing rows are
  never rewritten;
* a row is written **only after Greenplum has committed** the batch, so a `SUCCESS` row
  always means "this file's data is in the target table";
* `file_key` is the identity used for lookups (`FILE_KEY_MODE`); duplicates and malformed
  rows are tolerated on read (reported, skipped) so a truncated line from an old crash
  cannot stop the ETL;
* a file that failed to parse inside an otherwise successful batch is **not** recorded -
  it is retried on the next run.

To force a file to be processed again, delete its row. With the default load strategy the
re-run replaces that file's rows in Greenplum rather than duplicating them.

---

## How Parquet staging works

Per batch:

1. `parquet/batch_nnnn` is cleared if a leftover exists.
2. The parsed DataFrame is written there (`mode=overwrite`, configurable compression and
   `coalesce`).
3. The write is **validated**: `_SUCCESS` marker present, directory readable, row count
   taken from reading the parquet back. That read-back count is the number used
   everywhere else, which also means the parse runs exactly once (counting the DataFrame
   before the write would have run it twice).
4. Greenplum is loaded **from the parquet**, not from the in-memory DataFrame.
5. Only after the Greenplum commit *and* the tracking update is the directory deleted.

A failed batch keeps its parquet (`PARQUET_KEEP_FAILED_BATCHES`) for troubleshooting;
those directories are swept by age at the start of later runs.

---

## How Greenplum loading works

The write is the plain Spark JDBC write:

```python
df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://<host>:5432/<database>") \
    .option("dbtable", "schema.table_name") \
    .option("user", "username") \
    .option("password", "password") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()
```

`url`, `user`, `password`, `driver`, `schema` and `table` come straight from the
`greenplum:` section; the DataFrame is the batch read back from its parquet, never the
in-memory parse result.

With `GREENPLUM_WRITE_FORMAT: greenplum` the same write goes through the greenplum-spark
connector instead (segments writing in parallel through gpfdist), taking `dbschema` and
`dbtable` separately plus the `GREENPLUM_CONNECTOR_OPTIONS` - `server.port`,
`segment.num`, `numWriteTasks`, `gpfdist.sessions`, `compression`. That needs
`greenplum-connector-apache-spark-*.jar` on `SPARK_JARS`.

**Strategies.** `append` (default) and `overwrite` do exactly the write above against the
target table. The staged strategies write the batch into `GREENPLUM_STAGING_TABLE` with
`mode("overwrite")` first and then promote it inside one transaction:

```
parquet/batch_0001 --JDBC write--> atm.atm_ejournal_withdrawals_stg
                                            |
                          BEGIN             |
                            DELETE target rows for this batch's source files
                            INSERT INTO target SELECT * FROM staging
                          COMMIT
```

| Strategy | Re-loading the same file | Use it when |
| --- | --- | --- |
| `append` | rows are appended again | the feed is loaded once and duplicates are impossible or handled downstream |
| `overwrite` | table is replaced | small table, full reload each run |
| `delete_insert_by_source_file` | the file's previous rows are replaced | a journal file may be re-delivered or reprocessed |
| `merge_by_key` | rows matching `GREENPLUM_MERGE_KEYS` are replaced | the table's grain is a business key |
| `truncate_load` | table is truncated first | full reload with the staging validation |

With `append`, a batch that is retried within the same run does not double: the loader
first deletes rows carrying this `ETL_RUN_ID` + `BATCH_ID`. Across runs (for example a
file whose tracking row was lost) `append` *will* load the file again - switch to
`delete_insert_by_source_file` if that must not happen.

**Bookkeeping.** With `GREENPLUM_USE_CONTROL_TABLE` on (default) every loaded batch gets a
row in the control table; that row is what a restart consults to tell a committed batch
from one that never reached the database. With it off, the same question is answered by
counting the batch's rows in the target table - every row carries `ETL_RUN_ID`,
`BATCH_ID`, `SOURCE_FILE_KEY` and `LOAD_TS`.

The control statements (DDL, promote, counts) go through `psycopg2` when it is installed;
otherwise the loader borrows the PostgreSQL JDBC driver already on the Spark classpath
through the JVM, so no extra Python dependency is needed on the edge node.

Output schema: the columns produced by the parser, typed and stable
(`ATM_NO`, `TRANSACTION_DATETIME`, `DATE`, `TIME`, `ACCOUNT_NO`, `CARD_NO`, `AMOUNT`,
`REQUESTED_AMOUNT`, `CURRENCY`, `STATUS`, `RESPONSE_CODE`, `TRANSACTION_REF`, `TRACE_ID`,
`TERMINAL_ID`, `CARD_SCHEME`, `DISPENSE_RESULT`, `DENOMINATION`, `NOTES_COUNT`,
`DENOM_AMOUNT`, `DENOM_MATCHES_AMOUNT`, `CASH_TAKEN`, `TRX_ERROR`, `ATTEMPT_NO`,
`ATTEMPT_COUNT`, `IS_RETRY`, `SESSION_ID`, `TXN_SEQ`, `SOURCE_FILE`, `SOURCE_LINE`, ...).
The pandas version's dynamic `NOTES_<value>` columns cannot be part of a fixed table
schema; the same information is carried by `DENOMINATION` (`5000x9 + 1000x3`) and
`DENOM_BREAKDOWN` (JSON), from which per-note columns are derived in SQL when needed.
`GREENPLUM_CREATE_OBJECTS` lets the ETL create the target and control tables from that
schema on first use.

---

## Credentials

The configuration file holds the Greenplum and SMTP passwords in clear text, so:

* `chown` it to the ETL account and `chmod 600` it;
* keep it out of version control (the shipped `.gitignore` ignores `config/*.conf`);
* or leave the values as `${GREENPLUM_PASSWORD}` and export them in the ETL account's
  environment - `${VAR}` and `${VAR:-default}` are expanded when the file is read.

Passwords are never written to a log line, an exception message or the run summary:
`cfg.safe_dump()` masks every `*PASSWORD*`/`*SECRET*`/`*TOKEN*` key, and the loader logs
the URL and user only.

## Logging and log retention

```
logs/
    atm_ejournal_etl_20260912_200000.log      <- whole run
    batch_BATCH_0001_20260912_200001.log      <- one per batch
    batch_BATCH_0002_20260912_201015.log
```

Every record carries its batch (`[BATCH_0001]`). The run log records: ETL start, start
time, input directory and effective configuration (passwords masked); discovery totals
(discovered / previously processed / remaining / batch size / number of batches); per
batch - id, start, file count, first and last file, reading, transformations, record
count, parquet write start/end, Greenplum load start/end, rows loaded, completion and
duration; on failure - batch id, stage, exception, stack trace and the number of
successful/failed files.

At the start of every run, before any batch work:

```
delete logs older than LOG_RETENTION_DAYS (365)
    -> measure the directory
    -> while it is over MAX_LOG_SIZE_GB (1), delete the oldest log
```

The current run's log files are excluded from both passes, and a file that cannot be
deleted is logged and counted rather than aborting the ETL.

---

## Airflow deployment

```bash
export ATM_ETL_HOME=/etl/atm_ejournal
export ATM_ETL_CONFIG=$ATM_ETL_HOME/config/atm_ejournal.conf
export ATM_ETL_NAME=atm_ejournal
ln -s $ATM_ETL_HOME/dags/atm_ejournal_etl_dag.py $AIRFLOW_HOME/dags/
```

```
run_atm_ejournal_etl  ->  notify_success
```

* `run_atm_ejournal_etl` is a `BashOperator` that runs `src/run_etl.py`
  (or `spark-submit`, when `AIRFLOW_SPARK_SUBMIT_BIN` is set) with `ATM_ETL_RUN_ID`
  = `{{ ts_nodash }}`, and pushes its last stdout line to XCom.
* `notify_success` reads the run summary JSON and sends the success mail with the metrics
  the ETL recorded - files discovered, previously processed, processed, failed, batches,
  records processed and loaded, duration, and a per-batch table.
* `on_failure_callback` sends the failure mail: DAG, run id, execution date, task, batch id,
  failed stage, error, stack trace and log location. No credentials in either message.

Schedule, start date, retries, retry delay, timeout, owner and tags all come from the
`airflow:` section - nothing about the environment is written in the DAG file.

---

## Recovery after a failure

The ETL is restartable; normally the recovery procedure is "run it again".

| What failed | State left behind | What the next run does |
| --- | --- | --- |
| Spark session / parse | nothing marked, parquet partial | the batch's files are still unprocessed -> retried |
| Parquet write or validation | parquet kept | same - retried; the kept parquet helps diagnose |
| Greenplum write | nothing marked as processed; pending marker present | the batch is recognised as not loaded, its files are retried |
| Crash **after** the Greenplum write, before the tracking update | target rows (+ control row) exist; pending marker present | the control table - or the batch's rows in the target - is consulted: the batch is recognised as loaded and the tracking CSV is completed **without loading again** |
| Tracking CSV update itself failed | as above | as above |
| A single unreadable/corrupt file inside a batch | the rest of the batch loads | only that file stays unprocessed and is retried |
| Airflow task failed | failure mail sent | Airflow retries per `AIRFLOW_RETRIES`; the next attempt resumes from the first unprocessed file |

Nothing is ever marked `SUCCESS` before the data is committed, and no parquet is deleted
before the tracking CSV has been appended to. Batches that already completed are never
reprocessed: a failure in batch 5 leaves batches 1-4 recorded, and the next run starts at
the files of batch 5.

Manual inspection:

```bash
ls processed/pending/                       # batches that were in flight
cat processed/run_summary/atm_ejournal_latest.json
tail -100 logs/atm_ejournal_etl_<run>.log
```

---

## Performance notes

* **Batching** bounds memory: the driver holds one batch of paths, never the directory.
* **Lazy discovery** (generators end to end) means a directory with millions of files is
  walked without building a list of it; `PRESCAN_ENABLED` makes the reporting pass
  metadata-only and can be switched off entirely.
* **Parsing is parallel in both engines**: the Spark engine ships one task per file
  (`SPARK_PARSE_PARTITIONS` defaults to the executor core slots, capped at the file count);
  the local engine runs `PARSE_WORKERS` parser processes on the ETL host and streams the
  rows into the parquet in chunks, so neither engine holds a batch in memory.
* **Parquet between the stages** keeps the transformed batch out of memory across the
  Greenplum load and makes the load re-runnable; `PARQUET_COALESCE_PARTITIONS` avoids a
  swarm of tiny files, and compression is configurable.
* **The parse runs once**: the row count comes from reading the parquet back, not from a
  `count()` on the parse DataFrame.
* **No `collect()`/`toPandas()`** anywhere in the ETL path; counters travel back through
  Spark accumulators, so no extra action is needed to collect them.
* **Processed-file lookup is a hash set** built in one pass, holding only file keys - not
  the whole CSV.
* **Greenplum writes are bulk**: the JDBC writer sends `GREENPLUM_JDBC_BATCH_SIZE` rows
  per round trip from `GREENPLUM_WRITE_PARTITIONS` parallel writers; where a staged
  strategy is used, the promote is set-based SQL inside the database, not row-by-row
  traffic from Spark.

---

## Troubleshooting

### `PicklingError: Could not serialize object: IndexError: tuple index out of range`

The driver cannot serialise Python code for the executors. It is an installation
mismatch, not an ETL bug: PySpark ships every Python function with the cloudpickle
version it bundles, and a cloudpickle older than the interpreter cannot read that
interpreter's byte code. On such an install **every** PySpark job fails the same way -
including `sc.parallelize([1, 2]).map(lambda x: x + 1)`.

```bash
python3 src/check_environment.py          # takes a second, no cluster needed
python3 src/check_environment.py --spark  # also runs one distributed task
```

| Python | needs PySpark |
| --- | --- |
| 3.9 | >= 3.1 |
| 3.10 | >= 3.2 |
| 3.11 | >= 3.4 |
| 3.12 | >= 3.5 |
| 3.13 | >= 4.0 |

**The ETL runs anyway.** `parser.PARSE_ENGINE: auto` (the default) detects this and
switches to the local engine: the journals are parsed on the ETL host, the parquet is
written with PyArrow, and Spark only reads that parquet and performs the JDBC write - no
Python crosses into Spark. Nothing else about the run changes. This is the supported way
to run on, say, **Spark 3.1.3 with Python 3.11**; it needs `pip install pyarrow`.

To parse on the executors instead, install the PySpark that matches the cluster in the ETL
virtualenv (`pip install "pyspark==4.0.0"` for a Spark 4.0 cluster) or run the ETL with an
interpreter the installed PySpark supports, then set `PARSE_ENGINE: spark` (or leave
`auto`). Keep the driver and the executors on the same Python (`PYSPARK_PYTHON`,
`PYSPARK_DRIVER_PYTHON`).

The ETL runs this check itself before starting a cluster application
(`spark.SPARK_PRECHECK_ENABLED`), so a broken installation fails in the
`environment_check` stage with the versions and the remedy in the log and the failure
e-mail, instead of mid-batch with a stack trace.

Everything the ETL ships to the executors (`parse_partition`, `parse_journal_file`,
`record_to_row`, the accumulator parameters) is defined at module level and bound with
`functools.partial`, so cloudpickle serialises it **by reference** rather than by value;
a test asserts that no `<locals>` closure can creep back in.

### Other things worth checking first

| Symptom | Look at |
| --- | --- |
| `ModuleNotFoundError: atm_ejournal_parser` on the executors | the modules are shipped with `addPyFile`; check the Spark log for "shipped ... module(s)" and that `src/` holds both `atm_ejournal_parser.py` and `spark_parser.py` |
| `No suitable driver` / `ClassNotFoundException: org.postgresql.Driver` | `spark.SPARK_JARS` must list the PostgreSQL JDBC jar, and it must exist on the driver *and* the executors |
| The ETL finds no files | `input.FILE_PATTERN` (globs are case sensitive), `input.MIN_FILE_AGE_SECONDS`, and `python3 -c "from atm_ejournal_parser import scan_input_tree"` for the parser's own discovery report |
| A batch fails but the next run reprocesses everything | the tracking CSV was not written - check `processed/processed_files.csv` is writable and look for "processed-file tracking" lines in the run log |

---

## Tests

```bash
python3 -m pytest tests -q                 # everything that needs no database
python3 -m pytest tests -q -m "not spark"  # without a local SparkSession
```

SMTP and Airflow are mocked; Spark and parquet are exercised for real on a local session,
and the Greenplum path is exercised through a loader double that mimics the target and
control tables. Covered: batch sizing (empty directory, one batch, many batches, batch
size 1, invalid batch size), tracking (first run, second run, mixed, duplicates, malformed
rows, append-only), failure and restart (failed batch, crash after the write, reload
behaviour per strategy), parquet (write, validation, failed write, cleanup, keeping failed
batches), logging (creation, one-year retention, 1 GB limit, oldest-first deletion,
active-log protection, deletion errors), configuration (missing file, invalid YAML,
missing/invalid values, profiles, masking), the JDBC write options and every load
strategy, the CLI, the Airflow DAG (loads, wiring, retries, both e-mails), the environment
preflight (version matrix, failure recognition, the remedy message, and that nothing
shipped to the executors is a closure), and both parse engines - including that they
produce the same rows, that `auto` selects the local engine when Python cannot be shipped,
and that a local dry run starts no Spark application at all.

### Against a real database

`tests/test_greenplum_integration.py` runs the whole pipeline into a real PostgreSQL or
Greenplum instance - no mock in the Greenplum path. It is skipped unless you point it at
one:

```bash
export ATM_ETL_TEST_JDBC_URL="jdbc:postgresql://127.0.0.1:5432/bidb"
export ATM_ETL_TEST_DB_USER="etl_user"
export ATM_ETL_TEST_DB_PASSWORD="etl_password"
export ATM_ETL_TEST_JDBC_JAR="/opt/jars/postgresql-42.7.4.jar"
python3 -m pytest tests/test_greenplum_integration.py -q
```

It checks that the tables are created, that the JDBC write lands the parsed rows, that the
control table is filled in, that a second run loads nothing, and that the staged strategy
promotes through the staging table.
