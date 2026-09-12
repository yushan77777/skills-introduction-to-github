# ATM E-Journal ETL

Batched, restartable ETL around the existing ATM e-journal parser:

```
Input files
    -> processed-file check          processed/processed_files.csv
    -> unprocessed files
    -> batch selection               input.BATCH_SIZE
    -> Spark ETL                     the existing parser, on the executors
    -> Parquet                       parquet/batch_nnnn
    -> Greenplum                     staging -> target, one transaction
    -> processed-file tracking       append-only, after the commit
    -> Parquet cleanup
    -> next batch
```

The parsing logic is unchanged: `src/atm_ejournal_parser.py` is the module that was
already in use (grammar, retry collapsing, denominations, audit counters - see
[`docs/PARSER_README.md`](docs/PARSER_README.md)). What is new is everything around it:
batching, tracking, parquet staging, Greenplum loading, configuration, encryption,
logging with retention, Airflow orchestration and notifications.

---

## Contents

| Path | Purpose |
| --- | --- |
| `config/atm_ejournal.conf` | Central configuration (YAML). One section per concern, one profile per ETL |
| `config/atm_ejournal.conf.example` | The same file with dummy values only |
| `config/pickle/` | Encryption key store (`encryption.pkl`) - never committed |
| `src/atm_ejournal_parser.py` | **Existing** parser, unchanged |
| `src/config_loader.py` | Loads/validates a configuration profile |
| `src/encryption_util.py` | Reuses the project encryptor to decrypt passwords (+ CLI to encrypt one) |
| `src/log_manager.py` | Run log, per-batch logs, retention / size sweep |
| `src/file_registry.py` | Discovery, batching, `processed_files.csv`, pending markers |
| `src/spark_session.py` | SparkSession from the configuration |
| `src/spark_parser.py` | Runs the existing parser on the executors, fixed output schema |
| `src/parquet_stage.py` | Batch parquet write, validation, read-back, cleanup |
| `src/greenplum_loader.py` | Staging -> target in one transaction + batch control table |
| `src/mail_util.py` | Success/failure e-mails |
| `src/etl_runner.py` | The orchestrator (batch loop, recovery, run summary) |
| `src/run_etl.py` | Command line entry point |
| `dags/atm_ejournal_etl_dag.py` | Airflow DAG with failure/success e-mails |
| `notebooks/run_atm_ejournal_etl.ipynb` | Manual run: dry run, batch limit, inspection |
| `notebooks/run_atm_ejournal_parser.ipynb` | **Existing** parser notebook, unchanged. It imports the parser from the folder it runs in, so set `MODULE_DIR` in its first cell to `../src` |
| `tests/` | Unit and integration tests (Greenplum, SMTP and Airflow mocked) |
| `logs/`, `parquet/`, `processed/` | Runtime directories (created if missing) |

---

## Quick start

```bash
pip install -r requirements.txt

cp config/atm_ejournal.conf.example config/atm_ejournal.conf   # then edit it
export ATM_ETL_HOME=/etl/atm_ejournal

# one-off: create the key store, then encrypt the Greenplum password
python3 src/encryption_util.py --init-key
python3 src/encryption_util.py            # prompts, prints the token for the conf file

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
| `GREENPLUM_HOST` / `PORT` / `DATABASE` / `SCHEMA` / `TABLE` | Target |
| `GREENPLUM_STAGING_TABLE` | Per-batch staging table (overwritten every batch) |
| `GREENPLUM_CONTROL_TABLE` | Batch control table - the authority for restart decisions |
| `GREENPLUM_USER` | Database user |
| `GREENPLUM_PASSWORD` | **Encrypted** token, decrypted through the project encryptor |
| `GREENPLUM_PICKLE` | Key store for that token |
| `GREENPLUM_DRIVER` | JDBC driver class |
| `GREENPLUM_WRITE_FORMAT` | `greenplum` (connector) or `jdbc` |
| `GREENPLUM_JDBC_BATCH_SIZE` | JDBC `batchsize` |
| `GREENPLUM_WRITE_PARTITIONS` | Parallel writers (`0` = leave the DataFrame as it is) |
| `GREENPLUM_LOAD_STRATEGY` | `delete_insert_by_source_file` (default), `merge_by_key`, `insert_only`, `truncate_load` |
| `GREENPLUM_MERGE_KEYS` | Key columns for `merge_by_key` |
| `GREENPLUM_TARGET_DISTRIBUTED_BY` / `..._CONTROL_DISTRIBUTED_BY` | `DISTRIBUTED BY` used when the ETL creates the tables (leave empty on plain PostgreSQL) |
| `GREENPLUM_CREATE_OBJECTS` | Create the control/target tables when missing |
| `GREENPLUM_QUERY_TIMEOUT_SECONDS` | Control statement timeout |

### `parser`

`KEEP_LAST_FAILURE`, `LINK_FAILED_ACROSS_AMOUNTS`, `RETRY_WINDOW_SECONDS` are passed
straight to the existing `deduplicate_attempts()`. `KEEP_UNPARSED_RECORDS` decides whether
low-confidence blocks (`PARSE_CONFIDENT = false`) are loaded or dropped.

### `mail`

`MAIL_ENABLED`, `SMTP_HOST`, `SMTP_PORT`, `SMTP_USE_TLS`, `SMTP_USER`, `SMTP_PASSWORD`
(encrypted), `SMTP_PICKLE`, `SMTP_TIMEOUT_SECONDS`, `MAIL_FROM`, `MAIL_TO`, `MAIL_CC`,
`MAIL_SUBJECT_SUCCESS`, `MAIL_SUBJECT_FAILURE`.

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
        atm_ejournal.conf
        atm_ejournal.conf.example
        pickle/encryption.pkl          <- key store (never committed)
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

```
parquet/batch_0001 --Spark write--> atm.atm_ejournal_withdrawals_stg   (overwrite)
                                            |
                          BEGIN             |
                            DELETE target rows for this batch's source files
                            INSERT INTO target SELECT * FROM staging
                            INSERT INTO atm.atm_ejournal_batch_control (...)
                          COMMIT
```

The promote and the control row commit together, so the target rows and the "this batch is
loaded" marker can never disagree. The control table is what a restart consults.

Control statements go through `psycopg2` when it is installed; otherwise the loader borrows
the PostgreSQL JDBC driver already on the Spark classpath through the JVM, so no extra
Python dependency is needed on the edge node.

**Idempotency.** The existing target is an append-style withdrawal table with no natural
primary key (the same card can legitimately withdraw the same amount twice), so
de-duplicating on business values would silently delete real transactions. The unit that
*is* unambiguous is the source file: everything in a journal file is reproduced exactly by
re-parsing it. Hence the default `delete_insert_by_source_file` - it removes what those
files loaded before and inserts the new result, which makes any re-run (a retried batch, a
restored tracking CSV, a manual reprocess) produce the same table content. Every row also
carries `SOURCE_FILE_KEY`, `BATCH_ID`, `ETL_RUN_ID` and `LOAD_TS` for auditing.

Choose another strategy when the table's grain differs: `merge_by_key` (delete by
`GREENPLUM_MERGE_KEYS`), `truncate_load` (full reload), `insert_only` (plain append - no
idempotency; only for a feed de-duplicated downstream).

Output schema: the columns produced by the parser, typed and stable
(`ATM_NO`, `TRANSACTION_DATETIME`, `DATE`, `TIME`, `ACCOUNT_NO`, `CARD_NO`, `AMOUNT`,
`REQUESTED_AMOUNT`, `CURRENCY`, `STATUS`, `RESPONSE_CODE`, `TRANSACTION_REF`, `TRACE_ID`,
`TERMINAL_ID`, `CARD_SCHEME`, `DISPENSE_RESULT`, `DENOMINATION`, `NOTES_COUNT`,
`DENOM_AMOUNT`, `DENOM_MATCHES_AMOUNT`, `CASH_TAKEN`, `TRX_ERROR`, `ATTEMPT_NO`,
`ATTEMPT_COUNT`, `IS_RETRY`, `SESSION_ID`, `TXN_SEQ`, `SOURCE_FILE`, `SOURCE_LINE`, ...).
The pandas version's dynamic `NOTES_<value>` columns cannot be part of a fixed table
schema; the same information is carried by `DENOMINATION` (`5000x9 + 1000x3`) and
`DENOM_BREAKDOWN` (JSON), from which per-note columns are derived in SQL when needed.

---

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

## Encryption

The ETL reuses the encryptor already in the project:

1. drop the encryptor file into `src/` and the key pickle into `config/pickle/`;
2. point `encryption.MODULE` at the module name and `encryption.PICKLE_PATH` at the pickle
   (per-secret overrides: `GREENPLUM_PICKLE`, `SMTP_PICKLE`);
3. store only encrypted tokens in the configuration.

Both shapes are supported: a class (`Encryptor(pickle_path).get_decrypt_data(token)`) and a
module-level `get_decrypt_data(token)` function - the same call the existing Oracle job
makes. If the module is absent, an equivalent built-in Fernet/pickle implementation is used
(same token format, same key store) and a warning is logged; set
`ALLOW_BUILTIN_FALLBACK: false` to make a missing encryptor a hard error instead.

Decrypted values are never logged, never put into an exception message and never written to
the run summary; `cfg.safe_dump()` masks every `*PASSWORD*`/`*SECRET*`/`*TOKEN*` key.

Encrypt a value (it is prompted for, never taken from the command line):

```bash
python3 src/encryption_util.py --config config/atm_ejournal.conf
```

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
| Greenplum load | transaction rolled back; no target rows, no control row; pending marker present | marker cleared, files retried, no duplicates |
| Crash **after** the Greenplum commit, before the tracking update | target rows + control row exist; pending marker present | the control table is consulted: the batch is recognised as committed, the tracking CSV is completed **without reloading** |
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
* **Parsing happens on the executors**, one file per task; only paths are shipped.
  `SPARK_PARSE_PARTITIONS` defaults to the executor core slots, capped at the file count.
* **Parquet between the stages** keeps the transformed batch out of memory across the
  Greenplum load and makes the load re-runnable; `PARQUET_COALESCE_PARTITIONS` avoids a
  swarm of tiny files, and compression is configurable.
* **The parse runs once**: the row count comes from reading the parquet back, not from a
  `count()` on the parse DataFrame.
* **No `collect()`/`toPandas()`** anywhere in the ETL path; counters travel back through
  Spark accumulators, so no extra action is needed to collect them.
* **Processed-file lookup is a hash set** built in one pass, holding only file keys - not
  the whole CSV.
* **Greenplum writes are bulk**: the connector (or JDBC with `batchsize`) writes the
  staging table in parallel (`GREENPLUM_WRITE_PARTITIONS`); the promote is set-based SQL
  inside the database, not row-by-row traffic from Spark.

---

## Tests

```bash
python3 -m pytest tests -q                 # everything
python3 -m pytest tests -q -m "not spark"  # without a local SparkSession
```

Greenplum, SMTP and Airflow are mocked; Spark and parquet are exercised for real on a
local session. Covered: batch sizing (empty directory, one batch, many batches, batch size
1, invalid batch size), tracking (first run, second run, mixed, duplicates, malformed rows,
append-only), failure and restart (failed batch, crash after commit, forced reprocessing),
parquet (write, validation, failed write, cleanup, keeping failed batches), logging
(creation, one-year retention, 1 GB limit, oldest-first deletion, active-log protection,
deletion errors), configuration (missing file, invalid YAML, missing/invalid values,
profiles, masking), encryption (project encryptor reuse, missing pickle, invalid token, no
secret ever logged), Greenplum SQL per strategy and rollback behaviour, the CLI, and the
Airflow DAG (loads, wiring, retries, both e-mails).
