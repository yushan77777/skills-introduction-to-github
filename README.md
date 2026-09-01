# ETL & Monitoring Platform

One Django project serving two applications that stay separate inside it:

```
┌─────────────────────────────────────────┐
│         ETL & MONITORING PLATFORM       │      /
│                                         │
│      ┌────────────┐  ┌────────────┐     │
│      │ Monitoring │  │    ETL     │     │
│      └────────────┘  └────────────┘     │
└─────────────────────────────────────────┘
             │                │
     /monitoring/          /etl/
     /disk-usage/          runs the real ETL on this server
```

The Monitoring application is the existing `monitoring_web_app_v2`, reused as
it was. The ETL application is new, and drives the existing ETL project rather
than reimplementing any part of it.

---

## 1. Architecture

```
manage.py
monitoring_site/            Django project (settings, urls, wsgi, asgi)
│
portal/                     NEW  — the unified landing page, nothing else
home/                       Monitoring — app launcher            (unchanged)
disk_usage/                 Monitoring — disk usage app          (unchanged)
etl/                        NEW  — ETL console
│   settings.py               where the ETL project and runtime are
│   discovery.py              reads the ETL source to find the jobs
│   registry.py               labels/inputs for the discovered parameters
│   validation.py             schema, runtime and environment checks
│   runner.py                 process control: start, watch, stop, one-at-a-time
│   worker.py                 the subprocess that calls the real run_etl()
│   etl_config.py             read-only view of the ETL's config.yaml
│   diagnostics.py            names the likely cause of a failed run
│   spark_preflight.py        Spark connectivity probe, run by spark_check
│   runtime_env.py            rebuilds the ETL's Linux environment
│   security.py               credential redaction
│   views.py / urls.py        page + JSON API
│   management/commands/etl_check.py
│
templates/                  base.html + Monitoring templates      (base.html: 5 lines)
static/                     site.css, diskusage.js                (unchanged)
etl_project/                the reconstructed ETL source — the thing that runs
deploy/                     systemd unit, gunicorn config, nginx example
docs/                       original READMEs kept for reference
```

Everything the ETL app needs to know about the outside world arrives through
environment variables. It never imports the ETL into the web process, never
holds a database connection of its own and defines no models.

### Request flow when you press Run

```
Browser
  └─ POST /etl/api/run/            Django view (CSRF-protected)
       └─ etl.validation            job must be one discovered in the source
            └─ etl.runner           takes the single run slot + lock file
                 └─ subprocess      ETL_PYTHON etl/worker.py   (own process group)
                      └─ run_etl()  the real etl_table_manual.run_etl
                           └─ Spark / Oracle / Greenplum
```

---

## 2. Monitoring application

Unchanged. `home` and `disk_usage` are the original code, the original
templates and the original `static/css/site.css` and `static/js/diskusage.js`.
`disk_usage/gp.py` still reads `config.ini` through `MONITORING_CONFIG`, still
speaks to Greenplum through psycopg2, and still supports the sqlite demo mode.

**Everything changed for the integration, and why:**

| Change | Why |
|---|---|
| `/` now shows the landing page; the Monitoring launcher moved to `/monitoring/` | The platform needs one entry point offering both applications |
| `templates/base.html`: brand links to `/`, brand text and page title become "ETL & Monitoring", nav "Home" is relabelled "Monitoring" and gains an **ETL** link, plus an empty `{% block styles %}` for app stylesheets | One navigation and one identity across both applications. Five lines changed, one block added |
| `settings.py`: `portal` and `etl` added to `INSTALLED_APPS` | Registering the new apps |
| `settings.py`: `SECRET_KEY`, `DEBUG`, `ALLOWED_HOSTS` read the environment, falling back to the previous literals | Needed to run with `DEBUG=0` in production without editing the file. Behaviour with no environment set is identical to before |

**Deliberately unchanged:** `/disk-usage/` and both of its JSON endpoints keep
their exact URLs, so existing links and bookmarks still work. The
`home:index` and `disk_usage:index` URL names are unchanged, so no Monitoring
template needed rewriting. No Monitoring view, model or query was touched.

**Every other Monitoring file is byte-identical to the supplied application** —
`home/`, `disk_usage/` (including `gp.py`), `templates/home/index.html`,
`templates/disk_usage/index.html`, `static/css/site.css`,
`static/js/diskusage.js`, `scripts/make_demo_data.py`, `manage.py`,
`monitoring_site/wsgi.py` and `monitoring_site/asgi.py`. Verify it against the
original with `cmp`.

`etl/tests.py` contains a `PlatformIntegrationTests` case that fails if the
URLs, names or pages stop behaving as they did.

---

## 3. ETL application

At `/etl/`:

* **Available ETL jobs** — a dropdown built from the jobs found in the ETL
  source, with the source → destination route and the notes that apply to each.
* **Configure a run** — the form is generated per job; only that job's
  parameters appear, and required means what the ETL itself requires.
* **Current execution** — job, start time, live duration, current stage, the
  ETL's real stage list, its live log, and **Run** / **Stop**.
* **Configuration** — a read-only summary of `config.yaml`. There is no
  editor, and no endpoint that writes configuration.
* **Execution history** — job, start, end, duration, status, who triggered it,
  the error message and a link to the run's log file.

No login of its own: the platform is assumed to be on an internal network, and
the Monitoring application has no login either. If Django authentication is
added later, the ETL app picks it up automatically — the execution history
records `request.user` when there is one, and the client address otherwise.

---

## 4. ETL job discovery

Jobs are **not** hard-coded. `etl/discovery.py` parses
`etl_table_manual.py` with Python's `ast` module — it is read, never imported
and never executed — and lifts three things out of `run_etl`:

| From the source | Used for |
|---|---|
| the `STEP_MAP` dict literal | the job identifiers **and** their real stage lists |
| each `if etl_name == "…":` branch | whether a job is actually implemented |
| each `if not (a and b …): raise ValueError` guard | the parameters the job refuses to start without |
| the `run_etl` signature | the default value of every parameter |

Add a branch to `run_etl` and an entry to its `STEP_MAP`, and the job appears
in the UI on the next page load with its stages and required parameters
correct. Nothing in this project needs editing for that to happen.

`etl/registry.py` adds only presentation: labels, input types, grouping, help
text and format hints, keyed by parameter name. A job it has never seen still
works — it renders with generic labels and a note saying a description has not
been registered for it yet.

Two places where the form asks for more than the source strictly guards, both
declared in `JOB_META[...].extra_required` and explained in the job's on-screen
notes:

* `_column` on the three `oracle_*` jobs. `run_etl` does not check it, but
  `read_oracle_df` interpolates it into 20 `MOD(ORA_HASH(<column>), 20) = n`
  predicates, so an empty value produces invalid SQL rather than a clear error.
* `gp_url` on `csv_to_greenplum`. The guard checks `gp_config_key`, but the
  code path that actually runs writes through the JDBC URL (the pandas
  `copy_from_dataframe` branch is commented out in the implementation).

The parsing is covered by `DiscoveryTests`, which asserts the exact job list,
stage lists and required tuples of the supplied source.

---

## 5. ETL execution

Pressing **Run** executes the real ETL. There is no simulation anywhere in this
project.

Each run is a **subprocess**, started as
`$ETL_PYTHON -u etl/worker.py` with the ETL project as its working directory,
because:

* Spark takes over the JVM and process-wide state — a hung or crashed run must
  not be able to take the web application down with it;
* `run_etl` reads `config/config.yaml` by relative path and the ETL module
  chdirs on import, so it needs its own working directory;
* the ETL runs under the **existing Linux runtime** (`ETL_PYTHON`), which is a
  different interpreter from the one serving Django;
* stopping becomes one signal to a process group instead of an uninterruptible
  thread.

`etl/worker.py` imports the existing module and calls its existing function.
It contains no ETL logic and no second connector implementation. The job
payload arrives as **JSON on stdin**, never on the command line, so credentials
never appear in `ps`.

### Only one ETL at a time

Enforced in the backend, twice:

1. A `threading.Lock` around the single run slot, covering threads inside one
   web worker.
2. An exclusive **lock file** (`$ETL_LOG_DIR/etl-run.lock`) recording the ETL's
   process group, covering everything else: a second gunicorn worker, a second
   web process, or a run that outlived a restart of the web application. A
   stale lock is detected by asking the operating system whether that process
   group still exists — never by trusting a flag.

A second run request gets **HTTP 409** with the details of the run that is
already going. The disabled Run button in the browser is a courtesy; the
server is the guard. `SingleRunTests` covers all of it, including four threads
racing to start at once.

### Live stage progress

The stage strip is the ETL's own. `StepBar` in `etl_table_manual.py` drives a
tqdm bar, and `etl/runner.py` reads that bar out of the captured output. No
percentage is invented, and the stage names come from `STEP_MAP`.

Two details that matter, both covered by `ProgressParsingTests`:

* `StepBar.next` sets the tqdm postfix *before* incrementing the counter, so a
  half-drawn bar can carry a count and a stage name that disagree by one. The
  **name wins** when it matches a stage in `STEP_MAP`.
* tqdm suppresses a redraw landing within `mininterval` (0.1 s) of the previous
  one, which silently drops the first transitions. The worker environment sets
  `TQDM_MININTERVAL=0` so every stage the ETL announces reaches the UI. That
  changes how often the existing progress bar repaints — nothing else.

---

### Reporting the cause, not the last symptom

A Spark failure reports itself twice, a minute apart, in the wrong order of
usefulness. The real event is a line like

```
ERROR StandaloneSchedulerBackend: Application has been killed.
      Reason: All masters are unresponsive! Giving up.
```

which stops the SparkContext. Nothing appears to go wrong at that moment. The
next operation needing a live context — usually the write — then fails with

```
An error occurred while calling o307.csv.
: java.util.NoSuchElementException: None.get
  at ...datasources.BasicWriteJobStatsTracker$.metrics
```

That method body is `SparkContext.getActive.get`, so `None.get` means "no
active SparkContext", not "something is wrong with the CSV write". Reporting
only the exception the ETL raised — which is all the ETL process itself knows
— sends whoever reads it to the wrong end of the pipeline.

`etl/diagnostics.py` therefore scans the captured output for a small set of
failures whose meaning is unambiguous and offers the first one as the **likely
cause**, above the raw exception, with the log line it matched. The execution
history's Message column shows the cause too.

Two rules keep it honest: every pattern is a verbatim message emitted by
Spark, the JDBC layer or the Oracle client — nothing is inferred from the
shape of a stack trace — and it is always presented as the *likely* cause
alongside the real error, never instead of it. A failure that matches nothing
reports the raw exception exactly as before.

Recognised today: Spark master registration failure, driver bind-address
failure, no executor resources, a stopped SparkContext, a write with no active
context, missing Oracle Instant Client (`DPI-1047`), rejected Oracle
credentials (`ORA-01017`), a missing JDBC driver, and `OutOfMemoryError`. Each
carries a next step pointing at the thing in this project that fixes it.

## 6. Stop behaviour

**Stop kills immediately.** `SIGKILL` to the ETL's whole process group — not a
graceful request the ETL can decline, and no shutdown phase before it.

The child is started with `start_new_session=True` precisely so it has its own
process group, which means one signal reaches the Spark driver and everything
the ETL spawned. This is what stops orphans being left behind.

The run is then marked **Stopped** — a distinct status, not a failure. The lock
is released, the log file is closed, and the slot is free again.

If the web application cannot signal the process (it is running as a different
user), the UI says so instead of silently reporting success.

`StopTests` asserts that the child process is actually gone, that the status is
`stopped`, and that the lock file has been removed.

---

## 7. Logs

The ETL's **existing** logging mechanism is used, not replaced.

`etl_table_manual.py` already routes its messages through a module-level
`logger` when one is set and falls back to `print` when it is not, and the
`greenplum` / `oracle` helpers already take a `logger` argument. `etl/worker.py`
sets that hook to a stdout logger. The ETL's own `__logger` calls keep working
exactly as written, and nothing in the ETL source was modified to make logging
work.

Everything the ETL writes — that logger, its `print` calls, the tqdm bar,
tracebacks — is captured and goes to two places:

* the **live log pane** in the browser, streamed incrementally while the run
  is in progress;
* a **file per run**, at
  `$ETL_LOG_DIR/<timestamp>-<job>-<run id>.log`, linked from the execution
  history and served by `/etl/api/log/?run=<id>`.

Every line passes through `etl/security.py` first, which masks the submitted
passwords plus anything that looks like a credential — including the
`user/password@host` embedded in the Oracle JDBC URLs in `config.yaml`.

> The supplied ETL source writes no log file of its own and names no log path,
> so there was no existing log location to read from. The per-run file is that
> same output stream persisted, not a second logging system.

The log path is looked up from the run record, never from the request, so
`/etl/api/log/` cannot be turned into an arbitrary file read.

---

## 8. Configuration

`config/config.yaml` stays **backend-controlled**. The UI shows a read-only
summary and offers no way to change any of it — there is no YAML editor, no
raw dump and no endpoint that writes configuration.

The Configuration panel shows:

* **Runtime** — project root, entry point, `ETL_PYTHON`, config path, log
  directory, each marked present or missing.
* **Spark cluster** — master URL, session name, executors, memory, cores,
  connector jars, read from `spark_properties`.
* **Connection profiles** — per profile: name, kind, user, host/port/database
  or service, and whether its credential material actually works.

That last check is the useful one. It reports whether the encrypted `password`
in each profile can be decrypted with the `pass1.pkl` the profile names, which
catches a rotated key long before a run fails against it. **In the supplied
configuration, three of the five Oracle profiles fail this check** —
`Test_Common_DB`, `uat_server_2` and `common_db_mckapn` were encrypted with a
different key from the one supplied. `oracle_50` and `oracle_135` decrypt
correctly. Run `manage.py etl_check` to see the current state.

No password, ciphertext or key ever reaches the browser; `EtlViewTests`
asserts that.

---

## 9. Execution history

Held in memory (the last `ETL_HISTORY_SIZE` runs, default 50) and mirrored to
the per-run log files on disk. **No new database table or migration was
introduced** — the ETL app defines no models at all.

Each row carries the job, start, end, duration, status, who triggered it, the
error message and a link to the log.

| Status | Meaning |
|---|---|
| **Running** | the process is alive and its output is being read |
| **Completed** | exit code 0 and no error reported by the ETL |
| **Failed** | the ETL raised, or the process exited non-zero, or it was killed from outside the application |
| **Stopped** | terminated by the Stop button |

Restarting the web application clears the table, not the logs. It also does not
stop a running ETL — that is a separate process group — and the next web
process discovers it through the lock file and offers a Stop button for it.

---

## 10. Linux deployment

The existing deployment method is preserved and extended: same venv, same
`manage.py`, same `monitoring_site.wsgi` entry point.

### 10.1 Install

```bash
cd <APP_ROOT>
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
./venv/bin/python manage.py migrate          # Django internals, local sqlite
./venv/bin/python manage.py collectstatic --noinput
```

### 10.2 Configure

```bash
cp config.example.ini config.ini             # Monitoring: Greenplum settings
chmod 600 config.ini
sudo mkdir -p /etc/etl-monitoring
sudo cp deploy/environment.example /etc/etl-monitoring/environment
sudo chmod 600 /etc/etl-monitoring/environment
sudo $EDITOR /etc/etl-monitoring/environment
```

The ETL variables that matter:

| Variable | Meaning | Default |
|---|---|---|
| `ETL_PROJECT_ROOT` | the existing ETL project directory | `<APP_ROOT>/etl_project` |
| `ETL_PYTHON` | the existing runtime with PySpark and the Oracle client | the interpreter running Django |
| `ETL_ENV_SCRIPT` | optional shell script sourced for the ETL subprocess — not needed, see §10.3 | none |
| `ETL_ENV_FILE` | optional extra `KEY=VALUE` lines — not needed | none |
| `ETL_LOG_DIR` | where per-run logs and the lock file go | `<ETL_PROJECT_ROOT>/logs` |
| `ETL_MODULE` | entry point module | `etl_table_manual` |
| `ETL_ENTRYPOINT` | entry point function | `run_etl` |
| `ETL_CONFIG_YAML` | config path, relative to the project root | `config/config.yaml` |
| `ETL_HISTORY_SIZE` | runs kept in the history table | `50` |
| `ETL_LOG_BUFFER` | log lines kept in memory per run | `4000` |

`ETL_PYTHON` should point at the **existing** ETL virtualenv. Do not build a
new one if a working one exists — see §11.

### 10.3 Spark configuration

**Spark is configured from `etl_project/config/config.yaml` and nowhere else.**
`build_spark()` reads `spark_properties` — master URL, connector jars,
executor instances, memory, cores, driver memory — and the ETL app sets no
Spark environment variable and passes no override. There is nothing to keep in
sync between the YAML and the service environment, because the service
environment does not configure Spark.

Check it before trusting it:

```bash
./venv/bin/python manage.py spark_check
```

That runs the ETL run minus the ETL: a session built by the project's **own**
`build_spark` from `config.yaml`, then one trivial job that needs a real
executor. A failure there is unambiguously Spark's; a success means the next
failure is not. It reports:

| Check | What it tells you |
|---|---|
| `master_url` | what `spark_properties` actually says |
| TCP probe of the master | whether the port is reachable from this host at all |
| cluster Spark version, alive workers, free cores | read from the master's own web UI and `/json/` |
| every jar in `spark_properties.jars` | whether it exists and this user can read it |
| client PySpark version | and whether its major/minor matches the cluster's |
| `spark.driver.host` | the address the driver advertises for the master to call back on |
| driver `java.io.tmpdir` | `build_spark` hard-codes one; whether it exists and is writable here |
| **whether the master ever listed the application** | the decisive one — see below |

That last check answers the symptom directly. **Nothing at all in the Spark
master UI** means the registration never arrived, which narrows it to the
network route or a client/cluster version mismatch — not a driver call-back
problem. An application that **appears and then fails** means registration did
arrive, and the fault is downstream of it, with the master's own reason visible
in its UI.

Add `--verbose-spark` to see Spark's own output, `--ui-port` if the master web
UI is not on 8080, and `--timeout` to change how long it waits (Spark gives up
on registration after about 60 seconds, so allow more than that).

#### If the driver's address is the problem

Spark logs `Your hostname, <host>, resolves to a loopback address … using
<address> instead`, and if that guess is wrong the master cannot call the
driver back. Fix it **without** an environment variable by correcting this
host's entry in `/etc/hosts` so its name resolves to the address the Spark
network reaches it on. `spark_check` reports the hostname, what it resolves
to, and what the driver ended up advertising.

#### The two environment hooks

`ETL_ENV_SCRIPT` and `ETL_ENV_FILE` exist for a host that needs something set
before Python starts — a dynamic-linker path, say. **Leave them unset.** The
supplied ETL needs neither: `oracle_connect.py` passes `lib_dir` to
`init_oracle_client` explicitly rather than relying on `LD_LIBRARY_PATH`, and
Spark takes everything from the YAML. They are documented in
`deploy/environment.example` and default to off.

### 10.4 Required directories and permissions

* `$ETL_LOG_DIR` must be **writable by the service user** — the per-run logs
  and the run lock both live there.
* The service user must be the one that **already owns the ETL runtime and its
  data paths**. The web application starts the ETL as a child process, so it
  inherits this identity. A different user fails on file permissions and, worse,
  cannot signal a run it did not start — the Stop button would report a
  permission error.
* `config.ini` and `etl_project/config/config.yaml`: `chmod 600`.
* `etl_project/config/pickle/pass1.pkl`: `chmod 600`.

### 10.5 Run it

```bash
sudo cp deploy/etl-monitoring.service /etc/systemd/system/   # edit placeholders
sudo systemctl daemon-reload
sudo systemctl enable --now etl-monitoring
```

`deploy/nginx.conf.example` serves `staticfiles/` and proxies the rest.
Set `X-Forwarded-For` as it does, or every history row will say `127.0.0.1`.

> **Run gunicorn with exactly one worker** (`deploy/gunicorn.conf.py` does).
> The live run and the execution history are in memory, so several workers
> would each hold their own copy and what you saw would depend on which worker
> answered. The one-ETL-at-a-time rule itself survives extra workers — the lock
> file covers that — but the display would not. Use threads for concurrency.

### 10.6 Verify

```bash
./venv/bin/python manage.py check
./venv/bin/python manage.py etl_check         # exits 1 if the ETL cannot run
./venv/bin/python manage.py test etl
```

`spark_check` is the one to run when Spark itself is suspect (§10.3).
`etl_check` prints the project paths, the environment the ETL will
receive, the discovered jobs with their stages and required parameters, every
connection profile with its credential state, a TCP probe of the Spark master,
and whether a run is in progress — without starting anything and without
printing a credential.

### 10.7 Try it without any of the above

```bash
./venv/bin/python scripts/make_demo_data.py
MONITORING_CONFIG=config.demo.ini ./venv/bin/python manage.py runserver 0.0.0.0:8000
```

Monitoring runs against a generated sqlite file. The ETL console loads,
discovers the jobs and shows the configuration; starting a run fails with a
clear message unless `ETL_PYTHON` has the ETL's dependencies.

---

## 11. Dependencies

Two environments, deliberately kept apart.

**`requirements.txt` — the web process:** Django, psycopg2-binary (both
unchanged from the Monitoring app), PyYAML and cryptography for the ETL app's
configuration summary. Nothing heavy: the ETL does not run here.

**`etl_project/requirements-etl.txt` — the ETL runtime (`ETL_PYTHON`):**
pyspark, pandas, numpy, PyYAML, tqdm, pyarrow, oracledb, psycopg2-binary,
cryptography — derived from the imports in the supplied ETL source.

> On the existing server this environment **already exists and already works**.
> That file documents what the runtime must provide; it is not meant to be
> pip-installed over a working environment. Versions are unpinned because the
> supplied source names none — pin them from `pip freeze` on the working host
> before using it to build a second one.

There is **no version conflict** between the two: the only shared packages are
PyYAML, cryptography and psycopg2-binary, and the web process does not import
the ETL, so they are resolved independently in separate interpreters.

Not pip-installable, and required on the ETL host:

* **Oracle Instant Client at `/usr/lib/oracle/12.2/client64/lib`** —
  `src/utils/oracle_connect.py` calls
  `oracledb.init_oracle_client(lib_dir=...)` with that exact path at import
  time, so the module cannot be imported without it.
* a reachable **Spark master** (`spark_properties.master_url`);
* the **connector jars** in `spark_properties.jars`, readable by the ETL user;
* a writable Spark temp directory at `/data/spark-4.0.0-bin-hadoop3/tmp/`.

---

## 12. Troubleshooting

Start with `manage.py etl_check` — most of the table below is something it
reports directly.

| Symptom | Cause | Fix |
|---|---|---|
| "No ETL jobs discovered" | `ETL_PROJECT_ROOT` wrong, or `etl_table_manual.py` not there | `etl_check` prints the path it looked in |
| A job shows "(unavailable)" | it is in `STEP_MAP` but has no `if etl_name == …` branch | add the branch, or remove it from `STEP_MAP` |
| Run fails instantly with "missing a dependency" | `ETL_PYTHON` is the Django venv, not the ETL runtime | point `ETL_PYTHON` at the existing ETL interpreter |
| `DPI-1047` / Oracle client errors | Instant Client missing at the hard-coded path | install it at `/usr/lib/oracle/12.2/client64/lib` |
| "Encrypted, but pass1.pkl does not decrypt it" | that profile's password was encrypted with a different key | re-encrypt it: `python src/utils/encrypt_module.py '<password>'` |
| **`All masters are unresponsive! Giving up.`** after ~1 minute, and **nothing in the Spark master UI** | the registration never reached the master | run `manage.py spark_check`. If it too reports the master never listed the application, it is the network route to `master_url` or a client/cluster Spark version mismatch — both of which that command checks directly |
| The application **appears** in the master UI and then fails | registration arrived; the master could not keep it | usually the master cannot open a connection back to the driver. `spark_check` reports `spark.driver.host` and the hostname resolution behind it — see §10.3 |
| `Master removed our application: FAILED` | the master gave up after repeated executor failures | the master UI shows the executors' own error. A driver `java.io.tmpdir` that the ETL user cannot write to is one cause; `spark_check` checks that path |
| `Py4JJavaError … NoSuchElementException: None.get` in `BasicWriteJobStatsTracker` | not a separate fault — `BasicWriteJobStatsTracker.metrics` is `SparkContext.getActive.get`, so this is a write running with no active context | fix whatever stopped the session (the row above, usually); the UI reports that as the likely cause and shows this only as the reported error |
| `ClassNotFoundException: org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions` | `spark-defaults.conf` sets `spark.sql.extensions`, but `build_spark` replaces `spark.jars` with the list from `config.yaml`, which has no Iceberg jar | a warning, not fatal. Add the Iceberg jar to `spark_properties.jars`, or drop the extension from `spark-defaults.conf` |
| `Service 'SparkUI' could not bind on port 4040/4041` | other Spark drivers are already running on the host | a warning; Spark takes the next free port |
| Run fails at "Build Spark" | Spark master unreachable, or the jars are missing or unreadable | check `spark_properties` in the Configuration panel and run `etl_check` |
| Oracle jobs fail with `DPI-1047` under the service but work by hand | `LD_LIBRARY_PATH` is not in the service environment | set it in `ETL_ENV_SCRIPT` — see §10.3 |
| "Another ETL run is already in progress" but nothing is running | a lock left by a killed web process whose ETL is *still alive* | `cat $ETL_LOG_DIR/etl-run.lock`; the Stop button handles it, or kill the process group named there |
| Stop reports a permission error | the web application runs as a different user from the ETL | run both as the same user (§10.3) |
| History empty after a restart | expected — it is in memory | the log files are still in `$ETL_LOG_DIR` |
| History or run status flickers between requests | more than one gunicorn worker | set `workers = 1` (§10.4) |
| CSS missing with `DJANGO_DEBUG=0` | static files not collected or not served | `manage.py collectstatic`, and point nginx at `staticfiles/` |
| Every history row says `127.0.0.1` | the proxy is not setting `X-Forwarded-For` | see `deploy/nginx.conf.example` |

---

## 13. Changing the ETL later

**Adding a new ETL job** — edit `etl_project/etl_table_manual.py` only:

1. add an entry to `STEP_MAP` in `run_etl`, listing its real stages;
2. add the `if etl_name == "<key>":` branch, guarded the same way as the
   others: `if not (a and b): raise ValueError(...)`;
3. reload `/etl/`.

The job appears with its stages and required parameters. Optionally add a
`JOB_META` entry in `etl/registry.py` for a proper label, description and the
optional parameters — nothing breaks without it, the form is just terser.

**Adding a parameter to an existing job** — add it to the `run_etl` signature,
then add its name to that job's `fields` list in `etl/registry.py` (a
`FieldSpec` gives it a label and help text; without one it renders as a plain
text input).

**Changing ETL logic** — edit the ETL source. Nothing in this project needs to
change: it never copies ETL logic, only calls `run_etl`. Re-run
`manage.py etl_check` afterwards to confirm discovery still agrees with the
source, and `manage.py test etl` to confirm nothing else drifted.

**Pointing at a different ETL project** — set `ETL_PROJECT_ROOT`,
and `ETL_MODULE` / `ETL_ENTRYPOINT` if the entry point is named differently.

---

## 14. The reconstructed ETL project

`etl_project/` was rebuilt from `etl_code.txt`. Each file was sliced out of
that text by line range — nothing was retyped, and no logic was altered:

| Master text entry | Reconstructed path | Notes |
|---|---|---|
| `etl_table_manual` (py) | `etl_table_manual.py` | the entry point, `run_etl` |
| `greenplum_connect` (py) | `src/utils/greenplum_connect.py` | imported as `src.utils.greenplum_connect` |
| `oracle_connect` (py) | `src/utils/oracle_connect.py` | imported as `src.utils.oracle_connect` |
| `encrypt_module` (py) | `src/utils/encrypt_module.py` | imported as `src.utils.encrypt_module` |
| `config` (yaml) | `config/config.yaml` | not in version control — see below |
| `pass1` (pkl) | `config/pickle/pass1.pkl` | not in version control — see below |

The import paths come from `etl_table_manual.py` itself:

```python
from src.utils.encrypt_module import Encrypter
from src.utils.greenplum_connect import greenplum as gp
from src.utils.oracle_connect import oracle
```

and the config path from `load_yaml("config/config.yaml")`, resolved relative
to the working directory — which is why the worker chdirs to the project root.

**Files added during reconstruction:** empty `src/__init__.py` and
`src/utils/__init__.py`. They are not in the master text. They contain nothing
and change no behaviour; they make `src.utils` a regular package so the import
resolves the same way under systemd and gunicorn as it does from a shell.

### About `pass1.pkl`

Despite the extension it is **not a pickle**. `Encrypter._load_fernet` reads
the file as raw bytes and hands them to `Fernet(key)`, so it is 44 bytes of
URL-safe base64 — exactly what `Fernet.generate_key()` writes. The master text
contained that key in full, so the file is reconstructed byte-for-byte; nothing
is unpickled and no fake artefact was created. `etl_project/config/pickle/README.md`
has the detail.

### Credentials are not in version control

`config/config.yaml` holds internal database hosts, usernames, encrypted
passwords **and plaintext passwords embedded in the Oracle JDBC URLs**, and
`pass1.pkl` is the key that decrypts the encrypted ones. This repository is
public, so both are excluded by `.gitignore` — the same practice the Monitoring
application already followed for its own `config.ini`.

Committed in their place:

* `etl_project/config/config.example.yaml` — the same structure and key names,
  placeholder values;
* `etl_project/config/pickle/README.md` — what the file is and how to restore it.

On the server, both real files are already present in the existing ETL project;
point `ETL_PROJECT_ROOT` at it and nothing needs copying. To run against
`etl_project/` in this repository instead, copy them in:

```bash
cp config/config.example.yaml etl_project/config/config.yaml   # then fill it in
cp /path/to/existing/config/pickle/pass1.pkl etl_project/config/pickle/
chmod 600 etl_project/config/config.yaml etl_project/config/pickle/pass1.pkl
```

**Because these credentials were transmitted in a plain text file, treat them
as exposed and rotate them** — the Oracle passwords, the Greenplum accounts and
the Fernet key.

---

## 15. Security

* **No arbitrary execution.** The only executable surface is `run_etl` with an
  `etl_name` that must be one of the identifiers found in the ETL source, and
  keyword arguments that must be parameters of that job. There is no endpoint
  that runs a shell command, evaluates Python, or reads a path from the request.
* **No configuration writes.** The configuration API is read-only; there is no
  write counterpart anywhere in the app.
* **CSRF is enforced** on every state-changing endpoint, using Django's own
  middleware — no `csrf_exempt` anywhere.
* **Credentials never persist.** Passwords submitted with a run are held in
  memory for that run, sent to the ETL on stdin, and dropped. They are not
  written to disk, not echoed back, not in `ps`, and masked in every captured
  line before it reaches the log pane or the log file.
* **Error messages carry the exception type and message, never a traceback.**
  Tracebacks go to the run log file on the server.
* Monitoring's existing practices are preserved: `config.ini` out of version
  control, table names validated as SQL identifiers, queries parameterised.

---

## 16. Restrictions and assumptions

1. **One ETL runs at a time**, platform-wide.
2. **Stop is a kill**, not a graceful shutdown. Work in flight is lost, and a
   partially written target is possible — the same as killing the ETL from a
   shell.
3. **Run gunicorn with one worker.** See §10.5.
4. **The web application and the ETL run as the same user.**
5. **Execution history is in memory** and is lost on restart. The log files are
   not.
6. **The ETL's business logic is untouched.** Every quirk of the supplied
   implementation is still there; where one affects the UI it is stated in the
   job's on-screen notes rather than being fixed behind your back.
7. **No login.** Anyone who can reach `/etl/` can run an ETL. The platform is
   assumed to be on an internal network.
8. `TQDM_MININTERVAL=0` is set for the ETL subprocess so no stage transition is
   dropped from the display. It affects only how often the existing progress
   bar repaints.
9. **Spark is configured from `config.yaml` alone.** No environment variable
   configures Spark, and the ETL app sets none. Anything Spark needs that the
   YAML has no key for — `spark.driver.host`, for instance — has to be solved
   at the host level (name resolution) or by adding it to `build_spark`, which
   is ETL code and was left untouched. See §10.3.

---

## 17. Tests

```bash
MONITORING_CONFIG=config.demo.ini ./venv/bin/python manage.py test etl
```

113 tests covering discovery against the real ETL source, the registry and the
kwargs it builds, validation, credential redaction, step-bar parsing, and — via
a fixture ETL project shaped like the real one — the full run lifecycle,
failure handling, the one-at-a-time rule (including a four-way race), stop, the
environment handed to the ETL process, root-cause reporting, the HTTP API, and
a regression guard on the Monitoring pages.

The runner tests use a fixture rather than the real ETL because the real one
needs PySpark, a Spark master, the Oracle Instant Client and live databases.
Discovery, registry and validation are tested against the real source.

---

## 18. Reference

* `docs/monitoring-app-original-README.md` — the Monitoring application's own
  README, as supplied.
* `docs/skills-course-README.md` — what was in this repository before.
