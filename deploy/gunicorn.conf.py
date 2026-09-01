"""Gunicorn configuration for the unified ETL & Monitoring platform.

Run it with:

    gunicorn -c deploy/gunicorn.conf.py monitoring_site.wsgi:application

The one setting that is not a matter of taste is ``workers = 1``. Read the
comment on it before changing it.
"""

import multiprocessing  # noqa: F401  (kept for the note below)
import os

bind = os.environ.get("GUNICORN_BIND", "0.0.0.0:8000")

# ---------------------------------------------------------------------------
# ONE worker, several threads — this is deliberate.
# ---------------------------------------------------------------------------
# The ETL app keeps the live run and the execution history in memory. With more
# than one worker process each would keep its own copy, so whether you saw a
# running job would depend on which worker answered your request.
#
# The "only one ETL at a time" rule itself does NOT depend on this: it is also
# enforced by an exclusive lock file that records the ETL's process group, so a
# second worker (or a second web process entirely) still cannot start a run.
# But the *display* would be inconsistent, so keep a single worker and get
# concurrency from threads instead. One ETL run occupies a thread only for as
# long as it takes to read a line of its output, so a handful is plenty.
workers = 1
threads = int(os.environ.get("GUNICORN_THREADS", "8"))
worker_class = "gthread"

# An ETL run is watched by a background thread, not by a request, so the normal
# request timeout is fine. Keep it generous anyway: the Monitoring app queries
# Greenplum synchronously and a slow scan should not be killed mid-flight.
timeout = int(os.environ.get("GUNICORN_TIMEOUT", "120"))
graceful_timeout = 30
keepalive = 5

# Restarting the web application does NOT stop a running ETL: it is a separate
# process group. The lock file is how the next process discovers it. Preloading
# is off so a reload cannot end up with two copies of the runner in one process.
preload_app = False

accesslog = os.environ.get("GUNICORN_ACCESS_LOG", "-")
errorlog = os.environ.get("GUNICORN_ERROR_LOG", "-")
loglevel = os.environ.get("GUNICORN_LOG_LEVEL", "info")
proc_name = "etl-monitoring"
