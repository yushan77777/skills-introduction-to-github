# The ETL app deliberately defines no models.
#
# Execution history lives in memory (etl.runner.JOBS) plus the per-run log
# files written next to the ETL project, exactly as the master specification
# requires: no new persistent history database is introduced.
