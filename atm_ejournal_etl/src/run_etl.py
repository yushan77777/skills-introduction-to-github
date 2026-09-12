#!/usr/bin/env python3
"""
Command line entry point for the ATM E-Journal ETL.

    python3 src/run_etl.py --config config/atm_ejournal.conf --etl atm_ejournal

Options only *override* the configuration file; nothing environment specific has
a default here. The process exits 0 when every batch succeeded and 1 otherwise,
which is what the Airflow task reacts to. The last stdout line is
``RUN_SUMMARY_PATH=<path>`` so the DAG can pick the metrics up from XCom.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config_loader import ConfigError, list_etls, load_config          # noqa: E402
from etl_runner import AtmEjournalEtl                                  # noqa: E402


def parse_arguments(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ATM E-Journal ETL (batch -> parquet -> Greenplum)")
    parser.add_argument("--config", default=None,
                        help="configuration file (default: $ATM_ETL_CONFIG or "
                             "config/atm_ejournal.conf)")
    parser.add_argument("--etl", default=None,
                        help="ETL profile under 'etls:' (default: $ATM_ETL_NAME or project.ETL_NAME)")
    parser.add_argument("--run-id", default=None, help="override the generated run id")
    parser.add_argument("--batch-size", type=int, default=None,
                        help="override input.BATCH_SIZE for this run")
    parser.add_argument("--input-path", default=None, help="override input.INPUT_PATH")
    parser.add_argument("--max-batches", type=int, default=None,
                        help="stop after N batches (0 = all)")
    parser.add_argument("--dry-run", action="store_true",
                        help="parse and write parquet only; no Greenplum load and no "
                             "processed-file tracking")
    parser.add_argument("--list-etls", action="store_true",
                        help="print the ETL profiles defined in the configuration and exit")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    options = parse_arguments(argv)

    try:
        if options.list_etls:
            for name in list_etls(options.config or os.environ.get("ATM_ETL_CONFIG")
                                  or _default_config()):
                print(name)
            return 0

        overrides = {}
        if options.batch_size is not None:
            overrides["input.BATCH_SIZE"] = options.batch_size
        if options.input_path:
            overrides["input.INPUT_PATH"] = options.input_path

        cfg = load_config(options.config, options.etl, overrides=overrides)
    except ConfigError as exc:
        print(f"CONFIGURATION ERROR: {exc}", file=sys.stderr)
        return 2

    etl = AtmEjournalEtl(cfg, run_id=options.run_id, dry_run=options.dry_run,
                         max_batches=options.max_batches)
    summary = etl.run()

    print(json.dumps({
        "status": summary.status,
        "run_id": summary.run_id,
        "files_discovered": summary.files_discovered,
        "files_processed": summary.files_processed,
        "files_failed": summary.files_failed,
        "batches_processed": summary.batches_processed,
        "batches_failed": summary.batches_failed,
        "records_processed": summary.records_processed,
        "records_loaded": summary.records_loaded,
    }, indent=2))
    # Last line: consumed by the Airflow DAG through XCom.
    print(f"RUN_SUMMARY_PATH={summary.summary_path}")
    return 0 if summary.status == "SUCCESS" else 1


def _default_config() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.normpath(os.path.join(here, "..", "config", "atm_ejournal.conf"))


if __name__ == "__main__":
    sys.exit(main())
