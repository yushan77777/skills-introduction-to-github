"""
End-to-end orchestration tests.

These run the real pipeline - the existing parser on a local SparkSession, a
real parquet write/read and the real tracking/pending logic - with Greenplum
replaced by :class:`fixtures.FakeGreenplumLoader`, which mimics the target table
and the batch control table.
"""

from __future__ import annotations

import json
import os

import pytest

import fixtures
from config_loader import load_config
from etl_runner import AtmEjournalEtl

pytestmark = pytest.mark.spark


def _run(config_path, run_id, loader=None, **kwargs):
    cfg = load_config(config_path, "atm_ejournal")
    etl = AtmEjournalEtl(cfg, run_id=run_id, **kwargs)
    etl.loader = loader if loader is not None else fixtures.FakeGreenplumLoader()
    summary = etl.run()
    return summary, etl.loader


def _processed_rows(etl_home):
    path = os.path.join(etl_home, "processed", "processed_files.csv")
    if not os.path.exists(path):
        return []
    import csv
    with open(path) as handle:
        return list(csv.DictReader(handle))


# --------------------------------------------------------------------------- #
# Happy paths
# --------------------------------------------------------------------------- #


def test_single_batch(etl_home, spark):
    fixtures.build_input_tree(os.path.join(etl_home, "ATM_EJOURNALS"),
                              atms=1, files_per_atm=2, transactions=2)
    config_path = fixtures.write_config(etl_home, batch_size=10)

    summary, loader = _run(config_path, "RUN1")

    assert summary.status == "SUCCESS"
    assert summary.files_discovered == 2
    assert summary.batches_planned == 1
    assert summary.batches_processed == 1
    assert summary.files_processed == 2
    assert summary.records_processed == 4              # 2 files x 2 withdrawals
    assert summary.records_loaded == 4
    assert len(loader.rows) == 4
    assert len(_processed_rows(etl_home)) == 2


def test_multiple_batches_respect_batch_size(etl_home, spark):
    fixtures.build_input_tree(os.path.join(etl_home, "ATM_EJOURNALS"),
                              atms=2, files_per_atm=2, transactions=1)
    config_path = fixtures.write_config(etl_home, batch_size=2)

    summary, loader = _run(config_path, "RUN1")

    assert summary.batches_planned == 2
    assert summary.batches_processed == 2
    assert [batch["file_count"] for batch in summary.batches] == [2, 2]
    assert loader.load_calls == ["BATCH_0001", "BATCH_0002"]
    assert len(loader.rows) == 4


def test_batch_size_of_one(etl_home, spark):
    fixtures.build_input_tree(os.path.join(etl_home, "ATM_EJOURNALS"),
                              atms=1, files_per_atm=3, transactions=1)
    config_path = fixtures.write_config(etl_home, batch_size=1)

    summary, loader = _run(config_path, "RUN1")

    assert summary.batches_processed == 3
    assert loader.load_calls == ["BATCH_0001", "BATCH_0002", "BATCH_0003"]


def test_empty_input_directory(etl_home, spark):
    os.makedirs(os.path.join(etl_home, "ATM_EJOURNALS"), exist_ok=True)
    config_path = fixtures.write_config(etl_home, batch_size=5)

    summary, loader = _run(config_path, "RUN1")

    assert summary.status == "SUCCESS"
    assert summary.files_discovered == 0
    assert summary.batches_processed == 0
    assert loader.load_calls == []


def test_max_batches_per_run_stops_early(etl_home, spark):
    fixtures.build_input_tree(os.path.join(etl_home, "ATM_EJOURNALS"),
                              atms=1, files_per_atm=4, transactions=1)
    config_path = fixtures.write_config(etl_home, batch_size=1)

    summary, _ = _run(config_path, "RUN1", max_batches=2)

    assert summary.batches_processed == 2
    assert summary.files_processed == 2
    assert len(_processed_rows(etl_home)) == 2


# --------------------------------------------------------------------------- #
# Restart / idempotency
# --------------------------------------------------------------------------- #


def test_second_execution_skips_processed_files(etl_home, spark):
    fixtures.build_input_tree(os.path.join(etl_home, "ATM_EJOURNALS"),
                              atms=1, files_per_atm=2, transactions=1)
    config_path = fixtures.write_config(etl_home, batch_size=1)

    _, loader = _run(config_path, "RUN1")
    summary, loader = _run(config_path, "RUN2", loader=loader)

    assert summary.files_previously_processed == 2
    assert summary.files_to_process == 0
    assert summary.batches_processed == 0
    assert len(loader.rows) == 2                        # nothing re-loaded


def test_failed_batch_leaves_its_files_unprocessed(etl_home, spark):
    fixtures.build_input_tree(os.path.join(etl_home, "ATM_EJOURNALS"),
                              atms=1, files_per_atm=3, transactions=1)
    config_path = fixtures.write_config(etl_home, batch_size=1)
    loader = fixtures.FakeGreenplumLoader(fail_on={"BATCH_0002"})

    summary, loader = _run(config_path, "RUN1", loader=loader)

    assert summary.status == "FAILED"
    assert summary.batches_processed == 2
    assert summary.batches_failed == 1
    assert summary.files_processed == 2
    failed = [batch for batch in summary.batches if batch["status"] == "FAILED"][0]
    assert failed["stage"] == "greenplum_load"
    assert failed["files_marked"] == 0
    # the failed batch's parquet is kept for troubleshooting
    assert os.path.isdir(os.path.join(etl_home, "parquet", "batch_0002"))

    # restart: only the file of the failed batch is processed again
    loader.fail_on = set()
    summary2, loader = _run(config_path, "RUN2", loader=loader)
    assert summary2.status == "SUCCESS"
    assert summary2.files_previously_processed == 2
    assert summary2.files_to_process == 1
    assert summary2.batches_processed == 1
    assert len(loader.rows) == 3
    assert len(_processed_rows(etl_home)) == 3


def test_crash_after_greenplum_commit_is_recovered_without_reloading(etl_home, spark):
    """The hard case: Greenplum committed, the tracking CSV never got written."""
    fixtures.build_input_tree(os.path.join(etl_home, "ATM_EJOURNALS"),
                              atms=1, files_per_atm=1, transactions=2)
    config_path = fixtures.write_config(etl_home, batch_size=1)
    loader = fixtures.FakeGreenplumLoader(crash_after={"BATCH_0001"})

    summary, loader = _run(config_path, "RUN1", loader=loader)

    assert summary.status == "FAILED"
    assert _processed_rows(etl_home) == []                        # nothing marked
    assert os.listdir(os.path.join(etl_home, "processed", "pending")) == ["BATCH_0001.json"]
    assert len(loader.rows) == 2                                  # but the data is in Greenplum

    # next run: the control table says the batch committed -> finish bookkeeping
    loader.crash_after = set()
    summary2, loader = _run(config_path, "RUN2", loader=loader)

    assert summary2.status == "SUCCESS"
    assert summary2.recovered_batches[0]["action"] == "COMPLETED_TRACKING"
    assert len(loader.rows) == 2                                  # no duplicate rows
    assert len(loader.load_calls) == 1                            # no reload
    assert len(_processed_rows(etl_home)) == 1
    assert os.listdir(os.path.join(etl_home, "processed", "pending")) == []


def test_reprocessing_the_same_file_does_not_duplicate_rows(etl_home, spark):
    """delete_insert_by_source_file keeps a forced re-run idempotent."""
    fixtures.build_input_tree(os.path.join(etl_home, "ATM_EJOURNALS"),
                              atms=1, files_per_atm=1, transactions=2)
    config_path = fixtures.write_config(etl_home, batch_size=1)

    _, loader = _run(config_path, "RUN1")
    assert len(loader.rows) == 2

    # simulate a lost tracking CSV (operator error / restore from backup)
    os.remove(os.path.join(etl_home, "processed", "processed_files.csv"))
    summary, loader = _run(config_path, "RUN2", loader=loader)

    assert summary.files_processed == 1
    assert len(loader.rows) == 2                                  # replaced, not appended


# --------------------------------------------------------------------------- #
# Parse failures, dry run, artefacts
# --------------------------------------------------------------------------- #


def test_unreadable_file_is_not_marked_processed(etl_home, spark, monkeypatch):
    root = os.path.join(etl_home, "ATM_EJOURNALS")
    fixtures.build_input_tree(root, atms=1, files_per_atm=2, transactions=1)
    broken = os.path.join(root, "ATM001", "EJOURNAL_10092026_00.TXT")
    config_path = fixtures.write_config(etl_home, batch_size=5)

    import spark_parser
    original = spark_parser.parse_journal_file

    def failing(path, atm_no, file_key, options):
        if path == broken:
            return [], {}, f"{os.path.basename(path)}: OSError: simulated read failure"
        return original(path, atm_no, file_key, options)

    monkeypatch.setattr(spark_parser, "parse_journal_file", failing)

    summary, loader = _run(config_path, "RUN1")

    assert summary.files_failed == 1
    assert summary.files_processed == 1
    processed = _processed_rows(etl_home)
    assert [row["file_name"] for row in processed] == ["EJOURNAL_11092026_00.TXT"]
    assert summary.failed_files[0]["path"] == broken


def test_dry_run_writes_parquet_but_touches_nothing_else(etl_home, spark):
    fixtures.build_input_tree(os.path.join(etl_home, "ATM_EJOURNALS"),
                              atms=1, files_per_atm=1, transactions=1)
    config_path = fixtures.write_config(etl_home, batch_size=5)

    summary, loader = _run(config_path, "RUN1", dry_run=True)

    assert summary.status == "SUCCESS"
    assert loader.load_calls == []
    assert _processed_rows(etl_home) == []
    assert os.path.isdir(os.path.join(etl_home, "parquet", "batch_0001"))


def test_run_artifacts_logs_summary_and_cleanup(etl_home, spark):
    fixtures.build_input_tree(os.path.join(etl_home, "ATM_EJOURNALS"),
                              atms=1, files_per_atm=2, transactions=1)
    config_path = fixtures.write_config(etl_home, batch_size=1)

    summary, _ = _run(config_path, "RUN_ART")

    logs = sorted(os.listdir(os.path.join(etl_home, "logs")))
    assert "atm_ejournal_etl_RUN_ART.log" in logs
    assert sum(1 for name in logs if name.startswith("batch_BATCH_")) == 2

    with open(summary.summary_path) as handle:
        stored = json.load(handle)
    assert stored["status"] == "SUCCESS"
    assert stored["records_loaded"] == 2
    assert stored["parser_stats"]["withdrawal_records_detected"] == 2
    assert os.path.isfile(os.path.join(etl_home, "processed", "run_summary",
                                       "atm_ejournal_latest.json"))

    # parquet of successful batches is cleaned up
    assert os.listdir(os.path.join(etl_home, "parquet")) == []

    run_log = open(os.path.join(etl_home, "logs", "atm_ejournal_etl_RUN_ART.log")).read()
    for expected in ("ATM E-Journal ETL started", "file discovery | total files discovered",
                     "batch started", "parquet write completed", "Greenplum load",
                     "batch completed successfully", "ATM E-Journal ETL finished"):
        assert expected in run_log


def test_parsed_columns_match_the_existing_parser_output(etl_home, spark):
    fixtures.build_input_tree(os.path.join(etl_home, "ATM_EJOURNALS"),
                              atms=1, files_per_atm=1, transactions=1)
    config_path = fixtures.write_config(etl_home, batch_size=5)

    _, loader = _run(config_path, "RUN1")

    row = loader.rows[0]
    assert row["ATM_NO"] == "ATM001"
    assert row["STATUS"] == "SUCCESS"
    assert row["AMOUNT"] == 50000.0
    assert row["DENOMINATION"] == "5000x10"
    assert row["NOTES_COUNT"] == 10
    assert row["DENOM_AMOUNT"] == 50000.0
    assert row["DENOM_MATCHES_AMOUNT"] is True
    assert row["CARD_NO"].startswith("5391")
    assert row["SOURCE_FILE"] == "EJOURNAL_10092026_00.TXT"
    assert row["SOURCE_FILE_KEY"] == os.path.join("ATM001", "EJOURNAL_10092026_00.TXT")
    assert row["BATCH_ID"] == "BATCH_0001"
    assert row["ETL_RUN_ID"] == "RUN1"
    assert json.loads(row["DENOM_BREAKDOWN"]) == {"5000": 10}
