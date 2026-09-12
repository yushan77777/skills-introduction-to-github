"""
The Spark bridge must produce exactly what the existing pandas parser produces.

These tests pin the two properties the enhancement depends on:

* per-file de-duplication on the executors == folder-level de-duplication in
  ``process_all_atms`` (retry chains never span files);
* every field of a parser record survives the mapping into the fixed schema.
"""

from __future__ import annotations

import json
import os
from datetime import datetime

import pytest

import fixtures
from atm_ejournal_parser import (ParseStats, deduplicate_attempts, extract_transaction_blocks,
                                 process_all_atms, read_ejournal_file)
from spark_parser import COLUMN_ORDER, parse_journal_file, record_to_row

pyspark = pytest.importorskip("pyspark", reason="pyspark is not installed")

OPTIONS = {
    "encoding": "latin-1",
    "keep_last_failure": True,
    "link_failed_across_amounts": False,
    "retry_window_seconds": 180,
    "keep_unparsed_records": True,
    "batch_id": "BATCH_0001",
    "run_id": "RUN1",
    "etl_name": "atm_ejournal",
    "load_ts": datetime(2026, 9, 12, 20, 0, 0),
}


def test_row_count_matches_the_pandas_parser(tmp_path):
    root = str(tmp_path / "ATM_EJOURNALS")
    fixtures.build_input_tree(root, atms=2, files_per_atm=2, transactions=3)

    reference = process_all_atms(root, verbose=False)

    total = 0
    for atm in sorted(os.listdir(root)):
        for name in sorted(os.listdir(os.path.join(root, atm))):
            rows, _stats, error = parse_journal_file(os.path.join(root, atm, name), atm,
                                                     f"{atm}/{name}", OPTIONS)
            assert error is None
            total += len(rows)

    assert total == len(reference)


def test_retry_chain_is_collapsed_the_same_way(tmp_path):
    """FAIL/FAIL/SUCCESS on one card collapses to first failure + final success."""
    path = str(tmp_path / "ATM001" / "EJ_RETRY.TXT")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    text = (fixtures.journal_text(transactions=1, status="Failed")
            + fixtures.journal_text(transactions=1, status="Failed")
            + fixtures.journal_text(transactions=1, status="OK"))
    with open(path, "w", encoding="latin-1") as handle:
        handle.write(text)

    stats = ParseStats()
    records = extract_transaction_blocks(read_ejournal_file(path, stats), path, stats)
    for record in records:
        record["ATM_NO"] = "ATM001"
        record["SESSION_ID"] = f"ATM001|{record['SESSION_ID']}"
    reference = deduplicate_attempts(records, ParseStats())

    rows, _stats, error = parse_journal_file(path, "ATM001", "ATM001/EJ_RETRY.TXT", OPTIONS)

    assert error is None
    assert len(rows) == len(reference)
    statuses = [row[COLUMN_ORDER.index("STATUS")] for row in rows]
    assert statuses == [record["STATUS"] for record in reference]


def test_failed_file_is_reported_not_raised(tmp_path):
    missing = str(tmp_path / "not_there.TXT")
    rows, stats, error = parse_journal_file(missing, "ATM001", "k", OPTIONS)
    assert rows == []
    assert stats["files_failed"] == 1
    assert "not_there.TXT" in error


def test_record_to_row_maps_every_column():
    record = {
        "ATM_NO": "ATM001",
        "TRANSACTION_DATETIME": datetime(2026, 9, 10, 6, 42, 30),
        "AMOUNT": 50000.0, "DENOM_AMOUNT": 50000.0, "STATUS": "SUCCESS",
        "DENOM_BREAKDOWN": {5000: 10}, "NOTES_COUNT": 10, "CASH_TAKEN": True,
        "SOURCE_FILE": "EJ.TXT", "SOURCE_LINE": 12, "TXN_SEQ": 1, "PARSE_CONFIDENT": True,
    }
    row = record_to_row(record, "ATM001/EJ.TXT", "BATCH_0001", "RUN1", "atm_ejournal",
                        datetime(2026, 9, 12))
    mapped = dict(zip(COLUMN_ORDER, row))

    assert len(row) == len(COLUMN_ORDER)
    assert mapped["DATE"] == datetime(2026, 9, 10).date()
    assert mapped["TIME"] == "06:42:30"
    assert mapped["DENOM_MATCHES_AMOUNT"] is True
    assert json.loads(mapped["DENOM_BREAKDOWN"]) == {"5000": 10}
    assert mapped["SOURCE_FILE_KEY"] == "ATM001/EJ.TXT"
    assert mapped["BATCH_ID"] == "BATCH_0001"
    assert mapped["ETL_NAME"] == "atm_ejournal"


def test_schema_matches_the_column_order():
    from spark_parser import build_schema

    assert [field.name for field in build_schema().fields] == COLUMN_ORDER


def test_low_confidence_records_can_be_excluded(tmp_path):
    """A withdrawal block with no amount and no outcome is PARSE_CONFIDENT = false."""
    path = str(tmp_path / "EJ_PARTIAL.TXT")
    with open(path, "w", encoding="latin-1") as handle:
        handle.write(
            "[10092026 064230 000][ATM][INF]> ===================== Trx Started ===========\n"
            "[10092026 064231 000][ATM][INF]> -----Card Number : 539157******0717\n"
            "[10092026 064232 000][ATM][INF]> #TRANSACTION-START#\n"
            "[10092026 064235 000][ATM][INF]> -Cash Withdraw Initiated -------------\n"
            "[10092026 064236 000][ATM][INF]> -----Trace ID        : 559198\n"
            "[10092026 064242 000][ATM][INF]> #TRANSACTION-END#\n")

    kept, _stats, _error = parse_journal_file(path, "ATM001", "k", OPTIONS)
    dropped, _stats, _error = parse_journal_file(path, "ATM001", "k",
                                                 dict(OPTIONS, keep_unparsed_records=False))

    assert len(kept) == 1
    assert kept[0][COLUMN_ORDER.index("PARSE_CONFIDENT")] is False
    assert dropped == []


@pytest.mark.spark
def test_build_batch_dataframe_uses_the_fixed_schema(tmp_path, spark, cfg):
    from file_registry import discover_files
    from spark_parser import build_batch_dataframe, build_schema
    from spark_session import ship_python_modules

    # The executors need the parser modules, exactly as the ETL ships them.
    ship_python_modules(spark)

    root = str(tmp_path / "ATM_EJOURNALS")
    fixtures.build_input_tree(root, atms=1, files_per_atm=2, transactions=2)
    files = list(discover_files(root, patterns=["*.TXT"]))

    dataframe, stats, failures = build_batch_dataframe(spark, files, cfg, "BATCH_0001", "RUN1",
                                                       num_partitions=2)
    rows = dataframe.collect()

    assert dataframe.schema == build_schema()
    assert len(rows) == 4
    assert stats.value["files_processed"] == 2
    assert failures.value == []
    assert {row["BATCH_ID"] for row in rows} == {"BATCH_0001"}
