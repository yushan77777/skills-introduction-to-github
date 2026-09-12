"""File discovery, batching, processed-file tracking and pending markers."""

from __future__ import annotations

import csv
import os
import time
from datetime import datetime

import pytest

import fixtures
from file_registry import (PROCESSED_CSV_COLUMNS, DiscoveredFile, PendingBatchStore,
                           ProcessedFileRegistry, derive_atm_no, discover_files,
                           format_batch_id, iter_batches)


def _files(root, **kwargs):
    return list(discover_files(root, patterns=["*.TXT"], **kwargs))


# --------------------------------------------------------------------------- #
# Discovery
# --------------------------------------------------------------------------- #


def test_empty_input_directory(tmp_path):
    empty = tmp_path / "empty"
    empty.mkdir()
    assert _files(str(empty)) == []
    assert list(iter_batches(iter([]), 500)) == []


def test_missing_input_directory(tmp_path):
    with pytest.raises(NotADirectoryError):
        _files(str(tmp_path / "nope"))


def test_discovery_finds_files_with_atm_number(tmp_path):
    root = str(tmp_path / "ATM_EJOURNALS")
    fixtures.build_input_tree(root, atms=2, files_per_atm=2)
    found = _files(root)
    assert len(found) == 4
    assert {item.atm_no for item in found} == {"ATM001", "ATM002"}
    assert all(item.size > 0 for item in found)
    assert found[0].relative_path == os.path.join("ATM001", "EJOURNAL_10092026_00.TXT")


def test_discovery_is_lazy(tmp_path):
    root = str(tmp_path / "ATM_EJOURNALS")
    fixtures.build_input_tree(root, atms=1, files_per_atm=3)
    stream = discover_files(root, patterns=["*.TXT"])
    assert isinstance(next(stream), DiscoveredFile)        # no full listing built


def test_file_pattern_filters(tmp_path):
    root = tmp_path / "in"
    fixtures.write_journal(str(root / "ATM001" / "EJOURNAL_1.TXT"))
    (root / "ATM001" / "notes.csv").write_text("x")
    (root / "ATM001" / ".hidden.TXT").write_text("x")
    assert len(_files(str(root))) == 1


def test_min_file_age_skips_fresh_files(tmp_path):
    root = tmp_path / "in"
    fixtures.write_journal(str(root / "ATM001" / "EJOURNAL_1.TXT"))
    assert _files(str(root), min_file_age_seconds=3600) == []
    assert len(_files(str(root), min_file_age_seconds=0)) == 1


def test_derive_atm_no_for_flat_directory(tmp_path):
    assert derive_atm_no("EJ_1.TXT", "/data/ATM_EJOURNALS") == "ATM_EJOURNALS"
    assert derive_atm_no(os.path.join("ATM007", "EJ_1.TXT"), "/data/x") == "ATM007"


# --------------------------------------------------------------------------- #
# Batching
# --------------------------------------------------------------------------- #


def _fake_files(count):
    return [DiscoveredFile(path=f"/in/f{i}.TXT", relative_path=f"f{i}.TXT", atm_no="A",
                           size=1, mtime=0.0) for i in range(count)]


@pytest.mark.parametrize("total,size,expected", [
    (0, 500, []),
    (1, 500, [1]),
    (500, 500, [500]),
    (10, 3, [3, 3, 3, 1]),
    (3, 1, [1, 1, 1]),
    (10000, 500, [500] * 20),
])
def test_batch_sizes(total, size, expected):
    batches = list(iter_batches(iter(_fake_files(total)), size))
    assert [len(batch) for batch in batches] == expected


@pytest.mark.parametrize("size", [0, -1])
def test_invalid_batch_size(size):
    with pytest.raises(ValueError, match="batch_size must be >= 1"):
        list(iter_batches(iter(_fake_files(3)), size))


def test_batch_id_format():
    assert format_batch_id(1) == "BATCH_0001"
    assert format_batch_id(20) == "BATCH_0020"


# --------------------------------------------------------------------------- #
# Processed-file tracking
# --------------------------------------------------------------------------- #


def test_first_execution_has_no_processed_files(tmp_path):
    registry = ProcessedFileRegistry(str(tmp_path / "processed" / "processed_files.csv"))
    assert registry.load_keys() == set()


def test_append_then_second_execution_skips(tmp_path):
    csv_path = str(tmp_path / "processed_files.csv")
    registry = ProcessedFileRegistry(csv_path)
    files = _fake_files(3)
    assert registry.append_batch(files, "BATCH_0001", "RUN1", records=9) == 3

    reread = ProcessedFileRegistry(csv_path)
    assert reread.load_keys() == {"f0.TXT", "f1.TXT", "f2.TXT"}
    assert reread.contains(files[0]) is True


def test_csv_is_append_only(tmp_path):
    csv_path = str(tmp_path / "processed_files.csv")
    registry = ProcessedFileRegistry(csv_path)
    registry.append_batch(_fake_files(2), "BATCH_0001", "RUN1")
    first_pass = open(csv_path).read()

    registry.append_batch(_fake_files(4)[2:], "BATCH_0002", "RUN1")
    second_pass = open(csv_path).read()

    assert second_pass.startswith(first_pass)           # nothing rewritten
    with open(csv_path) as handle:
        rows = list(csv.DictReader(handle))
    assert [row["batch_id"] for row in rows] == ["BATCH_0001"] * 2 + ["BATCH_0002"] * 2
    assert list(rows[0].keys()) == PROCESSED_CSV_COLUMNS
    assert rows[0]["status"] == "SUCCESS"


def test_mixed_processed_and_unprocessed(tmp_path):
    root = tmp_path / "in"
    fixtures.build_input_tree(str(root), atms=1, files_per_atm=3)
    discovered = _files(str(root))
    registry = ProcessedFileRegistry(str(tmp_path / "processed_files.csv"))
    registry.append_batch(discovered[:2], "BATCH_0001", "RUN1")

    processed = registry.load_keys(refresh=True)
    remaining = [item for item in discovered if item.key() not in processed]
    assert len(remaining) == 1
    assert remaining[0].relative_path == discovered[2].relative_path


def test_duplicate_entries_are_collapsed(tmp_path):
    csv_path = str(tmp_path / "processed_files.csv")
    registry = ProcessedFileRegistry(csv_path)
    files = _fake_files(2)
    registry.append_batch(files, "BATCH_0001", "RUN1")
    registry.append_batch(files, "BATCH_0002", "RUN2")       # same files again
    assert len(ProcessedFileRegistry(csv_path).load_keys()) == 2


def test_malformed_rows_do_not_break_the_run(tmp_path):
    csv_path = tmp_path / "processed_files.csv"
    registry = ProcessedFileRegistry(str(csv_path))
    registry.append_batch(_fake_files(1), "BATCH_0001", "RUN1")
    with open(csv_path, "a") as handle:                      # truncated line from a crash
        handle.write("broken,row\n,,,,\n")
    assert ProcessedFileRegistry(str(csv_path)).load_keys() == {"f0.TXT"}


def test_non_success_rows_are_not_treated_as_processed(tmp_path):
    csv_path = str(tmp_path / "processed_files.csv")
    registry = ProcessedFileRegistry(csv_path)
    registry.append_batch(_fake_files(1), "BATCH_0001", "RUN1", status="FAILED")
    assert ProcessedFileRegistry(csv_path).load_keys() == set()


def test_key_modes_react_to_a_changed_file(tmp_path):
    item = DiscoveredFile(path="/in/a.TXT", relative_path="a.TXT", atm_no="A",
                          size=10, mtime=1000.0)
    changed = DiscoveredFile(path="/in/a.TXT", relative_path="a.TXT", atm_no="A",
                             size=20, mtime=2000.0)
    assert item.key("path") == changed.key("path")
    assert item.key("path_size") != changed.key("path_size")
    assert item.key("path_mtime") != changed.key("path_mtime")


# --------------------------------------------------------------------------- #
# Pending markers
# --------------------------------------------------------------------------- #


def test_pending_marker_round_trip(tmp_path):
    store = PendingBatchStore(str(tmp_path / "pending"))
    files = _fake_files(2)
    store.write("BATCH_0001", "RUN1", files, "/parquet/batch_0001", record_count=42)

    pending = store.list_pending()
    assert len(pending) == 1
    assert pending[0]["batch_id"] == "BATCH_0001"
    assert pending[0]["record_count"] == 42
    assert [item.relative_path for item in PendingBatchStore.files_from_marker(pending[0])] == \
        ["f0.TXT", "f1.TXT"]

    store.remove("BATCH_0001")
    assert store.list_pending() == []
    store.remove("BATCH_0001")                              # idempotent


def test_unreadable_pending_marker_is_reported_not_fatal(tmp_path, caplog):
    pending_dir = tmp_path / "pending"
    pending_dir.mkdir()
    (pending_dir / "BATCH_0009.json").write_text("{not json")
    store = PendingBatchStore(str(pending_dir))
    assert store.list_pending() == []
    assert "unreadable pending marker" in caplog.text
