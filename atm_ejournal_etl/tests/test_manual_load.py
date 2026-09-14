"""
The manual runners: batch-wise process + insert, and "load this path".

Greenplum is replaced by :class:`fixtures.FakeGreenplumLoader`; the parsing, the
parquet and the tracking are real.
"""

from __future__ import annotations

import csv
import os

import pytest

import fixtures
from manual_load import (BatchReport, LoadedParquetRegistry, ManualLoadError,
                         find_parquet_targets, load_parquet_path, print_reports,
                         process_path, run_batches, unprocessed_files)

pytest.importorskip("pyarrow", reason="pyarrow is not installed")
pytestmark = pytest.mark.spark


@pytest.fixture
def manual_cfg(etl_home):
    """A project with four journal files and the local parse engine."""
    from config_loader import load_config

    fixtures.build_input_tree(os.path.join(etl_home, "ATM_EJOURNALS"),
                              atms=2, files_per_atm=2, transactions=2)
    path = fixtures.write_config(etl_home, batch_size=2, parse_engine="local")
    return load_config(path, "atm_ejournal")


def _processed(etl_home):
    path = os.path.join(etl_home, "processed", "processed_files.csv")
    if not os.path.exists(path):
        return []
    with open(path) as handle:
        return list(csv.DictReader(handle))


# --------------------------------------------------------------------------- #
# run_batches
# --------------------------------------------------------------------------- #


def test_batches_are_processed_and_inserted(manual_cfg, etl_home, spark):
    loader = fixtures.FakeGreenplumLoader()
    seen = []

    reports = run_batches(manual_cfg, spark=spark, loader=loader, on_batch=seen.append)

    assert [report.batch_id for report in reports] == ["BATCH_0001", "BATCH_0002"]
    assert [report.status for report in reports] == ["SUCCESS", "SUCCESS"]
    assert [report.files for report in reports] == [2, 2]
    assert sum(report.records for report in reports) == 8
    assert sum(report.rows_loaded for report in reports) == 8
    assert sum(report.files_marked for report in reports) == 4
    assert seen == reports                              # progress is reported as it happens

    assert len(loader.rows) == 8
    assert len(_processed(etl_home)) == 4
    assert os.listdir(os.path.join(etl_home, "parquet")) == []      # parquet cleaned up


def test_max_batches_and_batch_size_are_honoured(manual_cfg, etl_home, spark):
    loader = fixtures.FakeGreenplumLoader()

    reports = run_batches(manual_cfg, batch_size=1, max_batches=2, spark=spark, loader=loader)

    assert len(reports) == 2
    assert [report.files for report in reports] == [1, 1]
    assert len(_processed(etl_home)) == 2               # the other two files stay pending
    assert len(list(unprocessed_files(manual_cfg, _registry(manual_cfg)))) == 2


def _registry(cfg):
    from file_registry import ProcessedFileRegistry

    return ProcessedFileRegistry(cfg.path("tracking.PROCESSED_FILES_CSV"),
                                 key_mode=str(cfg.get("tracking.FILE_KEY_MODE", "path")))


def test_a_second_call_continues_where_the_first_stopped(manual_cfg, etl_home, spark):
    loader = fixtures.FakeGreenplumLoader()

    run_batches(manual_cfg, batch_size=2, max_batches=1, spark=spark, loader=loader)
    second = run_batches(manual_cfg, batch_size=2, spark=spark, loader=loader)

    assert len(second) == 1                             # only the remaining batch
    assert second[0].batch_id == "BATCH_0001"           # numbering restarts per call
    assert len(_processed(etl_home)) == 4
    assert len(loader.rows) == 8
    assert run_batches(manual_cfg, spark=spark, loader=loader) == []   # nothing left


def test_a_failed_batch_leaves_its_files_unprocessed(manual_cfg, etl_home, spark):
    loader = fixtures.FakeGreenplumLoader(fail_on={"BATCH_0001"})

    reports = run_batches(manual_cfg, batch_size=2, spark=spark, loader=loader)

    assert [report.status for report in reports] == ["FAILED", "SUCCESS"]
    assert "simulated Greenplum failure" in reports[0].error
    assert reports[0].files_marked == 0
    assert len(_processed(etl_home)) == 2               # only the batch that worked
    assert len(loader.rows) == 4


def test_keep_parquet_files_it_under_the_run_id(manual_cfg, etl_home, spark):
    """Two runs that keep their parquet must not overwrite each other's batch_0001."""
    run_batches(manual_cfg, batch_size=4, run_id="RUN_A", spark=spark,
                loader=fixtures.FakeGreenplumLoader(), keep_parquet=True)
    root = os.path.join(etl_home, "parquet")
    assert os.listdir(root) == ["RUN_A"]
    assert os.listdir(os.path.join(root, "RUN_A")) == ["batch_0001"]


def test_invalid_batch_size(manual_cfg):
    with pytest.raises(ManualLoadError, match="batch_size must be >= 1"):
        run_batches(manual_cfg, batch_size=0)


def test_process_path_loads_a_whole_directory(manual_cfg, etl_home, spark, tmp_path):
    other = tmp_path / "more_journals"
    fixtures.build_input_tree(str(other), atms=1, files_per_atm=3, transactions=1)
    loader = fixtures.FakeGreenplumLoader()

    reports = process_path(manual_cfg, str(other), batch_size=2, spark=spark, loader=loader)

    assert sum(report.files for report in reports) == 3
    assert sum(report.rows_loaded for report in reports) == 3
    assert len(_processed(etl_home)) == 3

    with pytest.raises(ManualLoadError, match="not a directory"):
        process_path(manual_cfg, str(tmp_path / "nope"))


# --------------------------------------------------------------------------- #
# load_parquet_path
# --------------------------------------------------------------------------- #


def test_find_parquet_targets(manual_cfg, etl_home, spark, tmp_path):
    run_batches(manual_cfg, batch_size=2, run_id="RUN_A", spark=spark,
                loader=fixtures.FakeGreenplumLoader(), keep_parquet=True)
    root = os.path.join(etl_home, "parquet", "RUN_A")

    # a folder of parquet directories -> one target each
    assert find_parquet_targets(root) == [os.path.join(root, "batch_0001"),
                                          os.path.join(root, "batch_0002")]
    # a parquet directory -> itself
    assert find_parquet_targets(os.path.join(root, "batch_0001")) == \
        [os.path.join(root, "batch_0001")]
    # a single file -> itself
    one_file = [name for name in os.listdir(os.path.join(root, "batch_0001"))
                if name.endswith(".parquet")][0]
    assert find_parquet_targets(os.path.join(root, "batch_0001", one_file)) == \
        [os.path.join(root, "batch_0001", one_file)]

    with pytest.raises(ManualLoadError, match="path does not exist"):
        find_parquet_targets(str(tmp_path / "missing"))
    (tmp_path / "empty").mkdir()
    with pytest.raises(ManualLoadError, match="no parquet file or directory"):
        find_parquet_targets(str(tmp_path / "empty"))


def test_load_parquet_path_writes_everything_under_the_path(manual_cfg, etl_home, spark):
    run_batches(manual_cfg, batch_size=2, run_id="RUN_A", spark=spark,
                loader=fixtures.FakeGreenplumLoader(), keep_parquet=True)
    target_loader = fixtures.FakeGreenplumLoader()
    seen = []

    reports = load_parquet_path(manual_cfg, os.path.join(etl_home, "parquet", "RUN_A"),
                                spark=spark, loader=target_loader, on_target=seen.append)

    assert len(reports) == 2
    assert [report.status for report in reports] == ["SUCCESS", "SUCCESS"]
    assert sum(report.rows for report in reports) == 8
    assert sum(report.rows_loaded for report in reports) == 8
    assert len(target_loader.rows) == 8
    assert seen == reports


def test_only_new_skips_what_was_already_inserted(manual_cfg, etl_home, spark):
    run_batches(manual_cfg, batch_size=2, run_id="RUN_A", spark=spark,
                loader=fixtures.FakeGreenplumLoader(), keep_parquet=True)
    root = os.path.join(etl_home, "parquet", "RUN_A")
    loader = fixtures.FakeGreenplumLoader()

    first = load_parquet_path(manual_cfg, root, spark=spark, loader=loader)
    second = load_parquet_path(manual_cfg, root, spark=spark, loader=loader)

    assert len(first) == 2
    assert second == []                                 # nothing new to insert
    assert len(loader.rows) == 8                        # not loaded twice

    # only_new=False loads them again
    third = load_parquet_path(manual_cfg, root, only_new=False, spark=spark, loader=loader)
    assert len(third) == 2


def test_a_new_parquet_in_the_folder_is_the_only_one_loaded(manual_cfg, etl_home, tmp_path,
                                                            spark):
    """The 'write the files that were not inserted yet' case."""
    root = str(tmp_path / "collected")
    os.makedirs(root)

    def keep_one_batch(run_id, destination):
        run_batches(manual_cfg, batch_size=2, max_batches=1, run_id=run_id, spark=spark,
                    loader=fixtures.FakeGreenplumLoader(), keep_parquet=True)
        import shutil
        shutil.copytree(os.path.join(etl_home, "parquet", run_id, "batch_0001"),
                        os.path.join(root, destination))

    keep_one_batch("RUN_A", "part_one")
    loader = fixtures.FakeGreenplumLoader()
    load_parquet_path(manual_cfg, root, spark=spark, loader=loader)
    assert len(loader.rows) == 4

    keep_one_batch("RUN_B", "part_two")                 # a new parquet arrives
    reports = load_parquet_path(manual_cfg, root, spark=spark, loader=loader)

    assert len(reports) == 1                            # only the one not inserted yet
    assert reports[0].parquet_path.endswith("part_two")
    assert len(loader.rows) == 8


def test_loaded_parquet_registry_records_what_was_written(manual_cfg, etl_home, spark):
    run_batches(manual_cfg, batch_size=4, run_id="RUN_A", spark=spark,
                loader=fixtures.FakeGreenplumLoader(), keep_parquet=True)
    load_parquet_path(manual_cfg, os.path.join(etl_home, "parquet", "RUN_A"), spark=spark,
                      loader=fixtures.FakeGreenplumLoader())

    csv_path = os.path.join(etl_home, "processed", "loaded_parquet.csv")
    with open(csv_path) as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["rows_loaded"] == "8"
    assert rows[0]["parquet_path"].endswith("batch_0001")
    assert LoadedParquetRegistry(csv_path).contains(rows[0]["parquet_path"])


def test_a_failing_target_is_reported_and_not_recorded(manual_cfg, etl_home, spark):
    run_batches(manual_cfg, batch_size=4, run_id="RUN_A", spark=spark,
                loader=fixtures.FakeGreenplumLoader(), keep_parquet=True)
    loader = fixtures.FakeGreenplumLoader(fail_on={"PATH_0001"})

    reports = load_parquet_path(manual_cfg, os.path.join(etl_home, "parquet", "RUN_A"),
                                spark=spark, loader=loader)

    assert reports[0].status == "FAILED"
    assert "simulated Greenplum failure" in reports[0].error
    assert not os.path.exists(os.path.join(etl_home, "processed", "loaded_parquet.csv"))


def test_print_reports_handles_both_shapes(capsys):
    print_reports([])
    assert "nothing to do" in capsys.readouterr().out

    print_reports([BatchReport(batch_id="BATCH_0001", files=2, records=4, rows_loaded=4,
                               files_marked=2, duration_seconds=1.5)])
    output = capsys.readouterr().out
    assert "BATCH_0001" in output and "TOTAL" in output


def test_two_calls_in_the_same_second_get_different_run_ids(manual_cfg, etl_home, spark):
    """
    Rows are keyed by ETL_RUN_ID + BATCH_ID and a repeated batch of one run is
    treated as a retry, so two quick calls must not share a run id - otherwise
    the second would delete the first one's rows.
    """
    from manual_load import new_run_id

    assert new_run_id() != new_run_id()

    loader = fixtures.FakeGreenplumLoader()
    run_batches(manual_cfg, batch_size=2, max_batches=1, spark=spark, loader=loader)
    run_batches(manual_cfg, batch_size=2, max_batches=1, spark=spark, loader=loader)

    assert len(loader.rows) == 8                        # both batches survived
    assert len({row["ETL_RUN_ID"] for row in loader.rows}) == 2


def test_path_load_stamps_this_load_s_ids(manual_cfg, etl_home, spark):
    """
    Without the stamp the rows would still carry the ids they were parsed under,
    and the control row, the row count check and the retry guard would all miss
    them.
    """
    run_batches(manual_cfg, batch_size=4, run_id="PARSE_RUN", spark=spark,
                loader=fixtures.FakeGreenplumLoader(), keep_parquet=True)
    loader = fixtures.FakeGreenplumLoader()

    reports = load_parquet_path(manual_cfg, os.path.join(etl_home, "parquet", "PARSE_RUN"),
                                run_id="LOAD_RUN", spark=spark, loader=loader)

    assert reports[0].rows == 8
    assert reports[0].rows_loaded == 8                  # verified, not silently zero
    assert {row["ETL_RUN_ID"] for row in loader.rows} == {"LOAD_RUN"}
    assert {row["BATCH_ID"] for row in loader.rows} == {"PATH_0001"}
    assert loader.is_batch_committed("LOAD_RUN", "PATH_0001") == 8


def test_path_load_can_keep_the_original_ids(manual_cfg, etl_home, spark):
    run_batches(manual_cfg, batch_size=4, run_id="PARSE_RUN", spark=spark,
                loader=fixtures.FakeGreenplumLoader(), keep_parquet=True)
    loader = fixtures.FakeGreenplumLoader()

    load_parquet_path(manual_cfg, os.path.join(etl_home, "parquet", "PARSE_RUN"),
                      run_id="LOAD_RUN", stamp_ids=False, spark=spark, loader=loader)

    assert {row["ETL_RUN_ID"] for row in loader.rows} == {"PARSE_RUN"}
