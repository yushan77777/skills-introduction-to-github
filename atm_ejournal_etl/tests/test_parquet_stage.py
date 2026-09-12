"""Parquet staging: write, validation, read-back and safe cleanup."""

from __future__ import annotations

import os
import shutil
import time

import pytest

from parquet_stage import ParquetStage, ParquetStageError

pytestmark = pytest.mark.spark


@pytest.fixture
def stage(cfg, spark):
    return ParquetStage(cfg, spark)


def _dataframe(spark, rows=5):
    return spark.createDataFrame(
        [(f"ATM{index:03d}", float(index * 1000), f"key{index}") for index in range(rows)],
        schema="ATM_NO string, AMOUNT double, SOURCE_FILE_KEY string")


def test_write_validate_and_read_back(stage, spark):
    path, count = stage.write_batch(_dataframe(spark, 5), "BATCH_0001")

    assert path.endswith(os.path.join("parquet", "batch_0001"))
    assert count == 5
    assert os.path.isfile(os.path.join(path, "_SUCCESS"))
    assert stage.read_batch(path).count() == 5


def test_coalesce_limits_the_number_of_files(stage, spark):
    path, _ = stage.write_batch(_dataframe(spark, 20).repartition(8), "BATCH_0002")
    data_files = [name for name in os.listdir(path) if name.endswith(".parquet")]
    assert len(data_files) <= stage.coalesce_partitions


def test_failed_write_raises_a_stage_error(stage, spark):
    class Exploding:
        @property
        def write(self):
            raise RuntimeError("disk full")

        @property
        def rdd(self):
            raise RuntimeError("disk full")

    with pytest.raises(ParquetStageError, match="parquet write failed"):
        stage.write_batch(Exploding(), "BATCH_0003")


def test_validation_detects_a_missing_commit_marker(stage, spark):
    path, _ = stage.write_batch(_dataframe(spark, 3), "BATCH_0004")
    os.remove(os.path.join(path, "_SUCCESS"))
    with pytest.raises(ParquetStageError, match="_SUCCESS"):
        stage.validate_batch(path, "BATCH_0004")


def test_validation_detects_a_missing_directory(stage):
    with pytest.raises(ParquetStageError, match="missing after write"):
        stage.validate_batch(stage.batch_dir("BATCH_0404"), "BATCH_0404")


def test_cleanup_removes_a_successful_batch(stage, spark):
    path, _ = stage.write_batch(_dataframe(spark, 2), "BATCH_0005")
    assert stage.cleanup_batch("BATCH_0005", success=True) is True
    assert not os.path.exists(path)


def test_failed_batch_parquet_is_kept_for_troubleshooting(stage, spark):
    path, _ = stage.write_batch(_dataframe(spark, 2), "BATCH_0006")
    assert stage.cleanup_batch("BATCH_0006", success=False) is False
    assert os.path.isdir(path)


def test_cleanup_can_be_disabled(cfg, spark):
    cfg._data["parquet"]["PARQUET_CLEANUP_ENABLED"] = False      # noqa: SLF001
    stage = ParquetStage(cfg, spark)
    path, _ = stage.write_batch(_dataframe(spark, 2), "BATCH_0007")
    assert stage.cleanup_batch("BATCH_0007", success=True) is False
    assert os.path.isdir(path)


def test_prepare_clears_a_leftover_directory(stage, spark):
    path, _ = stage.write_batch(_dataframe(spark, 2), "BATCH_0008")
    stage.prepare_for_batch("BATCH_0008")
    assert not os.path.exists(path)


def test_stale_failed_batches_are_swept_by_age(stage, spark):
    path, _ = stage.write_batch(_dataframe(spark, 1), "BATCH_0009")
    keep, _ = stage.write_batch(_dataframe(spark, 1), "BATCH_0010")
    old = time.time() - 30 * 86400
    os.utime(path, (old, old))

    removed = stage.cleanup_stale_batches(keep_batch_ids={"BATCH_0010"})

    assert removed == 1
    assert not os.path.exists(path)
    assert os.path.isdir(keep)


def test_cleanup_error_is_logged_not_raised(stage, spark, monkeypatch, caplog):
    path, _ = stage.write_batch(_dataframe(spark, 1), "BATCH_0011")

    def refuse(target):
        raise OSError("device busy")
    monkeypatch.setattr(shutil, "rmtree", refuse)

    assert stage.cleanup_batch("BATCH_0011", success=True) is False
    assert "could not remove parquet directory" in caplog.text
    assert os.path.isdir(path)
