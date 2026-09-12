"""The command line entry point: option handling and exit codes."""

from __future__ import annotations

import json
import os

import pytest

import fixtures
import run_etl


class DummySummary:
    def __init__(self, status="SUCCESS", summary_path="/tmp/summary.json"):
        self.status = status
        self.summary_path = summary_path
        self.run_id = "RUN1"
        self.files_discovered = 4
        self.files_processed = 4
        self.files_failed = 0
        self.batches_processed = 2
        self.batches_failed = 0
        self.records_processed = 8
        self.records_loaded = 8


@pytest.fixture
def captured_etl(monkeypatch):
    """Replace the ETL with a recorder so the CLI is tested on its own."""
    seen = {}

    class Recorder:
        def __init__(self, cfg, run_id=None, dry_run=False, max_batches=None):
            seen["cfg"] = cfg
            seen["run_id"] = run_id
            seen["dry_run"] = dry_run
            seen["max_batches"] = max_batches

        def run(self):
            return seen.get("summary", DummySummary())

    monkeypatch.setattr(run_etl, "AtmEjournalEtl", Recorder)
    return seen


def test_exit_code_zero_on_success(config_path, captured_etl, capsys):
    code = run_etl.main(["--config", config_path, "--etl", "atm_ejournal"])
    output = capsys.readouterr().out

    assert code == 0
    assert json.loads(output.split("RUN_SUMMARY_PATH=")[0])["status"] == "SUCCESS"
    assert output.strip().splitlines()[-1] == "RUN_SUMMARY_PATH=/tmp/summary.json"


def test_exit_code_one_on_failure(config_path, captured_etl):
    captured_etl["summary"] = DummySummary(status="FAILED")
    assert run_etl.main(["--config", config_path]) == 1


def test_exit_code_two_on_configuration_error(tmp_path):
    assert run_etl.main(["--config", str(tmp_path / "missing.conf")]) == 2


def test_overrides_reach_the_configuration(config_path, captured_etl, tmp_path):
    other_input = tmp_path / "other_input"
    other_input.mkdir()

    run_etl.main(["--config", config_path, "--batch-size", "7",
                  "--input-path", str(other_input), "--run-id", "MANUAL_1",
                  "--max-batches", "3", "--dry-run"])

    cfg = captured_etl["cfg"]
    assert cfg.get_int("input.BATCH_SIZE") == 7
    assert cfg.get("input.INPUT_PATH") == str(other_input)
    assert captured_etl["run_id"] == "MANUAL_1"
    assert captured_etl["max_batches"] == 3
    assert captured_etl["dry_run"] is True


def test_invalid_batch_size_is_rejected_before_running(config_path, captured_etl):
    assert run_etl.main(["--config", config_path, "--batch-size", "0"]) == 2


def test_list_etls(config_path, capsys):
    assert run_etl.main(["--config", config_path, "--list-etls"]) == 0
    assert capsys.readouterr().out.strip() == "atm_ejournal"
