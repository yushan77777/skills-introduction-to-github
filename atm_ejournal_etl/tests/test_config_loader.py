"""Configuration loading, validation and secret masking."""

from __future__ import annotations

import os

import pytest

from config_loader import ConfigError, EtlConfig, list_etls, load_config


def test_loads_profile_and_merges_defaults(config_path):
    cfg = load_config(config_path, "atm_ejournal")
    assert cfg.etl_name == "atm_ejournal"
    assert cfg.get_int("input.BATCH_SIZE") == 2
    # taken from defaults, not repeated in the profile
    assert cfg.get("greenplum.GREENPLUM_SCHEMA") == "atm"
    # bare keys resolve too
    assert cfg.get("BATCH_SIZE") == 2


def test_paths_are_anchored_on_base_dir(config_path, etl_home):
    cfg = load_config(config_path, "atm_ejournal")
    assert cfg.path("tracking.PROCESSED_FILES_CSV") == os.path.join(
        etl_home, "processed", "processed_files.csv")
    assert os.path.isabs(cfg.path("parquet.PARQUET_PATH"))


def test_missing_configuration_file():
    with pytest.raises(ConfigError, match="not found"):
        load_config("/no/such/config.conf", "atm_ejournal")


def test_invalid_yaml(tmp_path):
    bad = tmp_path / "bad.conf"
    bad.write_text("defaults:\n  input:\n   - [unbalanced\n")
    with pytest.raises(ConfigError, match="not valid YAML"):
        load_config(str(bad), "atm_ejournal")


def test_unknown_etl_profile(config_path):
    with pytest.raises(ConfigError, match="is not defined"):
        load_config(config_path, "does_not_exist")


def test_missing_required_key(config_path):
    import yaml

    with open(config_path) as handle:
        data = yaml.safe_load(handle)
    data["defaults"]["greenplum"].pop("GREENPLUM_TABLE")
    data["etls"]["atm_ejournal"].pop("greenplum", None)
    with open(config_path, "w") as handle:
        yaml.safe_dump(data, handle)
    with pytest.raises(ConfigError, match="missing configuration value"):
        load_config(config_path, "atm_ejournal")


@pytest.mark.parametrize("batch_size", [0, -5])
def test_invalid_batch_size_rejected(config_path, batch_size):
    with pytest.raises(ConfigError, match="BATCH_SIZE must be >= 1"):
        load_config(config_path, "atm_ejournal",
                    overrides={"input.BATCH_SIZE": batch_size})


def test_non_numeric_batch_size_rejected(config_path):
    with pytest.raises(ConfigError, match="must be an integer"):
        load_config(config_path, "atm_ejournal", overrides={"input.BATCH_SIZE": "many"})


def test_invalid_load_strategy_rejected(config_path):
    with pytest.raises(ConfigError, match="GREENPLUM_LOAD_STRATEGY"):
        load_config(config_path, "atm_ejournal",
                    overrides={"greenplum.GREENPLUM_LOAD_STRATEGY": "upsert_maybe"})


def test_invalid_parse_engine_rejected(config_path):
    with pytest.raises(ConfigError, match="PARSE_ENGINE"):
        load_config(config_path, "atm_ejournal",
                    overrides={"parser.PARSE_ENGINE": "sideways"})


def test_invalid_write_chunk_rejected(config_path):
    with pytest.raises(ConfigError, match="PARSE_WRITE_CHUNK_RECORDS"):
        load_config(config_path, "atm_ejournal",
                    overrides={"parser.PARSE_WRITE_CHUNK_RECORDS": 0})


def test_invalid_file_key_mode_rejected(config_path):
    with pytest.raises(ConfigError, match="FILE_KEY_MODE"):
        load_config(config_path, "atm_ejournal",
                    overrides={"tracking.FILE_KEY_MODE": "inode"})


def test_invalid_log_retention_rejected(config_path):
    with pytest.raises(ConfigError, match="LOG_RETENTION_DAYS"):
        load_config(config_path, "atm_ejournal",
                    overrides={"logging.LOG_RETENTION_DAYS": 0})


def test_environment_variables_are_expanded(tmp_path, monkeypatch):
    monkeypatch.setenv("ATM_TEST_INPUT", "/data/journals")
    config = tmp_path / "c.conf"
    config.write_text(
        'project:\n  BASE_DIR: "%s"\n  ETL_NAME: "x"\n'
        'defaults:\n  input:\n    INPUT_PATH: "${ATM_TEST_INPUT}"\n'
        '    FALLBACK: "${NOT_SET:-default_value}"\netls:\n  x: {}\n' % tmp_path)
    cfg = load_config(str(config), "x", validate=False)
    assert cfg.get("input.INPUT_PATH") == "/data/journals"
    assert cfg.get("input.FALLBACK") == "default_value"


def test_secrets_are_masked_in_safe_dump(config_path):
    cfg = load_config(config_path, "atm_ejournal",
                      overrides={"greenplum.GREENPLUM_PASSWORD": "gAAAA-secret-token"})
    dumped = cfg.safe_dump()
    assert dumped["greenplum"]["GREENPLUM_PASSWORD"] == "********"
    assert "gAAAA-secret-token" not in str(dumped)


def test_list_etls(config_path):
    assert list_etls(config_path) == ["atm_ejournal"]


def test_several_profiles_require_a_selection(tmp_path):
    config = tmp_path / "two.conf"
    config.write_text('project: {BASE_DIR: "."}\ndefaults: {}\netls:\n  a: {}\n  b: {}\n')
    with pytest.raises(ConfigError, match="select one with --etl"):
        load_config(str(config), None, validate=False)
    assert list_etls(str(config)) == ["a", "b"]


def test_profile_overrides_defaults(tmp_path):
    config = tmp_path / "p.conf"
    config.write_text(
        'project: {BASE_DIR: ".", ETL_NAME: "second"}\n'
        'defaults:\n  input: {INPUT_PATH: "/a", BATCH_SIZE: 100}\n'
        'etls:\n  first: {}\n'
        '  second:\n    input: {INPUT_PATH: "/b", BATCH_SIZE: 7}\n')
    cfg = load_config(str(config), "second", validate=False)
    assert cfg.get("input.INPUT_PATH") == "/b"
    assert cfg.get_int("input.BATCH_SIZE") == 7
    first = load_config(str(config), "first", validate=False)
    assert first.get("input.INPUT_PATH") == "/a"


def test_get_bool_and_list(cfg):
    assert cfg.get_bool("parser.KEEP_LAST_FAILURE") is True
    assert cfg.get_bool("mail.MAIL_ENABLED") is False
    assert cfg.get_list("input.FILE_PATTERN") == ["*.TXT", "*.txt"]
    with pytest.raises(ConfigError, match="must be a boolean"):
        EtlConfig({"a": {"B": "perhaps"}}, "t", "x").get_bool("a.B")
