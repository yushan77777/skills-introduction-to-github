"""Shared pytest fixtures for the ATM E-Journal ETL tests."""

from __future__ import annotations

import importlib
import os
import sys

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(TESTS_DIR)
SRC_DIR = os.path.join(PROJECT_DIR, "src")

for path in (SRC_DIR, TESTS_DIR):
    if path not in sys.path:
        sys.path.insert(0, path)

import fixtures                                                       # noqa: E402


def pytest_configure(config):
    config.addinivalue_line("markers", "spark: test needs a local SparkSession")


@pytest.fixture(scope="session")
def project_dir() -> str:
    return PROJECT_DIR


@pytest.fixture
def etl_home(tmp_path):
    """An isolated project root: config/, ATM_EJOURNALS/, logs/, parquet/, processed/."""
    home = tmp_path / "etl_home"
    (home / "config" / "pickle").mkdir(parents=True)
    (home / "logs").mkdir()
    (home / "parquet").mkdir()
    (home / "processed").mkdir()
    return str(home)


@pytest.fixture
def input_tree(etl_home):
    """Two ATMs with two journal files each (4 files, 2 withdrawals per file)."""
    root = os.path.join(etl_home, "ATM_EJOURNALS")
    files = fixtures.build_input_tree(root, atms=2, files_per_atm=2, transactions=2)
    return {"root": root, "files": files}


@pytest.fixture
def config_path(etl_home, input_tree):
    return fixtures.write_config(etl_home, batch_size=2)


@pytest.fixture
def cfg(config_path):
    from config_loader import load_config
    return load_config(config_path, "atm_ejournal")


@pytest.fixture
def spark():
    """
    A local SparkSession for the Spark-dependent tests.

    Function scoped on purpose: the ETL stops its own session at the end of a
    run, so a session-scoped fixture would hand a dead context to later tests.
    ``getOrCreate`` reuses the live session when there is one and builds a fresh
    one after a stop, which keeps the suite fast without sharing dead state.
    """
    pytest.importorskip("pyspark", reason="pyspark is not installed")
    from pyspark.sql import SparkSession

    session = (SparkSession.builder
               .master("local[2]")
               .appName("atm_ejournal_tests")
               .config("spark.driver.memory", "1g")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "2")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    return session


@pytest.fixture
def reload_modules():
    """Re-import a module after a test has patched its dependencies."""
    def _reload(name: str):
        return importlib.reload(importlib.import_module(name))
    return _reload
