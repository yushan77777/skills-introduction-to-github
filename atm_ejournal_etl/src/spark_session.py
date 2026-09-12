"""
SparkSession construction for the ATM E-Journal ETL.

Every value comes from the ``spark:`` section of the configuration file; the
defaults shipped there are the settings the project already uses (master URL,
connector jars, executor sizing, ``java.io.tmpdir``/timezone options), so the
existing cluster behaviour is preserved.
"""

from __future__ import annotations

import logging
import os
from typing import Iterable, List, Optional

logger = logging.getLogger("atm_ejournal.spark")

#: Python sources shipped to the executors so the parser runs there, not on the
#: driver. Paths are relative to ``src/``.
EXECUTOR_PY_FILES = ("atm_ejournal_parser.py", "spark_parser.py")


def build_spark_session(cfg, extra_conf: Optional[dict] = None):
    """
    Create (or attach to) the SparkSession described by the configuration.

    Returns a ``pyspark.sql.SparkSession``. Import of pyspark is deferred so the
    configuration, tracking and logging modules stay importable - and unit
    testable - on a machine without Spark installed.
    """
    from pyspark.sql import SparkSession                # noqa: PLC0415 - deferred on purpose

    app_name = str(cfg.require("spark.SPARK_APP_NAME"))
    master = str(cfg.require("spark.SPARK_MASTER"))

    builder = (SparkSession.builder
               .appName(app_name)
               .master(master))

    simple_options = {
        "spark.jars": cfg.get("spark.SPARK_JARS"),
        "spark.executor.instances": cfg.get("spark.SPARK_EXECUTOR_INSTANCES"),
        "spark.executor.memory": cfg.get("spark.SPARK_EXECUTOR_MEMORY"),
        "spark.executor.cores": cfg.get("spark.SPARK_EXECUTOR_CORES"),
        "spark.cores.max": cfg.get("spark.SPARK_CORES_MAX"),
        "spark.driver.memory": cfg.get("spark.SPARK_DRIVER_MEMORY"),
        "spark.local.dir": cfg.get("spark.SPARK_LOCAL_DIR"),
        "spark.executor.extraJavaOptions": cfg.get("spark.SPARK_EXECUTOR_JAVA_OPTIONS"),
        "spark.driver.extraJavaOptions": cfg.get("spark.SPARK_DRIVER_JAVA_OPTIONS"),
        "spark.sql.shuffle.partitions": cfg.get("spark.SPARK_SHUFFLE_PARTITIONS"),
    }
    for key, value in simple_options.items():
        if value is not None and str(value).strip() != "":
            builder = builder.config(key, str(value))

    for key, value in (cfg.get("spark.EXTRA_CONF") or {}).items():
        builder = builder.config(str(key), str(value))
    for key, value in (extra_conf or {}).items():
        builder = builder.config(str(key), str(value))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(str(cfg.get("spark.SPARK_LOG_LEVEL", "WARN")))

    logger.info("SparkSession ready | app=%s | master=%s | executors=%s x %s cores x %s | "
                "driver memory=%s",
                app_name, master,
                cfg.get("spark.SPARK_EXECUTOR_INSTANCES"),
                cfg.get("spark.SPARK_EXECUTOR_CORES"),
                cfg.get("spark.SPARK_EXECUTOR_MEMORY"),
                cfg.get("spark.SPARK_DRIVER_MEMORY"))
    logger.debug("Spark application id: %s", spark.sparkContext.applicationId)
    return spark


def ship_python_modules(spark, source_dir: Optional[str] = None,
                        modules: Iterable[str] = EXECUTOR_PY_FILES) -> List[str]:
    """
    Ship the parser modules to the executors.

    The existing parser is reused *as it is* on the executor side, so it has to
    travel with the job unless it is already installed cluster-wide.
    """
    source_dir = source_dir or os.path.dirname(os.path.abspath(__file__))
    shipped: List[str] = []
    for module in modules:
        path = os.path.join(source_dir, module)
        if not os.path.isfile(path):
            logger.warning("module not shipped to executors, file not found: %s", path)
            continue
        try:
            spark.sparkContext.addPyFile(path)
            shipped.append(path)
        except Exception as exc:                       # noqa: BLE001 - already shipped, etc.
            logger.warning("could not ship %s to the executors: %s", module, exc)
    logger.debug("shipped %d module(s) to the executors", len(shipped))
    return shipped


def default_parse_partitions(cfg, file_count: int) -> int:
    """
    Tasks used to parse one batch.

    ``SPARK_PARSE_PARTITIONS`` wins when set; otherwise one task per executor
    core slot, capped at the number of files (an empty task is pure overhead).
    """
    configured = cfg.get_int("spark.SPARK_PARSE_PARTITIONS", 0)
    if configured > 0:
        return max(1, min(configured, max(file_count, 1)))
    instances = cfg.get_int("spark.SPARK_EXECUTOR_INSTANCES", 4)
    cores = cfg.get_int("spark.SPARK_EXECUTOR_CORES", 4)
    slots = max(1, instances * cores)
    return max(1, min(slots, max(file_count, 1)))


def stop_spark_session(spark) -> None:
    """Stop a session, never raising - used in a ``finally`` block."""
    if spark is None:
        return
    try:
        spark.stop()
        logger.info("SparkSession stopped")
    except Exception as exc:                           # noqa: BLE001
        logger.warning("SparkSession did not stop cleanly: %s", exc)
