#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from tqdm.auto import tqdm
import pandas as pd
import yaml
import os
import glob
import shutil
import logging
from pathlib import Path
from dataclasses import dataclass
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from datetime import datetime, timedelta
import subprocess
import sys
import csv
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pyarrow.parquet as pq

current_dir = os.getcwd()

# Get parent directory (v1)
code_directory = os.path.abspath(os.path.join(current_dir))
os.chdir(code_directory)
sys.path.append(code_directory)


# # your project imports
# code_directory = "/analyticsShare/yushan/04.daily_use/5.all_etl/v1/"
# # Change the directory to the original directory
# os.chdir(code_directory)
# sys.path.append(code_directory)
from src.utils.encrypt_module import Encrypter
from src.utils.greenplum_connect import greenplum as gp
from src.utils.oracle_connect import oracle

class StepBar:
    """Simple step progress bar: moves 1 tick per ETL stage."""
    def __init__(self, etl_name: str, steps: list[str]):
        self.steps = steps
        self.pbar = tqdm(total=len(steps), desc=etl_name, unit="step", leave=True)

    def next(self, step_name: str):
        self.pbar.set_postfix_str(step_name)
        self.pbar.update(1)
    def close(self):
        self.pbar.close()
logger=None
def __logger(severity, msg):
    import inspect 
    function_name = inspect.currentframe().f_back.f_code.co_name
    if logger is not None:
        logger_method = getattr(logger, severity.lower(), None)
        if logger_method:
            logger_method(f'[Class funcName: {function_name}] - {msg}')
        else:
            print(f'Invalid severity level: {severity}')
    else:
        print(f'{datetime.now()}-[{severity}] : {msg}')

def load_yaml(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def build_spark(cfg: dict, app_name: str = "Yushan ETL") -> SparkSession:
    __logger("INFO", "Staring the Spark")
    sp = cfg["spark_properties"]
    java_opts = "-Duser.timezone=UTC -Djava.io.tmpdir=/data/spark-4.0.0-bin-hadoop3/tmp/ -Doracle.jdbc.timezoneAsRegion=false"
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", sp["jars"])
        .config("spark.executor.instances", sp["executor_instances"])
        .config("spark.executor.memory", sp["executor_memory"])
        .config("spark.executor.cores", sp["executor_cores"])
        .config("spark.cores.max", sp["cores_max"])
        .config("spark.driver.memory", sp["driver_memory"])
        .config("spark.network.timeout", 600)
        .config("spark.memory.offHeap.size","5g")
        .config("spark.executor.heartbeatInterval",60)
        .config("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=/data/spark-4.0.0-bin-hadoop3/tmp/")
        .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=/data/spark-4.0.0-bin-hadoop3/tmp/")
        .config("spark.driver.extraJavaOptions", java_opts)
        .config("spark.executor.extraJavaOptions", java_opts)
        .master(sp["master_url"])
        .getOrCreate()
    )
    __logger("INFO", "Ending the Spark config")

def sanitize_null_bytes(df):
    """Lowercase column names + remove null bytes from string cols."""
    df = df.toDF(*[c.lower() for c in df.columns])
    string_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() == "string"]
    for c in string_cols:
        df = df.withColumn(c, regexp_replace(col(c), "\u0000", ""))
    return df
    __logger("INFO", "sanitize_null_bytes")

def read_oracle_df(spark, cfg: dict, oracle_config_name: str, query: str, _column: str ):
    oc = cfg[oracle_config_name]
    enc = Encrypter(oc["pickle"])

    # if oracle_target not in configs:
    #      raise KeyError(
    #     f"Unknown oracle_target '{oracle_target}'. "
    #     f"Valid options: {', '.join(configs.keys())}"
    #     )
    # occ = dict(configs[oracle_target])  

    # base_sql_cnt=f"SELECT count(*) VCOUNT FROM ({query.rstrip(';')}) subq"
    # with oracle(config = occ, logger = None) as ORA:
    #     results = ORA.run_sql(base_sql_cnt.rstrip(';'))

    number_of_partitions=20
    partition_column=_column
    preds = [f"MOD(ORA_HASH({partition_column}), {number_of_partitions}) = {i}" for i in range(number_of_partitions)]
    props = {
    'user'    : oc["user"],
    'password': enc.get_decrypt_data(oc["password"]),
    'driver'  : oc["driver"],
    "oracle.jdbc.timezoneAsRegion":  "false",}
    __logger("INFO", "Start Read from Spark")
    df = spark.read.jdbc(url=oc["url"],table=f"({query}) subq", predicates=preds, properties=props)


    #vcount = results.get("VCOUNT")[0]
    
    # df = (
    #     spark.read.format("jdbc")
    #     .option("url", oc["url"])
    #     .option("dbtable",query)
    #     .option("user", oc["user"])
    #     .option("password", enc.get_decrypt_data(oc["password"]))
    #     .option("driver", oc["driver"])
    #     .option("oracle.jdbc.timezoneAsRegion", "false")
    #     .option("predicates", preds)
    #     .load()
    # )
    return sanitize_null_bytes(df)

def write_single_csv(df, tmp_dir: str, final_csv: str, mode: str = "overwrite", header: bool = True):

    """
    Spark writes into folder with part-*.csv. This makes it a single final file.
    """
    tmp_dir = Path(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    (df.coalesce(1)
       .write
       .option("header", header)
       .mode(mode)
       .csv(str(tmp_dir)))

    part_files = list(tmp_dir.glob("part-*.csv"))
    if not part_files:
        raise RuntimeError(f"No part-*.csv found in {tmp_dir}")

    Path(final_csv).parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(part_files[0]), final_csv)

    # cleanup leftover files (_SUCCESS etc.)
    for p in tmp_dir.iterdir():
        if p.is_file() and not p.name.endswith(".csv"):
            p.unlink(missing_ok=True)


    final_csv_folder = tmp_dir.parent
    final_csv_folder.mkdir(parents=True, exist_ok=True)
    print(final_csv_folder)

    for file in os.listdir(final_csv_folder):
        file_path = os.path.join(final_csv_folder, file)

    # Skip the final file we just moved/created
        if file_path == final_csv:
            continue

        if not file.endswith(".csv") or file.startswith("part-00000"):
            if os.path.isdir(file_path):
                shutil.rmtree(file_path) # Deletes directories like .ipynb_checkpoints
            else:
                os.remove(file_path)     # Deletes files like _SUCCESS

def write_single_parquet(df, tmp_dir: str, final_parquet: str, mode: str = "overwrite"):
    """
    Spark writes into folder with part-*.parquet. This makes it a single final file.
    """
    tmp_dir = Path(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    (df.coalesce(1)
       .write
       .mode(mode)
       .parquet(str(tmp_dir)))

    part_files = list(tmp_dir.glob("part-*.parquet"))
    if not part_files:
        raise RuntimeError(f"No part-*.parquet found in {tmp_dir}")

    Path(final_parquet).parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(part_files[0]), final_parquet)
    shutil.rmtree(str(tmp_dir), ignore_errors=True)


# ----------------------------
# Main dispatcher
# ----------------------------
def run_etl(
    etl_name: str,
    base_dir: str,
    oracle_config_name: str = None,
    query: str = None,
    csv_input: str = None,
    pqt_input: str = None,
    parq_name: str = None,
    output_path: str = None,
    tmp_dir: str = None,
    _column: str = None,
    p_column= None,
    lower_bound= None,
    upper_bound= None,
    gp_url: str = None,
    gp_table: str = None,
    gp_user: str = "gpadmin",
    gp_password: str = "",
    gp_server_port: str = "32768-42768",
    gp_mode: str = "overwrite",
    gp_truncate: str = "true",

    oracle_jdbc_url: str = None,
    oracle_table: str = None,
    oracle_user: str = None,
    oracle_password: str = None,
    oracle_driver: str = "oracle.jdbc.driver.OracleDriver",
    oracle_mode: str = "overwrite",
    gp_config_key: str = None,
    compression: str = "snappy",
    oracle_target: str = None
):
    etl_name = etl_name.lower().strip()

    # Define step lists per ETL (normal progress bar)
    STEP_MAP = {
        "oracle_to_csv": ["Build Spark", "Read Oracle", "Clean Columns", "Write CSV", "Finalize"],
        "oracle_to_parquet": ["Build Spark", "Read Oracle", "Clean Columns", "Write Parquet", "Finalize"],
        "oracle_to_greenplum": ["Build Spark", "Read Oracle", "Clean Columns", "Write Greenplum", "Finalize"],
        "csv_to_greenplum": ["Build Spark", "Copy to Greenplum", "Finalize"],
        "parquet_to_greenplum": ["Load CSV (Pandas)", "Copy to Greenplum", "Finalize"],
        "csv_to_oracle": ["Build Spark", "Read CSV", "Write Oracle", "Finalize"],
        "greenplum_read_to_parquet": ["Build Spark", "Read Greenplum", "Write Parquet", "Finalize"],
    }

    if etl_name not in STEP_MAP:
        raise ValueError(f"Unknown etl_name={etl_name}. Supported: {list(STEP_MAP.keys())}")

    bar = StepBar(etl_name, STEP_MAP[etl_name])



    # Load configs once
    cfg = load_yaml("config/config.yaml")
    #cfg_tables = load_yaml("config/etl_table_config.yaml")

    spark = None
    try:
        # Spark only needed for Spark-based ETLs
        if etl_name not in ["csv_to_greenplum1"]:
            bar.next("Build Spark")
            spark = build_spark(cfg, app_name=f"Yushan ETL - {etl_name}")

        # ---------------- ORACLE -> CSV ----------------
        if etl_name == "oracle_to_csv":
            if not (oracle_config_name and query and output_path and tmp_dir):
                raise ValueError("oracle_to_csv requires: oracle_config_name, query, output_path, tmp_dir")

            bar.next("Read Oracle")
            df = read_oracle_df(spark, cfg, oracle_config_name, query,_column)

            bar.next("Clean Columns")
            # read_oracle_df already sanitizes in our earlier design, so this can be optional
            df = sanitize_null_bytes(df)

            bar.next("Write CSV")
            write_single_csv(df, tmp_dir=tmp_dir, final_csv=output_path, mode="overwrite")

            bar.next("Finalize")
            return {"status": "ok", "etl": etl_name, "output": output_path}

        # ---------------- ORACLE -> PARQUET ----------------
        if etl_name == "oracle_to_parquet":
            if not (oracle_config_name and query and output_path and tmp_dir):
                raise ValueError("oracle_to_parquet requires: oracle_config_name, query, output_path, tmp_dir")

            bar.next("Read Oracle")
            df = read_oracle_df(spark, cfg, oracle_config_name, query,_column)

            bar.next("Clean Columns")
            df = sanitize_null_bytes(df)

            bar.next("Write Parquet")
            write_single_parquet(df, tmp_dir=tmp_dir, final_parquet=output_path, mode="overwrite")

            bar.next("Finalize")
            return {"status": "ok", "etl": etl_name, "output": output_path}

        # ---------------- ORACLE -> GREENPLUM ----------------
        if etl_name == "oracle_to_greenplum":
            if not (oracle_config_name and query and gp_url and gp_table):
                raise ValueError("oracle_to_greenplum requires: oracle_config_name, query, gp_url, gp_table")
            __logger("INFO", "Start oracle to greenplum funtion")
            bar.next("Read Oracle")
            df = read_oracle_df(spark, cfg, oracle_config_name, query,_column)


            bar.next("Clean Columns")
            df = sanitize_null_bytes(df)

            bar.next("Write Greenplum")
            gp_props = {
                "user": gp_user,
                "password": gp_password,
                "driver": "org.postgresql.Driver",
                "truncate": gp_truncate,
            }

            # batch_size = 100000
            
            # df_num = (
            #     df.withColumn(
            #         "_rn",
            #         F.row_number().over(
            #             Window.orderBy(F.monotonically_increasing_id())
            #         )
            #     )
            # )
            # total_rows = df_num.count()
            
            __logger("INFO", "Write to the Greenplum")


            # for i, start in enumerate(range(1, total_rows + 1, batch_size)):
            #     end = start + batch_size - 1
                
            #     write_mode = "overwrite" if i == 0 else "append"
            #     (df_num.filter((F.col("_rn") >= start) & (F.col("_rn") <= end))
            #         .drop("_rn")
            #         .write
            #         .format("greenplum")
            #         .option("url", gp_url)
            #         .option("server.port", gp_server_port)
            #         .option("dbtable", gp_table)
            #         .option("segment.num", "16")
            #         .option("numWriteTasks", "32")
            #         .option("gpfdist.sessions", "32")
            #         .option("compression", "gzip")
            #         .mode(write_mode)
            #         .options(**gp_props)
            #         .save())
                
            #    __logger("INFO",f"Written rows {end}")
                
                
            (df.write
                    .format("greenplum")
                    .option("url", gp_url)
                    .option("server.port", gp_server_port)
                    .option("dbtable", gp_table)
                    .option("segment.num", "16")
                    .option("numWriteTasks", "32")
                    .option("gpfdist.sessions", "32")
                    .option("compression", "gzip")
                    .mode(gp_mode)
                    .options(**gp_props)
                    .save())
                
            __logger("INFO", "Write to the Greenplum end")

            bar.next("Finalize")
            return {"status": "ok", "etl": etl_name, "target": gp_table}
            __logger("INFO", "End oracle to greenplum funtion")

        # ---------------- CSV -> GREENPLUM (Pandas + copy) ----------------
        if etl_name == "csv_to_greenplum":
            if not (csv_input and gp_table and gp_config_key):
                raise ValueError("csv_to_greenplum requires: csv_input, gp_table, gp_config_key")
            
            
            bar.next("Read CSV")
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_input)

            df = sanitize_null_bytes(df)

            bar.next("Write Greenplum")
            gp_props = {
                "user": gp_user,
                "password": gp_password,
                "driver": "org.postgresql.Driver",
                "truncate": gp_truncate,
            }
            (df.write
                    .format("greenplum")
                    .option("url", gp_url)
                    .option("server.port", gp_server_port)
                    .option("dbtable", gp_table)
                    .option("segment.num", "16")
                    .option("numWriteTasks", "32")
                    .option("gpfdist.sessions", "32")
                    .option("compression", "gzip")
                    .mode(gp_mode)
                    .options(**gp_props)
                    .save())
                

            # bar.next("Load CSV (Pandas)")
            # pdf = pd.read_csv(csv_input,low_memory=False,quoting=csv.QUOTE_ALL)
            # #pdf = pdf.replace({np.nan: None})
            # pdf = pdf.fillna('')
            
            # bar.next("Copy to Greenplum")
            # with gp(config=cfg_tables[gp_config_key], logger=None) as db:
            #     db.copy_from_dataframe(dataframe=pdf, table_name=gp_table)

            bar.next("Finalize")
            return {"status": "ok", "etl": etl_name, "target": gp_table}

        # ---------------- parquet -> GREENPLUM (Pandas + copy) ----------------
        if etl_name == "parquet_to_greenplum":
            __logger("INFO", code_directory)
            if not (pqt_input and gp_table and gp_config_key):
                raise ValueError("parquet_to_greenplum requires: pqt_input, gp_table, gp_config_key")

            bar.next("Load CSV (Pandas)")
            pdf = pd.read_parquet(pqt_input)

            bar.next("Copy to Greenplum")
            with gp(config=cfg[gp_config_key], logger=None) as db:
                db.copy_from_dataframe(dataframe=pdf, table_name=gp_table)

            bar.next("Finalize")
            return {"status": "ok", "etl": etl_name, "target": gp_table, "rows": len(pdf)}

        # ---------------- CSV -> ORACLE ----------------
        if etl_name == "csv_to_oracle":
            if not (csv_input and oracle_jdbc_url and oracle_table and oracle_user and oracle_password):
                raise ValueError("csv_to_oracle requires: csv_input, oracle_jdbc_url, oracle_table, oracle_user, oracle_password")

            bar.next("Read CSV")
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_input)

            bar.next("Write Oracle")
            (df.write.format("jdbc")
                .option("url", oracle_jdbc_url)
                .option("dbtable", oracle_table)
                .option("user", oracle_user)
                .option("batchsize", 50000) 
                .option("password", oracle_password)
                .option("driver", oracle_driver)
                .mode(oracle_mode)
                .save())

            bar.next("Finalize")
            return {"status": "ok", "etl": etl_name, "target": oracle_table}

        # ---------------- GREENPLUM READ -> PARQUET DIR ----------------
        if etl_name == "greenplum_read_to_parquet":
            
        
            if not (gp_url and query and output_path and parq_name):
                raise ValueError(
                    "greenplum_read_to_parquet requires: gp_url, query, output_path, parq_name"
                )
        
            bar.next("Read Greenplum")
        
            number_of_partitions = 10
            
        
            gp_props = {
                "user": gp_user,
                "password": gp_password,
                "driver": "org.postgresql.Driver"
            }

            if p_column == "":
                df = (
                    spark.read.format("jdbc")
                    .option("url", gp_url)
                    .option("dbtable", f"({query}) subq")
                    .options(**gp_props)
                    .option("fetchsize", "500000")
                    .option("oracle.jdbc.timezoneAsRegion", "false")
                    .load()
                )
            else:
                df = (
                    spark.read.format("jdbc")
                    .option("url", gp_url)
                    .option("dbtable", f"({query}) subq")
                    .options(**gp_props)
                    .option("partitionColumn", p_column)
                    .option("lowerBound", lower_bound)
                    .option("upperBound", upper_bound)
                    .option("numPartitions", "50")
                    .option("fetchsize", "500000")
                    .option("oracle.jdbc.timezoneAsRegion", "false")
                    .load()
                )
            

            batch_size = 100000000
            
            df_num = (
                df.withColumn(
                    "_rn",
                    F.row_number().over(
                        Window.orderBy(F.monotonically_increasing_id())
                    )
                )
            )
            total_rows = df_num.count()   
            bar.next("Write Parquet")
            df.write.mode("overwrite").parquet(output_path)
            
            # Write batches
            # for start in range(1, total_rows + 1, batch_size):
            # write
            #     end = start + batch_size - 1
            
            #     (
            #         df_num
            #         .filter((F.col("_rn") >= start) & (F.col("_rn") <= end))
            #         .drop("_rn")
            #         .coalesce(1)
            #         .write
            #         .mode("append")
            #         .option("compression", compression)
            #         .parquet(output_path)
            #     )
            #     __logger("INFO",f"Written rows {end}")
            
            # # Merge all part files into one parquet
            
            # part_files = sorted([
            #     os.path.join(output_path, f)
            #     for f in os.listdir(output_path)
            #     if f.startswith("part-") and f.endswith(".parquet")
            # ])
            # final_file = os.path.join(output_path, parq_name)
            
            # writer = None
            
            # for file in part_files:
            #     table = pq.read_table(file)
            
            #     if writer is None:
            #         writer = pq.ParquetWriter(
            #             final_file,
            #             table.schema,
            #             compression=compression
            #         )
            
            #     writer.write_table(table)
            
            # writer.close()
            # # Cleanup
            # for file in part_files:
            #     os.remove(file)
            # final_file = os.path.join(output_path, parq_name)
            # for f in os.listdir(output_path):
            #     if f.startswith("_SUCCESS") or f.startswith("._"):
            #         os.remove(os.path.join(output_path, f))
            final_file = os.path.join(output_path, parq_name)
            __logger("INFO", f"Created single parquet file: {final_file}")
            
            bar.next("Finalize")
            return {"status": "ok", "etl": etl_name, "target": final_file}
            

    finally:
        bar.close()
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                pass
