"""
BankPulse Utility: Common Functions
Provides SparkSession lifecycle management, parameter retrieval across Databricks and
local testing environments, audit column enrichment, row hashing, and Delta upsert helpers.
"""

import os
import sys
import hashlib
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

try:
    from delta import configure_spark_with_delta_pip
    from delta.tables import DeltaTable
    HAS_DELTA = True
except ImportError:
    HAS_DELTA = False

def get_spark_session(app_name="BankPulse_Lakehouse"):
    """
    Returns an active SparkSession.
    Configures Delta Lake extensions if running in local standalone mode.
    """
    builder = SparkSession.builder.appName(app_name)
    
    # In Databricks, SparkSession is pre-configured. In local, configure Delta
    if "DATABRICKS_RUNTIME_VERSION" not in os.environ and HAS_DELTA:
        builder = (
            builder.master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", os.path.abspath("spark-warehouse"))
            .config("spark.driver.memory", "2g")
            .config("spark.ui.enabled", "false")
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()
    else:
        return builder.getOrCreate()

def get_widget_or_arg(name, default_value=""):
    """
    Retrieves widget parameter from Databricks dbutils, falling back to
    environment variable, sys.argv, or default_value for local execution.
    """
    # 1. Try Databricks dbutils
    try:
        import IPython
        ipy = IPython.get_ipython()
        if ipy is not None and "dbutils" in ipy.user_ns:
            return ipy.user_ns["dbutils"].widgets.get(name)
    except Exception:
        pass
    
    # 2. Check environment variable
    env_key = f"BP_{name.upper()}"
    if env_key in os.environ:
        return os.environ[env_key]
    if name in os.environ:
        return os.environ[name]

    # 3. Check CLI flags: --name=value or --name value
    for i, arg in enumerate(sys.argv):
        if arg == f"--{name}" and i + 1 < len(sys.argv):
            return sys.argv[i + 1]
        if arg.startswith(f"--{name}="):
            return arg.split("=", 1)[1]

    return default_value

def add_audit_columns(df, batch_id, source_system="CORE_BANKING", source_file=None, load_type="INCREMENTAL"):
    """
    Adds standard Bronze audit tracking columns:
    _ingest_ts, _source_system, _source_file, _batch_id, _load_type
    """
    file_col = F.lit(source_file) if source_file else F.input_file_name()
    
    return (
        df.withColumn("_ingest_ts", F.current_timestamp())
          .withColumn("_source_system", F.lit(source_system))
          .withColumn("_source_file", file_col)
          .withColumn("_batch_id", F.lit(batch_id))
          .withColumn("_load_type", F.lit(load_type))
    )

def compute_row_hash(df, key_and_val_cols=None):
    """
    Calculates MD5/SHA256 row hash across non-audit columns to detect record state changes.
    """
    cols_to_hash = key_and_val_cols if key_and_val_cols else [c for c in df.columns if not c.startswith("_")]
    cols_to_hash.sort()
    
    # Concatenate columns with a delimiter handling nulls
    concat_expr = F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("__NULL__")) for c in cols_to_hash])
    return df.withColumn("_row_hash", F.sha2(concat_expr, 256))

def deduplicate_dataframe(df, primary_keys, order_col="updated_at"):
    """
    Deduplicates a DataFrame on primary keys keeping the most recent record.
    """
    pk_list = [k.strip() for k in primary_keys.split(",") if k.strip()]
    if not pk_list:
        return df

    # If order_col exists in DataFrame, order by it descending; otherwise use ingest_ts or any
    order_expr = F.col(order_col).desc() if order_col in df.columns else F.col("_ingest_ts").desc() if "_ingest_ts" in df.columns else F.monotonically_increasing_id().desc()
    
    window_spec = Window.partitionBy(*pk_list).orderBy(order_expr)
    return df.withColumn("_rn", F.row_number().over(window_spec)).filter(F.col("_rn") == 1).drop("_rn")

def upsert_delta_table(spark, target_path, source_df, primary_keys, partition_col=None):
    """
    Idempotent MERGE upsert into Delta table.
    Creates table if it does not exist.
    """
    pk_list = [k.strip() for k in primary_keys.split(",") if k.strip()]
    merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in pk_list])

    if not DeltaTable.isDeltaTable(spark, target_path):
        writer = source_df.write.format("delta").mode("overwrite")
        if partition_col and partition_col in source_df.columns:
            writer = writer.partitionBy(partition_col)
        writer.save(target_path)
        return {"action": "created", "count": source_df.count()}
    else:
        target_table = DeltaTable.forPath(spark, target_path)
        
        # When matched and row_hash differs then update; when not matched then insert
        (target_table.alias("target")
            .merge(source_df.alias("source"), merge_condition)
            .whenMatchedUpdateAll(condition="target._row_hash != source._row_hash" if "_row_hash" in source_df.columns else None)
            .whenNotMatchedInsertAll()
            .execute())
            
        return {"action": "merged", "count": source_df.count()}
