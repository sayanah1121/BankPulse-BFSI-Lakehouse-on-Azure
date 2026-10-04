"""
Unit Tests for Deduplication, Row Hashing, and Watermark Handling
"""

import pytest
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if os.path.join(PROJECT_ROOT, "databricks") not in sys.path:
    sys.path.insert(0, os.path.join(PROJECT_ROOT, "databricks"))

from utils.common import get_spark_session, deduplicate_dataframe, compute_row_hash

@pytest.fixture(scope="module")
def spark():
    s = get_spark_session("Test_Dedup")
    yield s

def test_deduplicate_dataframe(spark):
    data = [
        ("CUST1", "John Older", "2026-01-01 10:00:00"),
        ("CUST1", "John Newer", "2026-01-02 10:00:00"),
        ("CUST2", "Alice Single", "2026-01-01 10:00:00")
    ]
    df = spark.createDataFrame(data, ["customer_id", "name", "updated_at"])
    df_dedup = deduplicate_dataframe(df, primary_keys="customer_id", order_col="updated_at")
    
    assert df_dedup.count() == 2
    row_cust1 = df_dedup.filter(df_dedup.customer_id == "CUST1").collect()[0]
    assert row_cust1["name"] == "John Newer"

def test_compute_row_hash(spark):
    data = [
        ("ACC1", "SAVINGS", 1000.0),
        ("ACC1", "SAVINGS", 1000.0),
        ("ACC1", "SAVINGS", 2000.0)   # Balance changed
    ]
    df = spark.createDataFrame(data, ["account_id", "type", "balance"])
    df_hashed = compute_row_hash(df)
    hashes = [r["_row_hash"] for r in df_hashed.collect()]

    # First two rows identical -> identical hash
    assert hashes[0] == hashes[1]
    # Third row different balance -> different hash
    assert hashes[0] != hashes[2]
