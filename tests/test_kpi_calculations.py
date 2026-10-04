"""
Unit Tests for BFSI Financial KPIs & Risk Metrics
Verifies NPA ratios, DPD aging buckets, and collection efficiency calculations.
"""

import pytest
from pyspark.sql import functions as F
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if os.path.join(PROJECT_ROOT, "databricks") not in sys.path:
    sys.path.insert(0, os.path.join(PROJECT_ROOT, "databricks"))

from utils.common import get_spark_session

@pytest.fixture(scope="module")
def spark():
    s = get_spark_session("Test_KPIs")
    yield s

def test_npa_ratio_calculation(spark):
    loans = [
        ("LN1", 1000000.0, 0, 0),     # Current
        ("LN2", 500000.0, 45, 0),     # SMA-1
        ("LN3", 500000.0, 120, 1),    # NPA (>90 DPD)
    ]
    df = spark.createDataFrame(loans, ["loan_id", "principal_amt", "days_past_due", "npa_flag"])

    tot_adv = df.agg(F.sum("principal_amt")).collect()[0][0]  # 2,000,000
    gnpa_amt = df.filter(F.col("npa_flag") == 1).agg(F.sum("principal_amt")).collect()[0][0]  # 500,000

    gnpa_ratio = (gnpa_amt / tot_adv) * 100.0
    assert gnpa_ratio == 25.0

def test_dpd_bucketing(spark):
    dpd_test_cases = [
        (0, "0 (Current)"),
        (15, "1-30 (SMA-0)"),
        (45, "31-60 (SMA-1)"),
        (75, "61-90 (SMA-2)"),
        (120, "90+ (NPA)")
    ]
    df = spark.createDataFrame(dpd_test_cases, ["dpd", "expected_bucket"])
    
    df_classified = df.withColumn("bucket",
        F.when(F.col("dpd") == 0, "0 (Current)")
         .when(F.col("dpd") <= 30, "1-30 (SMA-0)")
         .when(F.col("dpd") <= 60, "31-60 (SMA-1)")
         .when(F.col("dpd") <= 90, "61-90 (SMA-2)")
         .otherwise("90+ (NPA)")
    )

    for row in df_classified.collect():
        assert row["bucket"] == row["expected_bucket"]

def test_collection_efficiency(spark):
    repayments = [
        ("R1", 10000.0, "PAID"),
        ("R2", 10000.0, "PAID"),
        ("R3", 10000.0, "PAID"),
        ("R4", 10000.0, "OVERDUE")
    ]
    df = spark.createDataFrame(repayments, ["rep_id", "emi_amount", "status"])
    total_due = df.agg(F.sum("emi_amount")).collect()[0][0] # 40,000
    total_coll = df.filter(F.col("status") == "PAID").agg(F.sum("emi_amount")).collect()[0][0] # 30,000

    efficiency = (total_coll / total_due) * 100.0
    assert efficiency == 75.0
