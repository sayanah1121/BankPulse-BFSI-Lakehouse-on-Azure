"""
Unit Tests for Data Quality Engine
Verifies rule evaluations, quarantine splitting, and pass/fail metric tracking.
"""

import pytest
import os
import sys
from datetime import datetime, timedelta

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if os.path.join(PROJECT_ROOT, "databricks") not in sys.path:
    sys.path.insert(0, os.path.join(PROJECT_ROOT, "databricks"))

from utils.common import get_spark_session
from utils.dq_engine import DQEngine

@pytest.fixture(scope="module")
def spark():
    s = get_spark_session("Test_DQ")
    yield s

def test_dq_not_null(spark):
    data = [("TXN1", 100.0), (None, 200.0), ("   ", 300.0)]
    df = spark.createDataFrame(data, ["txn_id", "amount"])
    
    rules = [
        {"rule_id": 1, "column_name": "txn_id", "rule_type": "NOT_NULL", "rule_param": None, "severity": "ERROR", "is_active": 1}
    ]
    engine = DQEngine(rules)
    valid_df, quarantine_df, stats = engine.evaluate(df, "transactions", "BATCH_01")
    
    assert valid_df.count() == 1
    assert quarantine_df.count() == 2
    assert stats[0]["rows_failed"] == 2
    assert stats[0]["pass_pct"] == 33.333

def test_dq_range_and_allowed_values(spark):
    data = [
        ("TXN1", 500.0, "DEBIT"),
        ("TXN2", -50.0, "DEBIT"),    # Range violation (amount <= 0)
        ("TXN3", 1000.0, "INVALID")  # Allowed values violation
    ]
    df = spark.createDataFrame(data, ["txn_id", "amount", "txn_type"])
    
    rules = [
        {"rule_id": 2, "column_name": "amount", "rule_type": "RANGE", "rule_param": "min=0.01;max=100000.0", "severity": "ERROR", "is_active": 1},
        {"rule_id": 3, "column_name": "txn_type", "rule_type": "ALLOWED_VALUES", "rule_param": "values=DEBIT,CREDIT", "severity": "ERROR", "is_active": 1}
    ]
    engine = DQEngine(rules)
    valid_df, quarantine_df, stats = engine.evaluate(df, "transactions", "BATCH_02")
    
    assert valid_df.count() == 1
    assert quarantine_df.count() == 2

def test_dq_regex_pan_and_phone(spark):
    data = [
        ("CUST1", "ABCDE1234F", "9876543210"),  # Valid
        ("CUST2", "INVALIDPAN", "9876543210"),  # Invalid PAN
        ("CUST3", "ABCDE1234F", "1234567890"),  # Invalid phone (doesn't start with 6-9)
    ]
    df = spark.createDataFrame(data, ["customer_id", "pan", "phone"])
    
    rules = [
        {"rule_id": 10, "column_name": "pan", "rule_type": "REGEX", "rule_param": "pattern=^[A-Z]{5}[0-9]{4}[A-Z]$", "severity": "ERROR", "is_active": 1},
        {"rule_id": 11, "column_name": "phone", "rule_type": "REGEX", "rule_param": "pattern=^[6-9][0-9]{9}$", "severity": "ERROR", "is_active": 1}
    ]
    engine = DQEngine(rules)
    valid_df, quarantine_df, stats = engine.evaluate(df, "customers", "BATCH_03")
    
    assert valid_df.count() == 1
    assert quarantine_df.count() == 2

def test_dq_date_not_future(spark):
    now = datetime.now()
    past = now - timedelta(days=2)
    future = now + timedelta(days=5)
    
    data = [
        ("T1", past.strftime("%Y-%m-%d %H:%M:%S")),
        ("T2", future.strftime("%Y-%m-%d %H:%M:%S"))
    ]
    df = spark.createDataFrame(data, ["txn_id", "txn_ts"])
    
    rules = [
        {"rule_id": 27, "column_name": "txn_ts", "rule_type": "DATE_NOT_FUTURE", "rule_param": None, "severity": "ERROR", "is_active": 1}
    ]
    engine = DQEngine(rules)
    valid_df, quarantine_df, stats = engine.evaluate(df, "transactions", "BATCH_04")
    
    assert valid_df.count() == 1
    assert quarantine_df.count() == 1
    assert quarantine_df.collect()[0]["txn_id"] == "T2"

def test_dq_unique_rule(spark):
    data = [
        ("TXN1", 100.0),
        ("TXN2", 200.0),
        ("TXN1", 300.0)   # Duplicate TXN1
    ]
    df = spark.createDataFrame(data, ["txn_id", "amount"])
    
    rules = [
        {"rule_id": 5, "column_name": "txn_id", "rule_type": "UNIQUE", "rule_param": None, "severity": "ERROR", "is_active": 1}
    ]
    engine = DQEngine(rules)
    valid_df, quarantine_df, stats = engine.evaluate(df, "transactions", "BATCH_05")
    
    assert valid_df.count() == 1
    assert quarantine_df.count() == 2
