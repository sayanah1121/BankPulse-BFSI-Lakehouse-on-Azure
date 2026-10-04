"""
Unit Tests for PII Protection and Masking Module
Verifies salted hashing, phone masking, card/account masking, email masking,
and multi-column PII transformation logic.
"""

import pytest
import os
import sys

# Ensure databricks is in sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if os.path.join(PROJECT_ROOT, "databricks") not in sys.path:
    sys.path.insert(0, os.path.join(PROJECT_ROOT, "databricks"))

from utils.common import get_spark_session
from utils.pii import (
    hash_salted,
    mask_card_or_account,
    mask_phone_number,
    mask_email_address,
    apply_pii_transformations,
    DEFAULT_SALT
)

@pytest.fixture(scope="module")
def spark():
    s = get_spark_session("Test_PII")
    yield s

def test_hash_salted_deterministic(spark):
    df = spark.createDataFrame([("ABCDE1234F",), ("ABCDE1234F",), ("XYZAB5678C",)], ["pan"])
    df_hashed = df.withColumn("hashed_pan", hash_salted("pan", salt="MySecretSalt123"))
    results = [row["hashed_pan"] for row in df_hashed.collect()]
    
    # Same PAN should yield same 64-character SHA-256 hash
    assert results[0] == results[1]
    assert len(results[0]) == 64
    # Different PAN should yield different hash
    assert results[0] != results[2]

def test_mask_card_or_account(spark):
    data = [("1000000001",), ("4111000012345678",), ("1234",)]
    df = spark.createDataFrame(data, ["acct"])
    df_masked = df.withColumn("masked", mask_card_or_account("acct"))
    res = [r["masked"] for r in df_masked.collect()]

    assert res[0] == "XXXXXX0001"
    assert res[1] == "XXXXXXXXXXXX5678"
    assert res[2] == "1234"

def test_mask_phone_number(spark):
    data = [("9876543210",), ("8123456789",)]
    df = spark.createDataFrame(data, ["phone"])
    df_masked = df.withColumn("masked", mask_phone_number("phone"))
    res = [r["masked"] for r in df_masked.collect()]

    assert res[0] == "XXXXXX3210"
    assert res[1] == "XXXXXX6789"

def test_mask_email_address(spark):
    data = [("sayan.banker@bankpulse.com",), ("contact@hdfc.in",)]
    df = spark.createDataFrame(data, ["email"])
    df_masked = df.withColumn("masked", mask_email_address("email"))
    res = [r["masked"] for r in df_masked.collect()]

    assert res[0] == "s***@bankpulse.com"
    assert res[1] == "c***@hdfc.in"

def test_apply_pii_transformations(spark):
    data = [
        ("CUST01", "ABCDE1234F", "9876543210", "john.doe@example.com", "1000000001")
    ]
    df = spark.createDataFrame(data, ["customer_id", "pan", "phone", "email", "account_number"])
    
    pii_cols = "pan,phone,email,account_number"
    pii_methods = "pan:HASH,phone:MASK,email:MASK,account_number:MASK"
    
    df_protected = apply_pii_transformations(df, pii_cols, pii_methods, salt="TestSalt")
    row = df_protected.collect()[0]

    assert len(row["pan"]) == 64
    assert row["phone"] == "XXXXXX3210"
    assert row["email"] == "j***@example.com"
    assert row["account_number"] == "XXXXXX0001"
    assert row["customer_id"] == "CUST01"
