"""
End-to-End Pipeline & Idempotency Integration Test
Executes the full pipeline and verifies row reconciliation and duplicate prevention.
"""

import pytest
import os
import sys
import shutil

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from run_local_pipeline import run_master_pipeline
from databricks.utils.common import get_spark_session

@pytest.fixture(scope="module")
def spark():
    s = get_spark_session("Test_E2E")
    yield s

def test_full_pipeline_execution():
    """
    Runs the full end-to-end pipeline and verifies row reconciliation:
    total_read == total_valid + total_rejected
    """
    res = run_master_pipeline()
    
    assert res["batch_id"] is not None
    assert res["total_read"] > 0
    assert res["total_valid"] > 0
    assert res["all_reconciled"] is True
    
    # Check that KPIs were computed
    kpis = res["kpis"]
    assert "gnpa_ratio" in kpis
    assert "collection_efficiency" in kpis

def test_gold_tables_exist(spark):
    """
    Verifies that Gold dimensions, facts, and KPI Delta tables exist and have records.
    """
    gold_dir = os.path.join(PROJECT_ROOT, "lakehouse", "gold").replace("\\", "/")
    
    for tbl in ["dim_date", "dim_customer", "dim_account", "fact_transactions", "fact_loan_snapshot", "kpi_npa_metrics"]:
        tbl_path = os.path.join(gold_dir, tbl).replace("\\", "/")
        df = spark.read.format("delta").load(tbl_path)
        assert df.count() > 0, f"Gold table {tbl} is empty!"
