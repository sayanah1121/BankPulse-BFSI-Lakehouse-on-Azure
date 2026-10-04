"""
BankPulse Local Pipeline Runner
Simulates the entire Azure Data Factory (PL_Master) + Azure Databricks orchestration locally.
Executes Bronze, Silver (DQ Engine, PII Masking, Quarantine, Dedup), and Gold (Dimensions, Facts, KPIs).
"""

import os
import sys
import sqlite3
import json
from datetime import datetime

# Setup project directories
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "databricks"))

from databricks.utils.common import get_spark_session
from databricks.bronze.nb_bronze_load import run_bronze_load
from databricks.silver.nb_silver_transform import run_silver_transform
from databricks.gold.nb_gold_build import run_gold_build
from data_generator.generate_data import main as run_data_generator

DB_FILE = os.path.join(PROJECT_ROOT, "metadata.db")

def init_metadata_db():
    """Initializes local SQLite database with metadata schema, config and rules."""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS pipeline_config (
        config_id INTEGER PRIMARY KEY AUTOINCREMENT,
        entity_name TEXT NOT NULL UNIQUE,
        source_system TEXT NOT NULL,
        source_type TEXT NOT NULL,
        source_schema TEXT,
        source_object TEXT,
        source_path TEXT,
        load_type TEXT NOT NULL,
        watermark_column TEXT,
        last_watermark TEXT,
        watermark_overlap_minutes INTEGER DEFAULT 0,
        primary_keys TEXT NOT NULL,
        pii_columns TEXT,
        pii_method TEXT,
        dq_ruleset TEXT,
        silver_scd_type INTEGER DEFAULT 1,
        load_order INTEGER DEFAULT 100,
        is_active INTEGER DEFAULT 1
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS pipeline_run_log (
        log_id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id TEXT NOT NULL,
        entity_name TEXT NOT NULL,
        layer TEXT NOT NULL,
        status TEXT NOT NULL,
        rows_read INTEGER,
        rows_written INTEGER,
        rows_rejected INTEGER,
        start_time TEXT,
        end_time TEXT,
        error_message TEXT
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS dq_results (
        result_id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id TEXT NOT NULL,
        entity_name TEXT NOT NULL,
        rule_id INTEGER,
        rule_type TEXT,
        column_name TEXT,
        rows_checked INTEGER,
        rows_failed INTEGER,
        pass_pct REAL,
        severity TEXT,
        checked_at TEXT
    );
    """)

    # Seed 12 entities if empty
    cur.execute("SELECT COUNT(*) FROM pipeline_config")
    if cur.fetchone()[0] == 0:
        entities = [
            ('branches', 'CORE_BANKING', 'SQL', 'src', 'branches', 'data/src_sql/branches.csv', 'FULL', None, None, 0, 'branch_id', None, None, 'branches_rules', 1, 10, 1),
            ('products', 'CORE_BANKING', 'SQL', 'src', 'products', 'data/src_sql/products.csv', 'FULL', None, None, 0, 'product_id', None, None, 'products_rules', 1, 10, 1),
            ('merchants', 'CORE_BANKING', 'SQL', 'src', 'merchants', 'data/src_sql/merchants.csv', 'FULL', None, None, 0, 'merchant_id', None, None, 'merchants_rules', 1, 10, 1),
            ('customers', 'CORE_BANKING', 'SQL', 'src', 'customers', 'data/src_sql/customers.csv', 'INCREMENTAL', 'updated_at', None, 30, 'customer_id', 'full_name,pan,phone,email,address', 'pan:HASH,phone:MASK,email:MASK,full_name:MASK,address:MASK', 'customers_rules', 2, 20, 1),
            ('accounts', 'CORE_BANKING', 'SQL', 'src', 'accounts', 'data/src_sql/accounts.csv', 'INCREMENTAL', 'updated_at', None, 30, 'account_id', 'account_number', 'account_number:MASK', 'accounts_rules', 1, 30, 1),
            ('cards', 'CORE_BANKING', 'SQL', 'src', 'cards', 'data/src_sql/cards.csv', 'INCREMENTAL', 'updated_at', None, 30, 'card_id', 'card_number', 'card_number:MASK', 'cards_rules', 1, 40, 1),
            ('loans', 'CORE_BANKING', 'SQL', 'src', 'loans', 'data/src_sql/loans.csv', 'INCREMENTAL', 'updated_at', None, 30, 'loan_id', None, None, 'loans_rules', 1, 40, 1),
            ('loan_repayments', 'CORE_BANKING', 'SQL', 'src', 'loan_repayments', 'data/src_sql/loan_repayments.csv', 'INCREMENTAL', 'updated_at', None, 30, 'repayment_id', None, None, 'loan_repayments_rules', 1, 50, 1),
            ('transactions', 'CORE_BANKING', 'SQL', 'src', 'transactions', 'data/src_sql/transactions.csv', 'INCREMENTAL', 'txn_ts', None, 60, 'txn_id', None, None, 'transactions_rules', 1, 60, 1),
            ('card_transactions', 'CARDS_FILE', 'CSV', None, None, 'data/landing/CARDS_FILE/card_transactions', 'INCREMENTAL', 'txn_ts', None, 60, 'card_txn_id', 'card_number', 'card_number:MASK', 'card_transactions_rules', 1, 60, 1),
            ('upi_transactions', 'UPI_FILE', 'CSV', None, None, 'data/landing/UPI_FILE/upi_transactions', 'INCREMENTAL', 'txn_ts', None, 60, 'upi_txn_id', 'payer_vpa,payee_vpa', 'payer_vpa:HASH,payee_vpa:HASH', 'upi_transactions_rules', 1, 60, 1),
            ('customer_kyc_events', 'KYC_FILE', 'CSV', None, None, 'data/landing/KYC_FILE/customer_kyc_events', 'INCREMENTAL', 'event_ts', None, 60, 'event_id', 'pan', 'pan:HASH', 'kyc_events_rules', 1, 70, 1)
        ]
        cur.executemany("""
        INSERT INTO pipeline_config (
            entity_name, source_system, source_type, source_schema, source_object, source_path,
            load_type, watermark_column, last_watermark, watermark_overlap_minutes,
            primary_keys, pii_columns, pii_method, dq_ruleset, silver_scd_type, load_order, is_active
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, entities)
        conn.commit()

    conn.close()

def run_master_pipeline():
    print("=================================================================")
    print("         BankPulse: BFSI Lakehouse Execution Pipeline           ")
    print("=================================================================")

    init_metadata_db()

    # Step 1: Generate synthetic source and landing data if not present
    data_dir = os.path.join(PROJECT_ROOT, "data")
    if not os.path.exists(os.path.join(data_dir, "src_sql", "transactions.csv")):
        print("\n[*] Initializing synthetic data generation...")
        sys.argv = ["generate_data.py", "--customers", "200", "--txns", "1000", "--card-txns", "600", "--upi-txns", "600", "--output-dir", data_dir]
        run_data_generator()

    batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
    print(f"\n[*] Starting Master Batch Run: {batch_id}")

    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT * FROM pipeline_config WHERE is_active = 1 ORDER BY load_order ASC, entity_name ASC")
    configs = cur.fetchall()
    cols = [d[0] for d in cur.description]
    config_dicts = [dict(zip(cols, row)) for row in configs]

    lakehouse_dir = os.path.join(PROJECT_ROOT, "lakehouse")
    bronze_base = os.path.join(lakehouse_dir, "bronze")
    silver_base = os.path.join(lakehouse_dir, "silver")
    quarantine_base = os.path.join(lakehouse_dir, "quarantine")
    gold_base = os.path.join(lakehouse_dir, "gold")

    reconciliation_data = []

    # Step 2: Ingest Entities (Bronze & Silver)
    print("\n--- Phase 1: Ingesting 12 Entities to Bronze & Silver ---")
    for cfg in config_dicts:
        entity = cfg["entity_name"]
        print(f"\n--> Processing Entity [{cfg['load_order']}]: {entity}")

        # Determine source path
        src_path = cfg["source_path"]
        if not os.path.isabs(src_path):
            src_path = os.path.join(PROJECT_ROOT, src_path)

        # In case of CSV directory, find file
        if os.path.isdir(src_path):
            files = [os.path.join(src_path, f) for f in os.listdir(src_path) if f.endswith(".csv")]
            if files:
                src_path = files[0]

        # Log Start
        start_ts = datetime.utcnow().isoformat()
        cur.execute("""
            INSERT INTO pipeline_run_log (batch_id, entity_name, layer, status, start_time)
            VALUES (?, ?, ?, ?, ?)
        """, (batch_id, entity, "BRONZE", "STARTED", start_ts))
        conn.commit()
        log_id = cur.lastrowid

        # 1. Run Bronze
        os.environ["BP_ENTITY"] = entity
        os.environ["BP_SOURCE_PATH"] = src_path
        os.environ["BP_LOAD_TYPE"] = cfg["load_type"]
        os.environ["BP_BATCH_ID"] = batch_id
        os.environ["BP_SOURCE_SYSTEM"] = cfg["source_system"]
        os.environ["BP_WATERMARK_COLUMN"] = cfg["watermark_column"] or ""
        os.environ["BP_TARGET_BASE_DIR"] = bronze_base

        bronze_res = run_bronze_load()

        # 2. Run Silver
        os.environ["BP_PRIMARY_KEYS"] = cfg["primary_keys"]
        os.environ["BP_PII_COLUMNS"] = cfg["pii_columns"] or ""
        os.environ["BP_PII_METHOD"] = cfg["pii_method"] or ""
        os.environ["BP_DQ_RULESET"] = cfg["dq_ruleset"] or f"{entity}_rules"
        os.environ["BP_BRONZE_BASE_DIR"] = bronze_base
        os.environ["BP_SILVER_BASE_DIR"] = silver_base
        os.environ["BP_QUARANTINE_BASE_DIR"] = quarantine_base

        silver_res = run_silver_transform()

        # 3. Log results and update watermark
        end_ts = datetime.utcnow().isoformat()
        cur.execute("""
            UPDATE pipeline_run_log
            SET status = ?, rows_read = ?, rows_written = ?, rows_rejected = ?, end_time = ?
            WHERE log_id = ?
        """, ("SUCCESS", silver_res["rows_read"], silver_res["rows_written"], silver_res["rows_rejected"], end_ts, log_id))

        if silver_res.get("max_watermark"):
            cur.execute("""
                UPDATE pipeline_config
                SET last_watermark = ?
                WHERE entity_name = ?
            """, (silver_res["max_watermark"], entity))

        # Log DQ rule stats
        for stat in silver_res.get("rule_stats", []):
            cur.execute("""
                INSERT INTO dq_results (batch_id, entity_name, rule_id, rule_type, column_name, rows_checked, rows_failed, pass_pct, severity, checked_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (batch_id, entity, stat["rule_id"], stat["rule_type"], stat["column_name"], stat["rows_checked"], stat["rows_failed"], stat["pass_pct"], stat["severity"], end_ts))

        conn.commit()

        reconciliation_data.append({
            "entity": entity,
            "read": silver_res["rows_read"],
            "valid": silver_res["rows_written"],
            "rejected": silver_res["rows_rejected"],
            "reconciled": (silver_res["rows_read"] == (silver_res["rows_written"] + silver_res["rows_rejected"]))
        })

    # Step 3: Run Gold Layer (Star Schema & KPIs)
    print("\n--- Phase 2: Building Gold Star Schema Dimensions, Facts, and KPIs ---")
    os.environ["BP_SILVER_BASE_DIR"] = silver_base
    os.environ["BP_GOLD_BASE_DIR"] = gold_base
    os.environ["BP_BATCH_ID"] = batch_id
    gold_res = run_gold_build()

    # Step 4: Display Summary & Reconciliation Table
    print("\n=================================================================")
    print("                     EXECUTION RECONCILIATION                   ")
    print("=================================================================")
    print(f"{'Entity':<22} | {'Rows Read':<10} | {'Silver Valid':<12} | {'Quarantine':<10} | {'Status'}")
    print("-" * 67)
    all_reconciled = True
    total_read = 0
    total_valid = 0
    total_rejected = 0

    for r in reconciliation_data:
        total_read += r["read"]
        total_valid += r["valid"]
        total_rejected += r["rejected"]
        status_str = "BALANCED" if r["reconciled"] else "DISCREPANCY"
        if not r["reconciled"]:
            all_reconciled = False
        print(f"{r['entity']:<22} | {r['read']:<10} | {r['valid']:<12} | {r['rejected']:<10} | {status_str}")

    print("-" * 67)
    print(f"{'TOTAL':<22} | {total_read:<10} | {total_valid:<12} | {total_rejected:<10} | {'ALL BALANCED' if all_reconciled else 'DISCREPANCY'}")

    # Display KPI highlights
    kpis = gold_res.get("kpi_summary", {})
    print("\n=================================================================")
    print("                   BUSINESS KPI DASHBOARD SUMMARY                ")
    print("=================================================================")
    print(f"  * Gross NPA Ratio        : {kpis.get('gnpa_ratio', 'N/A')}%")
    print(f"  * Provision Coverage     : {kpis.get('pcr', 'N/A')}%")
    print(f"  * Collection Efficiency  : {kpis.get('collection_efficiency', 'N/A')}%")
    print("=================================================================")

    conn.close()
    return {
        "batch_id": batch_id,
        "total_read": total_read,
        "total_valid": total_valid,
        "total_rejected": total_rejected,
        "all_reconciled": all_reconciled,
        "kpis": kpis
    }

if __name__ == "__main__":
    run_master_pipeline()
