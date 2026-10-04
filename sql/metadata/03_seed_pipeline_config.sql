-- Seed Data: Pipeline Configuration for 12 Entities
-- Schema: meta

TRUNCATE TABLE meta.pipeline_config;
GO

INSERT INTO meta.pipeline_config (
    entity_name, source_system, source_type, source_schema, source_object, source_path,
    load_type, watermark_column, last_watermark, watermark_overlap_minutes,
    primary_keys, pii_columns, pii_method, dq_ruleset, silver_scd_type, load_order, is_active
)
VALUES
-- Reference Entities (Load Order 10)
(
    'branches', 'CORE_BANKING', 'SQL', 'src', 'branches', NULL,
    'FULL', NULL, NULL, 0,
    'branch_id', NULL, NULL, 'branches_rules', 1, 10, 1
),
(
    'products', 'CORE_BANKING', 'SQL', 'src', 'products', NULL,
    'FULL', NULL, NULL, 0,
    'product_id', NULL, NULL, 'products_rules', 1, 10, 1
),
(
    'merchants', 'CORE_BANKING', 'SQL', 'src', 'merchants', NULL,
    'FULL', NULL, NULL, 0,
    'merchant_id', NULL, NULL, 'merchants_rules', 1, 10, 1
),

-- Master Entities (Load Order 20-40)
(
    'customers', 'CORE_BANKING', 'SQL', 'src', 'customers', NULL,
    'INCREMENTAL', 'updated_at', NULL, 30,
    'customer_id', 'full_name,pan,phone,email,address', 'pan:HASH,phone:MASK,email:MASK,full_name:MASK,address:MASK', 'customers_rules', 2, 20, 1
),
(
    'accounts', 'CORE_BANKING', 'SQL', 'src', 'accounts', NULL,
    'INCREMENTAL', 'updated_at', NULL, 30,
    'account_id', 'account_number', 'account_number:MASK', 'accounts_rules', 1, 30, 1
),
(
    'cards', 'CORE_BANKING', 'SQL', 'src', 'cards', NULL,
    'INCREMENTAL', 'updated_at', NULL, 30,
    'card_id', 'card_number', 'card_number:MASK', 'cards_rules', 1, 40, 1
),
(
    'loans', 'CORE_BANKING', 'SQL', 'src', 'loans', NULL,
    'INCREMENTAL', 'updated_at', NULL, 30,
    'loan_id', NULL, NULL, 'loans_rules', 1, 40, 1
),

-- Transaction & Event Entities (Load Order 50-70)
(
    'loan_repayments', 'CORE_BANKING', 'SQL', 'src', 'loan_repayments', NULL,
    'INCREMENTAL', 'updated_at', NULL, 30,
    'repayment_id', NULL, NULL, 'loan_repayments_rules', 1, 50, 1
),
(
    'transactions', 'CORE_BANKING', 'SQL', 'src', 'transactions', NULL,
    'INCREMENTAL', 'txn_ts', NULL, 60,
    'txn_id', NULL, NULL, 'transactions_rules', 1, 60, 1
),
(
    'card_transactions', 'CARDS_FILE', 'CSV', NULL, NULL, 'landing/CARDS_FILE/card_transactions/',
    'INCREMENTAL', 'txn_ts', NULL, 60,
    'card_txn_id', 'card_number', 'card_number:MASK', 'card_transactions_rules', 1, 60, 1
),
(
    'upi_transactions', 'UPI_FILE', 'CSV', NULL, NULL, 'landing/UPI_FILE/upi_transactions/',
    'INCREMENTAL', 'txn_ts', NULL, 60,
    'upi_txn_id', 'payer_vpa,payee_vpa', 'payer_vpa:HASH,payee_vpa:HASH', 'upi_transactions_rules', 1, 60, 1
),
(
    'customer_kyc_events', 'KYC_FILE', 'CSV', NULL, NULL, 'landing/KYC_FILE/customer_kyc_events/',
    'INCREMENTAL', 'event_ts', NULL, 60,
    'event_id', 'pan', 'pan:HASH', 'kyc_events_rules', 1, 70, 1
);
GO
