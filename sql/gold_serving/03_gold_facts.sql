-- Gold Layer: Star Schema Facts
-- Schema: gold

-- 1. Unified Fact Transactions (Core + Card + UPI)
IF OBJECT_ID('gold.fact_transactions', 'U') IS NOT NULL DROP TABLE gold.fact_transactions;
CREATE TABLE gold.fact_transactions (
    txn_sk             BIGINT IDENTITY(1,1) PRIMARY KEY,
    txn_id             VARCHAR(50) NOT NULL,
    date_sk            INT NOT NULL,
    txn_ts             DATETIME2 NOT NULL,
    channel            VARCHAR(20) NOT NULL,       -- CORE, CARD, UPI
    txn_type           VARCHAR(10) NOT NULL,       -- DEBIT, CREDIT
    txn_mode           VARCHAR(20) NOT NULL,       -- NEFT, RTGS, IMPS, ATM, POS, ECOM, P2P, P2M
    customer_id        VARCHAR(20) NOT NULL,
    account_id         VARCHAR(20) NULL,
    merchant_id        VARCHAR(20) NULL,
    amount             DECIMAL(14,2) NOT NULL,
    fee_amount         DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    is_debit           BIT NOT NULL,
    status             VARCHAR(20) NOT NULL,
    fraud_flag         BIT NOT NULL DEFAULT 0,
    fraud_reason       VARCHAR(100) NULL,
    risk_score         DECIMAL(5,2) NOT NULL DEFAULT 0.00,
    batch_id           VARCHAR(50) NOT NULL
);
GO

CREATE INDEX idx_fact_txn_date ON gold.fact_transactions(date_sk, channel);
CREATE INDEX idx_fact_txn_cust ON gold.fact_transactions(customer_id);
GO

-- 2. Fact Loan Daily / Monthly Snapshot (NPA & Delinquency)
IF OBJECT_ID('gold.fact_loan_snapshot', 'U') IS NOT NULL DROP TABLE gold.fact_loan_snapshot;
CREATE TABLE gold.fact_loan_snapshot (
    snapshot_sk        BIGINT IDENTITY(1,1) PRIMARY KEY,
    snapshot_date_sk   INT NOT NULL,
    loan_id            VARCHAR(20) NOT NULL,
    customer_id        VARCHAR(20) NOT NULL,
    branch_id          VARCHAR(20) NOT NULL,
    product_id         VARCHAR(20) NOT NULL,
    principal_amt      DECIMAL(14,2) NOT NULL,
    emi_amount         DECIMAL(14,2) NOT NULL,
    tenure_months      INT NOT NULL,
    days_past_due      INT NOT NULL,
    dpd_bucket         VARCHAR(20) NOT NULL,       -- 0, 1-30, 31-60, 61-90, 90+ (NPA)
    npa_flag           BIT NOT NULL,               -- 1 if DPD > 90
    outstanding_amt    DECIMAL(14,2) NOT NULL,
    provision_amt      DECIMAL(14,2) NOT NULL,     -- Assumed standard RBI provisioning
    loan_status        VARCHAR(20) NOT NULL,
    batch_id           VARCHAR(50) NOT NULL
);
GO

CREATE INDEX idx_loan_snap_date ON gold.fact_loan_snapshot(snapshot_date_sk, npa_flag);
GO

-- 3. Fact Loan Repayments
IF OBJECT_ID('gold.fact_repayments', 'U') IS NOT NULL DROP TABLE gold.fact_repayments;
CREATE TABLE gold.fact_repayments (
    repayment_sk       BIGINT IDENTITY(1,1) PRIMARY KEY,
    repayment_id       VARCHAR(20) NOT NULL,
    loan_id            VARCHAR(20) NOT NULL,
    due_date_sk        INT NOT NULL,
    paid_date_sk       INT NULL,
    installment_no     INT NOT NULL,
    emi_amount         DECIMAL(14,2) NOT NULL,
    principal_paid     DECIMAL(14,2) NOT NULL,
    interest_paid      DECIMAL(14,2) NOT NULL,
    delay_days         INT NOT NULL DEFAULT 0,
    payment_status     VARCHAR(20) NOT NULL,       -- PAID, PENDING, OVERDUE, PARTIAL
    batch_id           VARCHAR(50) NOT NULL
);
GO

CREATE INDEX idx_fact_rep_loan ON gold.fact_repayments(loan_id, payment_status);
GO
