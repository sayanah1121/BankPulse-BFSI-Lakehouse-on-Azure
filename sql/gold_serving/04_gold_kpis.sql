-- Gold Layer: BFSI Aggregate KPI Tables for Executive Dashboards
-- Schema: gold

-- 1. NPA (Non-Performing Assets) Metrics
IF OBJECT_ID('gold.kpi_npa_metrics', 'U') IS NOT NULL DROP TABLE gold.kpi_npa_metrics;
CREATE TABLE gold.kpi_npa_metrics (
    metric_id              BIGINT IDENTITY(1,1) PRIMARY KEY,
    snapshot_date          DATE NOT NULL,
    total_loans_count      BIGINT NOT NULL,
    total_advances_amt     DECIMAL(16,2) NOT NULL,
    gross_npa_count        BIGINT NOT NULL,
    gross_npa_amt          DECIMAL(16,2) NOT NULL,
    gross_npa_ratio_pct    DECIMAL(6,3) NOT NULL,    -- (Gross NPA Amt / Total Advances) * 100
    total_provisions_amt   DECIMAL(16,2) NOT NULL,
    net_advances_amt       DECIMAL(16,2) NOT NULL,    -- Total Advances - Provisions
    net_npa_amt            DECIMAL(16,2) NOT NULL,    -- Gross NPA - Provisions
    net_npa_ratio_pct      DECIMAL(6,3) NOT NULL,    -- (Net NPA Amt / Net Advances) * 100
    provision_coverage_pct DECIMAL(6,3) NOT NULL,    -- (Provisions / Gross NPA) * 100
    calculated_at          DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- 2. Delinquency Aging Buckets (SMA 0, SMA 1, SMA 2, NPA)
IF OBJECT_ID('gold.kpi_delinquency_buckets', 'U') IS NOT NULL DROP TABLE gold.kpi_delinquency_buckets;
CREATE TABLE gold.kpi_delinquency_buckets (
    bucket_id              BIGINT IDENTITY(1,1) PRIMARY KEY,
    snapshot_date          DATE NOT NULL,
    dpd_bucket             VARCHAR(20) NOT NULL,     -- '0 (Current)', '1-30 (SMA-0)', '31-60 (SMA-1)', '61-90 (SMA-2)', '90+ (NPA)'
    loan_count             BIGINT NOT NULL,
    total_outstanding_amt  DECIMAL(16,2) NOT NULL,
    portfolio_share_pct    DECIMAL(6,3) NOT NULL,
    calculated_at          DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- 3. Daily Channel Performance & Volume Mix
IF OBJECT_ID('gold.kpi_channel_performance', 'U') IS NOT NULL DROP TABLE gold.kpi_channel_performance;
CREATE TABLE gold.kpi_channel_performance (
    channel_kpi_id         BIGINT IDENTITY(1,1) PRIMARY KEY,
    txn_date               DATE NOT NULL,
    channel                VARCHAR(20) NOT NULL,     -- CORE, CARD, UPI
    txn_count              BIGINT NOT NULL,
    total_volume_amt       DECIMAL(16,2) NOT NULL,
    avg_ticket_size        DECIMAL(14,2) NOT NULL,
    channel_volume_share   DECIMAL(6,3) NOT NULL,
    success_rate_pct       DECIMAL(6,3) NOT NULL,
    calculated_at          DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- 4. Fraud Risk Monitoring Summary
IF OBJECT_ID('gold.kpi_fraud_monitoring', 'U') IS NOT NULL DROP TABLE gold.kpi_fraud_monitoring;
CREATE TABLE gold.kpi_fraud_monitoring (
    fraud_kpi_id           BIGINT IDENTITY(1,1) PRIMARY KEY,
    txn_date               DATE NOT NULL,
    channel                VARCHAR(20) NOT NULL,
    total_txns             BIGINT NOT NULL,
    flagged_fraud_txns     BIGINT NOT NULL,
    fraud_rate_pct         DECIMAL(6,4) NOT NULL,
    fraud_exposure_amt     DECIMAL(16,2) NOT NULL,
    primary_reason         VARCHAR(100) NULL,
    calculated_at          DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- 5. Branch Level Performance & Portfolio
IF OBJECT_ID('gold.kpi_branch_performance', 'U') IS NOT NULL DROP TABLE gold.kpi_branch_performance;
CREATE TABLE gold.kpi_branch_performance (
    branch_kpi_id          BIGINT IDENTITY(1,1) PRIMARY KEY,
    branch_id              VARCHAR(20) NOT NULL,
    branch_name            VARCHAR(100) NOT NULL,
    region                 VARCHAR(30) NOT NULL,
    total_accounts         BIGINT NOT NULL,
    total_deposits_amt     DECIMAL(16,2) NOT NULL,
    total_loans_disbursed  DECIMAL(16,2) NOT NULL,
    total_txn_count        BIGINT NOT NULL,
    total_txn_volume       DECIMAL(16,2) NOT NULL,
    branch_npa_ratio_pct   DECIMAL(6,3) NOT NULL,
    calculated_at          DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- 6. Collection Efficiency
IF OBJECT_ID('gold.kpi_collection_efficiency', 'U') IS NOT NULL DROP TABLE gold.kpi_collection_efficiency;
CREATE TABLE gold.kpi_collection_efficiency (
    collection_id          BIGINT IDENTITY(1,1) PRIMARY KEY,
    period_date            DATE NOT NULL,
    total_emi_due_amt      DECIMAL(16,2) NOT NULL,
    total_collected_amt    DECIMAL(16,2) NOT NULL,
    collection_efficiency_pct DECIMAL(6,3) NOT NULL,
    calculated_at          DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
