-- Metadata Architecture: Tables & Schema
-- Schema: meta

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'meta')
BEGIN
    EXEC('CREATE SCHEMA meta');
END
GO

-- 1. Pipeline Control Table
IF OBJECT_ID('meta.pipeline_config', 'U') IS NOT NULL DROP TABLE meta.pipeline_config;
CREATE TABLE meta.pipeline_config (
    config_id                  INT IDENTITY(1,1) PRIMARY KEY,
    entity_name                VARCHAR(100)  NOT NULL UNIQUE,
    source_system              VARCHAR(50)   NOT NULL,         -- CORE_BANKING | CARDS_FILE | UPI_FILE | KYC_FILE
    source_type                VARCHAR(20)   NOT NULL,         -- SQL | CSV
    source_schema              VARCHAR(50)   NULL,
    source_object              VARCHAR(100)  NULL,             -- SQL table name
    source_path                VARCHAR(300)  NULL,             -- CSV folder pattern / path
    load_type                  VARCHAR(20)   NOT NULL,         -- FULL | INCREMENTAL
    watermark_column           VARCHAR(100)  NULL,
    last_watermark             DATETIME2     NULL,
    watermark_overlap_minutes  INT           NOT NULL DEFAULT 0,
    primary_keys               VARCHAR(200)  NOT NULL,
    pii_columns                VARCHAR(500)  NULL,
    pii_method                 VARCHAR(500)  NULL,             -- e.g. pan:HASH,phone:MASK
    dq_ruleset                 VARCHAR(100)  NULL,
    silver_scd_type            TINYINT       NOT NULL DEFAULT 1,
    load_order                 INT           NOT NULL DEFAULT 100,
    is_active                  BIT           NOT NULL DEFAULT 1,
    created_at                 DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at                 DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- 2. Data Quality Rules Repository
IF OBJECT_ID('meta.dq_rules', 'U') IS NOT NULL DROP TABLE meta.dq_rules;
CREATE TABLE meta.dq_rules (
    rule_id      INT IDENTITY(1,1) PRIMARY KEY,
    dq_ruleset   VARCHAR(100) NOT NULL,
    column_name  VARCHAR(100) NOT NULL,
    rule_type    VARCHAR(30)  NOT NULL,       -- NOT_NULL | UNIQUE | RANGE | REGEX | ALLOWED_VALUES | REF_INTEGRITY | DATE_NOT_FUTURE
    rule_param   VARCHAR(500) NULL,
    severity     VARCHAR(10)  NOT NULL,       -- ERROR (quarantine) | WARN (flag only)
    is_active    BIT          NOT NULL DEFAULT 1,
    description  VARCHAR(250) NULL
);
GO

-- 3. Pipeline Execution Run Log
IF OBJECT_ID('meta.pipeline_run_log', 'U') IS NOT NULL DROP TABLE meta.pipeline_run_log;
CREATE TABLE meta.pipeline_run_log (
    log_id         BIGINT IDENTITY(1,1) PRIMARY KEY,
    batch_id       VARCHAR(50)  NOT NULL,
    adf_run_id     VARCHAR(100) NULL,
    config_id      INT          NULL,
    entity_name    VARCHAR(100) NOT NULL,
    layer          VARCHAR(10)  NOT NULL,       -- BRONZE | SILVER | GOLD | SERVE
    status         VARCHAR(20)  NOT NULL,       -- STARTED | SUCCESS | FAILED
    rows_read      BIGINT       NULL,
    rows_written   BIGINT       NULL,
    rows_rejected  BIGINT       NULL,
    watermark_from DATETIME2    NULL,
    watermark_to   DATETIME2    NULL,
    start_time     DATETIME2    NOT NULL DEFAULT SYSUTCDATETIME(),
    end_time       DATETIME2    NULL,
    error_message  NVARCHAR(MAX) NULL
);
GO

CREATE INDEX idx_run_log_batch ON meta.pipeline_run_log(batch_id);
CREATE INDEX idx_run_log_entity ON meta.pipeline_run_log(entity_name, layer);
GO

-- 4. Data Quality Check Results Log
IF OBJECT_ID('meta.dq_results', 'U') IS NOT NULL DROP TABLE meta.dq_results;
CREATE TABLE meta.dq_results (
    result_id    BIGINT IDENTITY(1,1) PRIMARY KEY,
    batch_id     VARCHAR(50)   NOT NULL,
    entity_name  VARCHAR(100)  NOT NULL,
    rule_id      INT           NOT NULL,
    rule_type    VARCHAR(30)   NULL,
    column_name  VARCHAR(100)  NULL,
    rows_checked BIGINT        NOT NULL,
    rows_failed  BIGINT        NOT NULL,
    pass_pct     DECIMAL(6,3)  NOT NULL,
    severity     VARCHAR(10)   NULL,
    checked_at   DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE INDEX idx_dq_batch ON meta.dq_results(batch_id);
GO

-- 5. File Ingestion Audit (for CSV tracking & idempotency)
IF OBJECT_ID('meta.file_audit', 'U') IS NOT NULL DROP TABLE meta.file_audit;
CREATE TABLE meta.file_audit (
    file_id        BIGINT IDENTITY(1,1) PRIMARY KEY,
    batch_id       VARCHAR(50)   NOT NULL,
    file_path      VARCHAR(500)  NOT NULL UNIQUE,
    file_size_kb   DECIMAL(12,2) NULL,
    row_count      BIGINT        NULL,
    processed_at   DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
