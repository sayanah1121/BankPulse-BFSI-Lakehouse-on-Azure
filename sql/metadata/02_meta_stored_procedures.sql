-- Metadata Framework: Stored Procedures
-- Schema: meta

IF OBJECT_ID('meta.usp_get_active_configs', 'P') IS NOT NULL DROP PROCEDURE meta.usp_get_active_configs;
GO
CREATE PROCEDURE meta.usp_get_active_configs
AS
BEGIN
    SET NOCOUNT ON;
    SELECT 
        config_id,
        entity_name,
        source_system,
        source_type,
        source_schema,
        source_object,
        source_path,
        load_type,
        watermark_column,
        last_watermark,
        watermark_overlap_minutes,
        primary_keys,
        pii_columns,
        pii_method,
        dq_ruleset,
        silver_scd_type,
        load_order,
        is_active
    FROM meta.pipeline_config
    WHERE is_active = 1
    ORDER BY load_order ASC, entity_name ASC;
END
GO

IF OBJECT_ID('meta.usp_log_start', 'P') IS NOT NULL DROP PROCEDURE meta.usp_log_start;
GO
CREATE PROCEDURE meta.usp_log_start
    @batch_id       VARCHAR(50),
    @adf_run_id     VARCHAR(100) = NULL,
    @config_id      INT = NULL,
    @entity_name    VARCHAR(100),
    @layer          VARCHAR(10),
    @watermark_from DATETIME2 = NULL,
    @log_id         BIGINT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO meta.pipeline_run_log (
        batch_id,
        adf_run_id,
        config_id,
        entity_name,
        layer,
        status,
        watermark_from,
        start_time
    )
    VALUES (
        @batch_id,
        @adf_run_id,
        @config_id,
        @entity_name,
        @layer,
        'STARTED',
        @watermark_from,
        SYSUTCDATETIME()
    );

    SET @log_id = SCOPE_IDENTITY();
    SELECT @log_id AS log_id;
END
GO

IF OBJECT_ID('meta.usp_log_end', 'P') IS NOT NULL DROP PROCEDURE meta.usp_log_end;
GO
CREATE PROCEDURE meta.usp_log_end
    @log_id         BIGINT,
    @status         VARCHAR(20),
    @rows_read      BIGINT = NULL,
    @rows_written   BIGINT = NULL,
    @rows_rejected  BIGINT = NULL,
    @watermark_to   DATETIME2 = NULL,
    @error_message  NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE meta.pipeline_run_log
    SET 
        status = @status,
        rows_read = @rows_read,
        rows_written = @rows_written,
        rows_rejected = @rows_rejected,
        watermark_to = @watermark_to,
        end_time = SYSUTCDATETIME(),
        error_message = @error_message
    WHERE log_id = @log_id;
END
GO

IF OBJECT_ID('meta.usp_update_watermark', 'P') IS NOT NULL DROP PROCEDURE meta.usp_update_watermark;
GO
CREATE PROCEDURE meta.usp_update_watermark
    @entity_name   VARCHAR(100),
    @new_watermark DATETIME2
AS
BEGIN
    SET NOCOUNT ON;
    -- Watermark advances ONLY when explicitly called after end-to-end success
    IF @new_watermark IS NOT NULL
    BEGIN
        UPDATE meta.pipeline_config
        SET 
            last_watermark = @new_watermark,
            updated_at = SYSUTCDATETIME()
        WHERE entity_name = @entity_name
          AND (last_watermark IS NULL OR @new_watermark > last_watermark);
    END
END
GO

IF OBJECT_ID('meta.usp_log_dq_result', 'P') IS NOT NULL DROP PROCEDURE meta.usp_log_dq_result;
GO
CREATE PROCEDURE meta.usp_log_dq_result
    @batch_id     VARCHAR(50),
    @entity_name  VARCHAR(100),
    @rule_id      INT,
    @rule_type    VARCHAR(30) = NULL,
    @column_name  VARCHAR(100) = NULL,
    @rows_checked BIGINT,
    @rows_failed  BIGINT,
    @pass_pct     DECIMAL(6,3),
    @severity     VARCHAR(10) = 'ERROR'
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO meta.dq_results (
        batch_id,
        entity_name,
        rule_id,
        rule_type,
        column_name,
        rows_checked,
        rows_failed,
        pass_pct,
        severity,
        checked_at
    )
    VALUES (
        @batch_id,
        @entity_name,
        @rule_id,
        @rule_type,
        @column_name,
        @rows_checked,
        @rows_failed,
        @pass_pct,
        @severity,
        SYSUTCDATETIME()
    );
END
GO
