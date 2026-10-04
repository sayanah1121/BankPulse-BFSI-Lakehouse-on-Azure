-- Metadata Views for Monitoring & Power BI Pipeline Health
-- Schema: meta

IF OBJECT_ID('meta.vw_pipeline_health', 'V') IS NOT NULL DROP VIEW meta.vw_pipeline_health;
GO
CREATE VIEW meta.vw_pipeline_health
AS
SELECT 
    l.batch_id,
    l.entity_name,
    l.layer,
    l.status,
    l.rows_read,
    l.rows_written,
    l.rows_rejected,
    DATEDIFF(SECOND, l.start_time, ISNULL(l.end_time, SYSUTCDATETIME())) AS duration_seconds,
    l.start_time,
    l.end_time,
    l.error_message
FROM meta.pipeline_run_log l;
GO

IF OBJECT_ID('meta.vw_reconciliation', 'V') IS NOT NULL DROP VIEW meta.vw_reconciliation;
GO
CREATE VIEW meta.vw_reconciliation
AS
SELECT 
    l.batch_id,
    l.entity_name,
    l.layer,
    SUM(ISNULL(l.rows_read, 0)) AS total_rows_read,
    SUM(ISNULL(l.rows_written, 0)) AS total_rows_written,
    SUM(ISNULL(l.rows_rejected, 0)) AS total_rows_rejected,
    CASE 
        WHEN SUM(ISNULL(l.rows_read, 0)) = (SUM(ISNULL(l.rows_written, 0)) + SUM(ISNULL(l.rows_rejected, 0)))
        THEN 'BALANCED'
        ELSE 'DISCREPANCY'
    END AS reconciliation_status
FROM meta.pipeline_run_log l
WHERE l.status = 'SUCCESS'
GROUP BY l.batch_id, l.entity_name, l.layer;
GO

IF OBJECT_ID('meta.vw_dq_summary', 'V') IS NOT NULL DROP VIEW meta.vw_dq_summary;
GO
CREATE VIEW meta.vw_dq_summary
AS
SELECT 
    r.batch_id,
    r.entity_name,
    r.rule_id,
    r.rule_type,
    r.column_name,
    r.severity,
    r.rows_checked,
    r.rows_failed,
    r.pass_pct,
    r.checked_at
FROM meta.dq_results r;
GO
