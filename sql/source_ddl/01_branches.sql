-- Source DDL: Core Banking Master & Reference Tables
-- Schema: src

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'src')
BEGIN
    EXEC('CREATE SCHEMA src');
END
GO

-- 1. Branches Master
CREATE TABLE src.branches (
    branch_id          VARCHAR(20) PRIMARY KEY,
    branch_code        VARCHAR(20) NOT NULL UNIQUE,
    branch_name        VARCHAR(100) NOT NULL,
    city               VARCHAR(50) NOT NULL,
    state              VARCHAR(50) NOT NULL,
    region             VARCHAR(30) NOT NULL,  -- North, South, East, West, Central
    ifsc_code          VARCHAR(20) NOT NULL,
    is_active          BIT NOT NULL DEFAULT 1,
    created_at         DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
