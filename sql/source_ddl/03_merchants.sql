-- Source DDL: Merchant Directory
-- Schema: src

CREATE TABLE src.merchants (
    merchant_id        VARCHAR(20) PRIMARY KEY,
    merchant_name      VARCHAR(150) NOT NULL,
    mcc_code           VARCHAR(10) NOT NULL,  -- Merchant Category Code (e.g., 5411 Grocery, 5812 Restaurant)
    category_name      VARCHAR(50) NOT NULL,
    city               VARCHAR(50) NOT NULL,
    settlement_acct    VARCHAR(30) NOT NULL,
    is_active          BIT NOT NULL DEFAULT 1,
    created_at         DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
