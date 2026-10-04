-- Source DDL: Product Catalog
-- Schema: src

CREATE TABLE src.products (
    product_id         VARCHAR(20) PRIMARY KEY,
    product_code       VARCHAR(20) NOT NULL UNIQUE,
    product_name       VARCHAR(100) NOT NULL,
    product_category   VARCHAR(50) NOT NULL,  -- SAVINGS, CURRENT, HOME_LOAN, PERSONAL_LOAN, AUTO_LOAN, CREDIT_CARD
    interest_rate_pct  DECIMAL(5,2) NOT NULL,
    min_balance_amt    DECIMAL(14,2) NOT NULL DEFAULT 0.00,
    is_active          BIT NOT NULL DEFAULT 1,
    created_at         DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
