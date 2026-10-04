-- Source DDL: Customers Master
-- Schema: src

CREATE TABLE src.customers (
    customer_id        VARCHAR(20) PRIMARY KEY,
    full_name          VARCHAR(150) NOT NULL,
    pan                VARCHAR(20) NOT NULL,
    phone              VARCHAR(20) NOT NULL,
    email              VARCHAR(120) NOT NULL,
    address            VARCHAR(300) NOT NULL,
    city               VARCHAR(50) NOT NULL,
    state              VARCHAR(50) NOT NULL,
    pincode            VARCHAR(10) NOT NULL,
    dob                DATE NOT NULL,
    customer_segment   VARCHAR(30) NOT NULL,  -- RETAIL, HNI, CORPORATE, SME
    kyc_status         VARCHAR(20) NOT NULL,  -- VERIFIED, PENDING, REJECTED
    annual_income      DECIMAL(14,2) NOT NULL DEFAULT 0.00,
    created_at         DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at         DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE INDEX idx_customers_updated ON src.customers(updated_at);
GO
