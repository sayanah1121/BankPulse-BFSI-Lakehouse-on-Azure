-- Gold Layer: Star Schema Dimensions
-- Schema: gold

-- 1. Date Dimension (Static / Generated)
IF OBJECT_ID('gold.dim_date', 'U') IS NOT NULL DROP TABLE gold.dim_date;
CREATE TABLE gold.dim_date (
    date_sk            INT PRIMARY KEY,              -- YYYYMMDD
    full_date          DATE NOT NULL UNIQUE,
    day_of_month       INT NOT NULL,
    day_name           VARCHAR(20) NOT NULL,
    day_of_week        INT NOT NULL,
    month_num          INT NOT NULL,
    month_name         VARCHAR(20) NOT NULL,
    quarter_num        INT NOT NULL,
    year_num           INT NOT NULL,
    fiscal_quarter     VARCHAR(10) NOT NULL,
    fiscal_year        VARCHAR(10) NOT NULL,
    is_weekend         BIT NOT NULL
);
GO

-- 2. Customer Dimension (SCD Type 2)
IF OBJECT_ID('gold.dim_customer', 'U') IS NOT NULL DROP TABLE gold.dim_customer;
CREATE TABLE gold.dim_customer (
    customer_sk        BIGINT IDENTITY(1,1) PRIMARY KEY,
    customer_id        VARCHAR(20) NOT NULL,
    customer_segment   VARCHAR(30) NOT NULL,
    city               VARCHAR(50) NOT NULL,
    state              VARCHAR(50) NOT NULL,
    pincode            VARCHAR(10) NOT NULL,
    kyc_status         VARCHAR(20) NOT NULL,
    age_band           VARCHAR(20) NOT NULL,        -- e.g. 18-25, 26-35, 36-50, 50+
    masked_phone       VARCHAR(20) NOT NULL,
    masked_email       VARCHAR(120) NOT NULL,
    hashed_pan         VARCHAR(64) NOT NULL,
    annual_income      DECIMAL(14,2) NOT NULL,
    valid_from         DATETIME2 NOT NULL,
    valid_to           DATETIME2 NULL,
    is_current         BIT NOT NULL DEFAULT 1
);
GO

CREATE INDEX idx_dim_cust_id ON gold.dim_customer(customer_id, is_current);
GO

-- 3. Account Dimension (SCD Type 1)
IF OBJECT_ID('gold.dim_account', 'U') IS NOT NULL DROP TABLE gold.dim_account;
CREATE TABLE gold.dim_account (
    account_sk            BIGINT IDENTITY(1,1) PRIMARY KEY,
    account_id            VARCHAR(20) NOT NULL UNIQUE,
    customer_id           VARCHAR(20) NOT NULL,
    branch_id             VARCHAR(20) NOT NULL,
    product_id            VARCHAR(20) NOT NULL,
    masked_account_number VARCHAR(30) NOT NULL,
    account_type          VARCHAR(30) NOT NULL,
    account_status        VARCHAR(20) NOT NULL,
    currency              VARCHAR(5) NOT NULL,
    opened_date           DATE NOT NULL,
    current_balance       DECIMAL(14,2) NOT NULL,
    updated_at            DATETIME2 NOT NULL
);
GO

-- 4. Branch Dimension (SCD Type 1)
IF OBJECT_ID('gold.dim_branch', 'U') IS NOT NULL DROP TABLE gold.dim_branch;
CREATE TABLE gold.dim_branch (
    branch_sk          BIGINT IDENTITY(1,1) PRIMARY KEY,
    branch_id          VARCHAR(20) NOT NULL UNIQUE,
    branch_code        VARCHAR(20) NOT NULL,
    branch_name        VARCHAR(100) NOT NULL,
    city               VARCHAR(50) NOT NULL,
    state              VARCHAR(50) NOT NULL,
    region             VARCHAR(30) NOT NULL,
    ifsc_code          VARCHAR(20) NOT NULL,
    is_active          BIT NOT NULL
);
GO

-- 5. Product Dimension (SCD Type 1)
IF OBJECT_ID('gold.dim_product', 'U') IS NOT NULL DROP TABLE gold.dim_product;
CREATE TABLE gold.dim_product (
    product_sk         BIGINT IDENTITY(1,1) PRIMARY KEY,
    product_id         VARCHAR(20) NOT NULL UNIQUE,
    product_code       VARCHAR(20) NOT NULL,
    product_name       VARCHAR(100) NOT NULL,
    product_category   VARCHAR(50) NOT NULL,
    interest_rate_pct  DECIMAL(5,2) NOT NULL,
    is_active          BIT NOT NULL
);
GO

-- 6. Merchant Dimension (SCD Type 1)
IF OBJECT_ID('gold.dim_merchant', 'U') IS NOT NULL DROP TABLE gold.dim_merchant;
CREATE TABLE gold.dim_merchant (
    merchant_sk        BIGINT IDENTITY(1,1) PRIMARY KEY,
    merchant_id        VARCHAR(20) NOT NULL UNIQUE,
    merchant_name      VARCHAR(150) NOT NULL,
    mcc_code           VARCHAR(10) NOT NULL,
    category_name      VARCHAR(50) NOT NULL,
    city               VARCHAR(50) NOT NULL,
    is_active          BIT NOT NULL
);
GO
