-- Source DDL: Loans Master
-- Schema: src

CREATE TABLE src.loans (
    loan_id            VARCHAR(20) PRIMARY KEY,
    customer_id        VARCHAR(20) NOT NULL,
    branch_id          VARCHAR(20) NOT NULL,
    product_id         VARCHAR(20) NOT NULL,
    principal_amt      DECIMAL(14,2) NOT NULL,
    interest_rate_pct  DECIMAL(5,2) NOT NULL,
    tenure_months      INT NOT NULL,
    emi_amount         DECIMAL(14,2) NOT NULL,
    disbursed_date     DATE NOT NULL,
    maturity_date      DATE NOT NULL,
    loan_status        VARCHAR(20) NOT NULL,  -- ACTIVE, CLOSED, DEFAULTED
    days_past_due      INT NOT NULL DEFAULT 0,
    created_at         DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at         DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT fk_loans_customer FOREIGN KEY (customer_id) REFERENCES src.customers(customer_id),
    CONSTRAINT fk_loans_branch FOREIGN KEY (branch_id) REFERENCES src.branches(branch_id),
    CONSTRAINT fk_loans_product FOREIGN KEY (product_id) REFERENCES src.products(product_id)
);
GO

CREATE INDEX idx_loans_updated ON src.loans(updated_at);
CREATE INDEX idx_loans_dpd ON src.loans(days_past_due);
GO
