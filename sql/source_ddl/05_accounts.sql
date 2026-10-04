-- Source DDL: Accounts Master
-- Schema: src

CREATE TABLE src.accounts (
    account_id         VARCHAR(20) PRIMARY KEY,
    customer_id        VARCHAR(20) NOT NULL,
    branch_id          VARCHAR(20) NOT NULL,
    product_id         VARCHAR(20) NOT NULL,
    account_number     VARCHAR(30) NOT NULL UNIQUE,
    account_type       VARCHAR(30) NOT NULL,  -- SAVINGS, CURRENT, SALARY
    account_status     VARCHAR(20) NOT NULL,  -- ACTIVE, DORMANT, CLOSED, FROZEN
    current_balance    DECIMAL(14,2) NOT NULL DEFAULT 0.00,
    currency           VARCHAR(5) NOT NULL DEFAULT 'INR',
    opened_date        DATE NOT NULL,
    created_at         DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at         DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT fk_acc_customer FOREIGN KEY (customer_id) REFERENCES src.customers(customer_id),
    CONSTRAINT fk_acc_branch FOREIGN KEY (branch_id) REFERENCES src.branches(branch_id),
    CONSTRAINT fk_acc_product FOREIGN KEY (product_id) REFERENCES src.products(product_id)
);
GO

CREATE INDEX idx_accounts_updated ON src.accounts(updated_at);
CREATE INDEX idx_accounts_customer ON src.accounts(customer_id);
GO
