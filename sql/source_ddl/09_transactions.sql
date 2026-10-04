-- Source DDL: Core Account Transactions
-- Schema: src

CREATE TABLE src.transactions (
    txn_id             VARCHAR(30) PRIMARY KEY,
    account_id         VARCHAR(20) NOT NULL,
    txn_ts             DATETIME2 NOT NULL,
    txn_type           VARCHAR(10) NOT NULL,  -- DEBIT, CREDIT
    txn_mode           VARCHAR(20) NOT NULL,  -- NEFT, RTGS, IMPS, ATM, BRANCH
    amount             DECIMAL(14,2) NOT NULL,
    currency           VARCHAR(5) NOT NULL DEFAULT 'INR',
    balance_after      DECIMAL(14,2) NOT NULL,
    reference_no       VARCHAR(50) NOT NULL,
    description        VARCHAR(250) NULL,
    status             VARCHAR(20) NOT NULL DEFAULT 'SUCCESS',
    created_at         DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT fk_txn_account FOREIGN KEY (account_id) REFERENCES src.accounts(account_id)
);
GO

CREATE INDEX idx_transactions_ts ON src.transactions(txn_ts);
CREATE INDEX idx_transactions_account ON src.transactions(account_id);
GO
