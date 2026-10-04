-- Source DDL: Cards Master
-- Schema: src

CREATE TABLE src.cards (
    card_id            VARCHAR(20) PRIMARY KEY,
    account_id         VARCHAR(20) NOT NULL,
    customer_id        VARCHAR(20) NOT NULL,
    card_number        VARCHAR(30) NOT NULL UNIQUE,
    card_type          VARCHAR(20) NOT NULL,  -- DEBIT, CREDIT
    card_network       VARCHAR(20) NOT NULL,  -- VISA, MASTERCARD, RUPAY
    credit_limit       DECIMAL(14,2) NOT NULL DEFAULT 0.00,
    daily_limit        DECIMAL(14,2) NOT NULL DEFAULT 50000.00,
    card_status        VARCHAR(20) NOT NULL,  -- ACTIVE, BLOCKED, EXPIRED
    expiry_date        DATE NOT NULL,
    issued_date        DATE NOT NULL,
    created_at         DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at         DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT fk_cards_account FOREIGN KEY (account_id) REFERENCES src.accounts(account_id),
    CONSTRAINT fk_cards_customer FOREIGN KEY (customer_id) REFERENCES src.customers(customer_id)
);
GO

CREATE INDEX idx_cards_updated ON src.cards(updated_at);
GO
