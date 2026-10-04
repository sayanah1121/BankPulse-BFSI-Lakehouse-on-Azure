-- Source DDL: Loan Repayments Schedule & History
-- Schema: src

CREATE TABLE src.loan_repayments (
    repayment_id       VARCHAR(20) PRIMARY KEY,
    loan_id            VARCHAR(20) NOT NULL,
    installment_no     INT NOT NULL,
    due_date           DATE NOT NULL,
    paid_date          DATE NULL,
    emi_amount         DECIMAL(14,2) NOT NULL,
    principal_paid     DECIMAL(14,2) NOT NULL DEFAULT 0.00,
    interest_paid      DECIMAL(14,2) NOT NULL DEFAULT 0.00,
    payment_status     VARCHAR(20) NOT NULL,  -- PAID, PENDING, OVERDUE, PARTIAL
    delay_days         INT NOT NULL DEFAULT 0,
    created_at         DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at         DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT fk_rep_loan FOREIGN KEY (loan_id) REFERENCES src.loans(loan_id)
);
GO

CREATE INDEX idx_repayments_updated ON src.loan_repayments(updated_at);
CREATE INDEX idx_repayments_loan ON src.loan_repayments(loan_id);
GO
