-- Seed Data: 28+ Data Quality Rules
-- Schema: meta

TRUNCATE TABLE meta.dq_rules;
GO

INSERT INTO meta.dq_rules (dq_ruleset, column_name, rule_type, rule_param, severity, is_active, description)
VALUES
-- 1. Branches Rules
('branches_rules', 'branch_id', 'NOT_NULL', NULL, 'ERROR', 1, 'Branch ID must be present'),
('branches_rules', 'branch_code', 'UNIQUE', NULL, 'ERROR', 1, 'Branch Code must be unique'),
('branches_rules', 'region', 'ALLOWED_VALUES', 'values=North,South,East,West,Central', 'WARN', 1, 'Region must be valid Indian geographical zone'),

-- 2. Products Rules
('products_rules', 'product_id', 'NOT_NULL', NULL, 'ERROR', 1, 'Product ID is mandatory'),
('products_rules', 'interest_rate_pct', 'RANGE', 'min=0.0;max=36.0', 'ERROR', 1, 'Interest rate between 0% and 36%'),
('products_rules', 'product_category', 'ALLOWED_VALUES', 'values=SAVINGS,CURRENT,HOME_LOAN,PERSONAL_LOAN,AUTO_LOAN,CREDIT_CARD', 'ERROR', 1, 'Valid product categories only'),

-- 3. Merchants Rules
('merchants_rules', 'merchant_id', 'NOT_NULL', NULL, 'ERROR', 1, 'Merchant ID mandatory'),
('merchants_rules', 'mcc_code', 'REGEX', 'pattern=^[0-9]{4}$', 'WARN', 1, 'MCC code must be 4 digits'),

-- 4. Customers Rules
('customers_rules', 'customer_id', 'NOT_NULL', NULL, 'ERROR', 1, 'Customer ID must not be null'),
('customers_rules', 'customer_id', 'UNIQUE', NULL, 'ERROR', 1, 'Customer ID must be unique'),
('customers_rules', 'pan', 'REGEX', 'pattern=^[A-Z]{5}[0-9]{4}[A-Z]$', 'ERROR', 1, 'Standard Indian PAN format (5 letters, 4 digits, 1 letter)'),
('customers_rules', 'phone', 'REGEX', 'pattern=^[6-9][0-9]{9}$', 'WARN', 1, 'Valid 10-digit Indian mobile number starting 6-9'),
('customers_rules', 'kyc_status', 'ALLOWED_VALUES', 'values=VERIFIED,PENDING,REJECTED', 'WARN', 1, 'Valid KYC status flag'),
('customers_rules', 'annual_income', 'RANGE', 'min=0.0;max=500000000.0', 'ERROR', 1, 'Income must be non-negative'),

-- 5. Accounts Rules
('accounts_rules', 'account_id', 'NOT_NULL', NULL, 'ERROR', 1, 'Account ID mandatory'),
('accounts_rules', 'customer_id', 'NOT_NULL', NULL, 'ERROR', 1, 'Foreign key customer_id mandatory'),
('accounts_rules', 'account_status', 'ALLOWED_VALUES', 'values=ACTIVE,DORMANT,CLOSED,FROZEN', 'ERROR', 1, 'Account status must be valid'),
('accounts_rules', 'current_balance', 'RANGE', 'min=-5000000.0;max=1000000000.0', 'WARN', 1, 'Balance limits check'),

-- 6. Cards Rules
('cards_rules', 'card_id', 'NOT_NULL', NULL, 'ERROR', 1, 'Card ID mandatory'),
('cards_rules', 'card_type', 'ALLOWED_VALUES', 'values=DEBIT,CREDIT', 'ERROR', 1, 'Card type DEBIT or CREDIT'),
('cards_rules', 'credit_limit', 'RANGE', 'min=0.0;max=5000000.0', 'ERROR', 1, 'Credit limit bounds'),

-- 7. Loans Rules
('loans_rules', 'loan_id', 'NOT_NULL', NULL, 'ERROR', 1, 'Loan ID mandatory'),
('loans_rules', 'principal_amt', 'RANGE', 'min=10000.0;max=100000000.0', 'ERROR', 1, 'Loan principal amount bounds'),
('loans_rules', 'tenure_months', 'RANGE', 'min=3;max=360', 'ERROR', 1, 'Tenure between 3 months and 30 years'),
('loans_rules', 'days_past_due', 'RANGE', 'min=0;max=1000', 'ERROR', 1, 'DPD must be non-negative'),

-- 8. Loan Repayments Rules
('loan_repayments_rules', 'repayment_id', 'NOT_NULL', NULL, 'ERROR', 1, 'Repayment ID mandatory'),
('loan_repayments_rules', 'loan_id', 'NOT_NULL', NULL, 'ERROR', 1, 'Loan reference mandatory'),
('loan_repayments_rules', 'emi_amount', 'RANGE', 'min=100.0;max=5000000.0', 'ERROR', 1, 'EMI amount check'),

-- 9. Core Transactions Rules
('transactions_rules', 'txn_id', 'NOT_NULL', NULL, 'ERROR', 1, 'Transaction ID must not be null'),
('transactions_rules', 'txn_id', 'UNIQUE', NULL, 'ERROR', 1, 'Transaction ID must be unique'),
('transactions_rules', 'amount', 'RANGE', 'min=0.01;max=10000000.0', 'ERROR', 1, 'Amount must be strictly positive and under threshold'),
('transactions_rules', 'txn_type', 'ALLOWED_VALUES', 'values=DEBIT,CREDIT', 'ERROR', 1, 'Transaction type must be DEBIT or CREDIT'),
('transactions_rules', 'txn_ts', 'DATE_NOT_FUTURE', NULL, 'ERROR', 1, 'Transaction timestamp cannot be future dated'),

-- 10. Card Transactions Rules
('card_transactions_rules', 'card_txn_id', 'NOT_NULL', NULL, 'ERROR', 1, 'Card txn ID mandatory'),
('card_transactions_rules', 'amount', 'RANGE', 'min=0.01;max=500000.0', 'ERROR', 1, 'Card txn amount check'),
('card_transactions_rules', 'txn_ts', 'DATE_NOT_FUTURE', NULL, 'ERROR', 1, 'Card txn timestamp cannot be future dated'),

-- 11. UPI Transactions Rules
('upi_transactions_rules', 'upi_txn_id', 'NOT_NULL', NULL, 'ERROR', 1, 'UPI txn ID mandatory'),
('upi_transactions_rules', 'amount', 'RANGE', 'min=1.0;max=200000.0', 'ERROR', 1, 'UPI amount per NPCI limit (up to 2 Lakh INR)'),
('upi_transactions_rules', 'status', 'ALLOWED_VALUES', 'values=SUCCESS,FAILED,PENDING', 'ERROR', 1, 'UPI status must be SUCCESS, FAILED or PENDING'),
('upi_transactions_rules', 'txn_ts', 'DATE_NOT_FUTURE', NULL, 'ERROR', 1, 'UPI txn timestamp cannot be future dated'),

-- 12. KYC Events Rules
('kyc_events_rules', 'event_id', 'NOT_NULL', NULL, 'ERROR', 1, 'KYC Event ID mandatory'),
('kyc_events_rules', 'kyc_status', 'ALLOWED_VALUES', 'values=VERIFIED,PENDING,REJECTED,EXPIRED', 'ERROR', 1, 'Event KYC status check');
GO
