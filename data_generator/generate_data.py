"""
BankPulse Synthetic Data Generator
Generates realistic Indian banking, card, UPI, and loan datasets with deliberate
data quality anomalies (duplicates, nulls, format issues, range errors, orphans, whitespace)
for testing the Medallion Lakehouse DQ and Silver quarantine layers.
"""

import os
import sys
import random
import string
import argparse
from datetime import datetime, date, timedelta
import pandas as pd
from faker import Faker

fake = Faker('en_IN')
Faker.seed(42)
random.seed(42)

REGIONS = {
    'Mumbai': ('Maharashtra', 'West', 'BOM'),
    'Pune': ('Maharashtra', 'West', 'PUN'),
    'Bengaluru': ('Karnataka', 'South', 'BLR'),
    'Chennai': ('Tamil Nadu', 'South', 'MAA'),
    'Hyderabad': ('Telangana', 'South', 'HYD'),
    'Delhi': ('Delhi', 'North', 'DEL'),
    'Noida': ('Uttar Pradesh', 'North', 'NOI'),
    'Kolkata': ('West Bengal', 'East', 'CCU'),
    'Patna': ('Bihar', 'East', 'PAT'),
    'Bhopal': ('Madhya Pradesh', 'Central', 'BHO'),
    'Indore': ('Madhya Pradesh', 'Central', 'IND'),
    'Jaipur': ('Rajasthan', 'North', 'JAI'),
    'Ahmedabad': ('Gujarat', 'West', 'AMD')
}

CITIES = list(REGIONS.keys())

def generate_pan(valid=True):
    if valid:
        letters = ''.join(random.choices(string.ascii_uppercase, k=3))
        entity = random.choice(['P', 'C', 'H', 'F', 'A', 'T', 'B', 'L', 'J', 'G'])
        check_letter = random.choice(string.ascii_uppercase)
        digits = f"{random.randint(1000, 9999)}"
        last = random.choice(string.ascii_uppercase)
        return f"{letters}{entity}{check_letter}{digits}{last}"
    else:
        # Deliberately malformed PAN
        return random.choice([
            "ABCDE1234",      # 9 chars (short)
            "ABCDE12345Z",    # 11 chars (long)
            "12345ABCDE",     # inverted
            "INVALIDPAN#"     # special character
        ])

def generate_phone(valid=True):
    if valid:
        prefix = random.choice(['6', '7', '8', '9'])
        rest = ''.join(random.choices(string.digits, k=9))
        return f"{prefix}{rest}"
    else:
        # Deliberately invalid phone
        return random.choice([
            "1234567890",     # Invalid starting digit
            "987654321",      # 9 digits
            "98765432100",    # 11 digits
            "0000000000"      # All zeros
        ])

def generate_branches(n=15):
    branches = []
    for i in range(1, n + 1):
        city = CITIES[i % len(CITIES)]
        state, region, code_prefix = REGIONS[city]
        branch_id = f"BR_{i:04d}"
        branch_code = f"BP_{code_prefix}_{i:03d}"
        branches.append({
            'branch_id': branch_id,
            'branch_code': branch_code,
            'branch_name': f"BankPulse {city} Main Branch",
            'city': city,
            'state': state,
            'region': region,
            'ifsc_code': f"PULB000{i:04d}",
            'is_active': 1,
            'created_at': '2023-01-01 09:00:00'
        })
    return pd.DataFrame(branches)

def generate_products():
    products = [
        {'product_id': 'PRD_001', 'product_code': 'SAV_REG', 'product_name': 'Regular Savings Account', 'product_category': 'SAVINGS', 'interest_rate_pct': 3.50, 'min_balance_amt': 5000.00, 'is_active': 1},
        {'product_id': 'PRD_002', 'product_code': 'SAV_SAL', 'product_name': 'Corporate Salary Account', 'product_category': 'SAVINGS', 'interest_rate_pct': 4.00, 'min_balance_amt': 0.00, 'is_active': 1},
        {'product_id': 'PRD_003', 'product_code': 'CUR_PRO', 'product_name': 'Business Current Account', 'product_category': 'CURRENT', 'interest_rate_pct': 0.00, 'min_balance_amt': 25000.00, 'is_active': 1},
        {'product_id': 'PRD_004', 'product_code': 'LN_HOME', 'product_name': 'Prime Home Loan', 'product_category': 'HOME_LOAN', 'interest_rate_pct': 8.50, 'min_balance_amt': 0.00, 'is_active': 1},
        {'product_id': 'PRD_005', 'product_code': 'LN_AUTO', 'product_name': 'Express Car Loan', 'product_category': 'AUTO_LOAN', 'interest_rate_pct': 9.25, 'min_balance_amt': 0.00, 'is_active': 1},
        {'product_id': 'PRD_006', 'product_code': 'LN_PERS', 'product_name': 'Instant Personal Loan', 'product_category': 'PERSONAL_LOAN', 'interest_rate_pct': 12.50, 'min_balance_amt': 0.00, 'is_active': 1},
        {'product_id': 'PRD_007', 'product_code': 'CC_PLAT', 'product_name': 'Platinum Credit Card', 'product_category': 'CREDIT_CARD', 'interest_rate_pct': 36.00, 'min_balance_amt': 0.00, 'is_active': 1}
    ]
    df = pd.DataFrame(products)
    df['created_at'] = '2023-01-01 09:00:00'
    return df

def generate_merchants(n=25):
    mccs = [
        ('5411', 'Grocery & Supermarkets'),
        ('5812', 'Restaurants & Dining'),
        ('5541', 'Fuel & Service Stations'),
        ('5311', 'Department Stores'),
        ('5999', 'Miscellaneous Retail'),
        ('4814', 'Telecommunication Services'),
        ('4900', 'Utilities Electric Gas Water'),
        ('5732', 'Electronics & Digital Goods')
    ]
    merchants = []
    for i in range(1, n + 1):
        mcc, cat = random.choice(mccs)
        city = random.choice(CITIES)
        merchants.append({
            'merchant_id': f"MRC_{i:04d}",
            'merchant_name': f"{fake.company()} {cat.split()[0]}",
            'mcc_code': mcc,
            'category_name': cat,
            'city': city,
            'settlement_acct': f"SETTLE_{random.randint(100000000, 999999999)}",
            'is_active': 1,
            'created_at': '2023-01-01 09:00:00'
        })
    return pd.DataFrame(merchants)

def generate_customers(n=500, anomaly_rate=0.03):
    customers = []
    segments = ['RETAIL', 'RETAIL', 'RETAIL', 'HNI', 'SME', 'CORPORATE']
    now = datetime.now()
    
    for i in range(1, n + 1):
        cust_id = f"CUST_{i:06d}"
        city = random.choice(CITIES)
        state, _, _ = REGIONS[city]
        
        # Injected anomaly: invalid PAN or phone or whitespace
        is_bad = random.random() < anomaly_rate
        pan = generate_pan(valid=not is_bad)
        phone = generate_phone(valid=not is_bad)
        
        name = fake.name()
        if random.random() < 0.05:
            # Dirty whitespace and casing
            name = f"  {name.lower()}   "
        
        income = round(random.uniform(250000, 5000000), 2)
        if is_bad and random.random() < 0.2:
            income = -5000.0  # Range anomaly
            
        created = now - timedelta(days=random.randint(100, 700))
        updated = created + timedelta(days=random.randint(0, 90))
        
        customers.append({
            'customer_id': cust_id,
            'full_name': name,
            'pan': pan,
            'phone': phone,
            'email': f"{name.strip().replace(' ', '.').lower()}{random.randint(10,99)}@{fake.free_email_domain()}",
            'address': fake.address().replace('\n', ', '),
            'city': city,
            'state': state,
            'pincode': f"{random.randint(110001, 700099)}",
            'dob': str(date(random.randint(1965, 2003), random.randint(1, 12), random.randint(1, 28))),
            'customer_segment': random.choice(segments),
            'kyc_status': random.choice(['VERIFIED', 'VERIFIED', 'VERIFIED', 'PENDING', 'REJECTED']),
            'annual_income': income,
            'created_at': created.strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': updated.strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(customers)

def generate_accounts(customers_df, branches_df, products_df):
    accounts = []
    acc_idx = 1
    deposit_products = products_df[products_df['product_category'].isin(['SAVINGS', 'CURRENT'])].to_dict('records')
    branch_ids = branches_df['branch_id'].tolist()
    
    for _, cust in customers_df.iterrows():
        # Each customer has 1 or 2 accounts
        num_acc = random.choice([1, 1, 2])
        for _ in range(num_acc):
            prd = random.choice(deposit_products)
            acc_id = f"ACC_{acc_idx:07d}"
            acc_no = f"100{acc_idx:08d}"
            acc_idx += 1
            
            created = datetime.strptime(cust['created_at'], '%Y-%m-%d %H:%M:%S') + timedelta(days=random.randint(0, 10))
            updated = created + timedelta(days=random.randint(0, 60))
            
            accounts.append({
                'account_id': acc_id,
                'customer_id': cust['customer_id'],
                'branch_id': random.choice(branch_ids),
                'product_id': prd['product_id'],
                'account_number': acc_no,
                'account_type': prd['product_category'],
                'account_status': random.choice(['ACTIVE', 'ACTIVE', 'ACTIVE', 'DORMANT']),
                'current_balance': round(random.uniform(5000, 250000), 2),
                'currency': 'INR',
                'opened_date': created.strftime('%Y-%m-%d'),
                'created_at': created.strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': updated.strftime('%Y-%m-%d %H:%M:%S')
            })
    return pd.DataFrame(accounts)

def generate_cards(accounts_df):
    cards = []
    card_idx = 1
    for _, acc in accounts_df.iterrows():
        if random.random() < 0.75:  # 75% of accounts have a card
            card_id = f"CRD_{card_idx:07d}"
            card_no = f"4{random.randint(100, 999)}0000{card_idx:08d}"
            card_type = 'DEBIT' if acc['account_type'] == 'SAVINGS' else random.choice(['DEBIT', 'CREDIT'])
            created = datetime.strptime(acc['created_at'], '%Y-%m-%d %H:%M:%S')
            expiry = created + timedelta(days=365*5)
            
            cards.append({
                'card_id': card_id,
                'account_id': acc['account_id'],
                'customer_id': acc['customer_id'],
                'card_number': card_no,
                'card_type': card_type,
                'card_network': random.choice(['VISA', 'MASTERCARD', 'RUPAY']),
                'credit_limit': 150000.00 if card_type == 'CREDIT' else 0.00,
                'daily_limit': 50000.00,
                'card_status': 'ACTIVE',
                'expiry_date': expiry.strftime('%Y-%m-%d'),
                'issued_date': created.strftime('%Y-%m-%d'),
                'created_at': created.strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': created.strftime('%Y-%m-%d %H:%M:%S')
            })
            card_idx += 1
    return pd.DataFrame(cards)

def generate_loans(customers_df, branches_df, products_df):
    loans = []
    loan_idx = 1
    loan_products = products_df[products_df['product_category'].str.contains('LOAN')].to_dict('records')
    branch_ids = branches_df['branch_id'].tolist()
    
    # 30% of customers have loans
    sample_customers = customers_df.sample(frac=0.35, random_state=42)
    
    for _, cust in sample_customers.iterrows():
        prd = random.choice(loan_products)
        loan_id = f"LN_{loan_idx:06d}"
        loan_idx += 1
        
        tenure = random.choice([12, 24, 36, 60, 120, 240])
        principal = round(random.uniform(50000, 5000000), 2)
        rate = float(prd['interest_rate_pct'])
        r = (rate / 12) / 100
        emi = round(principal * r * ((1 + r)**tenure) / (((1 + r)**tenure) - 1), 2)
        
        disbursed = datetime.now() - timedelta(days=random.randint(60, 600))
        maturity = disbursed + timedelta(days=tenure * 30)
        
        # Simulate realistic DPD distribution for BFSI (most current, some SMA, some NPA > 90)
        rand_dpd = random.random()
        if rand_dpd < 0.82:
            dpd = 0
            status = 'ACTIVE'
        elif rand_dpd < 0.90:
            dpd = random.randint(1, 30)   # SMA-0
            status = 'ACTIVE'
        elif rand_dpd < 0.94:
            dpd = random.randint(31, 60)  # SMA-1
            status = 'ACTIVE'
        elif rand_dpd < 0.97:
            dpd = random.randint(61, 90)  # SMA-2
            status = 'ACTIVE'
        else:
            dpd = random.randint(91, 180) # NPA (>90)
            status = 'DEFAULTED'
            
        loans.append({
            'loan_id': loan_id,
            'customer_id': cust['customer_id'],
            'branch_id': random.choice(branch_ids),
            'product_id': prd['product_id'],
            'principal_amt': principal,
            'interest_rate_pct': rate,
            'tenure_months': tenure,
            'emi_amount': emi,
            'disbursed_date': disbursed.strftime('%Y-%m-%d'),
            'maturity_date': maturity.strftime('%Y-%m-%d'),
            'loan_status': status,
            'days_past_due': dpd,
            'created_at': disbursed.strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(loans)

def generate_loan_repayments(loans_df):
    repayments = []
    rep_idx = 1
    
    for _, loan in loans_df.iterrows():
        # Generate 1 to 6 installments for each loan
        num_installments = min(6, int(loan['tenure_months']))
        disbursed = datetime.strptime(loan['disbursed_date'], '%Y-%m-%d')
        
        for inst_no in range(1, num_installments + 1):
            due_date = disbursed + timedelta(days=inst_no * 30)
            emi = float(loan['emi_amount'])
            principal_comp = round(emi * 0.7, 2)
            interest_comp = round(emi * 0.3, 2)
            
            if loan['days_past_due'] > 60 and inst_no >= num_installments - 1:
                # Overdue installment
                paid_date = None
                payment_status = 'OVERDUE'
                delay_days = loan['days_past_due']
                p_paid = 0.0
                i_paid = 0.0
            else:
                paid_date = (due_date + timedelta(days=random.randint(0, 5))).strftime('%Y-%m-%d')
                payment_status = 'PAID'
                delay_days = max(0, (datetime.strptime(paid_date, '%Y-%m-%d') - due_date).days)
                p_paid = principal_comp
                i_paid = interest_comp
                
            repayments.append({
                'repayment_id': f"REP_{rep_idx:07d}",
                'loan_id': loan['loan_id'],
                'installment_no': inst_no,
                'due_date': due_date.strftime('%Y-%m-%d'),
                'paid_date': paid_date,
                'emi_amount': emi,
                'principal_paid': p_paid,
                'interest_paid': i_paid,
                'payment_status': payment_status,
                'delay_days': delay_days,
                'created_at': due_date.strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            rep_idx += 1
    return pd.DataFrame(repayments)

def generate_core_transactions(accounts_df, n=2000, anomaly_rate=0.03):
    txns = []
    acc_list = accounts_df['account_id'].tolist()
    now = datetime.now()
    
    for i in range(1, n + 1):
        txn_id = f"TXN_{i:08d}"
        acc_id = random.choice(acc_list)
        
        # Injected anomaly: orphan account reference
        is_bad = random.random() < anomaly_rate
        if is_bad and random.random() < 0.25:
            acc_id = "ACC_9999999_ORPHAN"
            
        txn_type = random.choice(['DEBIT', 'CREDIT'])
        mode = random.choice(['NEFT', 'RTGS', 'IMPS', 'ATM', 'BRANCH'])
        amount = round(random.uniform(100.0, 50000.0), 2)
        
        # Injected anomaly: null or negative amount
        if is_bad and random.random() < 0.4:
            amount = -150.00 if random.random() < 0.5 else None
            
        days_ago = random.randint(0, 30)
        txn_ts = now - timedelta(days=days_ago, hours=random.randint(0, 23), minutes=random.randint(0, 59))
        
        # Injected anomaly: future date
        if is_bad and random.random() < 0.2:
            txn_ts = now + timedelta(days=random.randint(2, 10))
            
        txns.append({
            'txn_id': txn_id,
            'account_id': acc_id,
            'txn_ts': txn_ts.strftime('%Y-%m-%d %H:%M:%S'),
            'txn_type': txn_type,
            'txn_mode': mode,
            'amount': amount,
            'currency': 'INR',
            'balance_after': round(random.uniform(5000, 200000), 2),
            'reference_no': f"REF_{i:09d}",
            'description': f"Bank transfer {mode} {txn_type}",
            'status': 'SUCCESS',
            'created_at': txn_ts.strftime('%Y-%m-%d %H:%M:%S')
        })
        
        # Injected anomaly: Duplicate row
        if is_bad and random.random() < 0.3:
            dup_txn = dict(txns[-1])
            txns.append(dup_txn)
            
    return pd.DataFrame(txns)

def generate_card_transactions(cards_df, merchants_df, n=1500, anomaly_rate=0.03):
    txns = []
    card_list = cards_df.to_dict('records')
    mrc_list = merchants_df['merchant_id'].tolist()
    now = datetime.now()
    
    for i in range(1, n + 1):
        c = random.choice(card_list)
        txn_id = f"CTXN_{i:08d}"
        amount = round(random.uniform(50.0, 25000.0), 2)
        
        is_bad = random.random() < anomaly_rate
        if is_bad and random.random() < 0.3:
            amount = 0.0  # Zero/negative range anomaly
            
        days_ago = random.randint(0, 30)
        txn_ts = now - timedelta(days=days_ago, hours=random.randint(0, 23), minutes=random.randint(0, 59))
        
        txns.append({
            'card_txn_id': txn_id,
            'card_number': c['card_number'],
            'customer_id': c['customer_id'],
            'merchant_id': random.choice(mrc_list),
            'amount': amount,
            'currency': 'INR',
            'txn_ts': txn_ts.strftime('%Y-%m-%d %H:%M:%S'),
            'pos_terminal_id': f"POS_{random.randint(1000, 9999)}",
            'channel': 'POS' if random.random() < 0.6 else 'ECOM',
            'status': 'SUCCESS'
        })
    return pd.DataFrame(txns)

def generate_upi_transactions(customers_df, n=1500, anomaly_rate=0.03):
    txns = []
    cust_list = customers_df['customer_id'].tolist()
    now = datetime.now()
    
    for i in range(1, n + 1):
        cust_id = random.choice(cust_list)
        txn_id = f"UPI_{i:08d}"
        amount = round(random.uniform(10.0, 15000.0), 2)
        
        is_bad = random.random() < anomaly_rate
        if is_bad and random.random() < 0.3:
            amount = 500000.0  # Exceeds NPCI 200,000 INR limit
            
        payer_vpa = f"user{random.randint(1000, 9999)}@bankpulse"
        payee_vpa = f"merchant{random.randint(100, 999)}@okhdfcbank"
        
        days_ago = random.randint(0, 30)
        txn_ts = now - timedelta(days=days_ago, hours=random.randint(0, 23), minutes=random.randint(0, 59))
        
        txns.append({
            'upi_txn_id': txn_id,
            'customer_id': cust_id,
            'payer_vpa': payer_vpa,
            'payee_vpa': payee_vpa,
            'amount': amount,
            'currency': 'INR',
            'txn_ts': txn_ts.strftime('%Y-%m-%d %H:%M:%S'),
            'status': 'SUCCESS' if random.random() < 0.95 else 'FAILED',
            'response_code': '00'
        })
    return pd.DataFrame(txns)

def generate_kyc_events(customers_df, n=200):
    events = []
    cust_list = customers_df.to_dict('records')
    now = datetime.now()
    
    for i in range(1, n + 1):
        c = random.choice(cust_list)
        event_id = f"KYCEV_{i:06d}"
        event_ts = now - timedelta(days=random.randint(1, 45))
        
        events.append({
            'event_id': event_id,
            'customer_id': c['customer_id'],
            'pan': c['pan'],
            'event_type': random.choice(['AADHAAR_OTP', 'CKYC_DOWNLOAD', 'BIOMETRIC', 'VIDEO_KYC']),
            'kyc_status': random.choice(['VERIFIED', 'PENDING', 'REJECTED']),
            'verification_agency': random.choice(['UIDAI', 'CERSAI', 'DIGILOCKER']),
            'event_ts': event_ts.strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(events)

def main():
    parser = argparse.ArgumentParser(description="BankPulse Synthetic BFSI Data Generator")
    parser.add_argument("--customers", type=int, default=500, help="Number of customers")
    parser.add_argument("--txns", type=int, default=2500, help="Number of core transactions")
    parser.add_argument("--card-txns", type=int, default=1500, help="Number of card transactions")
    parser.add_argument("--upi-txns", type=int, default=1500, help="Number of UPI transactions")
    parser.add_argument("--output-dir", type=str, default="data", help="Output directory")
    args = parser.parse_args()

    out_dir = os.path.abspath(args.output_dir)
    landing_dir = os.path.join(out_dir, "landing")
    src_dir = os.path.join(out_dir, "src_sql")
    
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(src_dir, exist_ok=True)
    os.makedirs(os.path.join(landing_dir, "CARDS_FILE", "card_transactions"), exist_ok=True)
    os.makedirs(os.path.join(landing_dir, "UPI_FILE", "upi_transactions"), exist_ok=True)
    os.makedirs(os.path.join(landing_dir, "KYC_FILE", "customer_kyc_events"), exist_ok=True)

    print(f"[*] Generating synthetic banking dataset with anomalies...")
    
    branches_df = generate_branches(15)
    products_df = generate_products()
    merchants_df = generate_merchants(25)
    customers_df = generate_customers(args.customers, anomaly_rate=0.03)
    accounts_df = generate_accounts(customers_df, branches_df, products_df)
    cards_df = generate_cards(accounts_df)
    loans_df = generate_loans(customers_df, branches_df, products_df)
    repayments_df = generate_loan_repayments(loans_df)
    txns_df = generate_core_transactions(accounts_df, args.txns, anomaly_rate=0.03)
    
    card_txns_df = generate_card_transactions(cards_df, merchants_df, args.card_txns, anomaly_rate=0.03)
    upi_txns_df = generate_upi_transactions(customers_df, args.upi_txns, anomaly_rate=0.03)
    kyc_events_df = generate_kyc_events(customers_df, 200)

    # Export SQL source entities (CSV/Parquet/DB)
    branches_df.to_csv(os.path.join(src_dir, "branches.csv"), index=False)
    products_df.to_csv(os.path.join(src_dir, "products.csv"), index=False)
    merchants_df.to_csv(os.path.join(src_dir, "merchants.csv"), index=False)
    customers_df.to_csv(os.path.join(src_dir, "customers.csv"), index=False)
    accounts_df.to_csv(os.path.join(src_dir, "accounts.csv"), index=False)
    cards_df.to_csv(os.path.join(src_dir, "cards.csv"), index=False)
    loans_df.to_csv(os.path.join(src_dir, "loans.csv"), index=False)
    repayments_df.to_csv(os.path.join(src_dir, "loan_repayments.csv"), index=False)
    txns_df.to_csv(os.path.join(src_dir, "transactions.csv"), index=False)

    # Export landing CSV drops (daily payment drops)
    today_str = datetime.now().strftime('%Y-%m-%d')
    card_txns_df.to_csv(os.path.join(landing_dir, "CARDS_FILE", "card_transactions", f"card_transactions_{today_str}.csv"), index=False)
    upi_txns_df.to_csv(os.path.join(landing_dir, "UPI_FILE", "upi_transactions", f"upi_transactions_{today_str}.csv"), index=False)
    kyc_events_df.to_csv(os.path.join(landing_dir, "KYC_FILE", "customer_kyc_events", f"kyc_events_{today_str}.csv"), index=False)

    print(f"[+] Successfully generated:")
    print(f"    - Branches: {len(branches_df)} rows")
    print(f"    - Products: {len(products_df)} rows")
    print(f"    - Merchants: {len(merchants_df)} rows")
    print(f"    - Customers: {len(customers_df)} rows (with PII & deliberate anomalies)")
    print(f"    - Accounts: {len(accounts_df)} rows")
    print(f"    - Cards: {len(cards_df)} rows")
    print(f"    - Loans: {len(loans_df)} rows (with SMA/NPA delinquencies)")
    print(f"    - Repayments: {len(repayments_df)} rows")
    print(f"    - Core Transactions: {len(txns_df)} rows (with injected duplicates & nulls)")
    print(f"    - Landing Card Transactions: {len(card_txns_df)} rows")
    print(f"    - Landing UPI Transactions: {len(upi_txns_df)} rows")
    print(f"    - Landing KYC Events: {len(kyc_events_df)} rows")
    print(f"[+] Output saved to {out_dir}")

if __name__ == "__main__":
    main()
