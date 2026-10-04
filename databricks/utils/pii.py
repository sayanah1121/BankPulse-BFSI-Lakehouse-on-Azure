"""
BankPulse Utility: PII Protection Framework
Implements cryptographic salted hashing and masking for Indian BFSI data
(PAN, Phone, Email, Card Numbers, Account Numbers, UPI VPAs).
"""

import os
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

DEFAULT_SALT = os.environ.get("BP_PII_SALT", "BankPulseSecureSalt2026!#")

def hash_salted(col_name, salt=DEFAULT_SALT):
    """
    Computes SHA-256 salted hash of a column for joinable pseudonymization.
    """
    return F.when(
        F.col(col_name).isNotNull(),
        F.sha2(F.concat(F.col(col_name).cast("string"), F.lit(salt)), 256)
    ).otherwise(None)

def mask_card_or_account(col_name):
    """
    Preserves the last 4 digits and masks the preceding digits with 'X'.
    e.g. 1000000001 -> XXXXXX0001, 411100001234 -> XXXXXXXX1234
    """
    col_str = F.col(col_name).cast("string")
    col_len = F.length(col_str)
    
    return F.when(
        col_str.isNull(), None
    ).when(
        col_len <= 4, col_str
    ).otherwise(
        F.concat(
            F.expr(f"repeat('X', length({col_name}) - 4)"),
            F.substring(col_str, -4, 4)
        )
    )

def mask_phone_number(col_name):
    """
    Masks Indian 10-digit mobile number to 'XXXXXX1234'.
    """
    col_str = F.col(col_name).cast("string")
    return F.when(
        col_str.isNull(), None
    ).when(
        F.length(col_str) == 10,
        F.concat(F.lit("XXXXXX"), F.substring(col_str, 7, 4))
    ).otherwise(
        mask_card_or_account(col_name)
    )

def mask_email_address(col_name):
    """
    Masks user part of email, preserving domain:
    e.g. john.doe@example.com -> j***@example.com
    """
    col_str = F.col(col_name).cast("string")
    split_user = F.split(col_str, "@").getItem(0)
    domain_part = F.split(col_str, "@").getItem(1)
    
    first_char = F.substring(split_user, 1, 1)
    return F.when(
        col_str.isNull(), None
    ).when(
        F.instr(col_str, "@") > 0,
        F.concat(first_char, F.lit("***@"), domain_part)
    ).otherwise(F.lit("***@masked.com"))

def mask_generic_text(col_name):
    """
    Masks text/names showing initial letter only.
    """
    col_str = F.col(col_name).cast("string")
    return F.when(
        col_str.isNull(), None
    ).otherwise(
        F.concat(F.substring(col_str, 1, 1), F.lit("*** REDACTED"))
    )

def apply_pii_transformations(df, pii_columns_str, pii_methods_str, salt=DEFAULT_SALT):
    """
    Parses PII configuration strings and applies appropriate masking/hashing.
    e.g.
      pii_columns_str = "pan,phone,email"
      pii_methods_str = "pan:HASH,phone:MASK,email:MASK"
    """
    if not pii_columns_str or not pii_methods_str:
        return df

    # Parse method mappings
    methods = {}
    for item in pii_methods_str.split(","):
        if ":" in item:
            col, method = item.split(":", 1)
            methods[col.strip()] = method.strip().upper()

    transformed_df = df
    for col in [c.strip() for c in pii_columns_str.split(",") if c.strip()]:
        if col not in transformed_df.columns:
            continue
            
        method = methods.get(col, "MASK")
        
        if method == "HASH":
            # For PAN or VPA, store hashed version
            transformed_df = transformed_df.withColumn(col, hash_salted(col, salt))
        elif method == "MASK":
            if "phone" in col.lower():
                transformed_df = transformed_df.withColumn(col, mask_phone_number(col))
            elif "email" in col.lower():
                transformed_df = transformed_df.withColumn(col, mask_email_address(col))
            elif any(k in col.lower() for k in ["account", "card", "acc_no"]):
                transformed_df = transformed_df.withColumn(col, mask_card_or_account(col))
            else:
                transformed_df = transformed_df.withColumn(col, mask_generic_text(col))
        elif method == "TOKENIZE":
            transformed_df = transformed_df.withColumn(col, F.concat(F.lit("TKN_"), hash_salted(col, salt).substr(1, 16)))
            
    return transformed_df
