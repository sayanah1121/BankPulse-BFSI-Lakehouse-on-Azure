"""
BankPulse Utility: Data Quality Engine
Evaluates configurable rules from meta.dq_rules against Spark DataFrames.
Separates passing records from quarantined records and generates detailed pass/fail audit stats.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

class DQEngine:
    def __init__(self, rules):
        """
        rules: list of dicts with keys:
          rule_id, column_name, rule_type, rule_param, severity, is_active
        """
        self.rules = [r for r in rules if r.get('is_active', 1)]

    @staticmethod
    def parse_params(param_str):
        if not param_str:
            return {}
        params = {}
        for part in param_str.split(";"):
            if "=" in part:
                k, v = part.split("=", 1)
                params[k.strip()] = v.strip()
        return params

    def build_condition(self, col_name, rule_type, param_str):
        """
        Returns a PySpark Column expression that evaluates to TRUE if valid, FALSE if invalid.
        """
        params = self.parse_params(param_str)
        c = F.col(col_name)

        if rule_type == "NOT_NULL":
            return c.isNotNull() & (F.trim(c.cast("string")) != "")

        elif rule_type == "RANGE":
            min_val = float(params.get("min", "-inf"))
            max_val = float(params.get("max", "inf"))
            num_c = c.cast("double")
            return num_c.isNotNull() & (num_c >= min_val) & (num_c <= max_val)

        elif rule_type == "REGEX":
            pattern = params.get("pattern", ".*")
            return c.isNotNull() & (c.cast("string").rlike(pattern))

        elif rule_type == "ALLOWED_VALUES":
            vals = [v.strip() for v in params.get("values", "").split(",") if v.strip()]
            return c.cast("string").isin(vals)

        elif rule_type == "DATE_NOT_FUTURE":
            # Column timestamp <= current timestamp
            return c.isNotNull() & (c.cast("timestamp") <= F.current_timestamp())

        elif rule_type == "UNIQUE":
            # Checked separately via window/aggregation
            return F.lit(True)

        return F.lit(True)

    def evaluate(self, df, entity_name, batch_id):
        """
        Evaluates rules against DataFrame.
        Returns:
            valid_df: Records passing all ERROR severity checks
            quarantine_df: Records failing any ERROR check, with audit rejection columns
            rule_stats: List of dicts with execution metrics per rule
        """
        total_rows = df.count()
        if total_rows == 0:
            return df, df.filter("1=0").withColumn("_reject_reason", F.lit("")).withColumn("_rule_id", F.lit(0)).withColumn("_batch_id", F.lit(batch_id)), []

        rule_stats = []
        error_conditions = []
        error_reasons = []

        # Check UNIQUE rules first by marking duplicate rows
        annotated_df = df
        for r in self.rules:
            if r['rule_type'] == 'UNIQUE' and r['column_name'] in annotated_df.columns:
                col_name = r['column_name']
                w = Window.partitionBy(col_name)
                annotated_df = annotated_df.withColumn(f"_cnt_{col_name}", F.count(F.lit(1)).over(w))
                
                # Check how many failed
                dup_count = annotated_df.filter(F.col(f"_cnt_{col_name}") > 1).count()
                pass_rate = round(((total_rows - dup_count) / total_rows) * 100.0, 3)
                
                rule_stats.append({
                    'batch_id': batch_id,
                    'entity_name': entity_name,
                    'rule_id': r.get('rule_id', 0),
                    'rule_type': r['rule_type'],
                    'column_name': col_name,
                    'severity': r.get('severity', 'ERROR'),
                    'rows_checked': total_rows,
                    'rows_failed': dup_count,
                    'pass_pct': pass_rate
                })
                
                if r.get('severity', 'ERROR') == 'ERROR':
                    fail_cond = F.col(f"_cnt_{col_name}") > 1
                    error_conditions.append(fail_cond)
                    error_reasons.append(
                        F.when(fail_cond, F.lit(f"Rule {r.get('rule_id', 0)} Failed: Duplicate key in {col_name}")).otherwise(None)
                    )

        # Evaluate standard rules
        for r in self.rules:
            if r['rule_type'] == 'UNIQUE':
                continue
                
            col_name = r['column_name']
            if col_name not in annotated_df.columns:
                continue

            valid_cond = self.build_condition(col_name, r['rule_type'], r.get('rule_param'))
            fail_cond = ~valid_cond

            # Calculate failure count for this specific rule
            failed_count = annotated_df.filter(fail_cond).count()
            pass_rate = round(((total_rows - failed_count) / total_rows) * 100.0, 3) if total_rows > 0 else 100.0

            rule_stats.append({
                'batch_id': batch_id,
                'entity_name': entity_name,
                'rule_id': r.get('rule_id', 0),
                'rule_type': r['rule_type'],
                'column_name': col_name,
                'severity': r.get('severity', 'ERROR'),
                'rows_checked': total_rows,
                'rows_failed': failed_count,
                'pass_pct': pass_rate
            })

            if r.get('severity', 'ERROR') == 'ERROR':
                error_conditions.append(fail_cond)
                error_reasons.append(
                    F.when(fail_cond, F.lit(f"Rule {r.get('rule_id', 0)} Failed: {r['rule_type']} check on {col_name}")).otherwise(None)
                )

        clean_base_cols = [c for c in annotated_df.columns if not c.startswith("_cnt_") and c not in ["_reject_reason", "_rule_id", "_quarantine_ts", "_batch_id", "_dq_status"]]

        if not error_conditions:
            # All passed
            valid_df = (
                annotated_df.withColumn("_batch_id", F.lit(batch_id))
                            .withColumn("_dq_status", F.lit("PASS"))
                            .select(*clean_base_cols, "_batch_id", "_dq_status")
            )
            quarantine_df = (
                annotated_df.filter("1=0")
                            .withColumn("_reject_reason", F.lit(""))
                            .withColumn("_rule_id", F.lit(0))
                            .withColumn("_batch_id", F.lit(batch_id))
                            .withColumn("_quarantine_ts", F.current_timestamp())
                            .select(*clean_base_cols, "_reject_reason", "_rule_id", "_batch_id", "_quarantine_ts")
            )
            return valid_df, quarantine_df, rule_stats

        # Combine any error conditions with OR
        combined_error = error_conditions[0]
        for cond in error_conditions[1:]:
            combined_error = combined_error | cond

        # Combine reject reasons with comma
        reject_reason_col = F.concat_ws("; ", *[F.coalesce(r, F.lit("")) for r in error_reasons])

        # Quarantine DataFrame
        quarantine_df = (
            annotated_df.filter(combined_error)
            .withColumn("_reject_reason", reject_reason_col)
            .withColumn("_rule_id", F.lit(0))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_quarantine_ts", F.current_timestamp())
            .select(*clean_base_cols, "_reject_reason", "_rule_id", "_batch_id", "_quarantine_ts")
        )

        # Valid DataFrame
        valid_df = (
            annotated_df.filter(~combined_error)
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_dq_status", F.lit("PASS"))
            .select(*clean_base_cols, "_batch_id", "_dq_status")
        )

        return valid_df, quarantine_df, rule_stats
