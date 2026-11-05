"""
Data quality pipeline (Lakeflow Spark Declarative Pipelines, Python)

What it does
------------
1. Holds the rule catalogue in ONE place (RULES) and publishes it as metadata.dq_rule.
2. For every bronze table builds a "checked" materialized view with Lakeflow
   expectations (dp.expect_all = warn: rows are kept, pass/fail metrics go to the
   pipeline event log, from where operations.dq_result is loaded).
3. Builds the explicit quarantine flow quarantine.invalid_record: one row per
   (invalid record, failed rule) with source table, record key, raw payload,
   rule id/version, failure reason and quarantine time.

Why a separate pipeline
-----------------------
Expectations are NOT supported on AUTO CDC FROM SNAPSHOT flows (used by bronze
and silver), so the checks run on their own datasets that read the CURRENT rows
of the bronze SCD2 tables (__END_AT IS NULL).

Pipeline settings
-----------------
serverless, default catalog = databricks_demo, default schema = operations,
and Advanced settings > event log published to
databricks_demo.operations.dq_pipeline_event_log (used by dq_operations.sql).

NOT IN THIS FILE (see dq_operations.sql): operations.dq_result,
operations.batch_control, quarantine history. pipeline_update_id is not written
here because reading the update id inside pipeline code is not verified; the
SQL job takes it from the event log.

ASSUMPTIONS TO CONFIRM
  * fully qualified dataset names (databricks_demo.quarantine.invalid_record)
    are accepted for publishing to other schemas - otherwise remove the prefix
  * thresholds in RULES are placeholders to agree with the business
"""
from pyspark import pipelines as dp

CATALOG = "databricks_demo"
BRONZE = f"{CATALOG}.bronze"
RULESET_VERSION = "DQ_RULES_V1"

TXN_TYPES = ["deposit", "withdrawal", "investment", "dividend"]
STRATEGY_TYPES = ["growth", "income", "value", "speculative"]
RISK_LEVELS = ["low", "medium", "high"]
HORIZON = (1, 30)
EXPECTED_RETURN = (0.01, 0.20)


def in_list(col, values):
    return f"{col} IN ({', '.join(repr(v) for v in values)})"


# table -> primary key and raw columns (used for the quarantine payload)
TABLES = {
    "users": {"key": "user_id",
              "cols": ["user_id", "full_name", "email", "created_at", "credit_score"]},
    "budgeting": {"key": "budget_id",
                  "cols": ["budget_id", "user_id", "monthly_income", "monthly_expenditure", "savings_rate"]},
    "portfolios": {"key": "portfolio_id",
                   "cols": ["portfolio_id", "user_id", "equity_allocation",
                            "fixed_income_allocation", "alternative_assets"]},
    "investment_strategies": {"key": "strategy_id",
                              "cols": ["strategy_id", "portfolio_id", "strategy_type", "risk_tolerance",
                                       "investment_horizon_years", "expected_return_rate"]},
    "transactions": {"key": "transaction_id",
                     "cols": ["transaction_id", "user_id", "amount", "transaction_date", "type"]},
}

# helper columns (_pk_count, _user_exists, _portfolio_exists) are added in prepared_sql()
RULES = []


def rule(table, rule_id, name, constraint, reason, version="1"):
    RULES.append({"table": table, "rule_id": rule_id, "name": name, "constraint": constraint,
                  "reason": reason, "rule_version": version})


# users
rule("users", "U001", "pk_not_null", "user_id IS NOT NULL", "Missing primary key")
rule("users", "U002", "pk_unique", "_pk_count = 1", "Duplicate primary key")
rule("users", "U003", "credit_score_range", "credit_score BETWEEN 300 AND 850", "Credit score outside 300-850")
rule("users", "U004", "created_not_future", "created_at <= current_timestamp()", "Future source timestamp")
# budgeting
rule("budgeting", "B001", "pk_not_null", "budget_id IS NOT NULL", "Missing primary key")
rule("budgeting", "B002", "pk_unique", "_pk_count = 1", "Duplicate primary key")
rule("budgeting", "B003", "user_exists", "_user_exists", "Budget references a missing user")
rule("budgeting", "B004", "income_positive", "monthly_income > 0", "Nonpositive income")
# portfolios
rule("portfolios", "P001", "pk_not_null", "portfolio_id IS NOT NULL", "Missing primary key")
rule("portfolios", "P002", "pk_unique", "_pk_count = 1", "Duplicate primary key")
rule("portfolios", "P003", "user_exists", "_user_exists", "Portfolio references a missing user")
rule("portfolios", "P004", "allocation_components_0_1",
     "equity_allocation BETWEEN 0 AND 1 AND fixed_income_allocation BETWEEN 0 AND 1 "
     "AND alternative_assets BETWEEN 0 AND 1", "Allocation component outside 0-1")
rule("portfolios", "P005", "allocation_total_not_zero",
     "(equity_allocation + fixed_income_allocation + alternative_assets) <> 0", "Zero allocation total")
# investment_strategies
rule("investment_strategies", "S001", "pk_not_null", "strategy_id IS NOT NULL", "Missing primary key")
rule("investment_strategies", "S002", "pk_unique", "_pk_count = 1", "Duplicate primary key")
rule("investment_strategies", "S003", "portfolio_exists", "_portfolio_exists",
     "Strategy references a missing portfolio")
rule("investment_strategies", "S004", "strategy_type_supported", in_list("strategy_type", STRATEGY_TYPES),
     "Unsupported strategy type")
rule("investment_strategies", "S005", "risk_supported", in_list("risk_tolerance", RISK_LEVELS),
     "Unsupported risk tolerance")
rule("investment_strategies", "S006", "horizon_range",
     f"investment_horizon_years BETWEEN {HORIZON[0]} AND {HORIZON[1]}", "Invalid investment horizon")
rule("investment_strategies", "S007", "expected_return_range",
     f"expected_return_rate BETWEEN {EXPECTED_RETURN[0]} AND {EXPECTED_RETURN[1]}",
     "Expected return outside valid range")
# transactions
rule("transactions", "T001", "pk_not_null", "transaction_id IS NOT NULL", "Missing primary key")
rule("transactions", "T002", "pk_unique", "_pk_count = 1", "Duplicate primary key")
rule("transactions", "T003", "user_exists", "_user_exists", "Transaction references a missing user")
rule("transactions", "T004", "amount_positive", "amount > 0", "Nonpositive transaction amount")
rule("transactions", "T005", "type_supported", in_list("type", TXN_TYPES), "Unsupported transaction type")
rule("transactions", "T006", "timestamp_not_future", "transaction_date <= current_timestamp()",
     "Future source timestamp")


def cur(table):
    return f"(SELECT * FROM {BRONZE}.{table}_history WHERE __END_AT IS NULL)"


def prepared_sql(table):
    """Current bronze rows plus helper columns used by the rules."""
    key = TABLES[table]["key"]
    pk = f"count(*) OVER (PARTITION BY t.{key}) AS _pk_count"
    if table == "users":
        return f"SELECT t.*, {pk} FROM {cur('users')} t"
    if table == "investment_strategies":
        return (f"SELECT t.*, {pk}, (p.portfolio_id IS NOT NULL) AS _portfolio_exists "
                f"FROM {cur(table)} t LEFT JOIN {cur('portfolios')} p ON t.portfolio_id = p.portfolio_id")
    return (f"SELECT t.*, {pk}, (u.user_id IS NOT NULL) AS _user_exists "
            f"FROM {cur(table)} t LEFT JOIN {cur('users')} u ON t.user_id = u.user_id")


def failures_expr(table):
    """Array of structs, one per failed rule. A NULL constraint result counts as failed."""
    parts = []
    for r in RULES:
        if r["table"] != table:
            continue
        reason = r["reason"].replace("'", "''")
        parts.append(
            f"CASE WHEN NOT coalesce({r['constraint']}, false) THEN "
            f"named_struct('rule_id', '{r['rule_id']}', 'rule_version', '{r['rule_version']}', "
            f"'failure_reason', '{reason}') END"
        )
    return f"filter(array({', '.join(parts)}), x -> x IS NOT NULL)"


# --------------------------------------------------------------------------
# 1. Rule catalogue -> metadata.dq_rule_bronze
# --------------------------------------------------------------------------
@dp.materialized_view(name=f"{CATALOG}.metadata.dq_rule_bronze", comment="Data quality rule catalogue")
def dq_rule_bronze():
    rows = [(r["rule_id"], r["rule_version"], RULESET_VERSION, r["table"], r["name"],
             r["constraint"], r["reason"], "WARN_AND_QUARANTINE", True) for r in RULES]
    return spark.createDataFrame(  # noqa: F821
        rows,
        "rule_id string, rule_version string, ruleset_version string, source_table string, "
        "rule_name string, constraint_sql string, failure_reason string, action string, is_active boolean",
    )


# --------------------------------------------------------------------------
# 2. Checked datasets with expectations (warn: keep rows, record metrics)
# --------------------------------------------------------------------------
def define_checked(table):
    expectations = {f"{r['rule_id']}_{r['name']}": r["constraint"] for r in RULES if r["table"] == table}

    @dp.materialized_view(name=f"dq_checked_bronze_{table}",
                          comment=f"Current bronze rows of {table} with expectation metrics")
    @dp.expect_all(expectations)
    def checked():
        return spark.sql(prepared_sql(table))  # noqa: F821


for _t in TABLES:
    define_checked(_t)


# --------------------------------------------------------------------------
# 3. Explicit quarantine flow -> quarantine.invalid_record_bronze (current invalid records)
# --------------------------------------------------------------------------
def quarantine_df(table):
    key = TABLES[table]["key"]
    payload_cols = ", ".join(TABLES[table]["cols"] + ["__START_AT"])
    return (
        spark.read.table(f"dq_checked_bronze_{table}")  # noqa: F821
        .selectExpr("*", f"{failures_expr(table)} AS _failures")
        .selectExpr("*", "explode(_failures) AS _f")
        .selectExpr(
            f"'{table}' AS source_table",
            f"CAST({key} AS STRING) AS record_key",
            f"to_json(struct({payload_cols})) AS raw_payload",
            "_f.rule_id AS rule_id",
            "_f.rule_version AS rule_version",
            "_f.failure_reason AS failure_reason",
            "current_timestamp() AS quarantine_time",
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.quarantine.invalid_record_bronze",
    comment="One row per invalid current record and failed rule. Current state; history is kept by dq_operations.sql",
)
def invalid_record_bronze():
    out = None
    for t in TABLES:
        df = quarantine_df(t)
        out = df if out is None else out.unionByName(df)
    return out