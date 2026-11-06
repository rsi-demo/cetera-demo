"""
Silver data quality pipeline (Lakeflow Spark Declarative Pipelines, Python)

Checks the four silver tables written by silver_pipeline.py:
  * keys           : not null, unique (composite keys included)
  * referential    : silver -> bronze (user, budget, portfolio, strategy,
                     transaction exist in the CURRENT bronze rows) and
                     silver -> silver (users exist in client_financial_profile,
                     daily activity rows exist in transaction_fact)
  * derived logic  : credit band, surplus, savings rate, normalized allocations,
                     type amounts, external cash flow, daily aggregates
  * reconciliation : bronze <-> silver completeness and orphan counts
                     (operations.recon_silver, one row per check)
Failed derived records are written to quarantine.invalid_record_silver. The rows
stay in silver (silver flags, it does not reject); quarantine is evidence.

Pipeline settings: serverless, default catalog = databricks_demo, default
schema = operations, event log published to
databricks_demo.operations.dq_silver_event_log.
Optional parameter dq.enforce_reconciliation = true makes a failed
reconciliation fail the update (use it to gate downstream tasks).

ASSUMPTIONS TO CONFIRM: fully qualified names for other schemas are accepted;
thresholds/tolerances are placeholders; credit bands must match silver_pipeline.py.
"""
from pyspark import pipelines as dp

CATALOG = "databricks_demo"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
LAYER = "silver"
PREFIX = "silver"
ENFORCE = spark.conf.get("dq.enforce_reconciliation", "false") == "true"  # noqa: F821
EXPECT = dp.expect_all_or_fail if ENFORCE else dp.expect_all

CREDIT_BANDS = [(300, 579, "POOR"), (580, 669, "FAIR"), (670, 739, "GOOD"),
                (740, 799, "VERY_GOOD"), (800, 850, "EXCEPTIONAL")]  # keep in sync with silver


def band_sql(col):
    parts = [f"WHEN {col} IS NULL THEN 'UNKNOWN'", f"WHEN {col} < 300 OR {col} > 850 THEN 'INVALID'"]
    parts += [f"WHEN {col} BETWEEN {lo} AND {hi} THEN '{n}'" for lo, hi, n in CREDIT_BANDS]
    return "CASE " + " ".join(parts) + " ELSE 'INVALID' END"


def bkeys(table, key):
    """Distinct keys of the CURRENT bronze rows."""
    return f"(SELECT DISTINCT {key} FROM {BRONZE}.{table}_history WHERE __END_AT IS NULL)"


KEYS = {
    "client_financial_profile": ["user_id", "budget_key"],
    "portfolio_strategy": ["portfolio_id", "strategy_key"],
    "transaction_fact": ["transaction_id"],
    "client_daily_activity": ["activity_date", "user_id"],
}

# ---- prepared datasets: silver rows + payload + helper columns for the rules ----
PREPARED = {
    "client_financial_profile": f"""
        SELECT to_json(struct(t.*)) AS _payload, t.*,
               count(*) OVER (PARTITION BY t.user_id, t.budget_key) AS _pk_count,
               (bu.user_id IS NOT NULL)   AS _user_in_bronze,
               (bb.budget_id IS NOT NULL) AS _budget_in_bronze
        FROM {SILVER}.client_financial_profile t
        LEFT JOIN {bkeys('users', 'user_id')} bu     ON t.user_id = bu.user_id
        LEFT JOIN {bkeys('budgeting', 'budget_id')} bb ON t.budget_id = bb.budget_id""",
    "portfolio_strategy": f"""
        SELECT to_json(struct(t.*)) AS _payload, t.*,
               count(*) OVER (PARTITION BY t.portfolio_id, t.strategy_key) AS _pk_count,
               (bp.portfolio_id IS NOT NULL) AS _portfolio_in_bronze,
               (bs.strategy_id  IS NOT NULL) AS _strategy_in_bronze,
               (sp.user_id      IS NOT NULL) AS _user_in_profile
        FROM {SILVER}.portfolio_strategy t
        LEFT JOIN {bkeys('portfolios', 'portfolio_id')} bp ON t.portfolio_id = bp.portfolio_id
        LEFT JOIN {bkeys('investment_strategies', 'strategy_id')} bs ON t.strategy_id = bs.strategy_id
        LEFT JOIN (SELECT DISTINCT user_id FROM {SILVER}.client_financial_profile) sp ON t.user_id = sp.user_id""",
    "transaction_fact": f"""
        SELECT to_json(struct(t.*)) AS _payload, t.*,
               count(*) OVER (PARTITION BY t.transaction_id) AS _pk_count,
               (bt.transaction_id IS NOT NULL) AS _in_bronze,
               (sp.user_id IS NOT NULL)        AS _user_in_profile
        FROM {SILVER}.transaction_fact t
        LEFT JOIN {bkeys('transactions', 'transaction_id')} bt ON t.transaction_id = bt.transaction_id
        LEFT JOIN (SELECT DISTINCT user_id FROM {SILVER}.client_financial_profile) sp ON t.user_id = sp.user_id""",
    "client_daily_activity": f"""
        SELECT to_json(struct(t.*)) AS _payload, t.*,
               count(*) OVER (PARTITION BY t.activity_date, t.user_id) AS _pk_count,
               (sp.user_id IS NOT NULL) AS _user_in_profile,
               (e.user_id  IS NOT NULL) AS _in_fact,
               e.exp_count AS _exp_count, e.exp_amount AS _exp_amount, e.exp_net AS _exp_net
        FROM {SILVER}.client_daily_activity t
        LEFT JOIN (SELECT DISTINCT user_id FROM {SILVER}.client_financial_profile) sp ON t.user_id = sp.user_id
        LEFT JOIN (SELECT transaction_date AS d, user_id, count(*) AS exp_count,
                          sum(amount) AS exp_amount, sum(external_cash_flow) AS exp_net
                   FROM {SILVER}.transaction_fact WHERE is_valid GROUP BY transaction_date, user_id) e
               ON t.activity_date = e.d AND t.user_id = e.user_id""",
}

RULES = []


def rule(ds, rule_id, name, constraint, reason, version="1"):
    RULES.append({"dataset": ds, "rule_id": rule_id, "name": name, "constraint": constraint,
                  "reason": reason, "rule_version": version})


P, S, T, D = "client_financial_profile", "portfolio_strategy", "transaction_fact", "client_daily_activity"
# client_financial_profile
rule(P, "SP01", "pk_not_null", "user_id IS NOT NULL AND budget_key IS NOT NULL", "Missing key")
rule(P, "SP02", "pk_unique", "_pk_count = 1", "Duplicate (user_id, budget_key)")
rule(P, "SP03", "ri_user_in_bronze", "_user_in_bronze", "User not found in current bronze users")
rule(P, "SP04", "ri_budget_in_bronze", "budget_id IS NULL OR _budget_in_bronze",
     "Budget not found in current bronze budgeting")
rule(P, "SP05", "credit_band_consistent", f"credit_score_band = {band_sql('credit_score')}",
     "Credit score band does not match credit score")
rule(P, "SP06", "surplus_consistent",
     "monthly_surplus IS NULL OR abs(monthly_surplus - (monthly_income - monthly_expenditure)) < 0.005",
     "Monthly surplus is not income minus expenditure")
rule(P, "SP07", "savings_rate_consistent",
     "CASE WHEN monthly_income IS NULL OR monthly_income = 0 THEN calculated_savings_rate IS NULL "
     "ELSE abs(calculated_savings_rate - monthly_surplus / monthly_income) < 0.001 END",
     "Calculated savings rate is inconsistent")
rule(P, "SP08", "budget_link_consistent", "(budget_row_count = 0) = (budget_id IS NULL)",
     "Budget row count and budget_id disagree")
# portfolio_strategy
rule(S, "SS01", "pk_not_null", "portfolio_id IS NOT NULL AND strategy_key IS NOT NULL", "Missing key")
rule(S, "SS02", "pk_unique", "_pk_count = 1", "Duplicate (portfolio_id, strategy_key)")
rule(S, "SS03", "ri_portfolio_in_bronze", "_portfolio_in_bronze", "Portfolio not found in current bronze portfolios")
rule(S, "SS04", "ri_strategy_in_bronze", "strategy_id IS NULL OR _strategy_in_bronze",
     "Strategy not found in current bronze strategies")
rule(S, "SS05", "ri_user_in_profile", "_user_in_profile", "User not found in silver client_financial_profile")
rule(S, "SS06", "raw_total_consistent",
     "raw_allocation_total IS NULL OR abs(raw_allocation_total - (raw_equity_allocation + "
     "raw_fixed_income_allocation + raw_alternative_allocation)) < 0.005", "Raw allocation total is wrong")
rule(S, "SS07", "normalized_sums_to_1",
     "raw_allocation_total IS NULL OR raw_allocation_total = 0 OR abs(normalized_equity_allocation + "
     "normalized_fixed_income_allocation + normalized_alternative_allocation - 1) < 0.001",
     "Normalized allocations do not sum to 1")
rule(S, "SS08", "strategy_domain_valid", "strategy_domain_valid", "Strategy fields outside supported domain")
# transaction_fact
rule(T, "ST01", "pk_not_null", "transaction_id IS NOT NULL", "Missing primary key")
rule(T, "ST02", "pk_unique", "_pk_count = 1", "Duplicate primary key")
rule(T, "ST03", "ri_in_bronze", "_in_bronze", "Transaction not found in current bronze transactions")
rule(T, "ST04", "ri_user_in_profile", "_user_in_profile", "User not found in silver client_financial_profile")
rule(T, "ST05", "type_amounts_consistent",
     "abs(deposit_amount + withdrawal_amount + investment_amount + dividend_amount - amount) < 0.005",
     "Type-specific amounts do not add up to amount")
rule(T, "ST06", "external_cash_flow_formula",
     "abs(external_cash_flow - (deposit_amount + dividend_amount - withdrawal_amount)) < 0.005",
     "External cash flow is not deposit + dividend - withdrawal")
rule(T, "ST07", "date_matches_timestamp", "transaction_date = CAST(transaction_timestamp AS DATE)",
     "Transaction date does not match timestamp")
rule(T, "ST08", "is_valid", "is_valid", "Transaction flagged invalid in silver")
# client_daily_activity
rule(D, "SD01", "pk_not_null", "activity_date IS NOT NULL AND user_id IS NOT NULL", "Missing key")
rule(D, "SD02", "pk_unique", "_pk_count = 1", "Duplicate (activity_date, user_id)")
rule(D, "SD03", "ri_user_in_profile", "_user_in_profile", "User not found in silver client_financial_profile")
rule(D, "SD04", "ri_in_transaction_fact", "_in_fact", "No valid transactions in transaction_fact for this row")
rule(D, "SD05", "matches_transaction_fact",
     "transaction_count = _exp_count AND abs(gross_activity_amount - _exp_amount) < 0.005 "
     "AND abs(net_external_cash_flow - _exp_net) < 0.005", "Aggregates differ from transaction_fact")
rule(D, "SD06", "counts_add_up",
     "transaction_count = deposit_count + withdrawal_count + investment_count + dividend_count",
     "Type counts do not add up")
rule(D, "SD07", "net_flow_formula",
     "abs(net_external_cash_flow - (deposit_amount + dividend_amount - withdrawal_amount)) < 0.005",
     "Net external cash flow formula broken")


def failures_expr(ds):
    parts = []
    for r in RULES:
        if r["dataset"] == ds:
            reason = r["reason"].replace("'", "''")
            parts.append(f"CASE WHEN NOT coalesce({r['constraint']}, false) THEN named_struct("
                         f"'rule_id', '{r['rule_id']}', 'rule_version', '{r['rule_version']}', "
                         f"'failure_reason', '{reason}') END")
    return f"filter(array({', '.join(parts)}), x -> x IS NOT NULL)"


# ---- rule catalogue ----
@dp.materialized_view(name=f"{CATALOG}.metadata.dq_rule_silver", comment="Silver data quality rule catalogue")
def dq_rule_silver():
    rows = [(r["rule_id"], r["rule_version"], LAYER, r["dataset"], r["name"], r["constraint"], r["reason"])
            for r in RULES]
    return spark.createDataFrame(  # noqa: F821
        rows, "rule_id string, rule_version string, layer string, source_table string, rule_name string, "
              "constraint_sql string, failure_reason string")


# ---- checked datasets with expectations (warn: keep rows, record metrics) ----
def define_checked(ds):
    expectations = {f"{r['rule_id']}_{r['name']}": r["constraint"] for r in RULES if r["dataset"] == ds}

    @dp.materialized_view(name=f"dq_checked_{PREFIX}_{ds}", comment=f"{LAYER} {ds} with expectation metrics")
    @dp.expect_all(expectations)
    def checked():
        return spark.sql(PREPARED[ds])  # noqa: F821


for _ds in KEYS:
    define_checked(_ds)


# ---- explicit quarantine flow ----
def quarantine_df(ds):
    key_expr = "concat_ws('|', " + ", ".join(f"coalesce(CAST({c} AS STRING), 'NULL')" for c in KEYS[ds]) + ")"
    return (
        spark.read.table(f"dq_checked_{PREFIX}_{ds}")  # noqa: F821
        .selectExpr("*", f"{failures_expr(ds)} AS _failures")
        .selectExpr("*", "explode(_failures) AS _f")
        .selectExpr(f"'{LAYER}' AS layer", f"'{ds}' AS source_table", f"{key_expr} AS record_key",
                    "_payload AS raw_payload", "_f.rule_id AS rule_id", "_f.rule_version AS rule_version",
                    "_f.failure_reason AS failure_reason", "current_timestamp() AS quarantine_time")
    )


@dp.materialized_view(name=f"{CATALOG}.quarantine.invalid_record_silver",
                      comment="Silver records failing a data quality rule (current state)")
def invalid_record_silver():
    out = None
    for ds in KEYS:
        df = quarantine_df(ds)
        out = df if out is None else out.unionByName(df)
    return out


# ---- reconciliation: bronze <-> silver, orphans ----
def cnt(sql):
    return f"coalesce(({sql}), 0)"


CHECKS = [
    ("SR01", "Bronze users all present in silver profile",
     cnt(f"SELECT count(*) FROM {bkeys('users', 'user_id')}"),
     cnt(f"SELECT count(DISTINCT user_id) FROM {SILVER}.client_financial_profile")),
    ("SR02", "Bronze budget rows all present in silver profile",
     cnt(f"SELECT count(*) FROM {bkeys('budgeting', 'budget_id')}"),
     cnt(f"SELECT count(DISTINCT budget_id) FROM {SILVER}.client_financial_profile")),
    ("SR03", "Bronze portfolios all present in silver portfolio_strategy",
     cnt(f"SELECT count(*) FROM {bkeys('portfolios', 'portfolio_id')}"),
     cnt(f"SELECT count(DISTINCT portfolio_id) FROM {SILVER}.portfolio_strategy")),
    ("SR04", "Bronze strategies all present in silver portfolio_strategy",
     cnt(f"SELECT count(*) FROM {bkeys('investment_strategies', 'strategy_id')}"),
     cnt(f"SELECT count(DISTINCT strategy_id) FROM {SILVER}.portfolio_strategy")),
    ("SR05", "Bronze transactions all present in silver transaction_fact",
     cnt(f"SELECT count(*) FROM {bkeys('transactions', 'transaction_id')}"),
     cnt(f"SELECT count(*) FROM {SILVER}.transaction_fact")),
    ("SR06", "Valid transaction count equals daily activity count",
     cnt(f"SELECT count(*) FROM {SILVER}.transaction_fact WHERE is_valid"),
     cnt(f"SELECT sum(transaction_count) FROM {SILVER}.client_daily_activity")),
    ("SR07", "Valid transaction amount equals daily gross activity amount",
     cnt(f"SELECT sum(amount) FROM {SILVER}.transaction_fact WHERE is_valid"),
     cnt(f"SELECT sum(gross_activity_amount) FROM {SILVER}.client_daily_activity")),
    ("SR08", "Orphan silver transactions (user not in profile) = 0",
     cnt(f"SELECT count(*) FROM {SILVER}.transaction_fact t LEFT ANTI JOIN "
         f"(SELECT DISTINCT user_id FROM {SILVER}.client_financial_profile) p ON t.user_id = p.user_id"),
     "0"),
    ("SR09", "Orphan silver portfolio rows (user not in profile) = 0",
     cnt(f"SELECT count(*) FROM {SILVER}.portfolio_strategy t LEFT ANTI JOIN "
         f"(SELECT DISTINCT user_id FROM {SILVER}.client_financial_profile) p ON t.user_id = p.user_id"),
     "0"),
]


def recon_sql():
    union = " UNION ALL ".join(
        f"SELECT '{i}' AS check_id, '{d}' AS description, "
        f"CAST({left} AS DOUBLE) AS left_value, CAST({right} AS DOUBLE) AS right_value"
        for i, d, left, right in CHECKS)
    return (f"SELECT check_id, description, left_value, right_value, left_value - right_value AS difference, "
            f"CASE WHEN abs(left_value - right_value) < 0.005 THEN 'PASS' ELSE 'FAIL' END AS status, "
            f"current_timestamp() AS checked_at FROM ({union})")


@dp.materialized_view(name=f"{CATALOG}.operations.recon_silver",
                      comment="Silver reconciliation checks, one row per check")
@EXPECT({"SR_reconciles": "status = 'PASS'"})
def recon_silver():
    return spark.sql(recon_sql())  # noqa: F821