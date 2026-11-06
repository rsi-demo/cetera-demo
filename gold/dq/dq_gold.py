"""
Gold data quality pipeline (Lakeflow Spark Declarative Pipelines, Python)

Checks the three gold tables written by gold_pipeline.py:
  * keys           : not null, unique (composite keys included)
  * referential    : gold -> silver (every client exists in silver
                     client_financial_profile; every month/type group exists in
                     valid silver transactions; every analytics group exists in
                     silver portfolio_strategy) and silver -> gold completeness
  * metric logic   : counts and amounts recomputed from silver and compared,
                     value domains, 30-day <= 90-day, label constants
  * reconciliation : totals gold vs silver (operations.recon_gold, one row per check)
Failed gold records are written to quarantine.invalid_record_gold (evidence only,
rows stay in gold).

Pipeline settings: serverless, default catalog = databricks_demo, default
schema = operations, event log published to
databricks_demo.operations.dq_gold_event_log.
Parameters: gold.as_of_date (must equal the value used by gold_pipeline.py,
default 2023-12-31) and dq.enforce_reconciliation (true = fail the update).

ASSUMPTIONS TO CONFIRM: fully qualified names for other schemas are accepted;
tolerances are placeholders.
"""
from typing import TYPE_CHECKING

from pyspark import pipelines as dp

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    spark: SparkSession

CATALOG = "databricks_demo"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"
LAYER = "gold"
PREFIX = "gold"
AS_OF_DATE = spark.conf.get("gold.as_of_date", "2023-12-31")  # noqa: F821
ENFORCE = spark.conf.get("dq.enforce_reconciliation", "false") == "true"  # noqa: F821
EXPECT = dp.expect_all_or_fail if ENFORCE else dp.expect_all

ASOF = f"DATE'{AS_OF_DATE}'"
IN30 = f"(transaction_date > date_sub({ASOF}, 30) AND transaction_date <= {ASOF})"
IN90 = f"(transaction_date > date_sub({ASOF}, 90) AND transaction_date <= {ASOF})"
SEGMENTS = ["AT_RISK", "WATCH", "HEALTHY", "UNCLASSIFIED"]
TXN_TYPES = ["deposit", "withdrawal", "investment", "dividend"]
STRATEGY_TYPES = ["growth", "income", "value", "speculative", "NO_STRATEGY"]
RISK_LEVELS = ["low", "medium", "high", "NOT_SET"]
BANDS = ["POOR", "FAIR", "GOOD", "VERY_GOOD", "EXCEPTIONAL", "UNKNOWN", "INVALID"]


def in_list(col, values):
    return f"{col} IN ({', '.join(repr(v) for v in values)})"


KEYS = {
    "client_financial_health": ["user_id"],
    "monthly_transaction_summary": ["txn_month", "transaction_type"],
    "portfolio_strategy_analytics": ["strategy_type", "risk_tolerance", "credit_score_band"],
}

SILVER_MONTHLY = f"""(
    SELECT CAST(date_trunc('month', transaction_timestamp) AS DATE) AS txn_month, transaction_type,
           count(*) AS n, count(DISTINCT user_id) AS clients,
           sum(amount) AS total, sum(external_cash_flow) AS net
    FROM {SILVER}.transaction_fact WHERE is_valid GROUP BY 1, 2)"""

SILVER_GROUPS = f"""(
    SELECT COALESCE(ps.strategy_type, 'NO_STRATEGY') AS strategy_type,
           COALESCE(ps.risk_tolerance, 'NOT_SET')   AS risk_tolerance,
           COALESCE(band.credit_score_band, 'UNKNOWN') AS credit_score_band,
           count(DISTINCT ps.portfolio_id) AS portfolios, count(DISTINCT ps.user_id) AS clients
    FROM {SILVER}.portfolio_strategy ps
    LEFT JOIN (SELECT user_id, max(credit_score_band) AS credit_score_band
               FROM {SILVER}.client_financial_profile GROUP BY user_id) band ON ps.user_id = band.user_id
    GROUP BY 1, 2, 3)"""

PREPARED = {
    "client_financial_health": f"""
        SELECT to_json(struct(t.*)) AS _payload, t.*,
               count(*) OVER (PARTITION BY t.user_id) AS _pk_count,
               (sp.user_id IS NOT NULL)      AS _user_in_silver,
               coalesce(pc.n, 0)             AS _silver_portfolio_count,
               coalesce(tx.n30, 0)           AS _silver_txn_30d,
               coalesce(tx.n90, 0)           AS _silver_txn_90d,
               coalesce(tx.net30, 0)         AS _silver_net_30d,
               coalesce(tx.net90, 0)         AS _silver_net_90d
        FROM {GOLD}.client_financial_health t
        LEFT JOIN (SELECT DISTINCT user_id FROM {SILVER}.client_financial_profile) sp ON t.user_id = sp.user_id
        LEFT JOIN (SELECT user_id, count(DISTINCT portfolio_id) AS n
                   FROM {SILVER}.portfolio_strategy GROUP BY user_id) pc ON t.user_id = pc.user_id
        LEFT JOIN (SELECT user_id,
                          count_if({IN30}) AS n30, count_if({IN90}) AS n90,
                          sum(CASE WHEN {IN30} THEN external_cash_flow ELSE 0 END) AS net30,
                          sum(CASE WHEN {IN90} THEN external_cash_flow ELSE 0 END) AS net90
                   FROM {SILVER}.transaction_fact WHERE is_valid GROUP BY user_id) tx ON t.user_id = tx.user_id""",
    "monthly_transaction_summary": f"""
        SELECT to_json(struct(t.*)) AS _payload, t.*,
               count(*) OVER (PARTITION BY t.txn_month, t.transaction_type) AS _pk_count,
               (s.txn_month IS NOT NULL) AS _in_silver,
               s.n AS _s_count, s.clients AS _s_clients, s.total AS _s_total, s.net AS _s_net
        FROM {GOLD}.monthly_transaction_summary t
        LEFT JOIN {SILVER_MONTHLY} s ON t.txn_month = s.txn_month AND t.transaction_type = s.transaction_type""",
    "portfolio_strategy_analytics": f"""
        SELECT to_json(struct(t.*)) AS _payload, t.*,
               count(*) OVER (PARTITION BY t.strategy_type, t.risk_tolerance, t.credit_score_band) AS _pk_count,
               (s.strategy_type IS NOT NULL) AS _in_silver,
               s.portfolios AS _s_portfolios, s.clients AS _s_clients
        FROM {GOLD}.portfolio_strategy_analytics t
        LEFT JOIN {SILVER_GROUPS} s ON t.strategy_type = s.strategy_type
             AND t.risk_tolerance = s.risk_tolerance AND t.credit_score_band = s.credit_score_band""",
}

RULES = []


def rule(ds, rule_id, name, constraint, reason, version="1"):
    RULES.append({"dataset": ds, "rule_id": rule_id, "name": name, "constraint": constraint,
                  "reason": reason, "rule_version": version})


C, M, A = "client_financial_health", "monthly_transaction_summary", "portfolio_strategy_analytics"
frac = lambda c: f"({c} IS NULL OR {c} BETWEEN 0 AND 1)"  # noqa: E731
# client_financial_health
rule(C, "GC01", "pk_not_null", "user_id IS NOT NULL", "Missing primary key")
rule(C, "GC02", "pk_unique", "_pk_count = 1", "Duplicate client row")
rule(C, "GC03", "ri_user_in_silver", "_user_in_silver", "Client not found in silver client_financial_profile")
rule(C, "GC04", "segment_supported", in_list("financial_health_segment", SEGMENTS), "Unsupported health segment")
rule(C, "GC05", "as_of_date_present", "as_of_date IS NOT NULL",
     "As-of date missing")
rule(C, "GC06", "counts_sane",
     "txn_count_30d <= txn_count_90d AND portfolio_count >= 0 AND strategy_count >= 0", "Count relationship broken")
rule(C, "GC07", "portfolio_count_matches_silver", "portfolio_count = _silver_portfolio_count",
     "Portfolio count differs from silver")
rule(C, "GC08", "txn_counts_match_silver", "txn_count_30d = _silver_txn_30d AND txn_count_90d = _silver_txn_90d",
     "Transaction counts differ from silver")
rule(C, "GC09", "net_flow_matches_silver",
     "abs(net_external_cash_flow_30d - _silver_net_30d) < 0.005 AND "
     "abs(net_external_cash_flow_90d - _silver_net_90d) < 0.005", "Net external cash flow differs from silver")
rule(C, "GC10", "allocations_in_0_1",
     " AND ".join(frac(c) for c in ["avg_normalized_equity_allocation",
                                    "avg_normalized_fixed_income_allocation",
                                    "avg_normalized_alternative_allocation"]), "Average allocation outside 0-1")
# monthly_transaction_summary
rule(M, "GM01", "pk_not_null", "txn_month IS NOT NULL AND transaction_type IS NOT NULL", "Missing key")
rule(M, "GM02", "pk_unique", "_pk_count = 1", "Duplicate (month, type) row")
rule(M, "GM03", "ri_group_in_silver", "_in_silver", "Month/type group has no valid silver transactions")
rule(M, "GM04", "type_supported", in_list("transaction_type", TXN_TYPES), "Unsupported transaction type")
rule(M, "GM05", "metrics_sane",
     "max_amount >= average_amount AND transaction_count >= distinct_clients AND average_amount > 0",
     "Metric relationship broken")
rule(M, "GM06", "counts_match_silver", "transaction_count = _s_count AND distinct_clients = _s_clients",
     "Counts differ from silver")
rule(M, "GM07", "amounts_match_silver",
     "abs(total_amount - _s_total) < 0.005 AND abs(net_external_cash_flow - _s_net) < 0.005",
     "Amounts differ from silver")
# portfolio_strategy_analytics
rule(A, "GP01", "pk_not_null",
     "strategy_type IS NOT NULL AND risk_tolerance IS NOT NULL AND credit_score_band IS NOT NULL", "Missing key")
rule(A, "GP02", "pk_unique", "_pk_count = 1", "Duplicate analytics group")
rule(A, "GP03", "ri_group_in_silver", "_in_silver", "Group not found in silver portfolio_strategy")
rule(A, "GP04", "domains_supported",
     f"{in_list('strategy_type', STRATEGY_TYPES)} AND {in_list('risk_tolerance', RISK_LEVELS)} "
     f"AND {in_list('credit_score_band', BANDS)}", "Unsupported strategy, risk or band value")
rule(A, "GP05", "counts_sane", "portfolio_count > 0 AND client_count > 0 AND client_count <= portfolio_count",
     "Portfolio and client counts inconsistent")
rule(A, "GP06", "values_in_range",
     f"{frac('avg_normalized_equity_allocation')} AND {frac('avg_normalized_fixed_income_allocation')} AND "
     f"{frac('avg_normalized_alternative_allocation')} AND "
     "(avg_investment_horizon_years IS NULL OR avg_investment_horizon_years BETWEEN 1 AND 30) AND "
     "expected_return_label = 'SOURCE_EXPECTATION_NOT_REALIZED_PERFORMANCE'",
     "Average out of range or expected-return label missing")
rule(A, "GP07", "counts_match_silver", "portfolio_count = _s_portfolios AND client_count = _s_clients",
     "Counts differ from silver")


def failures_expr(ds):
    parts = []
    for r in RULES:
        if r["dataset"] == ds:
            reason = r["reason"].replace("'", "''")
            parts.append(f"CASE WHEN NOT coalesce({r['constraint']}, false) THEN named_struct("
                         f"'rule_id', '{r['rule_id']}', 'rule_version', '{r['rule_version']}', "
                         f"'failure_reason', '{reason}') END")
    return f"filter(array({', '.join(parts)}), x -> x IS NOT NULL)"


@dp.materialized_view(name=f"{CATALOG}.metadata.dq_rule_gold", comment="Gold data quality rule catalogue")
def dq_rule_gold():
    rows = [(r["rule_id"], r["rule_version"], LAYER, r["dataset"], r["name"], r["constraint"], r["reason"])
            for r in RULES]
    return spark.createDataFrame(  # noqa: F821
        rows, "rule_id string, rule_version string, layer string, source_table string, rule_name string, "
              "constraint_sql string, failure_reason string")


def define_checked(ds):
    expectations = {f"{r['rule_id']}_{r['name']}": r["constraint"] for r in RULES if r["dataset"] == ds}

    @dp.materialized_view(name=f"dq_checked_{PREFIX}_{ds}", comment=f"{LAYER} {ds} with expectation metrics")
    @dp.expect_all(expectations)
    def checked():
        return spark.sql(PREPARED[ds])  # noqa: F821


for _ds in KEYS:
    define_checked(_ds)


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


@dp.materialized_view(name=f"{CATALOG}.quarantine.invalid_record_gold",
                      comment="Gold records failing a data quality rule (current state)")
def invalid_record_gold():
    out = None
    for ds in KEYS:
        df = quarantine_df(ds)
        out = df if out is None else out.unionByName(df)
    return out


def cnt(sql):
    return f"coalesce(({sql}), 0)"


CHECKS = [
    ("GR01", "Silver clients equal gold client rows",
     cnt(f"SELECT count(DISTINCT user_id) FROM {SILVER}.client_financial_profile"),
     cnt(f"SELECT count(*) FROM {GOLD}.client_financial_health")),
    ("GR02", "Silver clients missing in gold = 0",
     cnt(f"SELECT count(*) FROM (SELECT DISTINCT user_id FROM {SILVER}.client_financial_profile) s "
         f"LEFT ANTI JOIN {GOLD}.client_financial_health g ON s.user_id = g.user_id"), "0"),
    ("GR03", "Gold clients missing in silver (orphans) = 0",
     cnt(f"SELECT count(*) FROM {GOLD}.client_financial_health g LEFT ANTI JOIN "
         f"(SELECT DISTINCT user_id FROM {SILVER}.client_financial_profile) s ON g.user_id = s.user_id"), "0"),
    ("GR04", "Monthly summary count equals valid silver transactions",
     cnt(f"SELECT count(*) FROM {SILVER}.transaction_fact WHERE is_valid"),
     cnt(f"SELECT sum(transaction_count) FROM {GOLD}.monthly_transaction_summary")),
    ("GR05", "Monthly summary amount equals valid silver amount",
     cnt(f"SELECT sum(amount) FROM {SILVER}.transaction_fact WHERE is_valid"),
     cnt(f"SELECT sum(total_amount) FROM {GOLD}.monthly_transaction_summary")),
    ("GR06", "Monthly net external cash flow equals silver",
     cnt(f"SELECT sum(external_cash_flow) FROM {SILVER}.transaction_fact WHERE is_valid"),
     cnt(f"SELECT sum(net_external_cash_flow) FROM {GOLD}.monthly_transaction_summary")),
    ("GR07", "Silver month/type groups equal gold rows",
     cnt(f"SELECT count(*) FROM {SILVER_MONTHLY}"),
     cnt(f"SELECT count(*) FROM {GOLD}.monthly_transaction_summary")),
    ("GR08", "Silver analytics groups equal gold rows",
     cnt(f"SELECT count(*) FROM {SILVER_GROUPS}"),
     cnt(f"SELECT count(*) FROM {GOLD}.portfolio_strategy_analytics")),
    ("GR09", "Client portfolio counts add up to silver distinct portfolios",
     cnt(f"SELECT count(DISTINCT portfolio_id) FROM {SILVER}.portfolio_strategy"),
     cnt(f"SELECT sum(portfolio_count) FROM {GOLD}.client_financial_health")),
    ("GR10", "Client 90-day transaction counts add up to silver",
     cnt(f"SELECT count_if({IN90}) FROM {SILVER}.transaction_fact WHERE is_valid"),
     cnt(f"SELECT sum(txn_count_90d) FROM {GOLD}.client_financial_health")),
]


def recon_sql():
    union = " UNION ALL ".join(
        f"SELECT '{i}' AS check_id, '{d}' AS description, "
        f"CAST({left} AS DOUBLE) AS left_value, CAST({right} AS DOUBLE) AS right_value"
        for i, d, left, right in CHECKS)
    return (f"SELECT check_id, description, left_value, right_value, left_value - right_value AS difference, "
            f"CASE WHEN abs(left_value - right_value) < 0.005 THEN 'PASS' ELSE 'FAIL' END AS status, "
            f"current_timestamp() AS checked_at FROM ({union})")


@dp.materialized_view(name=f"{CATALOG}.operations.recon_gold",
                      comment="Gold reconciliation checks, one row per check")
@EXPECT({"GR_reconciles": "status = 'PASS'"})
def recon_gold():
    return spark.sql(recon_sql())  # noqa: F821