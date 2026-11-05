from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import expr

CATALOG = "databricks_demo"
SILVER = f"{CATALOG}.silver"
AS_OF_DATE = spark.conf.get("gold.as_of_date", "2023-12-31") 
GOLD_PROPS = {"delta.enableChangeDataFeed": "true"}


def silver_cdf(table):
    return (spark.readStream.option("readChangeFeed", "true").table(f"{SILVER}.{table}"))


def silver_events(table, source_rank, row_id_columns):
    """Add a deterministic, globally comparable sequence to Silver CDF rows."""
    df = silver_cdf(table)
    row_id = F.concat_ws(
        "|",
        *[
            F.coalesce(F.col(column).cast("string"), F.lit("<NULL>"))
            for column in row_id_columns
        ],
    )
    change_type_rank = (
        F.when(F.col("_change_type") == "delete", F.lit(0))
        .when(F.col("_change_type") == "update_preimage", F.lit(1))
        .when(F.col("_change_type") == "insert", F.lit(2))
        .when(F.col("_change_type") == "update_postimage", F.lit(3))
        .otherwise(F.lit(4))
    )
    return (
        df.withColumn("change_ts", F.col("_commit_timestamp"))
        .withColumn("change_source_rank", F.lit(source_rank))
        .withColumn(
            "change_commit_version",
            F.coalesce(F.col("_commit_version"), F.lit(-1)),
        )
        .withColumn("change_type_rank", change_type_rank)
        .withColumn(
            "change_event_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.lit(table),
                    row_id,
                    F.coalesce(F.col("_change_type"), F.lit("<NULL>")),
                ),
                256,
            ),
        )
    )


CHANGE_METADATA_COLUMNS = [
    "change_ts",
    "change_source_rank",
    "change_commit_version",
    "change_type_rank",
    "change_event_id",
]


def change_columns():
    return [F.col(column) for column in CHANGE_METADATA_COLUMNS]


def branch_event(df, branch):
    """Give separate recalculation branches distinct sequence values."""
    return df.withColumn(
        "change_event_id",
        F.sha2(F.concat_ws("|", F.col("change_event_id"), F.lit(branch)), 256),
    )


def finish(df):
    return df.withColumn("is_deleted", F.col("_present").isNull()).drop("_present")


# --------------------------------------------------------------------------
# Static (current-state) aggregates, computed from the silver tables
# --------------------------------------------------------------------------
def client_health_current_sql():
    asof = f"DATE'{AS_OF_DATE}'"
    in30 = f"(transaction_date > date_sub({asof}, 30) AND transaction_date <= {asof})"
    in90 = f"(transaction_date > date_sub({asof}, 90) AND transaction_date <= {asof})"
    return f"""
    WITH prof AS (
        -- one row per client: when a client has several budgets use the highest budget_key
        SELECT * FROM (
            SELECT p.*, row_number() OVER (PARTITION BY user_id ORDER BY budget_key DESC) AS rn
            FROM {SILVER}.client_financial_profile p)
        WHERE rn = 1),
         per_port AS (
           SELECT user_id, portfolio_id,
                  max(normalized_equity_allocation)       AS eq,
                  max(normalized_fixed_income_allocation) AS fi,
                  max(normalized_alternative_allocation)  AS alt
           FROM {SILVER}.portfolio_strategy
           GROUP BY user_id, portfolio_id),
         port AS (
           SELECT user_id, count(*) AS portfolio_count,
                  avg(eq)  AS avg_normalized_equity_allocation,
                  avg(fi)  AS avg_normalized_fixed_income_allocation,
                  avg(alt) AS avg_normalized_alternative_allocation
           FROM per_port GROUP BY user_id),
         strat AS (
           SELECT user_id, count(DISTINCT strategy_id) AS strategy_count
           FROM {SILVER}.portfolio_strategy
           WHERE strategy_id IS NOT NULL
           GROUP BY user_id),
         top_type AS (
           SELECT user_id, strategy_type AS primary_strategy_type FROM (
             SELECT user_id, strategy_type,
                    row_number() OVER (PARTITION BY user_id ORDER BY c DESC, strategy_type) AS rn
             FROM (SELECT user_id, strategy_type, count(*) AS c
                   FROM {SILVER}.portfolio_strategy
                   WHERE strategy_id IS NOT NULL
                   GROUP BY user_id, strategy_type))
           WHERE rn = 1),
         top_risk AS (
           SELECT user_id, risk_tolerance AS primary_risk_tolerance FROM (
             SELECT user_id, risk_tolerance,
                    row_number() OVER (PARTITION BY user_id ORDER BY c DESC, risk_tolerance) AS rn
             FROM (SELECT user_id, risk_tolerance, count(*) AS c
                   FROM {SILVER}.portfolio_strategy
                   WHERE strategy_id IS NOT NULL
                   GROUP BY user_id, risk_tolerance))
           WHERE rn = 1),
         tx AS (
           SELECT user_id,
                  count_if({in30}) AS txn_count_30d,
                  count_if({in90}) AS txn_count_90d,
                  sum(CASE WHEN {in30} THEN external_cash_flow ELSE 0 END) AS net_external_cash_flow_30d,
                  sum(CASE WHEN {in90} THEN external_cash_flow ELSE 0 END) AS net_external_cash_flow_90d
           FROM {SILVER}.transaction_fact
           WHERE is_valid
           GROUP BY user_id),
         f AS (
           SELECT prof.user_id, prof.full_name, prof.email,
                  prof.credit_score, prof.credit_score_band,
                  prof.monthly_income, prof.monthly_expenditure, prof.monthly_surplus,
                  prof.calculated_savings_rate, prof.budget_health_status, prof.budget_link_status,
                  COALESCE(port.portfolio_count, 0)  AS portfolio_count,
                  COALESCE(strat.strategy_count, 0)  AS strategy_count,
                  port.avg_normalized_equity_allocation,
                  port.avg_normalized_fixed_income_allocation,
                  port.avg_normalized_alternative_allocation,
                  top_type.primary_strategy_type,
                  top_risk.primary_risk_tolerance,
                  COALESCE(tx.txn_count_30d, 0)               AS txn_count_30d,
                  COALESCE(tx.txn_count_90d, 0)               AS txn_count_90d,
                  COALESCE(tx.net_external_cash_flow_30d, 0)  AS net_external_cash_flow_30d,
                  COALESCE(tx.net_external_cash_flow_90d, 0)  AS net_external_cash_flow_90d,
                  {asof} AS as_of_date
           FROM prof
           LEFT JOIN port     ON prof.user_id = port.user_id
           LEFT JOIN strat    ON prof.user_id = strat.user_id
           LEFT JOIN top_type ON prof.user_id = top_type.user_id
           LEFT JOIN top_risk ON prof.user_id = top_risk.user_id
           LEFT JOIN tx       ON prof.user_id = tx.user_id)
    SELECT f.*,
           -- placeholder rules, agree them with the business
           CASE WHEN f.budget_health_status = 'DEFICIT'
                     OR f.credit_score_band IN ('POOR', 'INVALID')            THEN 'AT_RISK'
                WHEN f.budget_health_status = 'LOW_SAVINGS'
                     OR f.net_external_cash_flow_90d < 0                       THEN 'WATCH'
                WHEN f.budget_health_status IN ('HEALTHY', 'MODERATE')
                     AND f.credit_score_band IN ('GOOD', 'VERY_GOOD', 'EXCEPTIONAL') THEN 'HEALTHY'
                ELSE 'UNCLASSIFIED' END AS financial_health_segment,
           true AS _present
    FROM f
    """


def monthly_current_sql():
    return f"""
    SELECT CAST(date_trunc('month', transaction_timestamp) AS DATE) AS txn_month,
           transaction_type,
           count(*)                    AS transaction_count,
           count(DISTINCT user_id)     AS distinct_clients,
           sum(amount)                 AS total_amount,
           avg(amount)                 AS average_amount,
           max(amount)                 AS max_amount,
           sum(external_cash_flow)     AS net_external_cash_flow,
           true AS _present
    FROM {SILVER}.transaction_fact
    WHERE is_valid
    GROUP BY 1, 2
    """


def user_band_sql():
    return f"""
    SELECT user_id, credit_score_band
    FROM (
        SELECT user_id, credit_score_band,
               row_number() OVER (
                   PARTITION BY user_id ORDER BY budget_key DESC
               ) AS rn
        FROM {SILVER}.client_financial_profile
    )
    WHERE rn = 1
    """


def strategy_analytics_current_sql():
    return f"""
    WITH band AS ({user_band_sql()})
    SELECT COALESCE(ps.strategy_type, 'NO_STRATEGY')   AS strategy_type,
           COALESCE(ps.risk_tolerance, 'NOT_SET')      AS risk_tolerance,
           COALESCE(band.credit_score_band, 'UNKNOWN') AS credit_score_band,
           count(DISTINCT ps.portfolio_id)             AS portfolio_count,
           count(DISTINCT ps.user_id)                  AS client_count,
           avg(ps.investment_horizon_years)            AS avg_investment_horizon_years,
           -- source expectation, NOT realized performance
           avg(ps.expected_return_rate)                AS avg_expected_return_rate_source_expectation,
           'SOURCE_EXPECTATION_NOT_REALIZED_PERFORMANCE' AS expected_return_label,
           avg(ps.normalized_equity_allocation)        AS avg_normalized_equity_allocation,
           avg(ps.normalized_fixed_income_allocation)  AS avg_normalized_fixed_income_allocation,
           avg(ps.normalized_alternative_allocation)   AS avg_normalized_alternative_allocation,
           true AS _present
    FROM {SILVER}.portfolio_strategy ps
    LEFT JOIN band ON ps.user_id = band.user_id
    GROUP BY 1, 2, 3
    """


# --------------------------------------------------------------------------
# Source views: affected keys (from silver CDF) joined to recomputed aggregates
# --------------------------------------------------------------------------
@dp.temporary_view(name="stg_client_financial_health")
def stg_client_financial_health():
    changed_users = None
    sources = (
        ("client_financial_profile", 0, ["user_id", "budget_key"]),
        ("portfolio_strategy", 1, ["portfolio_id", "strategy_key"]),
        ("transaction_fact", 2, ["transaction_id"]),
    )
    for table, source_rank, row_ids in sources:
        part = silver_events(table, source_rank, row_ids).select(
            "user_id", *change_columns()
        )
        changed_users = part if changed_users is None else changed_users.unionByName(part)
    current = spark.sql(client_health_current_sql()) 
    return finish(changed_users.join(current, "user_id", "left"))


@dp.temporary_view(name="stg_monthly_transaction_summary")
def stg_monthly_transaction_summary():
    keys = silver_events("transaction_fact", 0, ["transaction_id"]).select(
        F.date_trunc("month", "transaction_timestamp").cast("date").alias("txn_month"),
        F.col("transaction_type"),
        *change_columns(),
    )
    current = spark.sql(monthly_current_sql()) 
    return finish(keys.join(current, ["txn_month", "transaction_type"], "left"))


@dp.temporary_view(name="stg_portfolio_strategy_analytics")
def stg_portfolio_strategy_analytics():
    band = spark.sql(user_band_sql())  
    user_strategies = spark.sql(  
        f"""SELECT DISTINCT user_id,
                   COALESCE(strategy_type, 'NO_STRATEGY') AS strategy_type,
                   COALESCE(risk_tolerance, 'NOT_SET')    AS risk_tolerance
            FROM {SILVER}.portfolio_strategy"""
    )
    key_cols = ["strategy_type", "risk_tolerance", "credit_score_band"]
    output_cols = key_cols + CHANGE_METADATA_COLUMNS

    # (a) strategy / portfolio changes -> group of the changed row, with the user's current band
    ps_keys = (
        silver_events(
            "portfolio_strategy",
            0,
            ["portfolio_id", "strategy_key"],
        )
        .select(
            "user_id",
            F.coalesce("strategy_type", F.lit("NO_STRATEGY")).alias("strategy_type"),
            F.coalesce("risk_tolerance", F.lit("NOT_SET")).alias("risk_tolerance"),
            *change_columns(),
        )
        .join(band, "user_id", "left")
        .withColumn("credit_score_band", F.coalesce("credit_score_band", F.lit("UNKNOWN")))
        .select(*output_cols)
    )

    profile_events = silver_events(
        "client_financial_profile",
        1,
        ["user_id", "budget_key"],
    )

    # (b) CDF profile bands cover old and new groups for normal updates.
    profile_cdf_keys = (
        profile_events
        .select(
            "user_id",
            F.coalesce("credit_score_band", F.lit("UNKNOWN")).alias("credit_score_band"),
            *change_columns(),
        )
        .join(user_strategies, "user_id", "inner")
        .select(*output_cols)
    )

    # (c) Also include the user's currently selected band. This covers profile
    # deletion, where CDF contains the old band but the current group is UNKNOWN.
    profile_current_keys = (
        profile_events.select("user_id", *change_columns())
        .join(user_strategies, "user_id", "inner")
        .join(band, "user_id", "left")
        .withColumn(
            "credit_score_band",
            F.coalesce("credit_score_band", F.lit("UNKNOWN")),
        )
        .select(*output_cols)
    )

    keys = (
        ps_keys.unionByName(branch_event(profile_cdf_keys, "cdf-band"))
        .unionByName(branch_event(profile_current_keys, "current-band"))
    )
    current = spark.sql(strategy_analytics_current_sql()) 
    return finish(keys.join(current, key_cols, "left"))


# --------------------------------------------------------------------------
# Gold targets (streaming tables, CDF enabled, SCD type 1)
# --------------------------------------------------------------------------
def gold_table(name, keys, source_view):
    dp.create_streaming_table(name=name, table_properties=GOLD_PROPS)
    dp.create_auto_cdc_flow(
        target=name,
        source=source_view,
        keys=keys,
        sequence_by=F.struct(*change_columns()),
        apply_as_deletes=expr("is_deleted"),
        except_column_list=["is_deleted", *CHANGE_METADATA_COLUMNS],
        stored_as_scd_type=1,
    )


gold_table("client_financial_health", ["user_id"], "stg_client_financial_health")
gold_table("monthly_transaction_summary", ["txn_month", "transaction_type"], "stg_monthly_transaction_summary")
gold_table("portfolio_strategy_analytics",["strategy_type", "risk_tolerance", "credit_score_band"],"stg_portfolio_strategy_analytics")