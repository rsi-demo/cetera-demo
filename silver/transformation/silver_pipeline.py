from pyspark import pipelines as dp

# --------------------------------------------------------------------------
# Configuration 
# --------------------------------------------------------------------------
CATALOG = "databricks_demo"
BRONZE_SCHEMA = "bronze"
SOURCE_TZ = "UTC"  

CFG = {
    "credit_min": 300,
    "credit_max": 850,
    "credit_bands": [
        (300, 579, "POOR"),
        (580, 669, "FAIR"),
        (670, 739, "GOOD"),
        (740, 799, "VERY_GOOD"),
        (800, 850, "EXCEPTIONAL"),
    ],
    "low_savings": 0.10,       # calculated savings rate below this = LOW_SAVINGS
    "healthy_savings": 0.20,   # at or above this = HEALTHY
    "alloc_tolerance": 0.01,   # allowed deviation of raw allocation total from 1
    "txn_types": ["deposit", "withdrawal", "investment", "dividend"],
    "strategy_types": ["growth", "income", "value", "speculative"],
    "risk_levels": ["low", "medium", "high"],
    "horizon_years": (1, 30),
    "expected_return": (0.01, 0.20),
}

CDF_PROPS = {"delta.enableChangeDataFeed": "true"}


def cur(table):
    return f"(SELECT * FROM bronze_{table}_current)"


def in_list(col, values):
    quoted = ", ".join(f"'{v}'" for v in values)
    return f"{col} IN ({quoted})"


def credit_band_sql(col):
    parts = [
        f"WHEN {col} IS NULL THEN 'UNKNOWN'",
        f"WHEN {col} < {CFG['credit_min']} OR {col} > {CFG['credit_max']} THEN 'INVALID'",
    ]
    for lo, hi, name in CFG["credit_bands"]:
        parts.append(f"WHEN {col} BETWEEN {lo} AND {hi} THEN '{name}'")
    return "CASE " + " ".join(parts) + " ELSE 'INVALID' END"


def to_utc(col):
    return f"to_utc_timestamp(CAST({col} AS TIMESTAMP), '{SOURCE_TZ}')"


# --------------------------------------------------------------------------
# Bronze sources
# --------------------------------------------------------------------------
@dp.temporary_view(name="bronze_users_current")
def bronze_users_current():
    return (
        spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.users_history")  
        .filter("__END_AT IS NULL")
    )


@dp.temporary_view(name="bronze_budgeting_current")
def bronze_budgeting_current():
    return (
        spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.budgeting_history")  
        .filter("__END_AT IS NULL")
    )


@dp.temporary_view(name="bronze_portfolios_current")
def bronze_portfolios_current():
    return (
        spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.portfolios_history")  
        .filter("__END_AT IS NULL")
    )


@dp.temporary_view(name="bronze_investment_strategies_current")
def bronze_investment_strategies_current():
    return (
        spark.read.table(  
            f"{CATALOG}.{BRONZE_SCHEMA}.investment_strategies_history"
        ).filter("__END_AT IS NULL")
    )


@dp.temporary_view(name="bronze_transactions_current")
def bronze_transactions_current():
    return (
        spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.transactions_history") 
        .filter("__END_AT IS NULL")
    )


# --------------------------------------------------------------------------
# Source snapshot views (computed from bronze current rows)
# --------------------------------------------------------------------------
@dp.temporary_view(name="stg_client_financial_profile")
def stg_client_financial_profile():
    return spark.sql(
        f"""
        WITH u AS {cur('users')},
             b AS {cur('budgeting')},
             bc AS (SELECT user_id, COUNT(*) AS budget_row_count FROM b GROUP BY user_id),
             j AS (
               SELECT u.user_id, u.full_name, u.email,
                      {to_utc('u.created_at')} AS account_created_at,
                      u.credit_score,
                      b.budget_id,
                      COALESCE(b.budget_id, -1) AS budget_key,
                      COALESCE(bc.budget_row_count, 0) AS budget_row_count,
                      b.monthly_income, b.monthly_expenditure,
                      b.savings_rate AS reported_savings_rate,
                      greatest(u.__START_AT, b.__START_AT) AS bronze_start_at
               FROM u
               LEFT JOIN b  ON u.user_id = b.user_id
               LEFT JOIN bc ON u.user_id = bc.user_id),
             c AS (
               SELECT j.*, (monthly_income - monthly_expenditure) AS monthly_surplus FROM j),
             d AS (
               SELECT c.*, monthly_surplus / NULLIF(monthly_income, 0) AS calculated_savings_rate
               FROM c)
        SELECT d.user_id, d.budget_key, d.full_name, d.email, d.account_created_at,
               d.credit_score,
               {credit_band_sql('d.credit_score')} AS credit_score_band,
               d.budget_id, d.budget_row_count,
               CASE WHEN d.budget_row_count = 0 THEN 'NO_BUDGET'
                    WHEN d.budget_row_count > 1 THEN 'MULTIPLE_BUDGETS'
                    ELSE 'OK' END AS budget_link_status,
               d.monthly_income, d.monthly_expenditure,
               d.reported_savings_rate, d.calculated_savings_rate,
               (d.reported_savings_rate - d.calculated_savings_rate) AS savings_rate_variance,
               d.monthly_surplus,
               CASE WHEN d.monthly_income IS NULL THEN 'NO_BUDGET'
                    WHEN d.monthly_income = 0 THEN 'UNKNOWN'
                    WHEN d.monthly_surplus < 0 THEN 'DEFICIT'
                    WHEN d.calculated_savings_rate < {CFG['low_savings']} THEN 'LOW_SAVINGS'
                    WHEN d.calculated_savings_rate < {CFG['healthy_savings']} THEN 'MODERATE'
                    ELSE 'HEALTHY' END AS budget_health_status,
               d.bronze_start_at
        FROM d
        """
    )


@dp.temporary_view(name="stg_portfolio_strategy")
def stg_portfolio_strategy():
    lo_h, hi_h = CFG["horizon_years"]
    lo_r, hi_r = CFG["expected_return"]
    return spark.sql(
        f"""
        WITH p AS {cur('portfolios')},
             s AS {cur('investment_strategies')},
             u AS (SELECT user_id FROM {cur('users')}),
             j AS (
               SELECT p.portfolio_id, p.user_id,
                      COALESCE(s.strategy_id, -1) AS strategy_key,
                      s.strategy_id,
                      p.equity_allocation       AS raw_equity_allocation,
                      p.fixed_income_allocation AS raw_fixed_income_allocation,
                      p.alternative_assets      AS raw_alternative_allocation,
                      s.strategy_type, s.risk_tolerance,
                      s.investment_horizon_years, s.expected_return_rate,
                      (u.user_id IS NOT NULL) AS user_reference_valid,
                      greatest(p.__START_AT, s.__START_AT) AS bronze_start_at
               FROM p
               LEFT JOIN s ON p.portfolio_id = s.portfolio_id
               LEFT JOIN u ON p.user_id = u.user_id),
             t AS (
               SELECT j.*,
                      (raw_equity_allocation + raw_fixed_income_allocation
                       + raw_alternative_allocation) AS raw_allocation_total
               FROM j)
        SELECT t.portfolio_id, t.user_id, t.strategy_key, t.strategy_id,
               t.raw_equity_allocation, t.raw_fixed_income_allocation,
               t.raw_alternative_allocation, t.raw_allocation_total,
               t.raw_equity_allocation       / NULLIF(t.raw_allocation_total, 0) AS normalized_equity_allocation,
               t.raw_fixed_income_allocation / NULLIF(t.raw_allocation_total, 0) AS normalized_fixed_income_allocation,
               t.raw_alternative_allocation  / NULLIF(t.raw_allocation_total, 0) AS normalized_alternative_allocation,
               t.strategy_type, t.risk_tolerance, t.investment_horizon_years,
               t.expected_return_rate,
               t.user_reference_valid,
               -- flag, do not reject, totals that differ from 1
               CASE WHEN t.raw_allocation_total IS NULL THEN 'MISSING'
                    WHEN abs(t.raw_allocation_total - 1) > {CFG['alloc_tolerance']} THEN 'TOTAL_NOT_1'
                    ELSE 'OK' END AS allocation_total_flag,
               (t.raw_equity_allocation BETWEEN 0 AND 1
                AND t.raw_fixed_income_allocation BETWEEN 0 AND 1
                AND t.raw_alternative_allocation BETWEEN 0 AND 1) AS allocation_components_in_range,
               (t.strategy_id IS NULL OR (
                   {in_list('t.strategy_type', CFG['strategy_types'])}
                   AND {in_list('t.risk_tolerance', CFG['risk_levels'])}
                   AND t.investment_horizon_years BETWEEN {lo_h} AND {hi_h}
                   AND t.expected_return_rate BETWEEN {lo_r} AND {hi_r})) AS strategy_domain_valid,
               t.bronze_start_at
        FROM t
        """
    )


@dp.temporary_view(name="stg_transaction_fact")
def stg_transaction_fact():
    return spark.sql(
        f"""
        WITH t AS {cur('transactions')},
             u AS (SELECT user_id FROM {cur('users')}),
             base AS (
               SELECT t.transaction_id, t.user_id,
                      {to_utc('t.transaction_date')} AS transaction_timestamp,
                      t.type AS transaction_type, t.amount,
                      (u.user_id IS NOT NULL) AS user_reference_valid,
                      t.__START_AT AS bronze_start_at
               FROM t LEFT JOIN u ON t.user_id = u.user_id),
             s AS (
               SELECT base.*,
                      CAST(transaction_timestamp AS DATE) AS transaction_date,
                      CASE WHEN transaction_type = 'deposit'    THEN amount ELSE 0 END AS deposit_amount,
                      CASE WHEN transaction_type = 'withdrawal' THEN amount ELSE 0 END AS withdrawal_amount,
                      CASE WHEN transaction_type = 'investment' THEN amount ELSE 0 END AS investment_amount,
                      CASE WHEN transaction_type = 'dividend'   THEN amount ELSE 0 END AS dividend_amount
               FROM base)
        SELECT s.transaction_id, s.user_id, s.transaction_timestamp, s.transaction_date,
               s.transaction_type, s.amount,
               s.deposit_amount, s.withdrawal_amount, s.investment_amount, s.dividend_amount,
               (s.deposit_amount + s.dividend_amount - s.withdrawal_amount) AS external_cash_flow,
               s.user_reference_valid,
               (s.amount > 0) AS amount_positive,
               {in_list('s.transaction_type', CFG['txn_types'])} AS type_allowed,
               (s.transaction_timestamp <= current_timestamp()) AS not_future,
               (s.user_reference_valid AND s.amount > 0
                AND {in_list('s.transaction_type', CFG['txn_types'])}
                AND s.transaction_timestamp <= current_timestamp()) AS is_valid,
               s.bronze_start_at
        FROM s
        """
    ) 


@dp.temporary_view(name="stg_client_daily_activity")
def stg_client_daily_activity():
    return spark.sql( 
        f"""
        SELECT transaction_date AS activity_date, user_id,
               COUNT(*) AS transaction_count,
               count_if(transaction_type = 'deposit')    AS deposit_count,
               count_if(transaction_type = 'withdrawal') AS withdrawal_count,
               count_if(transaction_type = 'investment') AS investment_count,
               count_if(transaction_type = 'dividend')   AS dividend_count,
               SUM(amount)            AS gross_activity_amount,
               SUM(deposit_amount)    AS deposit_amount,
               SUM(withdrawal_amount) AS withdrawal_amount,
               SUM(investment_amount) AS investment_amount,
               SUM(dividend_amount)   AS dividend_amount,
               SUM(external_cash_flow) AS net_external_cash_flow,
               MAX(amount)            AS largest_transaction_amount
        FROM stg_transaction_fact
        WHERE is_valid
        GROUP BY transaction_date, user_id
        """
    )


# --------------------------------------------------------------------------
# Silver targets: streaming tables with CDF enabled, SCD type 1 (current state)
# --------------------------------------------------------------------------
def silver_table(name, keys, source_view):
    dp.create_streaming_table(name=name, table_properties=CDF_PROPS)
    dp.create_auto_cdc_from_snapshot_flow(
        target=name,
        source=source_view,
        keys=keys,
        stored_as_scd_type=1,
    )


silver_table("client_financial_profile", ["user_id", "budget_key"], "stg_client_financial_profile")
silver_table("portfolio_strategy", ["portfolio_id", "strategy_key"], "stg_portfolio_strategy")
silver_table("transaction_fact", ["transaction_id"], "stg_transaction_fact")
silver_table("client_daily_activity", ["activity_date", "user_id"], "stg_client_daily_activity")