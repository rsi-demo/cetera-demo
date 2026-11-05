from pyspark import pipelines as dp

@dp.temporary_view(name="lakebase_users_snapshot")
def lakebase_users_snapshot():
    return spark.read.table("cetera_demo.public.users") 


@dp.temporary_view(name="lakebase_portfolios_snapshot")
def lakebase_portfolios_snapshot():
    return spark.read.table("cetera_demo.public.portfolios")


@dp.temporary_view(name="lakebase_transactions_snapshot")
def lakebase_transactions_snapshot():
    return spark.read.table("cetera_demo.public.transactions")


@dp.temporary_view(name="lakebase_budgeting_snapshot")
def lakebase_budgeting_snapshot():
    return spark.read.table("cetera_demo.public.budgeting")


@dp.temporary_view(name="lakebase_investment_strategies_snapshot")
def lakebase_investment_strategies_snapshot():
    return spark.read.table("cetera_demo.public.investment_strategies")


TABLES = {
    "users": (["user_id"], "lakebase_users_snapshot"),
    "portfolios": (["portfolio_id"], "lakebase_portfolios_snapshot"),
    "transactions": (["transaction_id"], "lakebase_transactions_snapshot"),
    "budgeting": (["budget_id"], "lakebase_budgeting_snapshot"),
    "investment_strategies": (["strategy_id"],"lakebase_investment_strategies_snapshot",),
}

for table, (keys, source_view) in TABLES.items():
    target = f"{table}_history"
    dp.create_streaming_table(target)
    dp.create_auto_cdc_from_snapshot_flow(
        target=target,
        source=source_view,
        keys=keys,
        stored_as_scd_type=2,
    )