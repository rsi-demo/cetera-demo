# Pipelines

Lakeflow Spark Declarative Pipelines for the `databricks_demo` medallion architecture (bronze, silver, gold), each paired with a data quality (DQ) pipeline.

## Layout

| Folder | Pipeline | Target schema |
| --- | --- | --- |
| `bronze/transformation/` | bronze_ingestion_pipeline | `databricks_demo.bronze` |
| `bronze/dq/` | DQ Bronze Pipeline | `databricks_demo.operations` |
| `silver/transformation/` | silver_transformation_pipeline | `databricks_demo.silver` |
| `silver/dq/` | DQ Silver Pipeline | `databricks_demo.operations` |
| `gold/transformation/` | gold_transformation_pipeline | `databricks_demo.gold` |
| `gold/dq/` | DQ Gold Pipeline | `databricks_demo.default` |

Each pipeline's source library glob points at its folder, e.g. `/Workspace/Users/sourav.saxena@rsystems.com/cetera-demo/bronze/transformation/**`.

## Run order

bronze ingestion -> DQ bronze -> silver transformation -> DQ silver -> gold transformation -> DQ gold
