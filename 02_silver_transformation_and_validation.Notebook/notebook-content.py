# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e0f576c5-ccb8-4931-ac2b-3bbf98e82ce0",
# META       "default_lakehouse_name": "SecuritiesLakehouse",
# META       "default_lakehouse_workspace_id": "849c7e4c-ee44-4918-9929-be0623517f9b",
# META       "known_lakehouses": [
# META         {
# META           "id": "e0f576c5-ccb8-4931-ac2b-3bbf98e82ce0"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Portfolio baseline build
from pyspark.sql.functions import col, to_date, trim, upper, when

# 1. Load the raw tables
df_trades_raw = spark.read.table("fact_trade_raw")
df_securities_raw = spark.read.table("dim_security_raw")

# 2. Normalize trade columns
df_trades_clean = df_trades_raw \
    .withColumn("trade_id", trim(col("trade_id"))) \
    .withColumn("security_id", trim(upper(col("security_id")))) \
    .withColumn("trade_type", trim(upper(col("trade_type")))) \
    .withColumn("trade_date", to_date(col("trade_date"), "yyyy-MM-dd")) \
    .withColumn("settlement_date", to_date(col("settlement_date"), "yyyy-MM-dd")) \
    .withColumn("quantity", col("quantity").cast("int")) \
    .withColumn("price", col("price").cast("decimal(18, 4)"))

# 3. Collect valid security IDs from the master table
valid_securities = [row.security_id for row in df_securities_raw.select("security_id").distinct().collect()]

# 4. Apply validation business rules
df_validated = df_trades_clean.withColumn(
    "rejection_reason",
    when(col("quantity") <= 0, "INVALID_QUANTITY")
    .when(col("price") <= 0, "INVALID_PRICE")
    .when(~col("security_id").isin(valid_securities), "UNMAPPED_SECURITY")
    .otherwise(None)
)

# 5. Split into clean (Silver) vs. rejected datasets
df_silver_trades = df_validated.filter(col("rejection_reason").isNull()).drop("rejection_reason")
df_rejected_trades = df_validated.filter(col("rejection_reason").isNotNull())

# 6. Write to Delta tables
df_silver_trades.write.format("delta").mode("overwrite").saveAsTable("silver_trades")
df_rejected_trades.write.format("delta").mode("overwrite").saveAsTable("silver_trade_exceptions")

# 7. Print summary metrics
print(f"Total Raw Trades: {df_trades_raw.count()}")
print(f"Clean Trades Written to Silver: {df_silver_trades.count()}")
print(f"Rejected Trades Written to Exceptions: {df_rejected_trades.count()}")

raise Exception("Simulated daily production ETL failure for alert testing.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, count, current_timestamp, sum

# 1. Read Silver Trades and join with Security Master to enrich with Asset Class
df_silver_trades = spark.read.table("silver_trades")
df_securities = spark.read.table("dim_security_raw")

df_enriched_trades = df_silver_trades.join(
    df_securities, df_silver_trades.security_id == df_securities.security_id, "inner"
)

# 2. Build Gold Daily Trade Summary (Aggregated metrics)
df_gold_daily_summary = (
    df_enriched_trades.groupBy("trade_date", "asset_class")
    .agg(
        count("trade_id").alias("total_trades"),
        sum("quantity").alias("total_quantity"),
        sum(col("quantity") * col("price")).cast("decimal(18,2)").alias("total_notional_value"),
    )
    .withColumn("created_timestamp", current_timestamp())
)

# 3. Write Gold Daily Summary (Idempotent: overwrite mode ensures re-runs don't duplicate records)
df_gold_daily_summary.write.format("delta").mode("overwrite").saveAsTable("gold_daily_trade_summary")

display(df_gold_daily_summary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, count, current_timestamp, when

# 1. Read settlements and join with Silver trades
df_settlements = spark.read.table("fact_settlement_raw")

df_reconciliation = (
    df_silver_trades.join(df_settlements, df_silver_trades.trade_id == df_settlements.trade_id, "left")
    .groupBy(col("status").alias("settlement_status"))
    .agg(
        count(df_silver_trades.trade_id).alias("trade_count"),
        sum(col("settled_amount")).cast("decimal(18,2)").alias("total_settled_amount"),
    )
    .withColumn("processed_timestamp", current_timestamp())
)

# 2. Write to Gold Settlements Delta table
df_reconciliation.write.format("delta").mode("overwrite").saveAsTable("gold_settlement_reconciliation")

display(df_reconciliation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
