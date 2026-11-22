# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "92771532-2d5a-4f80-a693-d42719bf9e2e",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "592a7dae-6cb5-46cc-afdd-00678f682860",
# META       "known_lakehouses": [
# META         {
# META           "id": "92771532-2d5a-4f80-a693-d42719bf9e2e"
# META         },
# META         {
# META           "id": "ef5ffbaf-14d8-4ca6-b3b2-45d339a5d3a0"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

spark.sql("""
  SELECT 'silver_order_items' AS table_name, COUNT(*) AS row_count FROM lh_silver.dbo.silver_order_items
  UNION ALL SELECT 'quarantine_order_items', COUNT(*) FROM lh_silver.dbo.quarantine_order_items
  UNION ALL SELECT 'silver_order_payments', COUNT(*) FROM lh_silver.dbo.silver_order_payments
  UNION ALL SELECT 'quarantine_order_payments', COUNT(*) FROM lh_silver.dbo.quarantine_order_payments
""").show(truncate=False)

spark.sql("SELECT zero_value_flag, COUNT(*) AS rows FROM lh_silver.dbo.silver_order_payments GROUP BY zero_value_flag").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

VALID_TYPES = ["credit_card", "boleto", "voucher", "debit_card", "not_defined"]

pay = (dedupe_latest(pay_b, ["order_id", "payment_sequential"], "ingestion_timestamp")
       .join(orders_found, "order_id", "left"))

pay_rules = {
    "NULL_KEY":               F.col("order_id").isNull() | F.col("payment_sequential").isNull(),
    "INVALID_SEQUENTIAL":     F.col("payment_sequential") < 1,
    "NEGATIVE_PAYMENT_VALUE": F.col("payment_value") < 0,
    "INVALID_PAYMENT_TYPE":   F.col("payment_type").isNull() | ~F.col("payment_type").isin(VALID_TYPES),
    "ORPHAN_ORDER":           F.col("_order_found").isNull(),
}
pay_ok, pay_bad = split_valid_invalid(pay, pay_rules)
pay_ok = (pay_ok.drop("_order_found")
          .withColumn("zero_value_flag", F.coalesce(F.col("payment_value") == 0, F.lit(False)))
          .withColumn("silver_loaded_at", F.current_timestamp()))
pay_bad = pay_bad.drop("_order_found").withColumn("quarantined_at", F.current_timestamp())

write_upsert(pay_ok, "lh_silver.dbo.silver_order_payments", ["order_id", "payment_sequential"])
pay_bad.write.mode("overwrite").format("delta").saveAsTable("lh_silver.dbo.quarantine_order_payments")
print("payments valid:", pay_ok.count(), "| quarantine:", pay_bad.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orders_found = (spark.read.table("lh_silver.dbo.silver_orders")
                .select("order_id").distinct().withColumn("_order_found", F.lit(True)))

items = (dedupe_latest(items_b, ["order_id", "order_item_id"], "ingestion_timestamp")
         .join(orders_found, "order_id", "left"))

item_rules = {
    "NULL_KEY":         F.col("order_id").isNull() | F.col("order_item_id").isNull(),
    "INVALID_ITEM_ID":  F.col("order_item_id") < 1,
    "NEGATIVE_PRICE":   F.col("price") < 0,
    "NEGATIVE_FREIGHT": F.col("freight_value") < 0,
    "ORPHAN_ORDER":     F.col("_order_found").isNull(),
}
items_ok, items_bad = split_valid_invalid(items, item_rules)
items_ok  = items_ok.drop("_order_found").withColumn("silver_loaded_at", F.current_timestamp())
items_bad = items_bad.drop("_order_found").withColumn("quarantined_at", F.current_timestamp())

write_upsert(items_ok, "lh_silver.dbo.silver_order_items", ["order_id", "order_item_id"])
items_bad.write.mode("overwrite").format("delta").saveAsTable("lh_silver.dbo.quarantine_order_items")
print("items valid:", items_ok.count(), "| quarantine:", items_bad.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

items_b = snake_case_columns(spark.read.table("lh_bronze.dbo.ora_order_items"))
pay_b   = snake_case_columns(spark.read.table("lh_bronze.dbo.ora_order_payments"))
print("items:", items_b.count(), "| payments:", pay_b.count())

pay_b.groupBy("payment_type").count().orderBy(F.desc("count")).show()
items_b.agg(F.min("price"), F.min("freight_value"), F.min("order_item_id")).show()
pay_b.agg(F.min("payment_value"), F.min("payment_sequential"),
          F.sum((F.col("payment_value") == 0).cast("int")).alias("zero_payments")).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sys
sys.path.append('/lakehouse/default/Files/code')
from common import snake_case_columns, dedupe_latest, split_valid_invalid
from pyspark.sql import functions as F
from delta.tables import DeltaTable

def write_upsert(df, table, keys):
    """First run creates the table. Later runs MERGE: update matching rows, insert new ones."""
    try:
        spark.read.table(table).limit(1).count()
        exists = True
    except Exception:
        exists = False
    if not exists:
        df.write.format("delta").saveAsTable(table)
    else:
        cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        (DeltaTable.forName(spark, table).alias("t")
            .merge(df.alias("s"), cond)
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
