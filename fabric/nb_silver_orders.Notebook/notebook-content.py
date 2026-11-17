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
# META           "id": "ef5ffbaf-14d8-4ca6-b3b2-45d339a5d3a0"
# META         },
# META         {
# META           "id": "92771532-2d5a-4f80-a693-d42719bf9e2e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

spark.sql("""
  SELECT 'silver_orders' AS table_name, COUNT(*) AS row_count FROM lh_silver.dbo.silver_orders
  UNION ALL SELECT 'quarantine_orders', COUNT(*) FROM lh_silver.dbo.quarantine_orders
""").show()

spark.sql("""
  SELECT reason_code, COUNT(*) AS rows FROM lh_silver.dbo.quarantine_orders
  GROUP BY reason_code ORDER BY rows DESC
""").show()

spark.sql("""
  SELECT carrier_after_delivery_flag, COUNT(*) AS rows
  FROM lh_silver.dbo.silver_orders GROUP BY carrier_after_delivery_flag
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(silver_orders.write.mode("overwrite").format("delta")
    .saveAsTable("lh_silver.dbo.silver_orders"))

(quarantine.withColumn("quarantined_at", F.current_timestamp())
    .write.mode("overwrite").format("delta")
    .saveAsTable("lh_silver.dbo.quarantine_orders"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

VALID_STATUSES = ["created", "approved", "invoiced", "processing",
                  "shipped", "delivered", "unavailable", "canceled"]

# 1) One row per order: keep the newest copy
deduped = dedupe_latest(bronze, ["order_id"], "ingestion_timestamp")

# 2) Rows that must be rejected (checked in this order, first failure wins)
rules = {
    "NULL_ORDER_ID":             F.col("order_id").isNull(),
    "NULL_CUSTOMER_ID":          F.col("customer_id").isNull(),
    "INVALID_STATUS":            F.col("order_status").isNull() | ~F.col("order_status").isin(VALID_STATUSES),
    "APPROVED_BEFORE_PURCHASE":  F.col("order_approved_at") < F.col("order_purchase_timestamp"),
    "DELIVERED_BEFORE_PURCHASE": F.col("order_delivered_customer_date") < F.col("order_purchase_timestamp"),
}
valid, quarantine = split_valid_invalid(deduped, rules)

# 3) Rows that stay, but get a warning flag
silver_orders = (
    valid
    .withColumn("carrier_after_delivery_flag",
                F.coalesce(F.col("order_delivered_carrier_date") > F.col("order_delivered_customer_date"), F.lit(False)))
    .withColumn("silver_loaded_at", F.current_timestamp())
)

print("bronze:", bronze.count(), "| after dedupe:", deduped.count(),
      "| valid:", silver_orders.count(), "| quarantine:", quarantine.count())
assert silver_orders.count() + quarantine.count() == deduped.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

bronze = snake_case_columns(spark.read.table("lh_bronze.dbo.ora_orders"))

print("rows:", bronze.count(), "| distinct order_id:", bronze.select("order_id").distinct().count())
bronze.groupBy("order_status").count().orderBy(F.desc("count")).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sys
sys.path.append('/lakehouse/default/Files/code')
from common import snake_case_columns, dedupe_latest, split_valid_invalid

df = spark.read.table("lh_bronze.dbo.ora_orders")
print(df.count())
snake_case_columns(df).printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
