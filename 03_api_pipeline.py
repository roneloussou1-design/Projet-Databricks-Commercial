# Databricks notebook source
import requests
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import current_timestamp, col, to_date, lit

# ─── 1. Appel API taux de change ──────────────────────────────────────────
API_URL = "https://api.exchangerate-api.com/v4/latest/EUR"
print("📡 Appel API en cours...")
response = requests.get(API_URL, timeout=15)
response.raise_for_status()
api_data = response.json()
print(f"✅ API OK — {len(api_data['rates'])} devises reçues | Date : {api_data['date']}")

# ─── 2. Construction du DataFrame Spark ──────────────────────────────────
schema = StructType([
    StructField("base_currency",   StringType(), True),
    StructField("target_currency", StringType(), True),
    StructField("taux",            DoubleType(), True),
    StructField("date_update",     StringType(), True),
])

rows = [
    Row(base_currency="EUR", target_currency=currency,
        taux=float(rate), date_update=api_data["date"])
    for currency, rate in api_data["rates"].items()
]

df_api = (
    spark.createDataFrame(rows, schema)
    .withColumn("_ingestion_ts", current_timestamp())
)

# ─── 3. Bronze ─────────────────────────────────────────────────────────────
df_api.write.format("delta") \
    .mode("append") \
    .saveAsTable("hive_metastore.bronze.taux_change_raw")
print(f"✅ Bronze API : {df_api.count()} devises stockées")

# ─── 4. Silver — uniquement USD, GBP et CHF ────────────────────────────────────
df_silver_fx = (
    df_api
    .filter(col("target_currency").isin(["USD", "GBP", "CHF"]))
    .withColumn("date_update", to_date(col("date_update"), "yyyy-MM-dd"))
    .select("base_currency", "target_currency", "taux", "date_update")
)

df_silver_fx.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("hive_metastore.silver.taux_change")
print("✅ Silver FX prêt")
df_silver_fx.show()