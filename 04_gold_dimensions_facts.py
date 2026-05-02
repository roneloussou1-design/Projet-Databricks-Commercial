# Databricks notebook source
# MAGIC %sql
# MAGIC -- dim_client
# MAGIC CREATE OR REPLACE TABLE hive_metastore.gold.dim_client
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     DENSE_RANK() OVER (ORDER BY client) AS sk_client,
# MAGIC     client,
# MAGIC     MAX(pays) AS pays
# MAGIC FROM hive_metastore.silver.ventes_clean
# MAGIC GROUP BY client;
# MAGIC
# MAGIC -- dim_produit
# MAGIC CREATE OR REPLACE TABLE hive_metastore.gold.dim_produit
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     DENSE_RANK() OVER (ORDER BY produit) AS sk_produit,
# MAGIC     produit,
# MAGIC     MAX(segment_prix) AS segment_prix
# MAGIC FROM hive_metastore.silver.ventes_clean
# MAGIC GROUP BY produit;
# MAGIC
# MAGIC -- dim_date
# MAGIC CREATE OR REPLACE TABLE hive_metastore.gold.dim_date
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     DATE_FORMAT(date_commande, 'yyyyMMdd') AS sk_date,
# MAGIC     date_commande,
# MAGIC     YEAR(date_commande)     AS annee,
# MAGIC     QUARTER(date_commande)  AS trimestre,
# MAGIC     MONTH(date_commande)    AS mois,
# MAGIC     WEEKOFYEAR(date_commande) AS semaine
# MAGIC FROM hive_metastore.silver.ventes_clean
# MAGIC GROUP BY date_commande;

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, date_format

df_silver   = spark.table("hive_metastore.silver.ventes_clean")
dim_client  = spark.table("hive_metastore.gold.dim_client")
dim_produit = spark.table("hive_metastore.gold.dim_produit")

df_fact = (
    df_silver
    .join(dim_client.select("client", "sk_client"), on="client", how="left")
    .join(dim_produit.select("produit", "sk_produit"), on="produit", how="left")
    .withColumn("sk_date", date_format("date_commande", "yyyyMMdd"))
    .withColumn("sk_vente", monotonically_increasing_id())
    .select("sk_vente", "id_commande", "sk_client",
            "sk_produit", "sk_date", "quantite",
            "prix_unitaire_eur", "montant_total_eur", "montant_brut")
)

df_fact.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("sk_date") \
    .saveAsTable("hive_metastore.gold.fact_ventes")

print(f"✅ fact_ventes : {df_fact.count()} lignes")
df_fact.show()