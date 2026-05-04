# Databricks notebook source
from pyspark.sql.functions import col, to_date, upper, trim, round, when, lit, current_timestamp
from pyspark.sql.types import IntegerType, DoubleType

# Lire depuis la table Bronze
df = spark.table("hive_metastore.bronze.ventes_raw")

# Étape 1 - Typage
df_typed = (
    df
    .withColumn("quantite", col("quantite").cast(IntegerType()))
    .withColumn("date_commande", to_date(col("date_commande"), "yyyy-MM-dd"))
    .withColumn("prix_unitaire_eur", col("prix_unitaire_eur").cast(DoubleType()))
    .withColumn("montant_total_eur", col("montant_total_eur").cast(DoubleType()))
)

# Étape 2 - Nettoyage textuel
df_clean = (
    df_typed
    .withColumn("client", trim(col("client")))
    .withColumn("produit", trim(col("produit")))
    .withColumn("pays", upper(trim(col("pays"))))
)

# Étape 3 - Enrichissement
df_enriched = (
    df_clean
    .withColumn("montant_brut", round(col("quantite") * col("prix_unitaire_eur"), 2))
    .withColumn("segment_prix",
        when(col("prix_unitaire_eur") < 50, lit("Petit prix"))
        .when(col("prix_unitaire_eur") < 500, lit("Milieu de gamme"))
        .otherwise(lit("Premium"))
    )
    .withColumn("_silver_ts", current_timestamp())
)

# Étape 4 - Qualité
df_valid = (
    df_enriched
    .dropDuplicates(["id_commande"])
    .filter(col("id_commande").isNotNull())
    .filter(col("quantite") > 0)
    .filter(col("prix_unitaire_eur") > 0)
)

# Étape 5 - Écriture Silver
df_valid.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("hive_metastore.silver.ventes_clean")

print(f"✅ Silver : {df_valid.count()} lignes validées")
df_valid.select("id_commande","client","montant_total_eur","segment_prix").show(5)