# Databricks notebook source
from pyspark.sql.functions import col

def check_not_null(df, col_name, table):
    n = df.filter(col(col_name).isNull()).count()
    assert n == 0, f"❌ FAIL [{table}] {col_name} : {n} valeurs NULL"
    print(f"  ✅ [{table}] {col_name} : aucune valeur NULL")

def check_positive(df, col_name, table):
    n = df.filter(col(col_name) <= 0).count()
    assert n == 0, f"❌ FAIL [{table}] {col_name} : {n} valeurs <= 0"
    print(f"  ✅ [{table}] {col_name} : toutes valeurs positives")

def check_no_duplicates(df, key, table):
    total  = df.count()
    unique = df.select(key).distinct().count()
    assert total == unique, f"❌ FAIL [{table}] {key} : {total-unique} doublons"
    print(f"  ✅ [{table}] {key} : aucun doublon ({total} lignes)")

def check_row_count(df, min_rows, table):
    n = df.count()
    assert n >= min_rows, f"❌ FAIL [{table}] : {n} lignes < minimum {min_rows}"
    print(f"  ✅ [{table}] : {n} lignes (≥ {min_rows})")

df_silver  = spark.table("hive_metastore.silver.ventes_clean")
df_gold    = spark.table("hive_metastore.gold.fact_ventes")

print("🧪 Tests couche Silver :")
check_not_null(df_silver, "id_commande", "hive_metastore.silver.ventes_clean")
check_not_null(df_silver, "client", "hive_metastore.silver.ventes_clean")
check_not_null(df_silver, "produit", "hive_metastore.silver.ventes_clean")
check_positive(df_silver, "montant_total_eur", "hive_metastore.silver.ventes_clean")
check_no_duplicates(df_silver, "id_commande", "hive_metastore.silver.ventes_clean")
check_row_count(df_silver, 1, "hive_metastore.silver.ventes_clean")

print("🧪 Tests couche Gold :")
check_not_null(df_gold, "sk_client", "hive_metastore.gold.fact_ventes")
check_not_null(df_gold, "sk_produit", "hive_metastore.gold.fact_ventes")
check_not_null(df_gold, "sk_date", "hive_metastore.gold.fact_ventes")
check_positive(df_gold, "montant_total_eur", "hive_metastore.gold.fact_ventes")

print("🏆 Tous les tests réussis — Pipeline validé !")