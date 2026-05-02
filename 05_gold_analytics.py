# Databricks notebook source
# MAGIC %sql
# MAGIC -- Cellule 1 : Top clients par CA
# MAGIC SELECT
# MAGIC     c.client,
# MAGIC     c.pays,
# MAGIC     COUNT(f.id_commande)        AS nb_commandes,
# MAGIC     SUM(f.montant_total_eur)    AS chiffre_affaires
# MAGIC FROM hive_metastore.gold.fact_ventes f
# MAGIC INNER JOIN hive_metastore.gold.dim_client c ON f.sk_client = c.sk_client
# MAGIC GROUP BY c.client, c.pays
# MAGIC ORDER BY chiffre_affaires DESC;