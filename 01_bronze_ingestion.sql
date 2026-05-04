-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS hive_metastore.bronze
  COMMENT 'Couche Bronze : données brutes';

CREATE SCHEMA IF NOT EXISTS hive_metastore.silver
  COMMENT 'Couche Silver : données nettoyées';

CREATE SCHEMA IF NOT EXISTS hive_metastore.gold
  COMMENT 'Couche Gold : données curated';

-- Vérification
SHOW SCHEMAS IN hive_metastore;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import current_timestamp, lit, input_file_name
-- MAGIC from datetime import datetime
-- MAGIC import io
-- MAGIC
-- MAGIC # Lire le CSV manuellement via dbutils
-- MAGIC raw_content = dbutils.fs.head("abfss://landing@adlscommercialprod2.dfs.core.windows.net/ventes_commerciales.csv")
-- MAGIC
-- MAGIC # Parser le CSV
-- MAGIC lines = raw_content.strip().split("\n")
-- MAGIC headers = lines[0].split(",")
-- MAGIC rows = [line.split(",") for line in lines[1:]]
-- MAGIC
-- MAGIC # Créer le DataFrame depuis les données en mémoire
-- MAGIC df_raw = spark.createDataFrame(rows, headers)
-- MAGIC
-- MAGIC # Ajout métadonnées
-- MAGIC batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
-- MAGIC df_bronze = (
-- MAGIC     df_raw
-- MAGIC     .withColumn("_ingestion_ts", current_timestamp())
-- MAGIC     .withColumn("_source_file", lit("ventes_commerciales.csv"))
-- MAGIC     .withColumn("_batch_id", lit(batch_id))
-- MAGIC )
-- MAGIC
-- MAGIC # Enregistrer comme table Delta dans le metastore (sans écrire sur ADLS)
-- MAGIC df_bronze.write \
-- MAGIC     .format("delta") \
-- MAGIC     .mode("overwrite") \
-- MAGIC     .saveAsTable("hive_metastore.bronze.ventes_raw")
-- MAGIC
-- MAGIC print(f"✅ Bronze chargé : {df_bronze.count()} lignes")
-- MAGIC df_bronze.printSchema()