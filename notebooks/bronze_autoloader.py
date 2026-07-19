# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Autoloader — cadastro de clientes/bancos/seguradoras
# MAGIC Ingestão incremental dos CSVs de cadastro (Unity Catalog Volume) para a
# MAGIC camada Bronze, com schema evolution automática e checkpoint para nunca
# MAGIC reler arquivos já processados. Roda inteiramente no Databricks Free
# MAGIC Edition — sem storage externo (Azure/AWS/GCP).

# COMMAND ----------

dbutils.widgets.text("catalog", "consent_pipeline_dev")
dbutils.widgets.text("raw_path", "")  # vazio = usa o Volume padrão do catálogo
dbutils.widgets.text("checkpoint_base", "")  # vazio = usa o Volume padrão do catálogo

catalog = dbutils.widgets.get("catalog")
raw_path = dbutils.widgets.get("raw_path") or f"/Volumes/{catalog}/landing/cadastro/"
checkpoint_base = dbutils.widgets.get("checkpoint_base") or f"/Volumes/{catalog}/ops/checkpoints"

spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

bronze_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{checkpoint_base}/bronze/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("header", "true")
    # A landing zone recebe clientes.csv, bancos.csv e seguradoras.csv juntos
    # (ver producer/generate_reference_data.py), mas bronze.cadastro_clientes
    # só tem o schema de clientes (ver bootstrap_unity_catalog.sql) — sem este
    # filtro, o schema evolution une as colunas dos 3 arquivos e a escrita
    # falha com DELTA_METADATA_MISMATCH.
    .option("pathGlobFilter", "clientes.csv")
    .load(raw_path)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
)

# COMMAND ----------

query = (
    bronze_df.writeStream.format("delta")
    .option("checkpointLocation", f"{checkpoint_base}/bronze/consentimento_cadastro")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("bronze.cadastro_clientes")
)

query.awaitTermination()
