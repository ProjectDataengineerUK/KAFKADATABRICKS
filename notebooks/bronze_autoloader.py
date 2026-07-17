# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Autoloader — cadastro de clientes/bancos/seguradoras
# MAGIC Ingestão incremental dos CSVs de cadastro (Azure Storage) para a camada
# MAGIC Bronze, com schema evolution automática e checkpoint para nunca reler
# MAGIC arquivos já processados.

# COMMAND ----------

dbutils.widgets.text("catalog", "consent_pipeline_dev")
dbutils.widgets.text("raw_path", "/mnt/raw/cadastro/")
dbutils.widgets.text("checkpoint_base", "/mnt/checkpoints")

catalog = dbutils.widgets.get("catalog")
raw_path = dbutils.widgets.get("raw_path")
checkpoint_base = dbutils.widgets.get("checkpoint_base")

spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name

bronze_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{checkpoint_base}/bronze/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("header", "true")
    .load(raw_path)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
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
