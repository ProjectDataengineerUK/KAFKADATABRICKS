# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Autoloader — cadastro de clientes/bancos/seguradoras
# MAGIC Ingestão incremental dos CSVs de cadastro (Unity Catalog Volume) para a
# MAGIC camada Bronze, com schema evolution automática e checkpoint para nunca
# MAGIC reler arquivos já processados. Cada CSV (clientes/bancos/seguradoras)
# MAGIC tem seu próprio Autoloader e sua própria tabela Bronze — schemas
# MAGIC diferentes não podem ser misturados no mesmo stream/tabela. Roda
# MAGIC inteiramente no Databricks Free Edition — sem storage externo
# MAGIC (Azure/AWS/GCP).

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

# (nome_arquivo, tabela_destino, checkpoint_key) — um Autoloader por CSV,
# cada um com schema location/checkpoint próprios, já que schemas diferentes
# não podem compartilhar stream/tabela (ver DELTA_METADATA_MISMATCH).
# checkpoint_key de clientes preserva o nome original (consentimento_cadastro)
# — trocar o path do checkpoint faria o Autoloader reprocessar tudo do zero e
# duplicar linhas na Bronze (write é append, sem dedup).
FONTES = [
    ("clientes.csv", "bronze.cadastro_clientes", "consentimento_cadastro"),
    ("bancos.csv", "bronze.bancos", "bancos"),
    ("seguradoras.csv", "bronze.seguradoras", "seguradoras"),
]


def ingerir(nome_arquivo: str, tabela_destino: str, checkpoint_key: str) -> None:
    bronze_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{checkpoint_base}/bronze/schema/{checkpoint_key}")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .option("pathGlobFilter", nome_arquivo)
        .load(raw_path)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

    query = (
        bronze_df.writeStream.format("delta")
        .option("checkpointLocation", f"{checkpoint_base}/bronze/{checkpoint_key}")
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(tabela_destino)
    )
    query.awaitTermination()


# COMMAND ----------

for nome_arquivo, tabela_destino, checkpoint_key in FONTES:
    ingerir(nome_arquivo, tabela_destino, checkpoint_key)
