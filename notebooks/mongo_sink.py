# Databricks notebook source
# MAGIC %md
# MAGIC # Mongo Sink: regroup em struct aninhado → MongoDB ("base Susep" simulada)
# MAGIC Lê a Silver como stream, reagrupa os itens de consentimento por cliente
# MAGIC (`collect_list(struct(...))`) e grava o documento aninhado no MongoDB
# MAGIC Atlas via `foreachBatch`, com retry exponencial em caso de falha de conexão.

# COMMAND ----------

dbutils.widgets.text("catalog", "consent_pipeline_dev")
dbutils.widgets.text("checkpoint_base", "")  # vazio = usa o Volume padrão do catálogo
dbutils.widgets.text("secret_scope", "consent-pipeline")
dbutils.widgets.text("mongo_database", "susep_simulado")
dbutils.widgets.text("mongo_collection", "consentimentos_cliente")
dbutils.widgets.text("bundle_root", "")  # ${workspace.file_path} — ver jobs.yml

catalog = dbutils.widgets.get("catalog")
checkpoint_base = dbutils.widgets.get("checkpoint_base") or f"/Volumes/{catalog}/ops/checkpoints"
secret_scope = dbutils.widgets.get("secret_scope")
mongo_database = dbutils.widgets.get("mongo_database")
mongo_collection = dbutils.widgets.get("mongo_collection")

spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

import logging
import sys
import time

# Ver comentário equivalente em silver_consent_stream.py.
bundle_root = dbutils.widgets.get("bundle_root")
if bundle_root:
    sys.path.append(bundle_root)

from src.common.transforms import regroup_client_documents

logger = logging.getLogger("mongo_sink")

MAX_RETRIES = 3
BACKOFF_BASE_SECONDS = 2


def write_to_mongo(microbatch_df, batch_id: int) -> None:
    if microbatch_df.rdd.isEmpty():
        return

    documento_df = regroup_client_documents(microbatch_df)

    mongo_uri = dbutils.secrets.get(secret_scope, "mongo-uri")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            (
                documento_df.write.format("mongodb")
                .mode("append")
                .option("spark.mongodb.connection.uri", mongo_uri)
                .option("database", mongo_database)
                .option("collection", mongo_collection)
                .save()
            )
            logger.info("batch_id=%s gravado no MongoDB com sucesso", batch_id)
            return
        except Exception as exc:  # noqa: BLE001 - retry deliberado antes de propagar
            wait_seconds = BACKOFF_BASE_SECONDS ** attempt
            logger.warning(
                "batch_id=%s falha ao gravar no MongoDB (tentativa %s/%s): %s",
                batch_id,
                attempt,
                MAX_RETRIES,
                exc,
            )
            if attempt == MAX_RETRIES:
                raise
            time.sleep(wait_seconds)


# COMMAND ----------

query = (
    spark.readStream.table("silver.consentimentos")
    .writeStream.foreachBatch(write_to_mongo)
    .option("checkpointLocation", f"{checkpoint_base}/mongo/consentimentos")
    # Databricks Free Edition não suporta trigger de streaming contínuo — ver
    # comentário equivalente em silver_consent_stream.py.
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
