# Databricks notebook source
# MAGIC %md
# MAGIC # Mongo Sink: regroup em struct aninhado → MongoDB ("base Susep" simulada)
# MAGIC Lê a Silver como stream, reagrupa os itens de consentimento por cliente
# MAGIC (`collect_list(struct(...))`) e grava o documento aninhado no MongoDB
# MAGIC Atlas via `foreachBatch`, com retry exponencial em caso de falha de conexão.
# MAGIC
# MAGIC Grava via **pymongo** direto (não via `.write.format("mongodb")`): o
# MAGIC MongoDB Spark Connector é uma biblioteca JVM/Maven, e o Databricks Free
# MAGIC Edition só oferece compute serverless, que não permite anexar
# MAGIC bibliotecas JVM a um cluster — tentar usar o connector falha com
# MAGIC `[DATA_SOURCE_NOT_FOUND] Failed to find the data source: mongodb`. Como o
# MAGIC micro-batch já sai da Silver reagrupado por cliente (poucas linhas),
# MAGIC coletar para o driver e escrever com pymongo (mesma lib que a API usa)
# MAGIC é seguro e evita depender de um JAR que não pode ser instalado aqui.

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

from pymongo import MongoClient
from pyspark.sql import functions as F

from src.common.transforms import regroup_client_documents

logger = logging.getLogger("mongo_sink")

MAX_RETRIES = 3
BACKOFF_BASE_SECONDS = 2

# Buscado uma vez fora do foreachBatch: dbutils não pode ser usado dentro do
# closure (roda serializado em Spark Connect) — "Exception: You cannot use
# dbutils within a spark job". A string resultante, sim, é serializável.
mongo_uri = dbutils.secrets.get(secret_scope, "mongo-uri")


def write_to_mongo(microbatch_df, batch_id: int) -> None:
    if microbatch_df.isEmpty():
        return

    # microbatch_df.sparkSession, não a `spark` do escopo externo — ver
    # comentário equivalente em silver_consent_stream.py/gold_metricas.py.
    spark_session = microbatch_df.sparkSession

    # readChangeFeed traz update_preimage (linha antes da mudança) junto com
    # update_postimage (depois) — só nos importa saber QUAL cliente mudou,
    # não o valor da linha em si (relemos o estado completo dele abaixo).
    alterado_df = microbatch_df.filter(F.col("_change_type").isin("insert", "update_postimage"))
    if alterado_df.isEmpty():
        return

    clientes_afetados = [
        row["cliente_id"] for row in alterado_df.select("cliente_id").distinct().collect()
    ]

    # Relê o estado atual completo da Silver para os clientes afetados (não
    # só as linhas deste micro-batch): um mesmo cliente pode ter tipos de
    # consentimento diferentes gravados em micro-batches anteriores, e o
    # documento no Mongo precisa sempre refletir o conjunto completo — mesma
    # lógica de upsert_to_gold em gold_metricas.py.
    documento_df = regroup_client_documents(
        spark_session.table(f"{catalog}.silver.consentimentos").filter(
            F.col("cliente_id").isin(clientes_afetados)
        )
    )
    # .collect(), não .write: sem o connector JVM, o caminho é trazer o
    # resultado (já agregado por cliente) para o driver e gravar via
    # pymongo. asDict(recursive=True) resolve os Rows aninhados
    # (consentimentos: array<struct<...>>) em listas de dict puro, que o
    # pymongo serializa direto para BSON.
    documentos = [row.asDict(recursive=True) for row in documento_df.collect()]
    if not documentos:
        return

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            client = MongoClient(mongo_uri)
            try:
                colecao = client[mongo_database][mongo_collection]
                # replace_one upsert por cliente_id, não insert_many: agora
                # que updates chegam de verdade (era o próprio bug que
                # motivou trocar skipChangeCommits por readChangeFeed),
                # insert_many geraria um documento duplicado no Mongo a cada
                # vez que um cliente já existente mudasse.
                for documento in documentos:
                    colecao.replace_one(
                        {"cliente_id": documento["cliente_id"]}, documento, upsert=True
                    )
            finally:
                client.close()
            logger.info(
                "batch_id=%s gravado no MongoDB com sucesso (%s documentos)",
                batch_id,
                len(documentos),
            )
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
    spark.readStream
    # Ver comentário equivalente em gold_metricas.py: silver.consentimentos
    # recebe UPDATE via MERGE, então o Delta source recusa sem isto
    # ([DELTA_SOURCE_TABLE_IGNORE_CHANGES]) — readChangeFeed (não
    # skipChangeCommits) porque este último descarta o commit inteiro
    # sempre que há update, o que é frequente aqui (pool finito de clientes
    # demo reenviando os mesmos tipos de consentimento).
    .option("readChangeFeed", "true")
    .table("silver.consentimentos")
    .writeStream.foreachBatch(write_to_mongo)
    .option("checkpointLocation", f"{checkpoint_base}/mongo/consentimentos")
    # Databricks Free Edition não suporta trigger de streaming contínuo — ver
    # comentário equivalente em silver_consent_stream.py.
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
