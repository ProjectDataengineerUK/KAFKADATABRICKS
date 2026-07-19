# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: métricas agregadas de consentimento
# MAGIC Lê a Silver como stream e recalcula, para cada dia afetado pelo
# MAGIC micro-batch, as métricas agregadas por banco/seguradora/tipo de
# MAGIC consentimento — camada de consumo analítico (BI/dashboards),
# MAGIC completando a arquitetura medalhão Bronze/Silver/Gold.
# MAGIC
# MAGIC Recalcula o dia inteiro (não soma incrementalmente por micro-batch)
# MAGIC porque `total_clientes_distintos` não é uma métrica aditiva entre
# MAGIC batches — um mesmo cliente pode aparecer em micro-batches diferentes
# MAGIC do mesmo dia, e somar contagens distintas parciais gera duplicidade.
# MAGIC
# MAGIC Cada micro-batch também espelha as linhas de Gold afetadas para o
# MAGIC MongoDB (`gold_metricas`): a API roda no Render e só enxerga o Mongo,
# MAGIC não a Delta table do Databricks, então sem esse espelho o dashboard
# MAGIC não teria como ler a camada Gold.

# COMMAND ----------

dbutils.widgets.text("catalog", "consent_pipeline_dev")
dbutils.widgets.text("checkpoint_base", "")  # vazio = usa o Volume padrão do catálogo
dbutils.widgets.text("secret_scope", "consent-pipeline")
dbutils.widgets.text("mongo_database", "susep_simulado")
dbutils.widgets.text("mongo_gold_collection", "gold_metricas")

catalog = dbutils.widgets.get("catalog")
checkpoint_base = dbutils.widgets.get("checkpoint_base") or f"/Volumes/{catalog}/ops/checkpoints"
secret_scope = dbutils.widgets.get("secret_scope")
mongo_database = dbutils.widgets.get("mongo_database")
mongo_gold_collection = dbutils.widgets.get("mongo_gold_collection")

spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

from pymongo import MongoClient
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Ver comentário equivalente em mongo_sink.py: dbutils não pode ser usado
# dentro do foreachBatch (roda serializado em Spark Connect), então o
# segredo é buscado uma vez aqui fora — só a string mongo_uri é capturada
# pelo closure.
mongo_uri = dbutils.secrets.get(secret_scope, "mongo-uri")


def upsert_to_gold(microbatch_df, batch_id: int) -> None:
    if microbatch_df.isEmpty():
        return

    # microbatch_df.sparkSession, não a `spark` do escopo externo: em Spark
    # Connect, capturar no closure do foreachBatch qualquer objeto que
    # carregue uma SparkSession (a `spark` global aqui) falha com
    # [STREAMING_CONNECT_SERIALIZATION_ERROR] — ver comentário equivalente
    # em silver_consent_stream.py.
    spark_session = microbatch_df.sparkSession

    datas_afetadas = [
        row["data_referencia"]
        for row in (
            microbatch_df.withColumn("data_referencia", F.to_date("timestamp"))
            .select("data_referencia")
            .distinct()
            .collect()
        )
    ]

    agregado_df = (
        spark_session.table(f"{catalog}.silver.consentimentos")
        .withColumn("data_referencia", F.to_date("timestamp"))
        .filter(F.col("data_referencia").isin(datas_afetadas))
        .groupBy("data_referencia", "banco_origem", "seguradora_id", "tipo_consentimento")
        .agg(
            F.count("*").alias("total_eventos"),
            F.countDistinct("cliente_id").alias("total_clientes_distintos"),
        )
        .withColumn("_atualizado_em", F.current_timestamp())
    )

    target = DeltaTable.forName(spark_session, f"{catalog}.gold.metricas_consentimento")
    (
        target.alias("t")
        .merge(
            agregado_df.alias("s"),
            "t.data_referencia = s.data_referencia AND t.banco_origem = s.banco_origem "
            "AND t.seguradora_id = s.seguradora_id AND t.tipo_consentimento = s.tipo_consentimento",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    # Espelha só as linhas recalculadas neste micro-batch (não a tabela Gold
    # inteira) para o Mongo, via replace_one upsert — mesma chave composta do
    # MERGE acima, então repetir um dia recalculado não duplica documento.
    linhas = [row.asDict(recursive=True) for row in agregado_df.collect()]
    if linhas:
        client = MongoClient(mongo_uri)
        try:
            colecao = client[mongo_database][mongo_gold_collection]
            for linha in linhas:
                chave = {
                    "data_referencia": str(linha["data_referencia"]),
                    "banco_origem": linha["banco_origem"],
                    "seguradora_id": linha["seguradora_id"],
                    "tipo_consentimento": linha["tipo_consentimento"],
                }
                documento = {**chave, **linha, "data_referencia": str(linha["data_referencia"])}
                colecao.replace_one(chave, documento, upsert=True)
        finally:
            client.close()


# COMMAND ----------

query = (
    spark.readStream
    # silver.consentimentos recebe UPDATE via MERGE (upsert_to_silver), não só
    # append — sem isto o Delta source recusa com
    # [DELTA_SOURCE_TABLE_IGNORE_CHANGES]. Seguro pular aqui porque
    # upsert_to_gold só usa o microbatch para saber quais datas mudaram
    # (linha abaixo), e sempre relê o estado atual completo da Silver para
    # essas datas antes de agregar — não depende dos valores linha-a-linha
    # do stream.
    .option("skipChangeCommits", "true")
    .table("silver.consentimentos")
    .writeStream.foreachBatch(upsert_to_gold)
    .option("checkpointLocation", f"{checkpoint_base}/gold/metricas")
    # Databricks Free Edition não suporta trigger de streaming contínuo — ver
    # comentário equivalente em silver_consent_stream.py.
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

query.awaitTermination()
