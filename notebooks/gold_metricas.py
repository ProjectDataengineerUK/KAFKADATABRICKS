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

# COMMAND ----------

dbutils.widgets.text("catalog", "consent_pipeline_dev")
dbutils.widgets.text("checkpoint_base", "")  # vazio = usa o Volume padrão do catálogo

catalog = dbutils.widgets.get("catalog")
checkpoint_base = dbutils.widgets.get("checkpoint_base") or f"/Volumes/{catalog}/ops/checkpoints"

spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable


def upsert_to_gold(microbatch_df, batch_id: int) -> None:
    if microbatch_df.isEmpty():
        return

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
        spark.table(f"{catalog}.silver.consentimentos")
        .withColumn("data_referencia", F.to_date("timestamp"))
        .filter(F.col("data_referencia").isin(datas_afetadas))
        .groupBy("data_referencia", "banco_origem", "seguradora_id", "tipo_consentimento")
        .agg(
            F.count("*").alias("total_eventos"),
            F.countDistinct("cliente_id").alias("total_clientes_distintos"),
        )
        .withColumn("_atualizado_em", F.current_timestamp())
    )

    target = DeltaTable.forName(spark, f"{catalog}.gold.metricas_consentimento")
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


# COMMAND ----------

query = (
    spark.readStream.table("silver.consentimentos")
    .writeStream.foreachBatch(upsert_to_gold)
    .option("checkpointLocation", f"{checkpoint_base}/gold/metricas")
    # Databricks Free Edition não suporta trigger de streaming contínuo — ver
    # comentário equivalente em silver_consent_stream.py.
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

query.awaitTermination()
