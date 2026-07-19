# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: streaming Kafka + explode + join stream-static + MERGE
# MAGIC Consome o tópico Kafka `consentimentos`, explode o array aninhado de
# MAGIC consentimentos, enriquece com o cadastro (Bronze) via join stream-static
# MAGIC e faz upsert (`MERGE INTO`) na Silver por `cliente_id + tipo_consentimento`.
# MAGIC Eventos com schema inválido são desviados para uma tabela de quarentena.

# COMMAND ----------

dbutils.widgets.text("catalog", "consent_pipeline_dev")
dbutils.widgets.text("checkpoint_base", "")  # vazio = usa o Volume padrão do catálogo
dbutils.widgets.text("secret_scope", "consent-pipeline")
dbutils.widgets.text("bundle_root", "")  # ${workspace.file_path} — ver jobs.yml

catalog = dbutils.widgets.get("catalog")
checkpoint_base = dbutils.widgets.get("checkpoint_base") or f"/Volumes/{catalog}/ops/checkpoints"
secret_scope = dbutils.widgets.get("secret_scope")

spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

import sys

# Sincronizar src/ pelo bundle (databricks.yml sync.paths) não é suficiente
# sozinho: o Databricks só adiciona ao sys.path a pasta do próprio notebook,
# não a raiz do bundle onde src/ está como pasta irmã — sem isto, o import
# abaixo falha com "ModuleNotFoundError: No module named 'src'".
bundle_root = dbutils.widgets.get("bundle_root")
if bundle_root:
    sys.path.append(bundle_root)

from pyspark.sql import functions as F
from delta.tables import DeltaTable

from src.common.transforms import explode_consent_items, parse_and_split

# COMMAND ----------

kafka_df = (
    spark.readStream.format("kafka")
    .option(
        "kafka.bootstrap.servers",
        dbutils.secrets.get(secret_scope, "kafka-bootstrap"),
    )
    .option("kafka.security.protocol", "SASL_SSL")
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{dbutils.secrets.get(secret_scope, "kafka-api-key")}" '
        f'password="{dbutils.secrets.get(secret_scope, "kafka-api-secret")}";',
    )
    .option("subscribe", "consentimentos")
    .option("startingOffsets", "latest")
    .load()
)

raw_df = kafka_df.select(
    F.col("value").cast("string").alias("raw_json"),
    F.col("offset"),
    F.col("timestamp").alias("kafka_timestamp"),
)

validos_df, quarentena_df = parse_and_split(raw_df)
exploded_df = explode_consent_items(validos_df)

# COMMAND ----------

cadastro_df = spark.table("bronze.cadastro_clientes")


def upsert_to_silver(microbatch_df, batch_id: int) -> None:
    if microbatch_df.rdd.isEmpty():
        return

    enriched_df = microbatch_df.join(F.broadcast(cadastro_df), "cliente_id", "left")

    target = DeltaTable.forName(spark, f"{catalog}.silver.consentimentos")
    (
        target.alias("t")
        .merge(
            enriched_df.alias("s"),
            "t.cliente_id = s.cliente_id AND t.tipo_consentimento = s.tipo_consentimento",
        )
        .whenMatchedUpdateAll(condition="s.timestamp > t.timestamp")
        .whenNotMatchedInsertAll()
        .execute()
    )


def write_quarentena(microbatch_df, batch_id: int) -> None:
    if microbatch_df.rdd.isEmpty():
        return
    (
        microbatch_df.withColumn("_batch_id", F.lit(batch_id))
        .write.format("delta")
        .mode("append")
        .saveAsTable(f"{catalog}.silver.consentimentos_quarentena")
    )


# COMMAND ----------

silver_query = (
    exploded_df.writeStream.foreachBatch(upsert_to_silver)
    .option("checkpointLocation", f"{checkpoint_base}/silver/consentimentos")
    .trigger(processingTime="1 minute")
    .start()
)

quarentena_query = (
    quarentena_df.writeStream.foreachBatch(write_quarentena)
    .option("checkpointLocation", f"{checkpoint_base}/silver/quarentena")
    .trigger(processingTime="1 minute")
    .start()
)

# Sem awaitTermination(): dentro de um Databricks Job, essa chamada bloqueia
# o notebook para sempre e impede a task de reportar sucesso — o que por sua
# vez impede QUALQUER task downstream (depends_on) de rodar, já que ela
# nunca é satisfeita. O próprio Jobs service já mantém o run ativo enquanto
# houver streaming query rodando, sem precisar bloquear aqui.
