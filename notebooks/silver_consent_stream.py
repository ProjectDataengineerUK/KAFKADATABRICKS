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
from pyspark.sql.window import Window
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
    # Sem sasl.mechanism explícito, o cliente Kafka cai no mecanismo SASL
    # default (Kerberos/GSSAPI) em vez de usar o PLAIN configurado no jaas
    # config abaixo — falha com "No serviceName defined in either JAAS or
    # Kafka config" (Confluent Cloud usa SASL/PLAIN com API Key/Secret).
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        # O conector Kafka do Databricks usa classes "shaded"
        # (kafkashaded.org.apache.kafka.*) — referenciar a classe não-shaded
        # (org.apache.kafka.*) falha com "No LoginModule found", mesmo que
        # essa seja a forma documentada para Spark/Kafka puro.
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{dbutils.secrets.get(secret_scope, "kafka-api-key")}" '
        f'password="{dbutils.secrets.get(secret_scope, "kafka-api-secret")}";',
    )
    .option("subscribe", "consentimentos")
    # earliest na primeira implantação: consentimento é dado sensível/
    # compliance, não faz sentido ignorar eventos já publicados antes deste
    # job existir (ver guia técnico, Rodada 1 pergunta 13). Só importa na
    # criação do checkpoint — depois disso o offset salvo manda.
    .option("startingOffsets", "earliest")
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

def upsert_to_silver(microbatch_df, batch_id: int) -> None:
    # df.isEmpty(), não df.rdd.isEmpty(): Spark Connect (usado pelo runtime
    # atual) não implementa o atributo .rdd — PySparkNotImplementedError.
    if microbatch_df.isEmpty():
        return

    # microbatch_df.sparkSession, não a `spark` do escopo externo: em Spark
    # Connect, foreachBatch roda serializado, e qualquer objeto capturado do
    # closure que carregue uma SparkSession (a `spark` global, ou um
    # DataFrame construído fora da função, como um cadastro_df de nível de
    # módulo) falha com [STREAMING_CONNECT_SERIALIZATION_ERROR]. Por isso
    # cadastro_df também é recriado aqui dentro, não reaproveitado de fora.
    spark_session = microbatch_df.sparkSession

    # Só cliente_id/nome_cliente/cpf: o evento Kafka já traz banco_origem
    # (ver CONSENT_SCHEMA em transforms.py), e bronze.cadastro_clientes
    # também tem uma coluna banco_origem própria — trazer as duas causa
    # [AMBIGUOUS_REFERENCE] no MERGE (s.banco_origem vira ambíguo).
    cadastro_df = spark_session.table("bronze.cadastro_clientes").select(
        "cliente_id", "nome_cliente", "cpf"
    )

    # Dedup por (cliente_id, tipo_consentimento) — a mesma chave do MERGE
    # abaixo: se dois eventos do mesmo micro-batch caírem na mesma chave
    # (ex.: o produtor demo publica mais de um evento do mesmo tipo para o
    # mesmo cliente na janela do batch), o MERGE recusa com
    # [DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE] — SQL MERGE
    # não permite duas linhas de origem casarem a mesma linha de destino.
    # Mantém só o evento mais recente por chave, coerente com a condição
    # "s.timestamp > t.timestamp" do whenMatchedUpdateAll abaixo.
    janela_dedup = Window.partitionBy("cliente_id", "tipo_consentimento").orderBy(
        F.col("timestamp").desc()
    )
    enriched_df = (
        microbatch_df.join(F.broadcast(cadastro_df), "cliente_id", "left")
        .withColumn("_rn", F.row_number().over(janela_dedup))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    target = DeltaTable.forName(spark_session, f"{catalog}.silver.consentimentos")
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
    if microbatch_df.isEmpty():
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
    # Databricks Free Edition não suporta trigger de streaming contínuo
    # ([INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED]) — availableNow processa o
    # que está disponível e termina, o que também torna correto o
    # awaitTermination() abaixo (diferente de trigger contínuo, aqui a query
    # de fato termina sozinha) e é o que permite mongo_sink/gold_metricas
    # (depends_on) rodarem em seguida.
    .trigger(availableNow=True)
    .start()
)

quarentena_query = (
    quarentena_df.writeStream.foreachBatch(write_quarentena)
    .option("checkpointLocation", f"{checkpoint_base}/silver/quarentena")
    .trigger(availableNow=True)
    .start()
)

silver_query.awaitTermination()
quarentena_query.awaitTermination()
