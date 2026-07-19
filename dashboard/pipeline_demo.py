"""Passo a passo do pipeline real deste projeto, ligando cada etapa ao
código-fonte de fato implementado (não exemplos genéricos de curso) e aos
conceitos do guia técnico Databricks/PySpark/Kafka/Mongo. Ver a aba
"Pipeline em Ação" em app.py — cada etapa expõe seu código real e, quando
possível, uma prova ao vivo (dado real vindo da API).
"""

from __future__ import annotations

ETAPAS = [
    {
        "titulo": "1. Bronze — Autoloader (notebooks/bronze_autoloader.py)",
        "conceito": (
            "Ingestão incremental do cadastro (clientes/bancos/seguradoras) via "
            "**Auto Loader**, com schema location e checkpoint location em pastas "
            "separadas — reprocessar (apagar checkpoint) não obriga reinferir "
            "schema, e vice-versa. `trigger(availableNow=True)` processa tudo que "
            "está disponível e **para** — não fica cluster de streaming ligado "
            "24/7, é o mesmo ponto de economia do guia (seção 11.3)."
        ),
        "codigo": '''bronze_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{checkpoint_base}/bronze/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("header", "true")
    .load(raw_path)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

query = (
    bronze_df.writeStream.format("delta")
    .option("checkpointLocation", f"{checkpoint_base}/bronze/consentimento_cadastro")
    .outputMode("append")
    .trigger(availableNow=True)  # processa e desliga — não é cluster 24/7
    .toTable("bronze.cadastro_clientes")
)''',
        "prova": "workflow_status",
    },
    {
        "titulo": "2. Silver — Kafka + explode + join + MERGE (notebooks/silver_consent_stream.py + src/common/transforms.py)",
        "conceito": (
            "Consome o tópico Kafka, valida o schema (`parse_and_split` — schema "
            "malformado ou sem itens vai para quarentena, não derruba o job), "
            "**explode** o array `consentimentos` (uma linha por item, igual seção "
            "5 do guia), faz join stream-static (broadcast) com a Bronze, e grava "
            "via **MERGE INTO** com `whenMatchedUpdateAll(condition=\"s.timestamp > t.timestamp\")` "
            "— a mesma proteção contra reprocessamento de dado antigo sobrescrever "
            "dado novo descrita na seção 3 e na Rodada 1, pergunta 1."
        ),
        "codigo": '''validos_df, quarentena_df = parse_and_split(raw_df)
exploded_df = explode_consent_items(validos_df)  # explode: 1 linha por item

def upsert_to_silver(microbatch_df, batch_id):
    enriched_df = microbatch_df.join(F.broadcast(cadastro_df), "cliente_id", "left")
    target = DeltaTable.forName(spark, f"{catalog}.silver.consentimentos")
    (
        target.alias("t")
        .merge(enriched_df.alias("s"),
               "t.cliente_id = s.cliente_id AND t.tipo_consentimento = s.tipo_consentimento")
        .whenMatchedUpdateAll(condition="s.timestamp > t.timestamp")  # protege contra dado antigo
        .whenNotMatchedInsertAll()
        .execute()
    )''',
        "prova": "consulta_cliente",
    },
    {
        "titulo": "3. Mongo Sink — struct aninhado (notebooks/mongo_sink.py + src/common/transforms.py)",
        "conceito": (
            "Lê a Silver como stream e faz o caminho **inverso** do explode: "
            "`groupBy` + `collect_list(struct(...))` remonta um documento por "
            "cliente com o array de consentimentos aninhado — exatamente o padrão "
            "da seção 5 do guia. Grava no MongoDB via `foreachBatch` com retry "
            "exponencial (o sink não tem suporte nativo de streaming maduro, por "
            "isso `foreachBatch` em vez de `writeStream` direto)."
        ),
        "codigo": '''def regroup_client_documents(silver_df):
    return silver_df.groupBy("cliente_id", "banco_origem", "seguradora_id").agg(
        F.collect_list(
            F.struct("tipo_consentimento", "escopo", "status", "timestamp")
        ).alias("consentimentos")
    )

# write_to_mongo(): retry exponencial (2**tentativa) antes de propagar a falha
documento_df.write.format("mongodb").mode("append") \\
    .option("spark.mongodb.connection.uri", mongo_uri) \\
    .option("database", mongo_database).option("collection", mongo_collection) \\
    .save()''',
        "prova": "documento_raw",
    },
    {
        "titulo": "4. Governança — Unity Catalog (notebooks/bootstrap_unity_catalog.sql)",
        "conceito": (
            "GRANT centralizado por grupo (`data_engineers` acesso total, "
            "`analysts` leitura mascarada), **column masking** em `cpf`/`nome_cliente` "
            "via `SET MASK`, e **row filter** por seguradora via `SET ROW FILTER` "
            "— o mesmo conjunto de recursos da seção 6 e da Rodada 2 (perguntas 6, "
            "8, 9) do guia, já aplicado nos catálogos `consent_pipeline_dev` e "
            "`consent_pipeline_prod` deste projeto."
        ),
        "codigo": '''GRANT USE SCHEMA, SELECT ON SCHEMA `{catalog}`.silver TO `analysts`

CREATE OR REPLACE FUNCTION IDENTIFIER(:catalog || '.silver.mask_cpf')(cpf STRING)
RETURN CASE
  WHEN is_account_group_member('data_engineers') THEN cpf
  ELSE CONCAT('***.***.***-', RIGHT(cpf, 2))
END;

ALTER TABLE `{catalog}`.silver.consentimentos
  ALTER COLUMN cpf SET MASK `{catalog}`.silver.mask_cpf;

ALTER TABLE `{catalog}`.silver.consentimentos
  SET ROW FILTER `{catalog}`.silver.filter_seguradora ON (seguradora_id);''',
        "prova": None,
    },
    {
        "titulo": "5. Custo & Otimização — decisões reais deste projeto",
        "conceito": "Cada escolha de custo do guia (seção 11), mapeada para o que este projeto de fato faz — não teoria solta.",
        "codigo": None,
        "prova": "checklist_custo",
    },
]

CHECKLIST_CUSTO = [
    (
        "`trigger(availableNow=True)` no Autoloader",
        "Bronze processa e desliga — não paga cluster de streaming 24/7 só para ingerir 3 CSVs pequenos.",
    ),
    (
        "`trigger(processingTime=\"1 minute\")` na Silver/Mongo sink",
        "Trade-off deliberado: latência de até 1 min é aceitável para uma demo, evita o custo do modo `continuous`.",
    ),
    (
        "Databricks Free Edition",
        "Zero custo de compute/cluster — troca o \"Job Cluster efêmero\" do guia pelo equivalente gratuito da conta Free Edition.",
    ),
    (
        "MERGE com `s.timestamp > t.timestamp`",
        "Idempotência como economia (seção 11.4): reprocessar o mesmo batch não gera retrabalho manual de dedup.",
    ),
    (
        "Confluent Cloud trial + MongoDB Atlas M0 + Render free + Streamlit Community Cloud",
        "Toda a infra externa ao Databricks roda em tier gratuito — `terraform destroy` recomendado no README se for pausar por muito tempo, para não gerar cobrança após o crédito trial do Confluent acabar.",
    ),
    (
        "GitHub Actions (schedule) gerando dados de demo",
        "Orquestração do produtor Kafka + upload do cadastro roda nos 2.000 min/mês grátis do GitHub Actions, sem precisar de um worker sempre ligado.",
    ),
]
