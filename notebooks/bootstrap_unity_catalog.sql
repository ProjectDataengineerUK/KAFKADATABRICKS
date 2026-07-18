-- Databricks notebook source
-- Bootstrap Unity Catalog: catálogos dev/prod, schemas, volumes (landing
-- zone dos CSVs e checkpoints de streaming — substituem o Azure Blob
-- Storage no Databricks Free Edition), grants, mascaramento de PII e
-- row-level security por seguradora.
-- Execução: uma vez por ambiente (dev/prod), via job de setup ou manualmente
-- por um admin do workspace. Parametrizado por :catalog (widget).
--
-- NOTA (Free Edition): contas Free Edition normalmente já vêm com um
-- catálogo padrão e podem restringir `CREATE CATALOG` a um único catálogo
-- por conta. Se o `CREATE CATALOG` abaixo falhar por permissão, use o
-- catálogo padrão da conta e troque a separação dev/prod para o nível de
-- schema (ex.: `dev_bronze`/`prod_bronze` em vez de catálogos distintos) —
-- ajuste os widgets `catalog` nos outros notebooks e no `databricks.yml`
-- de acordo.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog", "consent_pipeline_dev")
-- MAGIC catalog = dbutils.widgets.get("catalog")

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS IDENTIFIER(:catalog);

CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.landing');
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.bronze');
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.silver');
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.ops');

-- COMMAND ----------

-- Volumes: landing zone dos CSVs de cadastro e diretório de checkpoints
-- de streaming. Substituem o Azure Blob Storage montado — tudo dentro do
-- próprio workspace Databricks Free Edition.
CREATE VOLUME IF NOT EXISTS IDENTIFIER(:catalog || '.landing.cadastro');
CREATE VOLUME IF NOT EXISTS IDENTIFIER(:catalog || '.ops.checkpoints');

-- COMMAND ----------

-- Tabela Bronze (criada implicitamente pelo Autoloader, DDL aqui apenas
-- documenta o contrato esperado para revisão/CI).
CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.bronze.cadastro_clientes') (
  cliente_id STRING NOT NULL,
  nome_cliente STRING,
  cpf STRING,
  banco_origem STRING,
  _ingested_at TIMESTAMP,
  _source_file STRING
) USING DELTA;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.silver.consentimentos') (
  cliente_id STRING NOT NULL,
  banco_origem STRING,
  seguradora_id STRING,
  nome_cliente STRING,
  cpf STRING,
  timestamp TIMESTAMP NOT NULL,
  offset BIGINT,
  tipo_consentimento STRING NOT NULL,
  escopo ARRAY<STRING>,
  status STRING
)
USING DELTA
PARTITIONED BY (banco_origem);

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.silver.consentimentos_quarentena') (
  raw_json STRING,
  offset BIGINT,
  kafka_timestamp TIMESTAMP,
  _batch_id BIGINT
) USING DELTA;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # GRANT não suporta a cláusula IDENTIFIER() (ver
-- MAGIC # https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-names-identifier-clause),
-- MAGIC # por isso o nome do catálogo é interpolado diretamente aqui em vez de
-- MAGIC # usar o parameter marker :catalog como nas células SQL acima.
-- MAGIC # Grupos: data_engineers (acesso total), analysts (leitura mascarada)
-- MAGIC for stmt in [
-- MAGIC     f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `data_engineers`",
-- MAGIC     f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `analysts`",
-- MAGIC     f"GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA `{catalog}`.landing TO `data_engineers`",
-- MAGIC     f"GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA `{catalog}`.bronze TO `data_engineers`",
-- MAGIC     f"GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA `{catalog}`.silver TO `data_engineers`",
-- MAGIC     f"GRANT USE SCHEMA, READ VOLUME, WRITE VOLUME ON SCHEMA `{catalog}`.ops TO `data_engineers`",
-- MAGIC     f"GRANT USE SCHEMA, SELECT ON SCHEMA `{catalog}`.silver TO `analysts`",
-- MAGIC ]:
-- MAGIC     spark.sql(stmt)

-- COMMAND ----------

-- Mascaramento de PII: cpf e nome_cliente ficam mascarados para quem não
-- pertence ao grupo data_engineers.
CREATE OR REPLACE FUNCTION IDENTIFIER(:catalog || '.silver.mask_cpf')(cpf STRING)
RETURN CASE
  WHEN is_account_group_member('data_engineers') THEN cpf
  ELSE CONCAT('***.***.***-', RIGHT(cpf, 2))
END;

CREATE OR REPLACE FUNCTION IDENTIFIER(:catalog || '.silver.mask_nome')(nome STRING)
RETURN CASE
  WHEN is_account_group_member('data_engineers') THEN nome
  ELSE 'CLIENTE PROTEGIDO'
END;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # SET MASK referencia a função mascaradora como argumento (não como
-- MAGIC # alvo do ALTER TABLE), mesma limitação do GRANT acima com IDENTIFIER().
-- MAGIC for stmt in [
-- MAGIC     f"ALTER TABLE `{catalog}`.silver.consentimentos "
-- MAGIC     f"ALTER COLUMN cpf SET MASK `{catalog}`.silver.mask_cpf",
-- MAGIC     f"ALTER TABLE `{catalog}`.silver.consentimentos "
-- MAGIC     f"ALTER COLUMN nome_cliente SET MASK `{catalog}`.silver.mask_nome",
-- MAGIC ]:
-- MAGIC     spark.sql(stmt)

-- COMMAND ----------

-- Row-level security: analistas só veem registros da seguradora à qual
-- pertencem. `current_user_seguradora` é um placeholder que sempre
-- retorna NULL (fail-closed: nenhuma seguradora liberada) até ser trocada
-- por um lookup real usuário -> seguradora_id (fora do escopo desta demo).
CREATE OR REPLACE FUNCTION IDENTIFIER(:catalog || '.silver.current_user_seguradora')()
RETURNS STRING
COMMENT 'Placeholder: substituir por lookup real usuário -> seguradora_id.'
RETURN CAST(NULL AS STRING);

CREATE OR REPLACE FUNCTION IDENTIFIER(:catalog || '.silver.filter_seguradora')(seguradora_id STRING)
RETURN is_account_group_member('data_engineers')
  OR seguradora_id = IDENTIFIER(:catalog || '.silver.current_user_seguradora')();

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Mesma limitação: SET ROW FILTER referencia a função como argumento.
-- MAGIC spark.sql(
-- MAGIC     f"ALTER TABLE `{catalog}`.silver.consentimentos "
-- MAGIC     f"SET ROW FILTER `{catalog}`.silver.filter_seguradora ON (seguradora_id)"
-- MAGIC )
