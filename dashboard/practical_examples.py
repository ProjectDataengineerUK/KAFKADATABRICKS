"""100 exemplos práticos e técnicos de padrões de pipeline de dados —
Databricks/PySpark, Kafka, MongoDB/Cosmos, Unity Catalog, API, custo,
qualidade de dados, CI/CD e observabilidade. Cada item é um padrão real
aplicável, não teoria abstrata — usado na aba "💡 100 Exemplos" do
dashboard (dashboard/app.py).
"""

from __future__ import annotations

EXEMPLOS = [
    # ---------- A. Ingestão / Bronze (Autoloader) ----------
    {
        "categoria": "Ingestão (Autoloader)",
        "titulo": "Schema evolution automática sem quebrar o pipeline",
        "codigo": '.option("cloudFiles.schemaEvolutionMode", "addNewColumns")',
        "explicacao": "Coluna nova no CSV/JSON de origem não derruba o stream — é adicionada automaticamente ao schema na próxima execução.",
    },
    {
        "categoria": "Ingestão (Autoloader)",
        "titulo": "Rescued data column para registros fora do schema",
        "codigo": '.option("cloudFiles.rescuedDataColumn", "_rescued_data")',
        "explicacao": "Campos que não batem com o schema esperado vão para uma coluna JSON separada em vez de quebrar o job ou serem descartados silenciosamente.",
    },
    {
        "categoria": "Ingestão (Autoloader)",
        "titulo": "File notification mode para alto volume de arquivos",
        "codigo": '.option("cloudFiles.useNotifications", "true")',
        "explicacao": "Troca directory listing por Event Grid/SQS — evita listar milhões de arquivos a cada trigger, reduzindo custo e latência de detecção.",
    },
    {
        "categoria": "Ingestão (Autoloader)",
        "titulo": "Controlar throughput por trigger com maxFilesPerTrigger",
        "codigo": '.option("cloudFiles.maxFilesPerTrigger", 1000)',
        "explicacao": "Evita que um backlog grande de arquivos sature o cluster num único micro-batch — processa em fatias previsíveis.",
    },
    {
        "categoria": "Ingestão (Autoloader)",
        "titulo": "Schema hints para tipos que a inferência erra",
        "codigo": '.option("cloudFiles.schemaHints", "cliente_id STRING, valor DECIMAL(10,2)")',
        "explicacao": "Força o tipo correto quando a inferência automática do Auto Loader chuta errado (ex: CPF virando bigint por ter só dígitos).",
    },
    {
        "categoria": "Ingestão (Autoloader)",
        "titulo": "Multiplexar N tabelas com um único stream cloudFiles",
        "codigo": (
            "def route_por_tabela(df, batch_id):\n"
            '    for tabela in df.select("_metadata.file_path").distinct().collect():\n'
            "        ...  # filtra e escreve cada tabela separadamente"
        ),
        "explicacao": "Quando várias origens caem na mesma landing zone, um único job com foreachBatch decide o destino por tabela — evita N streams idênticos consumindo cluster.",
    },
    {
        "categoria": "Ingestão (Autoloader)",
        "titulo": "Inferir colunas de partição a partir do path",
        "codigo": '.option("cloudFiles.partitionColumns", "ano,mes,dia")',
        "explicacao": "Se o path é .../ano=2026/mes=07/, o Auto Loader popula essas colunas sem precisar de parsing manual de string.",
    },
    {
        "categoria": "Ingestão (Autoloader)",
        "titulo": "Arquivar arquivos processados automaticamente",
        "codigo": '.option("cloudFiles.cleanSource", "MOVE").option("cloudFiles.cleanSource.moveDestination", "/archive/")',
        "explicacao": "Evita acúmulo indefinido de arquivos na landing zone e o custo de listagem crescente, sem apagar o dado bruto.",
    },
    {
        "categoria": "Ingestão (Autoloader)",
        "titulo": "Reprocessar arquivo que foi sobrescrito na origem",
        "codigo": '.option("cloudFiles.allowOverwrites", "true")',
        "explicacao": "Por padrão o Auto Loader ignora um arquivo já visto mesmo que o conteúdo mude — necessário quando a origem faz upsert do próprio arquivo.",
    },
    {
        "categoria": "Ingestão (Autoloader)",
        "titulo": "Metadados de proveniência obrigatórios na Bronze",
        "codigo": '.withColumn("_source_file", F.col("_metadata.file_path")).withColumn("_ingested_at", F.current_timestamp())',
        "explicacao": "Sem isso, um bug descoberto dias depois não tem como ser isolado a um arquivo/janela de tempo específica para reprocessamento cirúrgico.",
    },
    # ---------- B. Silver / Delta MERGE & Optimization ----------
    {
        "categoria": "Delta / MERGE",
        "titulo": "Sincronizar exclusões da origem (soft delete)",
        "codigo": '.whenNotMatchedBySourceUpdate(set={"status": "\'inativo\'"})',
        "explicacao": "Registro que sumiu da origem não é apagado — é marcado inativo, preservando histórico e auditoria.",
    },
    {
        "categoria": "Delta / MERGE",
        "titulo": "Update condicional por coluna dentro do mesmo MERGE",
        "codigo": '.whenMatchedUpdate(condition="s.status = \'cancelado\'", set={"status": "s.status", "motivo": "s.motivo"})',
        "explicacao": "Só sobrescreve campos quando a condição de negócio é satisfeita, evitando update indiscriminado de linhas que só mudaram de posição no batch.",
    },
    {
        "categoria": "Delta / MERGE",
        "titulo": "Deduplicação nativa em streaming com watermark",
        "codigo": 'df.withWatermark("timestamp", "10 minutes").dropDuplicatesWithinWatermark(["id_evento"])',
        "explicacao": "Deduplica dentro da janela de watermark sem precisar materializar estado ilimitado — mais barato que um window function global.",
    },
    {
        "categoria": "Delta / MERGE",
        "titulo": "Compactar small files e ordenar fisicamente",
        "codigo": "OPTIMIZE silver.consentimentos ZORDER BY (cliente_id)",
        "explicacao": "Resolve small file problem e habilita data skipping — leituras filtradas por cliente_id leem muito menos arquivo físico.",
    },
    {
        "categoria": "Delta / MERGE",
        "titulo": "Remover versões antigas do Delta com segurança",
        "codigo": "VACUUM silver.consentimentos RETAIN 168 HOURS",
        "explicacao": "Sem VACUUM, o storage cresce indefinidamente mesmo com o dado lógico estável — 168h (7 dias) é o mínimo seguro para não quebrar time travel em uso.",
    },
    {
        "categoria": "Delta / MERGE",
        "titulo": "Compactação automática sem job manual de OPTIMIZE",
        "codigo": "ALTER TABLE silver.consentimentos SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true)",
        "explicacao": "A cada escrita, o Delta já compacta arquivos pequenos — reduz a dependência de rodar OPTIMIZE manualmente em cron.",
    },
    {
        "categoria": "Delta / MERGE",
        "titulo": "Capturar apenas o que mudou (CDC) sem reprocessar tudo",
        "codigo": 'spark.readStream.format("delta").option("readChangeFeed", "true").table("silver.consentimentos")',
        "explicacao": "Consumidores downstream (Gold, exportação) leem só o delta de mudanças, não a tabela inteira a cada execução.",
    },
    {
        "categoria": "Delta / MERGE",
        "titulo": "Clustering adaptativo sem escolher partição fixa",
        "codigo": "ALTER TABLE silver.consentimentos CLUSTER BY (cliente_id, banco_origem)",
        "explicacao": "Liquid Clustering redistribui o dado conforme o padrão de consulta muda, sem o rigidez do particionamento tradicional.",
    },
    {
        "categoria": "Delta / MERGE",
        "titulo": "Garantir invariante de negócio no nível da tabela",
        "codigo": "ALTER TABLE silver.consentimentos ADD CONSTRAINT status_valido CHECK (status IN ('ativo','revogado','pendente'))",
        "explicacao": "Um valor de status inválido falha o write imediatamente, em vez de ser descoberto só quando o dashboard mostrar número estranho.",
    },
    {
        "categoria": "Delta / MERGE",
        "titulo": "Investigar/reverter para um estado anterior conhecido",
        "codigo": "SELECT * FROM silver.consentimentos VERSION AS OF 42",
        "explicacao": "Time travel permite comparar o estado antes/depois de um deploy suspeito sem precisar de backup externo.",
    },
    # ---------- C. Kafka / Structured Streaming ----------
    {
        "categoria": "Kafka / Streaming",
        "titulo": "Agregação por janela de tempo tolerando atraso",
        "codigo": 'df.withWatermark("ts", "15 minutes").groupBy(F.window("ts", "5 minutes"), "banco_origem").count()',
        "explicacao": "Eventos até 15 min atrasados ainda entram na janela correta — sem watermark, uma métrica de negócio de janela fixa fica incorreta silenciosamente.",
    },
    {
        "categoria": "Kafka / Streaming",
        "titulo": "Controlar custo/latência do micro-batch",
        "codigo": '.option("maxOffsetsPerTrigger", 10000)',
        "explicacao": "Evita que um pico no tópico gere um micro-batch gigante que estoura memória do executor — processa em fatias previsíveis.",
    },
    {
        "categoria": "Kafka / Streaming",
        "titulo": "Dead-letter pattern sem derrubar o stream principal",
        "codigo": (
            "def processa(df, batch_id):\n"
            "    try:\n"
            "        escreve_destino(df)\n"
            "    except Exception:\n"
            "        df.write.mode('append').saveAsTable('ops.dead_letter')"
        ),
        "explicacao": "Um batch problemático vai para quarentena e é investigado depois, em vez de travar o pipeline inteiro até alguém corrigir manualmente.",
    },
    {
        "categoria": "Kafka / Streaming",
        "titulo": "Exactly-once de ponta a ponta via sink idempotente",
        "codigo": '.merge(microbatch.alias("s"), "t.id = s.id").whenMatchedUpdateAll().whenNotMatchedInsertAll()',
        "explicacao": "Checkpoint sozinho evita perda de dado, mas não evita duplicidade — o upsert pela chave é o que garante exactly-once de fato no destino.",
    },
    {
        "categoria": "Kafka / Streaming",
        "titulo": "Consumir múltiplos tópicos relacionados com um padrão",
        "codigo": '.option("subscribePattern", "consentimentos-.*")',
        "explicacao": "Novo tópico por banco/produto entra automaticamente no consumo, sem precisar redeployar o job a cada novo tópico criado.",
    },
    {
        "categoria": "Kafka / Streaming",
        "titulo": "Particionar produção pela chave certa evita hot partition",
        "codigo": 'producer.produce(topic, key=cliente_id.encode(), value=payload)',
        "explicacao": "Chave de particionamento mal escolhida concentra volume em poucas partições — usar uma chave de alta cardinalidade (cliente_id) distribui melhor.",
    },
    {
        "categoria": "Kafka / Streaming",
        "titulo": "Join stream-stream com watermark nos dois lados",
        "codigo": 'stream_a.withWatermark("ts_a","10 minutes").join(stream_b.withWatermark("ts_b","10 minutes"), "id")',
        "explicacao": "Sem watermark nos dois lados, o Spark mantém estado de join ilimitado na memória — cresce até estourar o cluster.",
    },
    {
        "categoria": "Kafka / Streaming",
        "titulo": "Latência sub-segundo quando realmente necessário",
        "codigo": '.trigger(continuous="1 second")',
        "explicacao": "Trigger contínuo tem custo bem mais alto que micro-batch — reservar para os poucos casos onde latência de segundos é requisito real de negócio.",
    },
    {
        "categoria": "Kafka / Streaming",
        "titulo": "Isolar checkpoint por ambiente evita cross-contamination",
        "codigo": 'checkpointLocation = f"/Volumes/{catalog}/ops/checkpoints/{stream_name}"',
        "explicacao": "dev e prod compartilhando checkpoint causa offset corrompido entre ambientes — path do checkpoint deve incluir o catálogo/ambiente.",
    },
    {
        "categoria": "Kafka / Streaming",
        "titulo": "Schema versionado em vez de JSON solto",
        "codigo": 'from_avro(F.col("value"), avro_schema_str, {"mode": "PERMISSIVE"})',
        "explicacao": "Avro com schema registry detecta incompatibilidade de schema no consumo, em vez de silenciosamente parsear campo errado como null.",
    },
    # ---------- D. MongoDB / Cosmos modeling ----------
    {
        "categoria": "MongoDB / Cosmos",
        "titulo": "Escrever em lote em vez de documento a documento",
        "codigo": 'collection.bulk_write([UpdateOne({"cliente_id": c}, {"$set": doc}, upsert=True) for c, doc in batch])',
        "explicacao": "Reduz drasticamente overhead de rede/RU comparado a um write() por documento — essencial em foreachBatch de streaming.",
    },
    {
        "categoria": "MongoDB / Cosmos",
        "titulo": "Índice alinhado ao padrão de consulta real",
        "codigo": 'collection.create_index([("cliente_id", 1), ("consentimentos.status", 1)])',
        "explicacao": "Sem esse índice composto, filtrar por cliente + status de consentimento faz collection scan completo a cada consulta da API.",
    },
    {
        "categoria": "MongoDB / Cosmos",
        "titulo": "Expirar dado transitório automaticamente",
        "codigo": 'collection.create_index("criado_em", expireAfterSeconds=2592000)',
        "explicacao": "TTL index remove documentos de quarentena/log antigos sem precisar de job de limpeza manual agendado.",
    },
    {
        "categoria": "MongoDB / Cosmos",
        "titulo": "Enriquecer documento com dado de outra coleção",
        "codigo": 'collection.aggregate([{"$lookup": {"from": "seguradoras", "localField": "seguradora_id", "foreignField": "_id", "as": "seguradora"}}])',
        "explicacao": "Evita desnormalizar tudo no documento principal quando o dado de referência é consultado com pouca frequência.",
    },
    {
        "categoria": "MongoDB / Cosmos",
        "titulo": "Reagir a mudanças no Mongo em tempo real (CDC)",
        "codigo": "with collection.watch() as stream:\n    for change in stream:\n        processa(change)",
        "explicacao": "Change Streams evita polling — outro serviço (ex: notificação, cache invalidation) reage à escrita do Spark sem consultar o Mongo repetidamente.",
    },
    {
        "categoria": "MongoDB / Cosmos",
        "titulo": "Escolher shard key pelo padrão de escrita, não só leitura",
        "codigo": 'sh.shardCollection("susep.consentimentos", {"cliente_id": "hashed"})',
        "explicacao": "Shard key de baixa cardinalidade (ex: banco_origem, só 5 valores) concentra escrita em poucos shards — hash de cliente_id distribui melhor.",
    },
    {
        "categoria": "MongoDB / Cosmos",
        "titulo": "Decidir embed vs reference pelo tamanho do relacionamento",
        "codigo": "# 1-para-poucos (< ~100 itens): embed. 1-para-muitos/ilimitado: reference",
        "explicacao": "Array de consentimentos por cliente (dezenas) é seguro para embed; array de eventos de log (milhares) deveria ser uma coleção referenciada.",
    },
    {
        "categoria": "MongoDB / Cosmos",
        "titulo": "Materializar uma view agregada como coleção",
        "codigo": 'collection.aggregate([...pipeline..., {"$merge": "gold_resumo_consentimentos"}])',
        "explicacao": "Dashboard consulta a coleção materializada (rápida) em vez de rodar a agregação pesada a cada carregamento de página.",
    },
    {
        "categoria": "MongoDB / Cosmos",
        "titulo": "Ler de réplica secundária para relatórios pesados",
        "codigo": 'MongoClient(uri, read_preference=ReadPreference.SECONDARY_PREFERRED)',
        "explicacao": "Consultas analíticas pesadas do dashboard não competem por I/O com a escrita primária do pipeline em tempo real.",
    },
    {
        "categoria": "MongoDB / Cosmos",
        "titulo": "Garantir durabilidade em dado crítico de compliance",
        "codigo": 'collection.with_options(write_concern=WriteConcern(w="majority")).insert_one(doc)',
        "explicacao": "Para consentimento (dado sensível/LGPD), confirmar que a maioria das réplicas persistiu antes de considerar a escrita bem-sucedida.",
    },
    # ---------- E. Unity Catalog Governance ----------
    {
        "categoria": "Unity Catalog",
        "titulo": "Row filter combinando múltiplas condições de acesso",
        "codigo": "RETURN is_account_group_member('data_engineers')\n  OR (seguradora_id = current_user_seguradora() AND status != 'cancelado')",
        "explicacao": "Regras de negócio compostas (não só \"é o dono\") ficam centralizadas na função SQL, não espalhadas em cada query manual.",
    },
    {
        "categoria": "Unity Catalog",
        "titulo": "Column mask com parâmetro adicional de contexto",
        "codigo": "ALTER TABLE t ALTER COLUMN cpf SET MASK mask_cpf USING COLUMNS (banco_origem)",
        "explicacao": "A função de máscara pode decidir com base em outra coluna da mesma linha (ex: mascarar diferente por banco de origem).",
    },
    {
        "categoria": "Unity Catalog",
        "titulo": "Visão com colunas filtradas por grupo (dynamic view)",
        "codigo": "CREATE VIEW v_consentimentos AS SELECT * EXCEPT (cpf) FROM t WHERE is_account_group_member('analysts')",
        "explicacao": "Alternativa a masking quando é mais simples esconder a coluna inteira de um grupo do que mascará-la.",
    },
    {
        "categoria": "Unity Catalog",
        "titulo": "Registrar storage externo sem duplicar dado",
        "codigo": "CREATE EXTERNAL LOCATION landing URL 's3://bucket/landing/' WITH (STORAGE CREDENTIAL cred)",
        "explicacao": "Permite ler dado que já existe num bucket de terceiros sob a governança do Unity Catalog, sem copiar para dentro do metastore.",
    },
    {
        "categoria": "Unity Catalog",
        "titulo": "Compartilhar dado entre workspaces sem duplicar",
        "codigo": "CREATE SHARE parceiro_seguradora; ALTER SHARE parceiro_seguradora ADD TABLE gold.resumo_consentimentos",
        "explicacao": "Delta Sharing expõe uma tabela Gold a um parceiro externo sem exportar arquivo nem replicar storage.",
    },
    {
        "categoria": "Unity Catalog",
        "titulo": "Auditoria de acesso via tabela de sistema",
        "codigo": "SELECT user_identity, action_name, request_params FROM system.access.audit WHERE request_params.full_name_arg = 'silver.consentimentos'",
        "explicacao": "Responde \"quem acessou esse dado sensível e quando\" via SQL direto, sem precisar exportar log para outra ferramenta.",
    },
    {
        "categoria": "Unity Catalog",
        "titulo": "Tag de classificação no nível do catálogo",
        "codigo": "ALTER CATALOG consent_pipeline_prod SET TAGS ('sensibilidade' = 'lgpd', 'owner' = 'data-engineering')",
        "explicacao": "Tags no catálogo alimentam automação de política (ex: quem pode conceder acesso) e relatórios de governança sem depender de planilha manual.",
    },
    {
        "categoria": "Unity Catalog",
        "titulo": "Volume gerenciado vs volume externo",
        "codigo": "CREATE VOLUME landing.cadastro; -- gerenciado: ciclo de vida = ciclo da tabela/schema",
        "explicacao": "Volume gerenciado é apagado junto com o schema; volume externo aponta pra storage que sobrevive independente do catálogo — escolha conforme o dado.",
    },
    {
        "categoria": "Unity Catalog",
        "titulo": "Consultar dado de outro engine sem copiar (federation)",
        "codigo": "CREATE FOREIGN CATALOG pg_legado USING CONNECTION conn_postgres",
        "explicacao": "Lakehouse Federation permite juntar Delta com Postgres legado em uma query sem pipeline de replicação prévio.",
    },
    {
        "categoria": "Unity Catalog",
        "titulo": "Rastrear de onde uma coluna Gold realmente veio",
        "codigo": "GET /api/2.0/lineage-tracking/column-lineage {'table_name': 'gold.metricas', 'column_name': 'total_consentimentos'}",
        "explicacao": "Linhagem automática responde \"essa métrica do BI vem de qual tabela Silver/Bronze\" sem instrumentação manual no código.",
    },
    # ---------- F. API integração/resiliência ----------
    {
        "categoria": "API / Resiliência",
        "titulo": "Parar de chamar uma API degradada temporariamente",
        "codigo": (
            "if falhas_consecutivas > LIMITE:\n"
            "    aguardar(intervalo_recuperacao)\n"
            "    falhas_consecutivas = 0"
        ),
        "explicacao": "Circuit breaker evita que workers fiquem travados esperando uma API lenta/fora do ar, desperdiçando cluster inteiro.",
    },
    {
        "categoria": "API / Resiliência",
        "titulo": "Evitar duplicar efeito colateral em retry de POST",
        "codigo": 'headers={"Idempotency-Key": str(uuid4())}',
        "explicacao": "O servidor reconhece a mesma chave de idempotência e não processa a mesma criação de recurso duas vezes em caso de retry por timeout.",
    },
    {
        "categoria": "API / Resiliência",
        "titulo": "Respeitar rate limit antes de ser bloqueado",
        "codigo": "token_bucket.consume(1); if not token_bucket.has_tokens(): sleep(token_bucket.tempo_ate_proximo())",
        "explicacao": "Token bucket client-side evita bater no 429 na maioria dos casos, em vez de só reagir ao erro depois que já aconteceu.",
    },
    {
        "categoria": "API / Resiliência",
        "titulo": "Paginação resiliente a inserção concorrente",
        "codigo": 'params={"cursor": proxima_pagina, "limit": 100}',
        "explicacao": "Cursor-based não perde/duplica registros quando novos itens são inseridos durante a paginação, ao contrário de paginação por offset.",
    },
    {
        "categoria": "API / Resiliência",
        "titulo": "Retry com jitter evita thundering herd",
        "codigo": "sleep(2 ** tentativa + random.uniform(0, 1))",
        "explicacao": "Sem o componente aleatório, várias instâncias falhando ao mesmo tempo tentam de novo no mesmo instante, sincronizando a sobrecarga no servidor.",
    },
    {
        "categoria": "API / Resiliência",
        "titulo": "Validar que o webhook realmente veio do remetente esperado",
        "codigo": 'hmac.compare_digest(assinatura_recebida, hmac.new(segredo, corpo, hashlib.sha256).hexdigest())',
        "explicacao": "Sem validar a assinatura HMAC, qualquer um que descubra a URL do webhook pode injetar eventos falsos no pipeline.",
    },
    {
        "categoria": "API / Resiliência",
        "titulo": "Paralelizar chamadas de API sem estourar concorrência",
        "codigo": (
            "sem = asyncio.Semaphore(10)\n"
            "async def limitado(url):\n"
            "    async with sem:\n"
            "        return await client.get(url)"
        ),
        "explicacao": "asyncio.gather sem semáforo dispara N requisições simultâneas sem limite — o semáforo cap no que o provedor realmente suporta.",
    },
    {
        "categoria": "API / Resiliência",
        "titulo": "Validação automática de contrato na própria definição da API",
        "codigo": "class ConsentimentoItem(BaseModel):\n    tipo_consentimento: str\n    escopo: list[str]",
        "explicacao": "Pydantic/FastAPI rejeita payload fora do contrato automaticamente — o mesmo padrão já usado em api/models.py deste projeto.",
    },
    {
        "categoria": "API / Resiliência",
        "titulo": "Autenticação forte serviço-a-serviço",
        "codigo": "ssl_context.load_cert_chain(certfile=client_cert, keyfile=client_key)",
        "explicacao": "mTLS garante que ambos os lados (não só o servidor) provam identidade — mais forte que só JWT/API key para tráfego interno crítico.",
    },
    {
        "categoria": "API / Resiliência",
        "titulo": "Health check que reflete dependência real, não só o processo",
        "codigo": '@app.get("/health")\ndef health():\n    return {"api": "ok", "mongo": mongo_ping_ok()}',
        "explicacao": "Um /health que só responde 200 se o processo subiu esconde o cenário real deste projeto: API no ar, mas Mongo inacessível.",
    },
    # ---------- G. Custo ----------
    {
        "categoria": "Custo",
        "titulo": "Impedir cluster superdimensionado por engano",
        "codigo": "cluster_policy = {\"node_type_id\": {\"type\": \"allowlist\", \"values\": [\"i3.xlarge\"]}}",
        "explicacao": "Cluster policy no nível do workspace bloqueia alguém escolher uma instância 10x maior que o necessário por descuido.",
    },
    {
        "categoria": "Custo",
        "titulo": "Workers em spot com fallback automático",
        "codigo": '"aws_attributes": {"availability": "SPOT_WITH_FALLBACK"}',
        "explicacao": "Reduz custo de worker significativamente; se o spot for revogado, volta pra on-demand automaticamente sem falhar o job.",
    },
    {
        "categoria": "Custo",
        "titulo": "Reduzir tempo de startup de job cluster",
        "codigo": '"instance_pool_id": "pool-abc123"',
        "explicacao": "Instance pool mantém instâncias \"quentes\" prontas — job cluster sobe em segundos em vez de minutos, sem pagar cluster ocioso 24/7.",
    },
    {
        "categoria": "Custo",
        "titulo": "Desligar cluster interativo esquecido ligado",
        "codigo": '"autotermination_minutes": 30',
        "explicacao": "Cluster all-purpose sem uso por 30 min desliga sozinho — evita a categoria mais comum de desperdício (cluster interativo esquecido).",
    },
    {
        "categoria": "Custo",
        "titulo": "Pagar só pela query, não por cluster ligado",
        "codigo": "-- SQL Warehouse Serverless: liga sob demanda, desliga sozinho entre queries",
        "explicacao": "Para consultas ad-hoc/BI esporádicas, warehouse serverless elimina o custo de manter compute sempre disponível.",
    },
    {
        "categoria": "Custo",
        "titulo": "Acelerar MERGE/joins sem mudar código",
        "codigo": 'spark.conf.set("spark.databricks.photon.enabled", "true")',
        "explicacao": "Photon é um engine vetorizado — mesma lógica roda mais rápido, reduzindo o tempo (e custo) de cluster ligado para a mesma carga.",
    },
    {
        "categoria": "Custo",
        "titulo": "Saber exatamente quanto cada pipeline custa",
        "codigo": "SELECT usage_metadata.job_name, SUM(usage_quantity) FROM system.billing.usage GROUP BY 1",
        "explicacao": "Tabela de sistema de billing permite chargeback por job/squad sem precisar de ferramenta externa de FinOps.",
    },
    {
        "categoria": "Custo",
        "titulo": "Reduzir custo de storage sem perder performance de leitura",
        "codigo": "ALTER TABLE t SET TBLPROPERTIES ('delta.parquet.compression.codec' = 'zstd')",
        "explicacao": "ZSTD comprime melhor que o snappy padrão para a maioria das cargas analíticas, reduzindo custo de storage a médio prazo.",
    },
    {
        "categoria": "Custo",
        "titulo": "Escolher entre particionar e Z-ORDER pelo custo de manutenção",
        "codigo": "-- partição: barata de manter, cara se over-particionada. Z-ORDER: precisa rodar OPTIMIZE periodicamente",
        "explicacao": "Partição em excesso gera small file problem; Z-ORDER sem rotina de OPTIMIZE perde o benefício com o tempo — nenhuma é \"grátis\".",
    },
    {
        "categoria": "Custo",
        "titulo": "Reduzir tamanho de warehouse fora do horário de pico",
        "codigo": "-- schedule: warehouse Small 08h-18h, XSmall fora desse horário",
        "explicacao": "Dimensionar warehouse pelo pico de uso 24h é pagar capacidade ociosa a maior parte do dia — ajustar por horário é economia direta.",
    },
    # ---------- H. Testes / Qualidade de dados ----------
    {
        "categoria": "Testes / Qualidade",
        "titulo": "Suite declarativa de expectativas de dado",
        "codigo": 'validator.expect_column_values_to_not_be_null("cliente_id")',
        "explicacao": "Great Expectations documenta e valida regras de qualidade como código versionado, não como conhecimento tácito de quem escreveu o pipeline.",
    },
    {
        "categoria": "Testes / Qualidade",
        "titulo": "Testes de schema/relacionamento no modelo dbt",
        "codigo": "columns:\n  - name: cliente_id\n    tests: [not_null, unique]",
        "explicacao": "Falha o build do dbt antes de publicar um modelo com violação de integridade, em vez de descobrir no dashboard de produção.",
    },
    {
        "categoria": "Testes / Qualidade",
        "titulo": "Garantir que produtor e consumidor concordam no schema",
        "codigo": "pact.given('evento válido').upon_receiving('consentimento').will_respond_with(schema_esperado)",
        "explicacao": "Contract testing entre o producer Kafka e o consumidor Spark pega breaking change de schema antes de ir para produção.",
    },
    {
        "categoria": "Testes / Qualidade",
        "titulo": "Achar edge case que ninguém pensou em testar",
        "codigo": "@given(st.lists(st.text(), min_size=0, max_size=100))\ndef test_explode_nunca_quebra(lista): ...",
        "explicacao": "Property-based testing gera centenas de entradas (incluindo vazias/extremas) em vez de só os 2-3 casos que o autor lembrou de escrever.",
    },
    {
        "categoria": "Testes / Qualidade",
        "titulo": "Detectar mudança de schema não intencional",
        "codigo": "assert df.schema == schema_snapshot_salvo",
        "explicacao": "Snapshot testing pega uma alteração de schema acidental (ex: tipo mudou de INT pra STRING) antes do merge, não em produção.",
    },
    {
        "categoria": "Testes / Qualidade",
        "titulo": "Validar que o pipeline sobrevive a falha de executor",
        "codigo": "# kill -9 num executor durante o streaming; verificar retomada via checkpoint",
        "explicacao": "Chaos testing prova na prática a garantia teórica de \"o checkpoint recupera sozinho\" — não assume, testa.",
    },
    {
        "categoria": "Testes / Qualidade",
        "titulo": "Comparar resultado entre ambientes antes de promover",
        "codigo": "diff = df_prod.exceptAll(df_staging_com_mesma_logica)",
        "explicacao": "Roda a nova lógica em staging contra uma cópia do dado de prod e compara o resultado — pega divergência antes do deploy real.",
    },
    {
        "categoria": "Testes / Qualidade",
        "titulo": "Testar o contrato da API automaticamente a partir do schema",
        "codigo": "schemathesis run --checks all http://localhost:8000/openapi.json",
        "explicacao": "Gera casos de teste a partir do próprio OpenAPI schema da API — pega inconsistência entre o que o código faz e o que o schema promete.",
    },
    {
        "categoria": "Testes / Qualidade",
        "titulo": "Regressão contra um dataset de referência conhecido",
        "codigo": "assert transformar(dataset_golden) == resultado_esperado_congelado",
        "explicacao": "Golden dataset test pega regressão sutil de lógica de negócio que testes unitários pontuais não cobrem.",
    },
    {
        "categoria": "Testes / Qualidade",
        "titulo": "Medir se os testes realmente pegam bug, não só cobertura de linha",
        "codigo": "mutmut run  # muda operadores no código e vê se algum teste falha",
        "explicacao": "Mutation testing revela testes que rodam a linha mas não afirmam nada sobre o comportamento — cobertura alta não é qualidade de teste.",
    },
    # ---------- I. CI/CD & IaC ----------
    {
        "categoria": "CI/CD & IaC",
        "titulo": "Reutilizar infraestrutura entre ambientes sem copiar código",
        "codigo": 'module "kafka_topic" {\n  source = "../modules/kafka-topic"\n  name   = "consentimentos-${var.env}"\n}',
        "explicacao": "Módulo Terraform parametrizado por ambiente evita ter o mesmo HCL duplicado e divergente entre dev/staging/prod.",
    },
    {
        "categoria": "CI/CD & IaC",
        "titulo": "Evitar dois `apply` simultâneos corromperem o state",
        "codigo": 'backend "s3" {\n  bucket = "tfstate"\n  dynamodb_table = "tf-lock"\n}',
        "explicacao": "State lock via DynamoDB/backend remoto impede que duas execuções de CI concorrentes apliquem mudanças conflitantes ao mesmo tempo.",
    },
    {
        "categoria": "CI/CD & IaC",
        "titulo": "Testar contra múltiplas versões em paralelo",
        "codigo": "strategy:\n  matrix:\n    python-version: ['3.11', '3.12']",
        "explicacao": "Matrix build do GitHub Actions roda o mesmo teste contra várias versões de runtime sem duplicar o workflow.",
    },
    {
        "categoria": "CI/CD & IaC",
        "titulo": "Um bundle, múltiplos ambientes",
        "codigo": "targets:\n  dev:\n    variables: {catalog: dev}\n  prod:\n    variables: {catalog: prod}",
        "explicacao": "Databricks Asset Bundles com múltiplos targets (usado neste projeto) evita duplicar o jobs.yml inteiro por ambiente.",
    },
    {
        "categoria": "CI/CD & IaC",
        "titulo": "Trocar de versão do job sem downtime perceptível",
        "codigo": "# job_v2 roda em paralelo com job_v1; trafego migra gradualmente; v1 é desativado depois",
        "explicacao": "Blue/green em pipelines de dados evita o cenário \"parei o job antigo antes do novo estar 100% validado\".",
    },
    {
        "categoria": "CI/CD & IaC",
        "titulo": "Validar lógica nova com uma fração do tráfego real",
        "codigo": "if hash(cliente_id) % 100 < 5: usar_logica_nova(evento)",
        "explicacao": "Canary release processa 5% dos eventos com a lógica nova e compara resultado antes de rollout completo.",
    },
    {
        "categoria": "CI/CD & IaC",
        "titulo": "Girar credenciais sem editar cada serviço manualmente",
        "codigo": "aws secretsmanager rotate-secret --secret-id kafka-api-key --rotation-lambda-arn ...",
        "explicacao": "Rotação automatizada reduz a janela de exposição de uma credencial vazada, sem depender de alguém lembrar de trocar manualmente.",
    },
    {
        "categoria": "CI/CD & IaC",
        "titulo": "Detectar mudança manual feita fora do Terraform",
        "codigo": "terraform plan -detailed-exitcode  # roda em cron; exit 2 = drift detectado",
        "explicacao": "Alguém editando um recurso direto no console quebra a promessa de \"infra como código\" — plan agendado pega esse drift.",
    },
    {
        "categoria": "CI/CD & IaC",
        "titulo": "Ativar lógica nova sem novo deploy",
        "codigo": 'if feature_flags.is_enabled("nova_regra_dedup", cliente_id): ...',
        "explicacao": "Feature flag permite ligar/desligar comportamento em produção instantaneamente, sem esperar pipeline de CI/CD para reverter.",
    },
    {
        "categoria": "CI/CD & IaC",
        "titulo": "Reverter automaticamente se o smoke test pós-deploy falhar",
        "codigo": "deploy && run_smoke_test || (rollback && exit 1)",
        "explicacao": "Deploy que quebra a saúde básica do serviço é revertido sozinho, sem esperar alguém notar em produção.",
    },
    # ---------- J. Observabilidade ----------
    {
        "categoria": "Observabilidade",
        "titulo": "Logs que uma ferramenta consegue agregar/filtrar",
        "codigo": 'logger.info(json.dumps({"evento": "merge_concluido", "batch_id": batch_id, "linhas": n}))',
        "explicacao": "Log estruturado (JSON) permite consulta/alerta por campo — log de texto livre exige parsing frágil por regex.",
    },
    {
        "categoria": "Observabilidade",
        "titulo": "Rastrear uma requisição através de múltiplos serviços",
        "codigo": "with tracer.start_as_current_span('buscar_consentimentos'): ...",
        "explicacao": "OpenTelemetry conecta o span da API com o span do Mongo, mostrando onde exatamente o tempo foi gasto numa requisição lenta.",
    },
    {
        "categoria": "Observabilidade",
        "titulo": "Medir latência real, não só sucesso/falha",
        "codigo": "SELECT percentile_approx(latencia_ms, 0.95) FROM logs_api WHERE endpoint = '/consentimentos'",
        "explicacao": "p95/p99 de latência revela degradação silenciosa (ex: API respondendo em 8s sem erro) que uma métrica de erro não captura.",
    },
    {
        "categoria": "Observabilidade",
        "titulo": "Ser avisado antes do usuário perceber o problema",
        "codigo": "if job_status == 'FAILED': slack_webhook.send(f'Job {job_name} falhou: {erro}')",
        "explicacao": "Alerta automático no Slack/PagerDuty fecha o loop entre \"o job falhou\" e \"alguém sabe disso\", sem depender de checagem manual.",
    },
    {
        "categoria": "Observabilidade",
        "titulo": "Saber se o dado está desatualizado antes de alguém reclamar",
        "codigo": "idade = now() - max(_ingested_at); alertar_se(idade > timedelta(hours=2))",
        "explicacao": "Data freshness como métrica própria pega o pipeline \"rodando mas não avançando\" — um cenário que um health check de processo não detecta.",
    },
    {
        "categoria": "Observabilidade",
        "titulo": "Ver a fila de erro crescer antes que vire uma crise",
        "codigo": "gauge.set('dead_letter_queue_depth', contar_documentos('ops.dead_letter'))",
        "explicacao": "Quarentena/dead-letter sem métrica de profundidade vira um buraco negro que ninguém olha até o volume ficar grande demais para investigar manualmente.",
    },
    {
        "categoria": "Observabilidade",
        "titulo": "Evitar que uma métrica de alta cardinalidade exploda o custo de monitoramento",
        "codigo": "# usar cliente_id como *log field*, nunca como *label* de métrica Prometheus",
        "explicacao": "Cardinalidade explosiva (uma série temporal por cliente) derruba o backend de métricas — cliente_id pertence ao log, não ao label.",
    },
    {
        "categoria": "Observabilidade",
        "titulo": "Detectar volume anômalo de eventos automaticamente",
        "codigo": "if volume_hoje > media_movel_7d * 3: alertar('pico anômalo de eventos')",
        "explicacao": "Um threshold fixo não escala com o crescimento natural do negócio — comparar contra a média móvel recente pega anomalia real.",
    },
    {
        "categoria": "Observabilidade",
        "titulo": "Recuperação automática de falha conhecida e recorrente",
        "codigo": "if erro == 'ESTADO_CHECKPOINT_CORROMPIDO': reiniciar_com_checkpoint_limpo()",
        "explicacao": "Runbook automatizado para a causa de falha mais comum evita esperar alguém acordar de madrugada para uma correção já conhecida.",
    },
    {
        "categoria": "Observabilidade",
        "titulo": "Conectar métrica técnica a métrica de negócio",
        "codigo": "SELECT date_trunc('day', timestamp), count(*) FROM gold.consentimentos_ativos GROUP BY 1",
        "explicacao": "O dashboard de negócio (\"quantos consentimentos ativos hoje\") é o teste de fumaça definitivo de que o pipeline inteiro está saudável de ponta a ponta.",
    },
]
