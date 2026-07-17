# Consent Pipeline — Banco → Seguradora → Susep

Pipeline de dados fim a fim (**Kafka → Databricks → MongoDB**) com **API REST própria** e **dashboard** públicos, simulando o fluxo de consentimento de compartilhamento de dados entre banco, seguradora e o regulador (Susep).

Projeto de portfólio construído para demonstrar, na prática, competências de Databricks/Autoloader, Spark Structured Streaming + Kafka, `MERGE INTO` na Silver, structs aninhados no MongoDB, API REST com JSON aninhado e arquitetura Azure — incluindo IaC (Terraform), deploy multi-ambiente (Databricks Asset Bundles) e governança (Unity Catalog).

---

## Arquitetura

```text
[Producer Python]                    [Gerador dados sintéticos]
(simula app banco)                   (clientes/bancos/seguradoras)
      │ publish JSON                        │ CSV
      ▼                                      ▼
[Kafka: consentimentos]              [Azure Blob Storage]
(Confluent Cloud)                            │
      │                                      │ Autoloader
      │ Structured Streaming                 ▼
      │                              [Bronze Delta: cadastro_clientes]
      │ parse + explode(consentimentos)      │
      └──────────────► join stream-static ◄──┘
                              │
                              ▼
                  [Silver Delta: MERGE INTO]
                  (upsert cliente_id+tipo)
                              │
                              │ foreachBatch: groupBy + collect_list(struct)
                              ▼
                  [MongoDB Atlas: "base Susep" simulada]
                              │
                              │ PyMongo
                              ▼
                     [API FastAPI + JWT]
                              │
                              ▼
                  [Dashboard Streamlit] (público)
```

6 decisões de arquitetura (com alternativas rejeitadas e trade-offs) guiaram este desenho: broker Kafka gerenciado, MongoDB como "base Susep" simulada, API própria desacoplada do dashboard, Terraform + Databricks Asset Bundles para IaC/multi-ambiente, a estratégia `explode`/regroup para os structs aninhados, e o tier de workspace Databricks necessário para Unity Catalog.

---

## Estrutura do repositório

```
producer/          Gerador de dados sintéticos + producer Kafka (simula o app do banco)
sample_data/        Seeds de dados abertos/sintéticos (seguradoras)
notebooks/           Jobs Databricks: Autoloader (Bronze), streaming+Merge (Silver), Mongo sink
src/common/          Config, cliente Mongo e transformações PySpark compartilhadas
api/                 API FastAPI (JWT) que expõe os dados de consentimento
dashboard/           Dashboard Streamlit (consome a API, nunca o Mongo direto)
infra/               Terraform (Storage, Key Vault, Databricks workspace)
databricks/          Databricks Asset Bundle (targets dev/prod)
.github/workflows/   CI/CD (lint, testes, terraform, deploy do bundle)
tests/               Testes unitários (transformações PySpark + API)
```

---

## Rodando localmente (sem infraestrutura cloud)

Requer Python 3.11+, e Java 17 (necessário pelo PySpark para os testes de transformação).

```bash
cp .env.example .env   # preencha com credenciais de um Kafka/Mongo de teste
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pip install -r dashboard/requirements.txt

# 1. Gera o cadastro sintético (clientes/bancos/seguradoras)
python -m producer.generate_reference_data --clientes 100

# 2. Publica eventos de consentimento no Kafka configurado no .env
python -m producer.consent_event_producer --eventos 500

# 3. Sobe a API (lê do MongoDB configurado no .env)
uvicorn api.main:app --reload

# 4. Sobe o dashboard (consome a API acima)
streamlit run dashboard/app.py
```

Os jobs em `notebooks/` (Autoloader, streaming Silver, Mongo sink) rodam no Databricks — não localmente — pois dependem de `dbutils`/secret scopes do workspace.

---

## Deploy completo (infra + pipeline)

1. **Infraestrutura (Terraform):**
   ```bash
   cd infra
   terraform init -backend-config="key=dev.tfstate"
   terraform apply -var-file=environments/dev.tfvars \
     -var="kafka_bootstrap_servers=..." -var="kafka_api_key=..." \
     -var="kafka_api_secret=..." -var="mongo_uri=..."
   ```
2. **Governança (Unity Catalog):** rodar o job `bootstrap_unity_catalog_job` (definido em `databricks/resources/jobs.yml`) uma vez por ambiente.
3. **Pipeline (Databricks Asset Bundle):**
   ```bash
   cd databricks
   databricks bundle deploy --target dev
   databricks bundle run consent_pipeline_job --target dev
   ```
4. **API e dashboard:** deploy da API em Azure Container Apps/App Service; dashboard no Streamlit Community Cloud apontando `API_BASE_URL` para a API publicada.

O pipeline de CI/CD (`.github/workflows/ci-cd.yml`) automatiza os passos 1 e 3 para `dev` a cada push em `main`, promovendo para `prod` mediante aprovação manual (GitHub Environments).

> **Nota sobre custo:** todos os serviços usados têm tier gratuito/trial (Confluent Cloud, MongoDB Atlas M0, Databricks Trial de 14 dias). Rode `terraform destroy` entre sessões de demonstração para não esgotar os créditos.

---

## Testes

```bash
pytest tests/ -v        # testes unitários (transformações PySpark + API com mongomock)
ruff check .             # lint
terraform fmt -check     # formatação do Terraform (dentro de infra/)
```

Cenários de aceite (happy path, quarentena de schema inválido, revogação parcial, autenticação JWT, isolamento dev/prod) são cobertos em `tests/test_consent_transform.py` e `tests/test_api.py`.

---

## Roteiro de demo (entrevista)

1. Mostrar `producer/consent_event_producer.py` publicando eventos no Kafka (Confluent Cloud UI mostrando mensagens chegando).
2. Mostrar o job Databricks rodando (`notebooks/silver_consent_stream.py`) — Autoloader + explode + join + `MERGE INTO`.
3. Mostrar o documento resultante no MongoDB Atlas (array aninhado de consentimentos por cliente).
4. Abrir o dashboard Streamlit público, consultar um `cliente_id` e mostrar o histórico de consentimento.
5. Mostrar `databricks.yml` e o pipeline de CI/CD no GitHub Actions — deploy automatizado dev→prod com Unity Catalog e masking de PII.

---

## Status e limitações conhecidas

- Escopo completo por decisão explícita do autor (sem cortes de MVP): pipeline de dados, API própria, IaC, CI/CD e governança foram todos implementados, não apenas o núcleo de streaming.
- Integração real com a Susep, app bancário real e LGPD jurídico-formal completo estão fora de escopo — este é um projeto de demonstração técnica, não um sistema de produção regulatório.
- `current_user_seguradora()` (usada na row-level security do Unity Catalog) é um placeholder — precisa de uma função real de mapeamento usuário→seguradora antes de uso além da demo.
