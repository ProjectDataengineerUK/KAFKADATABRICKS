# Consent Pipeline — Banco → Seguradora → Susep

Pipeline de dados fim a fim (**Kafka → Databricks → MongoDB**) com **API REST própria** e **dashboard** públicos, simulando o fluxo de consentimento de compartilhamento de dados entre banco, seguradora e o regulador (Susep).

Projeto de portfólio construído para demonstrar, na prática, competências de Databricks/Autoloader, Spark Structured Streaming + Kafka, `MERGE INTO` na Silver, structs aninhados no MongoDB e API REST com JSON aninhado — incluindo IaC (Terraform), deploy multi-ambiente (Databricks Asset Bundles) e governança (Unity Catalog). Roda inteiramente em tiers gratuitos: **Databricks Free Edition**, **Confluent Cloud** (trial), **MongoDB Atlas M0** e **Render.com** — sem nenhuma conta Azure/AWS/GCP.

---

## Arquitetura

```text
[Producer Python]                    [Gerador dados sintéticos]
(simula app banco)                   (clientes/bancos/seguradoras)
      │ publish JSON                        │ CSV (upload manual/CLI)
      ▼                                      ▼
[Kafka: consentimentos]              [Unity Catalog Volume: landing.cadastro]
(Confluent Cloud, trial)                     │  (Databricks Free Edition)
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
                  [MongoDB Atlas M0: "base Susep" simulada]
                              │
                              │ PyMongo
                              ▼
                  [API FastAPI + JWT] (Render.com, free)
                              │
                              ▼
                  [Dashboard Streamlit] (Streamlit Community Cloud, público)
```

Todo o processamento (Autoloader, Structured Streaming, `MERGE INTO`, Unity Catalog) roda dentro de um único workspace **Databricks Free Edition** — sem storage/rede/compute de nenhum cloud provider pago. 6 decisões de arquitetura (com alternativas rejeitadas e trade-offs) guiaram este desenho: broker Kafka gerenciado, MongoDB como "base Susep" simulada, API própria desacoplada do dashboard, Terraform para o Kafka/Mongo + Databricks Asset Bundles para o pipeline, a estratégia `explode`/regroup para os structs aninhados, e o uso de Databricks Free Edition (dev/prod separados por catálogo, não por workspace).

---

## Estrutura do repositório

```
producer/          Gerador de dados sintéticos + producer Kafka (simula o app do banco)
sample_data/        Seeds de dados abertos/sintéticos (seguradoras)
notebooks/           Jobs Databricks: Autoloader (Bronze), streaming+Merge (Silver), Mongo sink
src/common/          Config, cliente Mongo e transformações PySpark compartilhadas
api/                 API FastAPI (JWT) que expõe os dados de consentimento
dashboard/           Dashboard Streamlit (consome a API, nunca o Mongo direto)
infra/               Terraform (Confluent Cloud: cluster/tópico; MongoDB Atlas: cluster M0/usuário)
databricks/          Databricks Asset Bundle (targets dev/prod, mesmo workspace Free Edition)
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

Os jobs em `notebooks/` (Autoloader, streaming Silver, Mongo sink) rodam no Databricks — não localmente — pois dependem de `dbutils`/secret scopes do workspace. Para o Autoloader enxergar os CSVs gerados no passo 1, envie-os para o Volume (depois de rodar o `bootstrap_unity_catalog_job`, ver seção "Deploy completo"):

```bash
databricks fs cp sample_data/clientes.csv "dbfs:/Volumes/consent_pipeline_dev/landing/cadastro/clientes.csv"
databricks fs cp sample_data/bancos.csv "dbfs:/Volumes/consent_pipeline_dev/landing/cadastro/bancos.csv"
databricks fs cp sample_data/seguradoras.csv "dbfs:/Volumes/consent_pipeline_dev/landing/cadastro/seguradoras.csv"
```

---

## Deploy completo (infra + pipeline) — passo a passo das contas e secrets

Nenhum passo abaixo depende de cartão de crédito ou conta cloud paga.

### 1. Crie as contas free

| Serviço | Onde criar | Uso |
|---------|------------|-----|
| Confluent Cloud | [confluent.cloud](https://confluent.cloud) | Kafka gerenciado (cluster Basic, trial) |
| MongoDB Atlas | [cloud.mongodb.com](https://cloud.mongodb.com) | Cluster M0 (free forever) — "base Susep" |
| Databricks Free Edition | [databricks.com/learn/free-edition](https://www.databricks.com/learn/free-edition) | Workspace com Unity Catalog, sem conta cloud |
| Render.com | [render.com](https://render.com) | Hospeda a API FastAPI (free web service) |
| Streamlit Community Cloud | [streamlit.io/cloud](https://streamlit.io/cloud) | Hospeda o dashboard |

### 2. Gere as chaves de API (para o Terraform)

- **Confluent Cloud** → *Administration → API keys → + Add API key → Cloud resource management* (Cloud API Key, nível conta — não confundir com a API Key do cluster, que o Terraform cria sozinho). Gera `confluent_cloud_api_key` / `confluent_cloud_api_secret`.
- **MongoDB Atlas** → *Organization → Access Manager → API Keys → Create API Key*, com permissão *Organization Owner* (ou *Project Creator*). Gera `mongodbatlas_public_key` / `mongodbatlas_private_key`. Anote também o **Organization ID** (em *Organization Settings*) → `mongodbatlas_org_id`.

### 3. Rode o Terraform localmente (uma vez por ambiente)

```bash
cd infra
terraform init   # sem backend remoto — state local, já no .gitignore
terraform apply -var-file=environments/dev.tfvars \
  -var="confluent_cloud_api_key=..."   -var="confluent_cloud_api_secret=..." \
  -var="mongodbatlas_public_key=..."   -var="mongodbatlas_private_key=..." \
  -var="mongodbatlas_org_id=..."       -var="mongodbatlas_app_password=$(openssl rand -base64 24)"
terraform output kafka_bootstrap_servers
terraform output -raw kafka_api_key
terraform output -raw kafka_api_secret
```

Isso cria o cluster Kafka Basic + tópico `consentimentos` no Confluent, e o projeto + cluster M0 + usuário de app no MongoDB Atlas. Guarde os outputs — são os valores de `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_API_KEY`, `KAFKA_API_SECRET` e (montando com a senha escolhida) `MONGO_URI`. Repita com `environments/prod.tfvars` para o ambiente prod.

### 4. Configure o Databricks Free Edition

```bash
# CLI oficial (o pacote pip "databricks-cli" é a versão legada e não
# suporta "bundle" — ver https://docs.databricks.com/dev-tools/cli/install.html)
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
databricks configure --host <sua-url-do-workspace> --token   # PAT gerado em User Settings > Developer > Access tokens

# Secret scope nativo (sem Key Vault/nenhum cloud provider)
databricks secrets create-scope consent-pipeline
databricks secrets put-secret consent-pipeline kafka-bootstrap --string-value "<kafka_bootstrap_servers>"
databricks secrets put-secret consent-pipeline kafka-api-key --string-value "<kafka_api_key>"
databricks secrets put-secret consent-pipeline kafka-api-secret --string-value "<kafka_api_secret>"
databricks secrets put-secret consent-pipeline mongo-uri --string-value "<mongo_uri>"

# Governança (catálogos, schemas, volumes, masking, RLS) — uma vez por catálogo
databricks bundle run bootstrap_unity_catalog_job --target dev
```

O **PAT token** gerado aqui é o `DATABRICKS_TOKEN`; a URL do workspace (ex.: `https://dbc-xxxxxxx.cloud.databricks.com`) é o `DATABRICKS_HOST`.

### 5. Configure os GitHub Secrets

Em **Settings → Secrets and variables → Actions → New repository secret**, um por linha:

| Secret | Valor |
|--------|-------|
| `KAFKA_BOOTSTRAP_SERVERS` | Output do Terraform (passo 3) |
| `KAFKA_API_KEY` | Output do Terraform |
| `KAFKA_API_SECRET` | Output do Terraform |
| `MONGO_URI` | Montado no passo 3 (`mongodb+srv://consentpipeline-app:<senha>@...`) |
| `DATABRICKS_HOST` | URL do workspace Free Edition (passo 4) |
| `DATABRICKS_TOKEN` | PAT token gerado no passo 4 |

`GITHUB_TOKEN` já existe automaticamente — não precisa criar.

Em **Settings → Environments**, crie `dev` e `prod`; em `prod`, marque **Required reviewers** com você mesmo, para o CI pedir aprovação manual antes de aplicar em prod (promoção dev→prod).

### 6. Deploy do pipeline (automático via CI, ou manual)

O CI/CD (`.github/workflows/ci-cd.yml`) faz `databricks bundle deploy` para `dev` a cada push em `main`, e para `prod` após aprovação. Para rodar manualmente:

```bash
cd databricks
databricks bundle deploy --target dev
databricks bundle run consent_pipeline_job --target dev
```

### 7. Deploy da API (Render) e do dashboard (Streamlit)

**Render:** *New → Web Service → conecte este repositório* — **Root Directory**: raiz do repo (não `api/`, os imports são `api.*`). Build command: `pip install -r api/requirements.txt`. Start command: `uvicorn api.main:app --host 0.0.0.0 --port $PORT`. Em *Environment*, adicione `MONGO_URI`, `MONGO_DB_NAME`, `JWT_SECRET`, `JWT_EXPIRE_MINUTES`, `DEMO_API_USERNAME`, `DEMO_API_PASSWORD` (mesmos valores do seu `.env`). Render faz auto-deploy a cada push em `main` — não precisa de step no GitHub Actions.

**Streamlit Community Cloud:** *New app* apontando para `dashboard/app.py`. Em *Secrets*, defina `API_BASE_URL` com a URL pública que o Render deu à API, e `DEMO_API_USERNAME`/`DEMO_API_PASSWORD`.

> **Nota sobre custo:** Confluent Cloud é trial (crédito por tempo limitado — depois cobra por uso); MongoDB Atlas M0, Databricks Free Edition, Render free tier e Streamlit Community Cloud não têm prazo de expiração, mas têm limites de uso. Rode `terraform destroy` em `infra/` se for pausar o projeto por muito tempo, para não gerar cobrança no Confluent depois que o crédito trial acabar.

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
- Databricks Free Edition costuma restringir a conta a um único catálogo — se `CREATE CATALOG` falhar no bootstrap, a separação dev/prod precisa migrar de catálogos distintos para schemas distintos dentro do catálogo padrão (ver nota em `notebooks/bootstrap_unity_catalog.sql`).
