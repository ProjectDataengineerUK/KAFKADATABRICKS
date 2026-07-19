"""Dashboard Streamlit — consulta o status de consentimento por cliente,
monitora o pipeline (API/Mongo/GitHub Actions) e demonstra passo a passo o
funcionamento real do pipeline (Bronze/Silver/Mongo/Governança/Custo).

Consome exclusivamente a API FastAPI (nunca o MongoDB diretamente), conforme
Decision 3 do DESIGN. Credenciais de demo (DEMO_API_USERNAME/PASSWORD) devem
ser configuradas como secrets do Streamlit Cloud, nunca hardcoded.
"""

from __future__ import annotations

import os

import requests
import streamlit as st

from pipeline_demo import CHECKLIST_CUSTO, ETAPAS

API_BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:8000")
GITHUB_REPO = "ProjectDataengineerUK/KAFKADATABRICKS"


@st.cache_data(ttl=60)
def obter_token(username: str, password: str) -> str | None:
    resposta = requests.post(
        f"{API_BASE_URL}/token",
        data={"username": username, "password": password},
        timeout=10,
    )
    if resposta.status_code != 200:
        return None
    return resposta.json()["access_token"]


def buscar_consentimentos(cliente_id: str, token: str) -> dict | None:
    resposta = requests.get(
        f"{API_BASE_URL}/clientes/{cliente_id}/consentimentos",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )
    if resposta.status_code == 404:
        return None
    resposta.raise_for_status()
    return resposta.json()


@st.cache_data(ttl=20)
def obter_status_pipeline() -> dict | None:
    try:
        resposta = requests.get(f"{API_BASE_URL}/status", timeout=10)
        resposta.raise_for_status()
        return resposta.json()
    except requests.RequestException:
        return None


@st.cache_data(ttl=20)
def obter_health_api() -> bool:
    try:
        resposta = requests.get(f"{API_BASE_URL}/health", timeout=10)
        return resposta.status_code == 200
    except requests.RequestException:
        return False


@st.cache_data(ttl=60)
def obter_workflow_runs() -> list[dict]:
    try:
        resposta = requests.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/actions/runs",
            params={"per_page": 8},
            timeout=10,
        )
        resposta.raise_for_status()
        return resposta.json().get("workflow_runs", [])
    except requests.RequestException:
        return []


ICONE_CONCLUSAO = {
    "success": "✅",
    "failure": "❌",
    "cancelled": "⏹️",
    None: "🔄",
}


def pagina_consulta() -> None:
    st.caption(
        "Demo de portfólio: pipeline Kafka → Databricks (Autoloader + Structured "
        "Streaming + Merge) → MongoDB, exposto via API REST própria."
    )

    demo_user = os.environ.get("DEMO_API_USERNAME", "demo")
    demo_password = os.environ.get("DEMO_API_PASSWORD", "")

    token = obter_token(demo_user, demo_password)
    if token is None:
        st.error("Não foi possível autenticar na API. Verifique as credenciais/configuração.")
        return

    cliente_id = st.text_input("ID do cliente", placeholder="cli-00001")
    if st.button("Consultar") and cliente_id:
        documento = buscar_consentimentos(cliente_id, token)
        if documento is None:
            st.warning("Cliente não encontrado ou ainda sem consentimentos processados.")
            return

        st.subheader(f"Cliente {documento['cliente_id']}")
        st.write(f"Banco de origem: `{documento['banco_origem']}`")
        st.write(f"Seguradora: `{documento['seguradora_id']}`")
        st.table(documento["consentimentos"])


def pagina_monitoramento() -> None:
    st.caption("Saúde da API/Mongo e últimas execuções dos workflows de CI/CD e geração de dados.")

    col1, col2, col3 = st.columns(3)

    api_ok = obter_health_api()
    col1.metric("API (Render)", "🟢 no ar" if api_ok else "🔴 fora do ar")

    status_pipeline = obter_status_pipeline()
    if status_pipeline and status_pipeline["mongo_conectado"]:
        col2.metric("Clientes processados (Mongo)", status_pipeline["total_clientes_processados"])
        col3.metric("Eventos de consentimento", status_pipeline["total_eventos_consentimento"])
    else:
        col2.metric("Mongo", "🔴 sem conexão")
        col3.metric("Eventos de consentimento", "—")

    st.divider()
    st.subheader("Últimas execuções — GitHub Actions")
    runs = obter_workflow_runs()
    if not runs:
        st.info("Não foi possível carregar o histórico de workflows agora.")
    else:
        for run in runs:
            icone = ICONE_CONCLUSAO.get(run.get("conclusion"), "🔄")
            st.markdown(
                f"{icone} **{run['name']}** — `{run['status']}` "
                f"[ver run]({run['html_url']}) · {run['created_at']}"
            )

    st.caption(
        "O `consent_pipeline_job` (Databricks) roda em streaming contínuo — não é "
        "medido aqui diretamente, mas o crescimento de 'Clientes processados' acima "
        "confirma que ele está consumindo o Kafka/Volume e escrevendo no Mongo."
    )


def pagina_pipeline_em_acao() -> None:
    st.caption(
        "Cada etapa abaixo é o código real deste projeto (não exemplo de curso), "
        "com uma prova ao vivo puxando dado real da API onde é possível."
    )

    runs = obter_workflow_runs()
    demo_data_runs = [r for r in runs if "Demo Data Generator" in r["name"]]

    demo_user = os.environ.get("DEMO_API_USERNAME", "demo")
    demo_password = os.environ.get("DEMO_API_PASSWORD", "")
    token = obter_token(demo_user, demo_password)

    for etapa in ETAPAS:
        with st.expander(etapa["titulo"], expanded=False):
            st.markdown(etapa["conceito"])
            if etapa["codigo"]:
                st.code(etapa["codigo"], language="python")

            if etapa["prova"] == "workflow_status":
                st.markdown("**Prova ao vivo:**")
                if demo_data_runs:
                    run = demo_data_runs[0]
                    icone = ICONE_CONCLUSAO.get(run.get("conclusion"), "🔄")
                    st.markdown(
                        f"{icone} Última execução do gerador de dados (sobe os CSVs pro "
                        f"Volume que este Autoloader lê): {run['created_at']} "
                        f"— [ver log]({run['html_url']})"
                    )
                else:
                    st.info("Ainda sem histórico do workflow de geração de dados.")

            elif etapa["prova"] == "consulta_cliente":
                st.markdown("**Prova ao vivo:** consulte um cliente e veja quantos itens o MERGE já agregou.")
                cliente_id_merge = st.text_input(
                    "ID do cliente", placeholder="cli-00001", key="prova_merge"
                )
                if st.button("Verificar", key="btn_merge") and cliente_id_merge and token:
                    documento = buscar_consentimentos(cliente_id_merge, token)
                    if documento is None:
                        st.warning("Cliente ainda não processado pela Silver.")
                    else:
                        st.success(
                            f"MERGE já upsertou {len(documento['consentimentos'])} "
                            f"item(ns) de consentimento para {cliente_id_merge}."
                        )

            elif etapa["prova"] == "documento_raw":
                st.markdown("**Prova ao vivo:** documento real gravado pelo Mongo Sink (struct aninhado).")
                cliente_id_doc = st.text_input(
                    "ID do cliente", placeholder="cli-00001", key="prova_doc"
                )
                if st.button("Ver documento", key="btn_doc") and cliente_id_doc and token:
                    documento = buscar_consentimentos(cliente_id_doc, token)
                    if documento is None:
                        st.warning("Cliente ainda não processado.")
                    else:
                        st.json(documento)

            elif etapa["prova"] == "checklist_custo":
                for titulo, explicacao in CHECKLIST_CUSTO:
                    st.markdown(f"✅ **{titulo}** — {explicacao}")


def main() -> None:
    st.set_page_config(page_title="Consentimentos — Base Susep (simulada)", page_icon="📋", layout="wide")
    st.title("📋 Status de Consentimento — Banco → Seguradora → Susep")

    aba_consulta, aba_monitoramento, aba_pipeline = st.tabs(
        ["🔍 Consulta", "📊 Monitoramento", "🔬 Pipeline em Ação"]
    )
    with aba_consulta:
        pagina_consulta()
    with aba_monitoramento:
        pagina_monitoramento()
    with aba_pipeline:
        pagina_pipeline_em_acao()


if __name__ == "__main__":
    main()
