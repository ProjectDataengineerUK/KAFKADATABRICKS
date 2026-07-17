"""Dashboard Streamlit — consulta o status de consentimento por cliente.

Consome exclusivamente a API FastAPI (nunca o MongoDB diretamente), conforme
Decision 3 do DESIGN. Credenciais de demo (DEMO_API_USERNAME/PASSWORD) devem
ser configuradas como secrets do Streamlit Cloud, nunca hardcoded.
"""

from __future__ import annotations

import os

import requests
import streamlit as st

API_BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:8000")


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


def main() -> None:
    st.set_page_config(page_title="Consentimentos — Base Susep (simulada)", page_icon="📋")
    st.title("📋 Status de Consentimento — Banco → Seguradora → Susep")
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


if __name__ == "__main__":
    main()
