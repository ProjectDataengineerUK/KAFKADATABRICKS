import pytest
from fastapi.testclient import TestClient

mongomock = pytest.importorskip("mongomock")

from api.main import app  # noqa: E402
import api.routers.consentimentos as consentimentos_module  # noqa: E402
import api.routers.gold as gold_module  # noqa: E402
import api.routers.status as status_module  # noqa: E402


@pytest.fixture()
def mongo_collection(monkeypatch):
    client = mongomock.MongoClient()
    collection = client["susep_simulado"]["consentimentos_cliente"]
    monkeypatch.setattr(consentimentos_module, "get_consent_collection", lambda: collection)
    monkeypatch.setattr(status_module, "get_consent_collection", lambda: collection)
    return collection


@pytest.fixture()
def gold_collection(monkeypatch):
    client = mongomock.MongoClient()
    collection = client["susep_simulado"]["gold_metricas"]
    monkeypatch.setattr(gold_module, "get_gold_metricas_collection", lambda: collection)
    return collection


@pytest.fixture()
def client():
    return TestClient(app)


def _obter_token(client: TestClient) -> str:
    resposta = client.post("/token", data={"username": "demo", "password": "demo-password"})
    assert resposta.status_code == 200
    return resposta.json()["access_token"]


def test_health(client):
    resposta = client.get("/health")
    assert resposta.status_code == 200
    assert resposta.json() == {"status": "ok"}


def test_login_sucesso(client):
    resposta = client.post("/token", data={"username": "demo", "password": "demo-password"})
    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["token_type"] == "bearer"
    assert corpo["access_token"]


def test_login_falha_credenciais_invalidas(client):
    resposta = client.post("/token", data={"username": "demo", "password": "senha-errada"})
    assert resposta.status_code == 401


def test_consulta_sem_token_retorna_401(client, mongo_collection):
    resposta = client.get("/clientes/cli-00001/consentimentos")
    assert resposta.status_code == 401


def test_consulta_cliente_inexistente_retorna_404(client, mongo_collection):
    token = _obter_token(client)
    resposta = client.get(
        "/clientes/cli-inexistente/consentimentos",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resposta.status_code == 404


def test_status_sem_dados(client, mongo_collection):
    resposta = client.get("/status")
    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["mongo_conectado"] is True
    assert corpo["total_clientes_processados"] == 0
    assert corpo["total_eventos_consentimento"] == 0


def test_status_com_dados(client, mongo_collection):
    mongo_collection.insert_one(
        {
            "cliente_id": "cli-00001",
            "banco_origem": "banco-001",
            "seguradora_id": "seg-001",
            "consentimentos": [{"tipo_consentimento": "x"}, {"tipo_consentimento": "y"}],
        }
    )
    resposta = client.get("/status")
    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["total_clientes_processados"] == 1
    assert corpo["total_eventos_consentimento"] == 2


def test_gold_metricas_sem_dados(client, gold_collection):
    resposta = client.get("/gold/metricas")
    assert resposta.status_code == 200
    assert resposta.json() == []


def test_gold_metricas_com_dados(client, gold_collection):
    gold_collection.insert_one(
        {
            "data_referencia": "2026-07-17",
            "banco_origem": "banco-001",
            "seguradora_id": "seg-001",
            "tipo_consentimento": "compartilhar_dados_cadastrais",
            "total_eventos": 5,
            "total_clientes_distintos": 3,
        }
    )
    resposta = client.get("/gold/metricas")
    assert resposta.status_code == 200
    corpo = resposta.json()
    assert len(corpo) == 1
    assert corpo[0]["total_eventos"] == 5
    assert corpo[0]["total_clientes_distintos"] == 3


def test_registrar_e_consultar_consentimento(client, mongo_collection):
    """AT-001 (happy path) via API: registra um consentimento e consulta em seguida."""
    token = _obter_token(client)
    headers = {"Authorization": f"Bearer {token}"}

    resposta_post = client.post(
        "/clientes/cli-00001/consentimentos",
        params={"banco_origem": "banco-001", "seguradora_id": "seg-001"},
        json={
            "tipo_consentimento": "compartilhar_dados_cadastrais",
            "escopo": ["nome", "cpf"],
            "status": "ativo",
            "timestamp": "2026-07-17T12:00:00",
        },
        headers=headers,
    )
    assert resposta_post.status_code == 201

    resposta_get = client.get("/clientes/cli-00001/consentimentos", headers=headers)
    assert resposta_get.status_code == 200
    corpo = resposta_get.json()
    assert corpo["cliente_id"] == "cli-00001"
    assert len(corpo["consentimentos"]) == 1
    assert corpo["consentimentos"][0]["tipo_consentimento"] == "compartilhar_dados_cadastrais"
