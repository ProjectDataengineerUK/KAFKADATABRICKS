"""Endpoint de leitura da camada Gold (métricas agregadas) — espelhada do
Unity Catalog para o Mongo por notebooks/gold_metricas.py, já que a API
(Render) só tem acesso ao Mongo, não à Delta table."""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from src.common.mongo_client import get_gold_metricas_collection

router = APIRouter(prefix="/gold", tags=["gold"])


class MetricaGold(BaseModel):
    data_referencia: str
    banco_origem: str
    seguradora_id: str
    tipo_consentimento: str
    total_eventos: int
    total_clientes_distintos: int


@router.get("/metricas", response_model=list[MetricaGold])
def listar_metricas(limite: int = 1000) -> list[MetricaGold]:
    colecao = get_gold_metricas_collection()
    documentos = (
        colecao.find({}, {"_id": 0})
        .sort("data_referencia", -1)
        .limit(limite)
    )
    return [MetricaGold(**documento) for documento in documentos]
