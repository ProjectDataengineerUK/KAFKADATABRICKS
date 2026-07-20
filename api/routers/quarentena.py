"""Endpoint de leitura dos eventos rejeitados por schema inválido — espelhados
do Unity Catalog para o Mongo por
notebooks/silver_consent_stream.py:write_quarentena, já que a API (Render)
só tem acesso ao Mongo, não à Delta table."""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from src.common.mongo_client import get_quarentena_collection

router = APIRouter(prefix="/quarentena", tags=["quarentena"])


class EventoQuarentena(BaseModel):
    raw_json: str
    offset: int
    kafka_timestamp: str
    batch_id: int


class TotalQuarentena(BaseModel):
    total: int


@router.get("/eventos", response_model=list[EventoQuarentena])
def listar_eventos(limite: int = 200) -> list[EventoQuarentena]:
    colecao = get_quarentena_collection()
    documentos = colecao.find({}, {"_id": 0}).sort("kafka_timestamp", -1).limit(limite)
    eventos = []
    for documento in documentos:
        documento["batch_id"] = documento.pop("_batch_id")
        eventos.append(EventoQuarentena(**documento))
    return eventos


@router.get("/total", response_model=TotalQuarentena)
def total_eventos() -> TotalQuarentena:
    colecao = get_quarentena_collection()
    return TotalQuarentena(total=colecao.count_documents({}))
