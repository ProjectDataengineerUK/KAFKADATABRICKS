"""Endpoint de status agregado do pipeline (sem dados individuais de cliente) —
usado pelo dashboard para monitorar se o pipeline Kafka -> Databricks -> Mongo
está de fato processando dados."""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from src.common.mongo_client import get_consent_collection

router = APIRouter(tags=["status"])


class PipelineStatus(BaseModel):
    mongo_conectado: bool
    total_clientes_processados: int
    total_eventos_consentimento: int
    erro_debug: str | None = None


@router.get("/status", response_model=PipelineStatus)
def status() -> PipelineStatus:
    try:
        colecao = get_consent_collection()
        total_clientes = colecao.count_documents({})
        pipeline = [{"$group": {"_id": None, "total": {"$sum": {"$size": "$consentimentos"}}}}]
        resultado = list(colecao.aggregate(pipeline))
        total_eventos = resultado[0]["total"] if resultado else 0
        return PipelineStatus(
            mongo_conectado=True,
            total_clientes_processados=total_clientes,
            total_eventos_consentimento=total_eventos,
        )
    except Exception as exc:  # noqa: BLE001 - debug temporário, ver TODO abaixo
        # TODO: remover erro_debug assim que a causa da falha de conexão
        # com o Mongo em produção for confirmada e corrigida.
        return PipelineStatus(
            mongo_conectado=False,
            total_clientes_processados=0,
            total_eventos_consentimento=0,
            erro_debug=f"{type(exc).__name__}: {exc}",
        )
