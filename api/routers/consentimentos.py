"""Endpoints REST de consulta e registro manual de consentimento."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status

from api.auth import get_current_subject
from api.models import ClienteConsentimentos, ConsentimentoItem
from src.common.mongo_client import get_consent_collection

router = APIRouter(prefix="/clientes", tags=["consentimentos"])


@router.get("/{cliente_id}/consentimentos", response_model=ClienteConsentimentos)
def obter_consentimentos(
    cliente_id: str, _subject: str = Depends(get_current_subject)
) -> ClienteConsentimentos:
    documento = get_consent_collection().find_one({"cliente_id": cliente_id})
    if documento is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Cliente não encontrado"
        )
    return ClienteConsentimentos(**documento)


@router.post(
    "/{cliente_id}/consentimentos",
    response_model=ClienteConsentimentos,
    status_code=status.HTTP_201_CREATED,
)
def registrar_consentimento(
    cliente_id: str,
    item: ConsentimentoItem,
    banco_origem: str,
    seguradora_id: str,
    _subject: str = Depends(get_current_subject),
) -> ClienteConsentimentos:
    """Registro manual de um item de consentimento (uso em demo/teste manual;
    o fluxo principal é via pipeline Kafka -> Databricks -> MongoDB)."""

    colecao = get_consent_collection()
    item_dict = item.model_dump()
    item_dict["timestamp"] = item_dict["timestamp"] or datetime.now(timezone.utc)

    colecao.update_one(
        {"cliente_id": cliente_id},
        {
            "$setOnInsert": {
                "cliente_id": cliente_id,
                "banco_origem": banco_origem,
                "seguradora_id": seguradora_id,
            },
            "$push": {"consentimentos": item_dict},
        },
        upsert=True,
    )

    documento = colecao.find_one({"cliente_id": cliente_id})
    return ClienteConsentimentos(**documento)
