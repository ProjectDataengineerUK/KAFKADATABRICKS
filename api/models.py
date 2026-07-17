"""Modelos Pydantic da API de consentimento."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class ConsentimentoItem(BaseModel):
    tipo_consentimento: str
    escopo: list[str]
    status: str
    timestamp: datetime


class ClienteConsentimentos(BaseModel):
    cliente_id: str
    banco_origem: str
    seguradora_id: str
    consentimentos: list[ConsentimentoItem] = Field(default_factory=list)


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in_minutes: int
