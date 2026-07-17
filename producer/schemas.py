"""Schemas para o evento Kafka de consentimento e para o documento MongoDB resultante."""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone

TIPOS_CONSENTIMENTO = (
    "compartilhar_dados_cadastrais",
    "compartilhar_dados_financeiros",
    "compartilhar_historico_sinistros",
)

STATUS_CONSENTIMENTO = ("ativo", "revogado")


@dataclass
class ConsentimentoItem:
    tipo: str
    escopo: list[str]
    status: str = "ativo"

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ConsentEvent:
    """Evento publicado no tópico Kafka `consentimentos`.

    Representa o consentimento capturado no app do banco e repassado
    pela seguradora, que alimentará a base da Susep.
    """

    cliente_id: str
    banco_origem: str
    seguradora_id: str
    consentimentos: list[ConsentimentoItem]
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> dict:
        return {
            "cliente_id": self.cliente_id,
            "banco_origem": self.banco_origem,
            "seguradora_id": self.seguradora_id,
            "timestamp": self.timestamp,
            "consentimentos": [item.to_dict() for item in self.consentimentos],
        }


CONSENT_EVENT_JSON_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "ConsentEvent",
    "type": "object",
    "required": ["cliente_id", "banco_origem", "seguradora_id", "timestamp", "consentimentos"],
    "properties": {
        "cliente_id": {"type": "string", "minLength": 1},
        "banco_origem": {"type": "string", "minLength": 1},
        "seguradora_id": {"type": "string", "minLength": 1},
        "timestamp": {"type": "string", "format": "date-time"},
        "consentimentos": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "required": ["tipo", "escopo", "status"],
                "properties": {
                    "tipo": {"type": "string", "enum": list(TIPOS_CONSENTIMENTO)},
                    "escopo": {
                        "type": "array",
                        "minItems": 1,
                        "items": {"type": "string"},
                    },
                    "status": {"type": "string", "enum": list(STATUS_CONSENTIMENTO)},
                },
            },
        },
    },
}


# Formato do documento gravado no MongoDB (coleção `consentimentos_cliente`,
# database `susep_simulado`) após o regroup feito em notebooks/mongo_sink.py.
MONGO_CONSENT_DOCUMENT_EXAMPLE = {
    "cliente_id": "cli-00001",
    "banco_origem": "banco-001",
    "seguradora_id": "seg-001",
    "consentimentos": [
        {
            "tipo_consentimento": "compartilhar_dados_cadastrais",
            "escopo": ["nome", "cpf", "endereco"],
            "status": "ativo",
            "timestamp": "2026-07-17T12:00:00+00:00",
        }
    ],
}
