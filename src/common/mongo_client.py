"""Helper de conexão MongoDB (PyMongo) — usado pela API e pelos testes."""

from __future__ import annotations

from functools import lru_cache

from pymongo import MongoClient
from pymongo.collection import Collection

from src.common.config import get_settings

CONSENT_COLLECTION_NAME = "consentimentos_cliente"
GOLD_METRICAS_COLLECTION_NAME = "gold_metricas"


@lru_cache(maxsize=1)
def get_mongo_client() -> MongoClient:
    settings = get_settings()
    return MongoClient(settings.mongo_uri)


def get_consent_collection(client: MongoClient | None = None) -> Collection:
    settings = get_settings()
    active_client = client or get_mongo_client()
    return active_client[settings.mongo_db_name][CONSENT_COLLECTION_NAME]


def get_gold_metricas_collection(client: MongoClient | None = None) -> Collection:
    """Espelho da camada Gold (notebooks/gold_metricas.py) — a API só
    enxerga o Mongo, não a Delta table no Unity Catalog."""
    settings = get_settings()
    active_client = client or get_mongo_client()
    return active_client[settings.mongo_db_name][GOLD_METRICAS_COLLECTION_NAME]
