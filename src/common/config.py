"""Configuração central lida de variáveis de ambiente (ver .env.example)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Variável de ambiente obrigatória não definida: {name}")
    return value


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_api_key: str
    kafka_api_secret: str
    mongo_uri: str
    mongo_db_name: str
    jwt_secret: str
    jwt_expire_minutes: int
    stream_trigger_interval: str
    demo_event_count: int
    demo_client_count: int

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            kafka_bootstrap_servers=_require("KAFKA_BOOTSTRAP_SERVERS"),
            kafka_topic=os.environ.get("KAFKA_TOPIC", "consentimentos"),
            kafka_api_key=_require("KAFKA_API_KEY"),
            kafka_api_secret=_require("KAFKA_API_SECRET"),
            mongo_uri=_require("MONGO_URI"),
            mongo_db_name=os.environ.get("MONGO_DB_NAME", "susep_simulado"),
            jwt_secret=_require("JWT_SECRET"),
            jwt_expire_minutes=int(os.environ.get("JWT_EXPIRE_MINUTES", 30)),
            stream_trigger_interval=os.environ.get("STREAM_TRIGGER_INTERVAL", "1 minute"),
            demo_event_count=int(os.environ.get("DEMO_EVENT_COUNT", 500)),
            demo_client_count=int(os.environ.get("DEMO_CLIENT_COUNT", 100)),
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings.from_env()
