"""Simula o app do banco publicando eventos de consentimento no Kafka.

Lê o cadastro sintético (clientes/bancos/seguradoras) gerado por
`generate_reference_data.py` e publica eventos de consentimento no tópico
Kafka configurado, incluindo uma fração de eventos de revogação para
exercitar o cenário AT-003 (revogação parcial).

Uso:
    python -m producer.consent_event_producer --eventos 500
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import random
import sys
from pathlib import Path

from jsonschema import ValidationError, validate

from producer.schemas import (
    TIPOS_CONSENTIMENTO,
    ConsentEvent,
    ConsentimentoItem,
    CONSENT_EVENT_JSON_SCHEMA,
)

SAMPLE_DATA_DIR = Path(__file__).resolve().parent.parent / "sample_data"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("consent_event_producer")


def carregar_csv(caminho: Path) -> list[dict]:
    with caminho.open(newline="", encoding="utf-8") as arquivo:
        return list(csv.DictReader(arquivo))


def montar_producer():
    from confluent_kafka import Producer

    config = {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.environ["KAFKA_API_KEY"],
        "sasl.password": os.environ["KAFKA_API_SECRET"],
    }
    return Producer(config)


def gerar_evento(cliente: dict, revogar: bool = False) -> ConsentEvent:
    tipos_sorteados = random.sample(TIPOS_CONSENTIMENTO, k=random.randint(1, 2))
    itens = [
        ConsentimentoItem(
            tipo=tipo,
            escopo=["nome", "cpf"] if tipo == "compartilhar_dados_cadastrais" else ["renda", "score"],
            status="revogado" if revogar else "ativo",
        )
        for tipo in tipos_sorteados
    ]
    return ConsentEvent(
        cliente_id=cliente["cliente_id"],
        banco_origem=cliente["banco_origem"],
        seguradora_id=random.choice(["seg-001", "seg-002", "seg-003"]),
        consentimentos=itens,
    )


def validar_evento(evento_dict: dict) -> None:
    validate(instance=evento_dict, schema=CONSENT_EVENT_JSON_SCHEMA)


def publicar_eventos(producer, topico: str, eventos: list[ConsentEvent]) -> tuple[int, int]:
    sucesso, falha = 0, 0

    def callback(err, _msg):
        nonlocal sucesso, falha
        if err is not None:
            falha += 1
            logger.error("Falha ao publicar evento: %s", err)
        else:
            sucesso += 1

    for evento in eventos:
        evento_dict = evento.to_dict()
        try:
            validar_evento(evento_dict)
        except ValidationError as exc:
            logger.warning("Evento inválido descartado (%s): %s", evento.cliente_id, exc.message)
            continue

        producer.produce(
            topico,
            key=evento.cliente_id.encode("utf-8"),
            value=json.dumps(evento_dict).encode("utf-8"),
            callback=callback,
        )
        producer.poll(0)

    producer.flush()
    return sucesso, falha


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--eventos", type=int, default=int(os.environ.get("DEMO_EVENT_COUNT", 500)))
    parser.add_argument("--revogacao-fracao", type=float, default=0.1, help="Fração de eventos de revogação")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    random.seed(args.seed)

    clientes = carregar_csv(SAMPLE_DATA_DIR / "clientes.csv")
    if not clientes:
        logger.error("Nenhum cliente encontrado. Rode generate_reference_data.py primeiro.")
        sys.exit(1)

    topico = os.environ.get("KAFKA_TOPIC", "consentimentos")
    producer = montar_producer()

    eventos = []
    n_revogacoes = int(args.eventos * args.revogacao_fracao)
    for i in range(args.eventos):
        cliente = random.choice(clientes)
        revogar = i < n_revogacoes
        eventos.append(gerar_evento(cliente, revogar=revogar))

    sucesso, falha = publicar_eventos(producer, topico, eventos)
    logger.info("Publicados %d eventos com sucesso, %d falhas, tópico=%s", sucesso, falha, topico)


if __name__ == "__main__":
    main()
