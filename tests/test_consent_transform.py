import json

import pytest

pytest.importorskip("pyspark")

from src.common.transforms import (  # noqa: E402
    explode_consent_items,
    parse_and_split,
    regroup_client_documents,
)


def _kafka_row(evento: dict | None, raw_override: str | None = None, offset: int = 1):
    raw_json = raw_override if raw_override is not None else json.dumps(evento)
    return {"raw_json": raw_json, "offset": offset, "kafka_timestamp": "2026-07-17T12:00:00"}


def test_parse_and_split_evento_valido(spark):
    evento = {
        "cliente_id": "cli-00001",
        "banco_origem": "banco-001",
        "seguradora_id": "seg-001",
        "timestamp": "2026-07-17T12:00:00",
        "consentimentos": [
            {"tipo": "compartilhar_dados_cadastrais", "escopo": ["nome", "cpf"], "status": "ativo"},
        ],
    }
    raw_df = spark.createDataFrame([_kafka_row(evento)])

    validos_df, quarentena_df = parse_and_split(raw_df)

    assert validos_df.count() == 1
    assert quarentena_df.count() == 0


def test_parse_and_split_evento_sem_consentimentos_vai_para_quarentena(spark):
    evento = {
        "cliente_id": "cli-00002",
        "banco_origem": "banco-001",
        "seguradora_id": "seg-001",
        "timestamp": "2026-07-17T12:00:00",
        "consentimentos": [],
    }
    raw_df = spark.createDataFrame([_kafka_row(evento)])

    validos_df, quarentena_df = parse_and_split(raw_df)

    assert validos_df.count() == 0
    assert quarentena_df.count() == 1


def test_parse_and_split_json_malformado_vai_para_quarentena(spark):
    raw_df = spark.createDataFrame([_kafka_row(None, raw_override="{isso nao e json valido")])

    validos_df, quarentena_df = parse_and_split(raw_df)

    assert validos_df.count() == 0
    assert quarentena_df.count() == 1


def test_explode_consent_items_gera_uma_linha_por_item(spark):
    evento = {
        "cliente_id": "cli-00003",
        "banco_origem": "banco-002",
        "seguradora_id": "seg-002",
        "timestamp": "2026-07-17T12:00:00",
        "consentimentos": [
            {"tipo": "compartilhar_dados_cadastrais", "escopo": ["nome"], "status": "ativo"},
            {"tipo": "compartilhar_dados_financeiros", "escopo": ["renda"], "status": "ativo"},
        ],
    }
    raw_df = spark.createDataFrame([_kafka_row(evento)])
    validos_df, _ = parse_and_split(raw_df)

    exploded_df = explode_consent_items(validos_df)

    assert exploded_df.count() == 2
    tipos = {row.tipo_consentimento for row in exploded_df.collect()}
    assert tipos == {"compartilhar_dados_cadastrais", "compartilhar_dados_financeiros"}


def test_regroup_client_documents_agrupa_por_cliente(spark):
    evento = {
        "cliente_id": "cli-00004",
        "banco_origem": "banco-003",
        "seguradora_id": "seg-001",
        "timestamp": "2026-07-17T12:00:00",
        "consentimentos": [
            {"tipo": "compartilhar_dados_cadastrais", "escopo": ["nome"], "status": "ativo"},
            {"tipo": "compartilhar_dados_financeiros", "escopo": ["renda"], "status": "ativo"},
        ],
    }
    raw_df = spark.createDataFrame([_kafka_row(evento)])
    validos_df, _ = parse_and_split(raw_df)
    exploded_df = explode_consent_items(validos_df)

    documento_df = regroup_client_documents(exploded_df)

    assert documento_df.count() == 1
    documento = documento_df.collect()[0]
    assert documento.cliente_id == "cli-00004"
    assert len(documento.consentimentos) == 2


def test_regroup_mantem_historico_apos_revogacao_parcial(spark):
    """AT-003: após o MERGE granular por cliente_id+tipo_consentimento, a
    Silver mantém uma linha por tipo com o status mais recente. Este teste
    simula esse estado pós-merge (um tipo revogado, outro ainda ativo) e
    verifica que regroup_client_documents preserva ambos no documento final,
    sem apagar o histórico do tipo que não foi revogado."""

    silver_pos_merge = spark.createDataFrame(
        [
            {
                "cliente_id": "cli-00005",
                "banco_origem": "banco-001",
                "seguradora_id": "seg-001",
                "timestamp": "2026-07-17T12:00:00",
                "tipo_consentimento": "compartilhar_dados_cadastrais",
                "escopo": ["nome"],
                "status": "ativo",
            },
            {
                "cliente_id": "cli-00005",
                "banco_origem": "banco-001",
                "seguradora_id": "seg-001",
                "timestamp": "2026-07-17T13:00:00",
                "tipo_consentimento": "compartilhar_dados_financeiros",
                "escopo": ["renda"],
                "status": "revogado",
            },
        ]
    )

    documento_df = regroup_client_documents(silver_pos_merge)
    documento = documento_df.collect()[0]

    status_por_tipo = {item.tipo_consentimento: item.status for item in documento.consentimentos}
    assert status_por_tipo["compartilhar_dados_cadastrais"] == "ativo"
    assert status_por_tipo["compartilhar_dados_financeiros"] == "revogado"
