"""Transformações PySpark reutilizadas pelos notebooks Silver e Mongo Sink.

Extraído para módulo próprio (em vez de código inline duplicado nos
notebooks) para permitir testes unitários com PySpark local — ver
tests/test_consent_transform.py.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CONSENT_SCHEMA = StructType(
    [
        StructField("cliente_id", StringType()),
        StructField("banco_origem", StringType()),
        StructField("seguradora_id", StringType()),
        StructField("timestamp", TimestampType()),
        StructField(
            "consentimentos",
            ArrayType(
                StructType(
                    [
                        StructField("tipo", StringType()),
                        StructField("escopo", ArrayType(StringType())),
                        StructField("status", StringType()),
                    ]
                )
            ),
        ),
    ]
)


def parse_and_split(raw_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Faz o parse do JSON bruto e separa eventos válidos de eventos inválidos
    (schema malformado ou sem itens de consentimento -> quarentena)."""

    parsed_df = raw_df.withColumn(
        "evento", F.from_json(F.col("raw_json"), CONSENT_SCHEMA)
    )

    quarentena_df = parsed_df.filter(
        F.col("evento").isNull()
        | F.col("evento.cliente_id").isNull()
        | F.size(F.col("evento.consentimentos")).isNull()
        | (F.size(F.col("evento.consentimentos")) == 0)
    ).select("raw_json", "offset", "kafka_timestamp")

    validos_df = parsed_df.filter(
        F.col("evento").isNotNull()
        & F.col("evento.cliente_id").isNotNull()
        & (F.size(F.col("evento.consentimentos")) > 0)
    )

    return validos_df, quarentena_df


def explode_consent_items(validos_df: DataFrame) -> DataFrame:
    """Uma linha por item de consentimento, pronta para join + MERGE granular."""

    return (
        validos_df.select("evento.*", "offset")
        .withColumn("item_consentimento", F.explode("consentimentos"))
        .select(
            "cliente_id",
            "banco_origem",
            "seguradora_id",
            "timestamp",
            "offset",
            F.col("item_consentimento.tipo").alias("tipo_consentimento"),
            F.col("item_consentimento.escopo").alias("escopo"),
            F.col("item_consentimento.status").alias("status"),
        )
    )


def regroup_client_documents(silver_df: DataFrame) -> DataFrame:
    """Remonta o array de consentimentos por cliente para o write no MongoDB."""

    return silver_df.groupBy("cliente_id", "banco_origem", "seguradora_id").agg(
        F.collect_list(
            F.struct("tipo_consentimento", "escopo", "status", "timestamp")
        ).alias("consentimentos")
    )
