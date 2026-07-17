"""Gera os CSVs de cadastro (clientes, bancos, seguradoras) usados pelo Autoloader.

Uso:
    python -m producer.generate_reference_data --clientes 100 --seed 42
"""

from __future__ import annotations

import argparse
import csv
import random
from pathlib import Path

from faker import Faker

SAMPLE_DATA_DIR = Path(__file__).resolve().parent.parent / "sample_data"

BANCOS = [
    {"banco_id": "banco-001", "nome": "Banco Aurora"},
    {"banco_id": "banco-002", "nome": "Banco Ipê"},
    {"banco_id": "banco-003", "nome": "Banco Latitude"},
    {"banco_id": "banco-004", "nome": "Banco Vetor"},
    {"banco_id": "banco-005", "nome": "Banco Cardeal"},
]

SEGURADORAS = [
    {"seguradora_id": "seg-001", "nome": "Seguradora Bússola"},
    {"seguradora_id": "seg-002", "nome": "Seguradora Meridiano"},
    {"seguradora_id": "seg-003", "nome": "Seguradora Litoral"},
]


def gerar_clientes(quantidade: int, seed: int) -> list[dict]:
    fake = Faker("pt_BR")
    Faker.seed(seed)
    random.seed(seed)

    clientes = []
    for i in range(1, quantidade + 1):
        banco = random.choice(BANCOS)
        clientes.append(
            {
                "cliente_id": f"cli-{i:05d}",
                "nome_cliente": fake.name(),
                "cpf": fake.cpf(),
                "banco_origem": banco["banco_id"],
            }
        )
    return clientes


def escrever_csv(caminho: Path, linhas: list[dict]) -> None:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with caminho.open("w", newline="", encoding="utf-8") as arquivo:
        writer = csv.DictWriter(arquivo, fieldnames=list(linhas[0].keys()))
        writer.writeheader()
        writer.writerows(linhas)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clientes", type=int, default=100, help="Quantidade de clientes sintéticos")
    parser.add_argument("--seed", type=int, default=42, help="Seed para reprodutibilidade")
    parser.add_argument("--output-dir", type=Path, default=SAMPLE_DATA_DIR)
    args = parser.parse_args()

    clientes = gerar_clientes(args.clientes, args.seed)
    escrever_csv(args.output_dir / "clientes.csv", clientes)
    escrever_csv(args.output_dir / "bancos.csv", BANCOS)
    escrever_csv(args.output_dir / "seguradoras.csv", SEGURADORAS)

    print(f"Gerados {len(clientes)} clientes, {len(BANCOS)} bancos, {len(SEGURADORAS)} seguradoras em {args.output_dir}")


if __name__ == "__main__":
    main()
