"""
Exporta o Postgres para parquet, no formato que o Databricks le.

  full -> a tabela inteira, so na carga inicial (data/full_load/<tabela>/)
  cdc  -> os eventos do cdc_log ainda nao exportados (data/cdc/<tabela>/)

Cada arquivo leva timestamp no nome e nunca e sobrescrito: o Auto Loader
reconhece arquivo novo pelo nome.

    python src/export_parquet.py full
    python src/export_parquet.py cdc [--tabela proposicoes]
"""

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)-7s | %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("export")

RAIZ = Path(__file__).resolve().parent.parent
DATA_DIR = RAIZ / "data"
SCHEMA_DIR = RAIZ / "schemas"

TABELAS = ["deputados", "proposicoes", "despesas"]


def conectar():
    return psycopg.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", "camara"),
        user=os.getenv("PGUSER", "camara"),
        password=os.getenv("PGPASSWORD", "camara"),
    )


def ler_schema(tabela: str) -> list[dict]:
    """Campos de schemas/<tabela>.json, o mesmo arquivo que o Spark usa."""
    doc = json.loads((SCHEMA_DIR / f"{tabela}.json").read_text(encoding="utf-8"))
    return doc["fields"]


def tipar(df: pd.DataFrame, tabela: str) -> pd.DataFrame:
    """Ordena e tipa as colunas conforme o schema."""
    campos = ler_schema(tabela)

    for campo in campos:
        nome, tipo = campo["name"], campo["type"]
        if nome not in df.columns:
            df[nome] = None

        # A largura do inteiro tem que bater com o Spark, senao a leitura
        # falha com PARQUET_COLUMN_DATA_TYPE_MISMATCH:
        # integer = INT32 (Int32), long = INT64 (Int64)
        if tipo == "integer":
            df[nome] = pd.to_numeric(df[nome], errors="coerce").astype("Int32")
        elif tipo == "long":
            df[nome] = pd.to_numeric(df[nome], errors="coerce").astype("Int64")
        elif tipo == "double":
            df[nome] = pd.to_numeric(df[nome], errors="coerce").astype("float64")
        elif tipo == "timestamp":
            df[nome] = pd.to_datetime(df[nome], errors="coerce")
        elif tipo == "date":
            df[nome] = pd.to_datetime(df[nome], errors="coerce").dt.date
        else:
            df[nome] = df[nome].astype("string")

    return df[[c["name"] for c in campos]]


def gravar(df: pd.DataFrame, destino: Path, tabela: str) -> Path:
    destino.mkdir(parents=True, exist_ok=True)
    carimbo = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    caminho = destino / f"{tabela}_{carimbo}.parquet"
    df.to_parquet(caminho, index=False, engine="pyarrow")
    logger.info("  -> %s (%s linhas)", caminho.relative_to(RAIZ), len(df))
    return caminho


def exportar_full(conn, tabela: str):
    colunas = [c["name"] for c in ler_schema(tabela) if c["name"] != "OP"]

    with conn.cursor() as cur:
        cur.execute(f"SELECT {', '.join(colunas)} FROM producao.{tabela}")
        df = pd.DataFrame(cur.fetchall(), columns=colunas)

    if df.empty:
        logger.warning("%s: tabela vazia, nada a exportar", tabela)
        return

    df["OP"] = "I"
    df = tipar(df, tabela)
    logger.info("%s (full-load):", tabela)
    gravar(df, DATA_DIR / "full_load" / tabela, tabela)


def exportar_cdc(conn, tabela: str):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT event_id, op, payload FROM producao.cdc_log "
            "WHERE table_name = %s AND exported_at IS NULL ORDER BY event_id",
            (tabela,),
        )
        eventos = cur.fetchall()

    if not eventos:
        logger.info("%s (cdc): nenhum evento pendente", tabela)
        return

    df = pd.DataFrame([ev[2] for ev in eventos])
    df["OP"] = [ev[1] for ev in eventos]
    df = tipar(df, tabela)

    resumo = df["OP"].value_counts().to_dict()
    logger.info("%s (cdc): %s eventos %s", tabela, len(df), resumo)
    gravar(df, DATA_DIR / "cdc" / tabela, tabela)

    # so marca como exportado depois que o arquivo foi gravado
    ids = [ev[0] for ev in eventos]
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE producao.cdc_log SET exported_at = now() "
            "WHERE event_id = ANY(%s)",
            (ids,),
        )
    conn.commit()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("modo", choices=["full", "cdc"])
    p.add_argument("--tabela", choices=TABELAS, help="se omitido, exporta as tres")
    args = p.parse_args()

    alvos = [args.tabela] if args.tabela else TABELAS

    with conectar() as conn:
        for tabela in alvos:
            if args.modo == "full":
                exportar_full(conn, tabela)
            else:
                exportar_cdc(conn, tabela)

    logger.info("Arquivos em %s", DATA_DIR)


if __name__ == "__main__":
    main()
