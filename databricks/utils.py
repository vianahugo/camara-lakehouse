"""Funcoes de apoio importadas pelos notebooks."""

import json

from pyspark.sql import types

VOLUME_ROOT = "/Volumes/camara/raw/landing"


def import_schema(tablename: str, volume_root: str = VOLUME_ROOT) -> types.StructType:
    """Le schemas/<tabela>.json do Volume, o mesmo arquivo que o export local usa."""
    caminho = f"{volume_root}/schemas/{tablename}.json"
    with open(caminho, "r", encoding="utf-8") as fh:
        return types.StructType.fromJson(json.load(fh))


def table_exists(spark, catalog: str, schemaname: str, tablename: str) -> bool:
    return (
        spark.sql(f"SHOW TABLES FROM {catalog}.{schemaname}")
        .filter(f"tableName = '{tablename}'")
        .limit(1)
        .count()
        > 0
    )
