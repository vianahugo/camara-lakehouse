# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup
# MAGIC
# MAGIC Cria o catalogo, os schemas e o Volume onde os parquet chegam.
# MAGIC Roda uma vez.

# COMMAND ----------

CATALOG = "camara"
VOLUME_ROOT = f"/Volumes/{CATALOG}/raw/landing"

TABELAS = ["deputados", "proposicoes", "despesas"]

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")

for camada in ["raw", "bronze", "silver"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{camada}")
    print(f"schema {CATALOG}.{camada} ok")

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.raw.landing")
print(f"volume criado: {VOLUME_ROOT}")

# COMMAND ----------

# MAGIC %md
# MAGIC Uma pasta por tabela. Os checkpoints ficam fora das pastas de dados,
# MAGIC senao o Auto Loader tentaria ler os arquivos de controle como dados.

# COMMAND ----------

import os

os.makedirs(f"{VOLUME_ROOT}/schemas", exist_ok=True)

for tabela in TABELAS:
    for sub in ["full_load", "cdc", "checkpoints"]:
        caminho = f"{VOLUME_ROOT}/{sub}/{tabela}"
        os.makedirs(caminho, exist_ok=True)
        print(caminho)

# COMMAND ----------

display(dbutils.fs.ls(f"{VOLUME_ROOT}/schemas"))
