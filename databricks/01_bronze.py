# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze
# MAGIC
# MAGIC Notebook parametrizado por tabela (widgets).
# MAGIC
# MAGIC - Se a tabela bronze nao existe: apaga o checkpoint e roda o full-load.
# MAGIC   Sem apagar o checkpoint, o Auto Loader acharia que os arquivos de CDC
# MAGIC   ja foram processados.
# MAGIC - Depois, sempre: CDC incremental (Auto Loader + MERGE).

# COMMAND ----------

dbutils.widgets.text("catalog", "camara")
dbutils.widgets.text("tablename", "deputados")
dbutils.widgets.text("id_field", "id")
dbutils.widgets.text("timestamp_field", "modified_date")

catalog = dbutils.widgets.get("catalog")
tablename = dbutils.widgets.get("tablename")
id_field = dbutils.widgets.get("id_field")
timestamp_field = dbutils.widgets.get("timestamp_field")

VOLUME_ROOT = f"/Volumes/{catalog}/raw/landing"
full_load_path = f"{VOLUME_ROOT}/full_load/{tablename}/"
cdc_path = f"{VOLUME_ROOT}/cdc/{tablename}/"
checkpoint_location = f"{VOLUME_ROOT}/checkpoints/{tablename}/"

print(f"tabela   : {catalog}.bronze.{tablename}")
print(f"pk       : {id_field}")
print(f"timestamp: {timestamp_field}")

# COMMAND ----------

import importlib
import sys

sys.path.append("./")

import ingestors
import utils

importlib.reload(utils)
importlib.reload(ingestors)

# COMMAND ----------

if not utils.table_exists(spark, catalog, "bronze", tablename):
    print("Tabela nao existe. Executando full-load...")

    dbutils.fs.rm(checkpoint_location, True)

    ingest_full = ingestors.Ingestor(
        spark=spark,
        catalog=catalog,
        schemaname="bronze",
        tablename=tablename,
        data_format="parquet",
    )
    ingest_full.execute(full_load_path)
    print("Tabela criada.")
else:
    print("Tabela ja existe. Pulando o full-load.")

# COMMAND ----------

print("Executando carga CDC...")

ingest_cdc = ingestors.IngestorCDC(
    spark=spark,
    catalog=catalog,
    schemaname="bronze",
    tablename=tablename,
    data_format="parquet",
    id_field=id_field,
    timestamp_field=timestamp_field,
    checkpoint_location=checkpoint_location,
)

ingest_cdc.execute(cdc_path)
print("CDC concluido.")

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE HISTORY {catalog}.bronze.{tablename}")
         .select("version", "timestamp", "operation", "operationMetrics")
)
