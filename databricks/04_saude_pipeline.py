# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Saude do pipeline
# MAGIC
# MAGIC Ultima tarefa do Job. Grava tres tabelas sobre o proprio pipeline:
# MAGIC
# MAGIC | Tabela | Grao |
# MAGIC |---|---|
# MAGIC | `pipeline_execucoes` | uma escrita numa tabela bronze |
# MAGIC | `pipeline_saude` | uma tabela de origem |
# MAGIC | `pipeline_testes` | um teste de qualidade |
# MAGIC
# MAGIC Se algum teste falhar, a tarefa termina com erro depois de gravar.

# COMMAND ----------

dbutils.widgets.text("catalog", "camara")
dbutils.widgets.dropdown("falhar_se_teste_falhar", "sim", ["sim", "nao"])

catalog = dbutils.widgets.get("catalog")
falhar_se_teste_falhar = dbutils.widgets.get("falhar_se_teste_falhar") == "sim"

spark.sql(f"USE CATALOG {catalog}")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# Horarios gravados em horario de Brasilia como TIMESTAMP_NTZ (sem fuso),
# para o notebook e o Power BI mostrarem o mesmo valor.
spark.conf.set("spark.sql.session.timeZone", "UTC")
FUSO = "America/Sao_Paulo"

# tabela de origem (bronze) -> (tabela correspondente na silver, nome para exibir)
ORIGENS = {
    "deputados":   ("dim_deputados",   "Deputados"),
    "proposicoes": ("dim_proposicoes", "Proposições"),
    "despesas":    ("fato_despesas",   "Despesas"),
}

print(f"Catalogo: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## pipeline_execucoes
# MAGIC
# MAGIC Copia do `DESCRIBE HISTORY` das bronze: cada MERGE traz as linhas
# MAGIC inseridas, atualizadas e excluidas. Fica numa tabela porque o Delta so
# MAGIC guarda 30 dias de historico e porque recriar uma bronze zera o historico
# MAGIC dela. Por isso a chave e (tabela, data_hora), e nao a versao.

# COMMAND ----------

import pyspark.sql.functions as F


def historico(tabela: str):
    """Escritas de dado (MERGE e carga completa) do historico de uma bronze."""
    h = spark.sql(f"DESCRIBE HISTORY bronze.{tabela}")
    metricas = F.col("operationMetrics")

    def num(chave):
        return F.coalesce(metricas[chave].cast("bigint"), F.lit(0).cast("bigint"))

    eh_merge = F.col("operation") == "MERGE"
    # a carga completa aparece como WRITE ou CREATE OR REPLACE TABLE AS SELECT
    eh_carga = (F.col("operation") == "WRITE") | F.col("operation").endswith("TABLE AS SELECT")

    return (
        h.filter(eh_merge | eh_carga)
        .select(
            F.lit(tabela).alias("tabela"),
            F.col("version").cast("bigint").alias("versao"),
            F.from_utc_timestamp(F.col("timestamp"), FUSO).cast("timestamp_ntz").alias("data_hora"),
            F.when(eh_merge, F.lit("CDC")).otherwise(F.lit("Carga completa")).alias("tipo"),
            F.col("operation").alias("operacao"),
            F.when(eh_merge, num("numTargetRowsInserted"))
             .otherwise(num("numOutputRows")).alias("linhas_inseridas"),
            F.when(eh_merge, num("numTargetRowsUpdated"))
             .otherwise(F.lit(0).cast("bigint")).alias("linhas_atualizadas"),
            F.when(eh_merge, num("numTargetRowsDeleted"))
             .otherwise(F.lit(0).cast("bigint")).alias("linhas_excluidas"),
        )
        .withColumn("data", F.to_date("data_hora"))
        .withColumn(
            "total_linhas",
            F.col("linhas_inseridas") + F.col("linhas_atualizadas") + F.col("linhas_excluidas"),
        )
    )


novas = None
for tabela in ORIGENS:
    df = historico(tabela)
    novas = df if novas is None else novas.unionByName(df)

novas.createOrReplaceTempView("execucoes_novas")

spark.sql("""
CREATE TABLE IF NOT EXISTS gold.pipeline_execucoes (
    tabela              STRING,
    versao              BIGINT,
    data_hora           TIMESTAMP_NTZ,
    data                DATE,
    tipo                STRING,
    operacao            STRING,
    linhas_inseridas    BIGINT,
    linhas_atualizadas  BIGINT,
    linhas_excluidas    BIGINT,
    total_linhas        BIGINT
)
TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')
""")

# para tabelas criadas antes do recurso (senao: DELTA_FEATURES_REQUIRE_MANUAL_ENABLEMENT)
spark.sql("""
ALTER TABLE gold.pipeline_execucoes
SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')
""")

spark.sql("""
MERGE INTO gold.pipeline_execucoes AS alvo
USING execucoes_novas AS n
   ON alvo.tabela = n.tabela AND alvo.data_hora = n.data_hora
WHEN NOT MATCHED THEN INSERT (
    tabela, versao, data_hora, data, tipo, operacao,
    linhas_inseridas, linhas_atualizadas, linhas_excluidas, total_linhas
) VALUES (
    n.tabela, n.versao, n.data_hora, n.data, n.tipo, n.operacao,
    n.linhas_inseridas, n.linhas_atualizadas, n.linhas_excluidas, n.total_linhas
)
""")

display(
    spark.sql("""
        SELECT data, tabela, tipo,
               sum(linhas_inseridas)   AS inseridas,
               sum(linhas_atualizadas) AS atualizadas,
               sum(linhas_excluidas)   AS excluidas
        FROM gold.pipeline_execucoes
        GROUP BY data, tabela, tipo
        ORDER BY data DESC, tabela
        LIMIT 30
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## pipeline_saude
# MAGIC
# MAGIC - `executado_em`: quando este notebook rodou (o Job chegou ao fim).
# MAGIC - `ultimo_cdc`: ultimo MERGE que mudou alguma linha.
# MAGIC
# MAGIC O Job pode rodar sem dado novo chegar; as duas datas juntas mostram isso.

# COMMAND ----------

contagens = " UNION ALL ".join(
    f"""
    SELECT '{origem}'  AS tabela,
           '{rotulo}'  AS rotulo,
           '{silver}'  AS tabela_silver,
           (SELECT count(*) FROM bronze.{origem}) AS linhas_bronze,
           (SELECT count(*) FROM silver.{silver}) AS linhas_silver
    """
    for origem, (silver, rotulo) in ORIGENS.items()
)

spark.sql(f"""
CREATE OR REPLACE TABLE gold.pipeline_saude
TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')
AS
WITH contagem AS ({contagens}),
ultimo AS (
    SELECT
        tabela,
        max(data_hora) AS ultima_carga,
        max(CASE WHEN tipo = 'CDC' AND total_linhas > 0 THEN data_hora END) AS ultimo_cdc
    FROM gold.pipeline_execucoes
    GROUP BY tabela
)
SELECT
    c.tabela,
    c.rotulo,
    c.tabela_silver,
    c.linhas_bronze,
    c.linhas_silver,
    c.linhas_bronze - c.linhas_silver                    AS linhas_descartadas,
    u.ultima_carga,
    u.ultimo_cdc,
    CAST(date_trunc('second', from_utc_timestamp(current_timestamp(), '{FUSO}')) AS TIMESTAMP_NTZ) AS executado_em
FROM contagem c
LEFT JOIN ultimo u ON c.tabela = u.tabela
""")

display(spark.table("gold.pipeline_saude"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## pipeline_testes
# MAGIC
# MAGIC Cada consulta devolve a quantidade de linhas com problema; zero e OK.
# MAGIC Os testes de tabela vazia existem porque a API ja devolveu lista vazia
# MAGIC com status 200 sem nenhum erro.

# COMMAND ----------

def vazia(tabela):
    return f"SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END FROM {tabela}"


TESTES = [
    # (camada, teste, consulta)
    ("Bronze", "Tabela deputados vazia",    vazia("bronze.deputados")),
    ("Bronze", "Tabela proposições vazia",  vazia("bronze.proposicoes")),
    ("Bronze", "Tabela despesas vazia",     vazia("bronze.despesas")),

    ("Silver", "Deputado duplicado", """
        SELECT count(*) FROM (
            SELECT id_deputado FROM silver.dim_deputados
            GROUP BY id_deputado HAVING count(*) > 1)"""),
    ("Silver", "Despesa duplicada", """
        SELECT count(*) FROM (
            SELECT id_despesa FROM silver.fato_despesas
            GROUP BY id_despesa HAVING count(*) > 1)"""),
    ("Silver", "Despesa sem deputado", """
        SELECT count(*) FROM silver.fato_despesas WHERE nome_deputado IS NULL"""),
    ("Silver", "Linhas perdidas entre bronze e silver", """
        SELECT abs((SELECT count(*) FROM bronze.despesas)
                 - (SELECT count(*) FROM silver.fato_despesas))"""),

    ("Gold", "Deputado e mês duplicados", """
        SELECT count(*) FROM (
            SELECT id_deputado, competencia FROM gold.gastos_agregados
            GROUP BY 1, 2 HAVING count(*) > 1)"""),
    ("Gold", "Gasto sem nome de deputado", """
        SELECT count(*) FROM gold.gastos_agregados WHERE nome IS NULL"""),
    ("Gold", "Total de gastos diverge da silver", """
        SELECT CASE WHEN abs(a - b) > 1 THEN 1 ELSE 0 END FROM (
            SELECT (SELECT round(sum(total_gasto), 0) FROM gold.gastos_agregados) AS a,
                   (SELECT round(sum(valor_liquido), 0) FROM silver.fato_despesas) AS b)"""),
    ("Gold", "Proposição duplicada", """
        SELECT count(*) FROM (
            SELECT id_proposicao FROM gold.proposicoes_painel
            GROUP BY 1 HAVING count(*) > 1)"""),
    ("Gold", "Proposição arquivada no painel", """
        SELECT count(*) FROM gold.proposicoes_painel
        WHERE lower(situacao) LIKE '%arquivad%'"""),
    ("Gold", "Fornecedor duplicado", """
        SELECT count(*) FROM (
            SELECT documento_fornecedor FROM gold.fornecedores_ranking
            GROUP BY 1 HAVING count(*) > 1)"""),
    ("Gold", "Alcance maior que o total de deputados", """
        SELECT count(*) FROM gold.fornecedores_ranking
        WHERE qtd_deputados_atendidos >
              (SELECT count(*) FROM silver.dim_deputados)"""),
    ("Gold", "Mês faltando no calendário", """
        SELECT count(*) FROM (
            SELECT competencia FROM gold.gastos_agregados
            EXCEPT
            SELECT competencia FROM gold.dim_calendario)"""),
]

linhas = []
for ordem, (camada, teste, consulta) in enumerate(TESTES, start=1):
    try:
        resultado = int(spark.sql(consulta).collect()[0][0] or 0)
        status = "OK" if resultado == 0 else "FALHA"
    except Exception as erro:
        # teste que nao roda (tabela sumiu, coluna renomeada) conta como falha
        resultado, status = -1, "FALHA"
        print(f"      erro ao rodar '{teste}': {str(erro)[:200]}")
    linhas.append((ordem, camada, teste, resultado, status))
    print(f"[{'OK   ' if status == 'OK' else 'FALHA'}] {camada:6s} | {teste}: {resultado}")

# criada com TIMESTAMP_NTZ habilitado; com saveAsTable direto a gravacao falha
spark.sql("""
CREATE OR REPLACE TABLE gold.pipeline_testes (
    ordem        INT,
    camada       STRING,
    teste        STRING,
    resultado    BIGINT,
    status       STRING,
    executado_em TIMESTAMP_NTZ
)
TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')
""")

(
    spark.createDataFrame(
        linhas, "ordem INT, camada STRING, teste STRING, resultado BIGINT, status STRING"
    )
    .withColumn("executado_em", F.date_trunc("second", F.from_utc_timestamp(F.current_timestamp(), FUSO)).cast("timestamp_ntz"))
    .write.format("delta").mode("append")
    .saveAsTable("gold.pipeline_testes")
)

falhas = sum(1 for linha in linhas if linha[4] != "OK")
print(f"\n{len(linhas) - falhas} de {len(linhas)} testes passaram.")

# COMMAND ----------

for tabela in ["pipeline_execucoes", "pipeline_saude", "pipeline_testes"]:
    n = spark.table(f"gold.{tabela}").count()
    print(f"  gold.{tabela:22s} {n:>8,} linhas")

# COMMAND ----------

if falhas > 0 and falhar_se_teste_falhar:
    raise Exception(
        f"{falhas} teste(s) de qualidade falharam. Veja gold.pipeline_testes."
    )

print("Pipeline saudavel.")
