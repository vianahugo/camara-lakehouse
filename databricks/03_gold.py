# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold
# MAGIC
# MAGIC Tabelas agregadas para o Power BI.
# MAGIC
# MAGIC | Tabela | Grao |
# MAGIC |---|---|
# MAGIC | `gastos_agregados` | deputado x mes |
# MAGIC | `proposicoes_painel` | proposicao em tramitacao |
# MAGIC | `fornecedores_ranking` | fornecedor (CNPJ/CPF) |
# MAGIC | `dim_calendario` | mes |

# COMMAND ----------

dbutils.widgets.text("catalog", "camara")
catalog = dbutils.widgets.get("catalog")

spark.sql(f"USE CATALOG {catalog}")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## gastos_agregados
# MAGIC
# MAGIC `categoria_principal` e a categoria de maior gasto do deputado no mes
# MAGIC (window function). `competencia` e data (dia 1 do mes) para o Power BI
# MAGIC ordenar e filtrar como tempo.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold.gastos_agregados AS
WITH por_categoria AS (
    SELECT
        id_deputado,
        competencia,
        categoria,
        sum(valor_liquido) AS total_categoria,
        row_number() OVER (
            PARTITION BY id_deputado, competencia
            ORDER BY sum(valor_liquido) DESC
        ) AS rn
    FROM silver.fato_despesas
    GROUP BY id_deputado, competencia, categoria
),
principal AS (
    SELECT id_deputado, competencia, categoria AS categoria_principal
    FROM por_categoria
    WHERE rn = 1
),
totais AS (
    SELECT
        id_deputado,
        competencia,
        sum(valor_liquido)                  AS total_gasto,   -- creditos negativos abatem, como no portal
        count_if(valor_liquido > 0)         AS qtd_notas,
        round(avg(CASE WHEN valor_liquido > 0 THEN valor_liquido END), 2)
                                            AS ticket_medio,
        max(valor_liquido)                  AS maior_nota,
        count(DISTINCT categoria)           AS qtd_categorias,
        count(DISTINCT documento_fornecedor) AS qtd_fornecedores
    FROM silver.fato_despesas
    GROUP BY id_deputado, competencia
)
SELECT
    t.id_deputado,
    d.nome,
    d.partido,
    d.uf,
    d.regiao,
    t.competencia,
    year(t.competencia)                 AS ano,
    month(t.competencia)                AS mes,
    p.categoria_principal,
    round(t.total_gasto, 2)             AS total_gasto,
    t.qtd_notas,
    t.ticket_medio,
    round(t.maior_nota, 2)              AS maior_nota,
    t.qtd_categorias,
    t.qtd_fornecedores
FROM totais t
LEFT JOIN principal p
       ON t.id_deputado = p.id_deputado
      AND t.competencia = p.competencia
LEFT JOIN silver.dim_deputados d
       ON t.id_deputado = d.id_deputado
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## proposicoes_painel
# MAGIC
# MAGIC So proposicoes em tramitacao. `faixa_parada` agrupa os dias sem
# MAGIC tramitacao em faixas.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold.proposicoes_painel AS
SELECT
    id_proposicao,
    identificador,
    tipo,
    ano,
    ementa,
    tamanho_ementa,
    data_apresentacao,
    situacao,
    ultima_tramitacao,
    orgao_atual,
    data_ultima_tramitacao,
    dias_desde_ultima_tramitacao,
    CASE
        WHEN dias_desde_ultima_tramitacao IS NULL      THEN 'Sem data'
        WHEN dias_desde_ultima_tramitacao <= 30        THEN '1. Ate 30 dias'
        WHEN dias_desde_ultima_tramitacao <= 90        THEN '2. 31 a 90 dias'
        WHEN dias_desde_ultima_tramitacao <= 365       THEN '3. 3 meses a 1 ano'
        WHEN dias_desde_ultima_tramitacao <= 365 * 3   THEN '4. 1 a 3 anos'
        ELSE                                                '5. Mais de 3 anos'
    END                                            AS faixa_parada,
    datediff(current_date(), data_apresentacao)    AS idade_dias,
    round(datediff(current_date(), data_apresentacao) / 365.25, 1) AS idade_anos
FROM silver.dim_proposicoes
WHERE status_macro = 'Em tramitacao'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fornecedores_ranking
# MAGIC
# MAGIC Agrupado pelo documento (CNPJ/CPF), porque o nome do fornecedor vem
# MAGIC com grafias diferentes na origem.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold.fornecedores_ranking AS
WITH base AS (
    SELECT
        documento_fornecedor,
        max(fornecedor)                       AS fornecedor,
        max(tipo_fornecedor)                  AS tipo_fornecedor,
        count(DISTINCT id_deputado)           AS qtd_deputados_atendidos,
        count(DISTINCT partido)               AS qtd_partidos_distintos,
        count(DISTINCT uf)                    AS qtd_ufs,
        count_if(valor_liquido > 0)           AS qtd_notas,
        sum(valor_liquido)                    AS total_recebido,
        avg(CASE WHEN valor_liquido > 0 THEN valor_liquido END)
                                              AS ticket_medio,
        max(valor_liquido)                    AS maior_nota,
        min(data_documento)                   AS primeira_nota,
        max(data_documento)                   AS ultima_nota,
        max(categoria)                        AS categoria_exemplo
    FROM silver.fato_despesas
    WHERE documento_fornecedor <> ''
    GROUP BY documento_fornecedor
)
SELECT
    documento_fornecedor,
    fornecedor,
    tipo_fornecedor,
    categoria_exemplo,
    qtd_deputados_atendidos,
    qtd_partidos_distintos,
    qtd_ufs,
    qtd_notas,
    round(total_recebido, 2)   AS total_recebido,
    round(ticket_medio, 2)     AS ticket_medio,
    round(maior_nota, 2)       AS maior_nota,
    primeira_nota,
    ultima_nota,
    CASE
        WHEN qtd_deputados_atendidos = 1   THEN '1. Exclusivo (1 deputado)'
        WHEN qtd_deputados_atendidos <= 5  THEN '2. Poucos (2 a 5)'
        WHEN qtd_deputados_atendidos <= 20 THEN '3. Muitos (6 a 20)'
        ELSE                                    '4. Amplo (mais de 20)'
    END                        AS faixa_alcance,
    rank() OVER (ORDER BY total_recebido DESC)          AS rank_valor,
    rank() OVER (ORDER BY qtd_deputados_atendidos DESC) AS rank_alcance
FROM base
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_calendario
# MAGIC
# MAGIC Um mes por linha, do primeiro ao ultimo mes com despesa.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold.dim_calendario AS
WITH limites AS (
    SELECT
        date_trunc('month', min(competencia)) AS inicio,
        date_trunc('month', max(competencia)) AS fim
    FROM gold.gastos_agregados
),
meses AS (
    SELECT explode(sequence(
        to_date(inicio), to_date(fim), interval 1 month
    )) AS competencia
    FROM limites
)
SELECT
    competencia,
    year(competencia)                              AS ano,
    month(competencia)                             AS mes,
    CASE month(competencia)
        WHEN 1 THEN 'jan' WHEN 2 THEN 'fev' WHEN 3 THEN 'mar'
        WHEN 4 THEN 'abr' WHEN 5 THEN 'mai' WHEN 6 THEN 'jun'
        WHEN 7 THEN 'jul' WHEN 8 THEN 'ago' WHEN 9 THEN 'set'
        WHEN 10 THEN 'out' WHEN 11 THEN 'nov' ELSE 'dez'
    END                                            AS mes_abrev,
    concat(
        CASE month(competencia)
            WHEN 1 THEN 'jan' WHEN 2 THEN 'fev' WHEN 3 THEN 'mar'
            WHEN 4 THEN 'abr' WHEN 5 THEN 'mai' WHEN 6 THEN 'jun'
            WHEN 7 THEN 'jul' WHEN 8 THEN 'ago' WHEN 9 THEN 'set'
            WHEN 10 THEN 'out' WHEN 11 THEN 'nov' ELSE 'dez'
        END,
        '/', substr(cast(year(competencia) AS string), 3, 2)
    )                                              AS rotulo,
    quarter(competencia)                           AS trimestre,
    concat('T', quarter(competencia), '/',
           cast(year(competencia) AS string))      AS rotulo_trimestre,
    -- chave numerica para o Power BI ordenar 'rotulo' (senao abr vem antes de jan)
    year(competencia) * 100 + month(competencia)   AS ordem
FROM meses
ORDER BY competencia
""")

# COMMAND ----------

for tabela in ["gastos_agregados", "proposicoes_painel",
               "fornecedores_ranking", "dim_calendario"]:
    n = spark.table(f"gold.{tabela}").count()
    print(f"  gold.{tabela:22s} {n:>8,} linhas")
