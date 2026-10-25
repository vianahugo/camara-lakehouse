# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver
# MAGIC
# MAGIC Limpeza e modelagem a partir da bronze: sem a coluna `OP`, texto
# MAGIC normalizado, colunas derivadas. As tabelas sao recriadas a cada
# MAGIC execucao; o CDC ja foi resolvido na bronze.
# MAGIC
# MAGIC Os testes de qualidade rodam no `04_saude_pipeline`.

# COMMAND ----------

dbutils.widgets.text("catalog", "camara")
catalog = dbutils.widgets.get("catalog")

spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE silver.dim_deputados AS
SELECT
    id                                        AS id_deputado,
    initcap(trim(nome))                       AS nome,
    upper(trim(sigla_partido))                AS partido,
    upper(trim(sigla_uf))                     AS uf,
    CASE upper(trim(sigla_uf))
        WHEN 'AC' THEN 'Norte'    WHEN 'AP' THEN 'Norte'
        WHEN 'AM' THEN 'Norte'    WHEN 'PA' THEN 'Norte'
        WHEN 'RO' THEN 'Norte'    WHEN 'RR' THEN 'Norte'
        WHEN 'TO' THEN 'Norte'
        WHEN 'AL' THEN 'Nordeste' WHEN 'BA' THEN 'Nordeste'
        WHEN 'CE' THEN 'Nordeste' WHEN 'MA' THEN 'Nordeste'
        WHEN 'PB' THEN 'Nordeste' WHEN 'PE' THEN 'Nordeste'
        WHEN 'PI' THEN 'Nordeste' WHEN 'RN' THEN 'Nordeste'
        WHEN 'SE' THEN 'Nordeste'
        WHEN 'DF' THEN 'Centro-Oeste' WHEN 'GO' THEN 'Centro-Oeste'
        WHEN 'MT' THEN 'Centro-Oeste' WHEN 'MS' THEN 'Centro-Oeste'
        WHEN 'ES' THEN 'Sudeste'  WHEN 'MG' THEN 'Sudeste'
        WHEN 'RJ' THEN 'Sudeste'  WHEN 'SP' THEN 'Sudeste'
        WHEN 'PR' THEN 'Sul'      WHEN 'RS' THEN 'Sul'
        WHEN 'SC' THEN 'Sul'
        ELSE 'Desconhecida'
    END                                       AS regiao,
    id_legislatura,
    situacao,
    condicao_eleitoral,
    lower(nullif(trim(email), ''))            AS email,
    url_foto,
    modified_date                             AS atualizado_em
FROM bronze.deputados
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE silver.dim_proposicoes AS
SELECT
    id                                     AS id_proposicao,
    sigla_tipo                             AS tipo,
    numero,
    ano,
    concat_ws(' ', sigla_tipo,
              concat(numero, '/', ano))    AS identificador,
    trim(ementa)                           AS ementa,
    length(trim(ementa))                   AS tamanho_ementa,
    date(data_apresentacao)                AS data_apresentacao,
    cod_situacao,
    coalesce(nullif(trim(descricao_situacao), ''),
             'Sem situacao registrada')    AS situacao,
    descricao_tramitacao                   AS ultima_tramitacao,
    sigla_orgao                            AS orgao_atual,
    date(status_data_hora)                 AS data_ultima_tramitacao,
    datediff(current_date(),
             date(status_data_hora))       AS dias_desde_ultima_tramitacao,
    CASE
        WHEN lower(descricao_situacao) LIKE '%arquivad%' THEN 'Arquivada'
        WHEN lower(descricao_situacao) LIKE '%transformad%' THEN 'Virou norma'
        WHEN descricao_situacao IS NULL THEN 'Indefinida'
        ELSE 'Em tramitacao'
    END                                    AS status_macro,
    modified_date                          AS atualizado_em
FROM bronze.proposicoes
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fato_despesas
# MAGIC
# MAGIC Valores negativos ficam: sao creditos (remarcacao ou reembolso de
# MAGIC passagem) e o portal da Camara os soma. Filtrar `valor_liquido > 0`
# MAGIC deixaria o total acima do oficial.
# MAGIC
# MAGIC O CNPJ/CPF fica so com digitos, para facilitar join com outras bases.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE silver.fato_despesas AS
SELECT
    d.cod_documento                           AS id_despesa,
    d.id_deputado,
    dep.nome                                  AS nome_deputado,
    dep.partido,
    dep.uf,
    dep.regiao,
    d.ano,
    d.mes,
    make_date(d.ano, d.mes, 1)                AS competencia,
    d.data_documento,
    trim(d.tipo_despesa)                      AS categoria,
    initcap(trim(d.nome_fornecedor))          AS fornecedor,
    regexp_replace(coalesce(d.cnpj_cpf_fornecedor, ''), '[^0-9]', '')
                                              AS documento_fornecedor,
    CASE
        WHEN length(regexp_replace(coalesce(d.cnpj_cpf_fornecedor,''),'[^0-9]','')) = 14
            THEN 'PJ'
        WHEN length(regexp_replace(coalesce(d.cnpj_cpf_fornecedor,''),'[^0-9]','')) = 11
            THEN 'PF'
        ELSE 'Indefinido'
    END                                       AS tipo_fornecedor,
    d.valor_documento,
    d.valor_glosa,
    d.valor_liquido,
    d.url_documento
FROM bronze.despesas d
LEFT JOIN silver.dim_deputados dep
       ON d.id_deputado = dep.id_deputado
""")
