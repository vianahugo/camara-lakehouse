-- Banco de "producao" local e captura de mudancas (CDC) por trigger.
-- Roda automaticamente no primeiro `docker compose up`.

CREATE SCHEMA IF NOT EXISTS producao;

CREATE TABLE producao.deputados (
    id                  INTEGER PRIMARY KEY,
    nome                TEXT,
    nome_civil          TEXT,
    sigla_partido       TEXT,
    sigla_uf            TEXT,
    id_legislatura      INTEGER,
    situacao            TEXT,
    condicao_eleitoral  TEXT,
    email               TEXT,
    url_foto            TEXT,
    modified_date       TIMESTAMP NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE producao.proposicoes (
    id                    INTEGER PRIMARY KEY,
    sigla_tipo            TEXT,
    numero                INTEGER,
    ano                   INTEGER,
    ementa                TEXT,
    data_apresentacao     TIMESTAMP,
    cod_situacao          INTEGER,
    descricao_situacao    TEXT,
    descricao_tramitacao  TEXT,
    sigla_orgao           TEXT,
    despacho              TEXT,
    status_data_hora      TIMESTAMP,
    modified_date         TIMESTAMP NOT NULL DEFAULT clock_timestamp()
);

-- cod_documento e TEXT: alguns documentos vem com GUID em vez de numero.
-- num_documento e cnpj_cpf_fornecedor tambem, por causa do zero a esquerda.
CREATE TABLE producao.despesas (
    cod_documento        TEXT PRIMARY KEY,
    id_deputado          INTEGER,
    ano                  INTEGER,
    mes                  INTEGER,
    tipo_despesa         TEXT,
    tipo_documento       TEXT,
    data_documento       DATE,
    num_documento        TEXT,
    valor_documento      DOUBLE PRECISION,
    valor_liquido        DOUBLE PRECISION,
    valor_glosa          DOUBLE PRECISION,
    nome_fornecedor      TEXT,
    cnpj_cpf_fornecedor  TEXT,
    url_documento        TEXT,
    modified_date        TIMESTAMP NOT NULL DEFAULT clock_timestamp()
);

-- ---------------------------------------------------------------------
-- Log de CDC: uma linha por alteracao, com a operacao (I, U, D) e a
-- linha inteira em JSON. O export_parquet.py le os eventos pendentes.
-- ---------------------------------------------------------------------

CREATE TABLE producao.cdc_log (
    event_id     BIGSERIAL PRIMARY KEY,
    table_name   TEXT      NOT NULL,
    op           CHAR(1)   NOT NULL,
    payload      JSONB     NOT NULL,
    event_ts     TIMESTAMP NOT NULL DEFAULT clock_timestamp(),
    exported_at  TIMESTAMP
);

-- eventos ainda nao exportados
CREATE INDEX idx_cdc_log_pendente
    ON producao.cdc_log (table_name, event_id)
    WHERE exported_at IS NULL;

-- ---------------------------------------------------------------------
-- Triggers de CDC
--
-- Duas triggers por tabela:
--   BEFORE: atualiza modified_date e ignora UPDATE que nao muda nada
--   AFTER:  grava o evento no cdc_log
-- O log fica no AFTER porque, num INSERT ... ON CONFLICT, o BEFORE INSERT
-- dispara mesmo quando a linha ja existe e o comando vira UPDATE.
-- ---------------------------------------------------------------------

CREATE OR REPLACE FUNCTION producao.fn_cdc_prep() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'UPDATE'
       AND (to_jsonb(OLD) - 'modified_date') = (to_jsonb(NEW) - 'modified_date') THEN
        RETURN OLD;
    END IF;
    NEW.modified_date := clock_timestamp();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION producao.fn_cdc_log() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        -- modified_date = hora da exclusao, para o D vencer no dedup da bronze
        INSERT INTO producao.cdc_log (table_name, op, payload)
        VALUES (TG_TABLE_NAME, 'D',
                to_jsonb(OLD) || jsonb_build_object('modified_date', clock_timestamp()::timestamp));
        RETURN OLD;
    END IF;

    IF TG_OP = 'UPDATE' AND to_jsonb(OLD) = to_jsonb(NEW) THEN
        RETURN NULL;
    END IF;

    INSERT INTO producao.cdc_log (table_name, op, payload)
    VALUES (TG_TABLE_NAME, CASE TG_OP WHEN 'INSERT' THEN 'I' ELSE 'U' END, to_jsonb(NEW));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_deputados_prep
    BEFORE INSERT OR UPDATE ON producao.deputados
    FOR EACH ROW EXECUTE FUNCTION producao.fn_cdc_prep();

CREATE TRIGGER trg_deputados_log
    AFTER INSERT OR UPDATE OR DELETE ON producao.deputados
    FOR EACH ROW EXECUTE FUNCTION producao.fn_cdc_log();

CREATE TRIGGER trg_proposicoes_prep
    BEFORE INSERT OR UPDATE ON producao.proposicoes
    FOR EACH ROW EXECUTE FUNCTION producao.fn_cdc_prep();

CREATE TRIGGER trg_proposicoes_log
    AFTER INSERT OR UPDATE OR DELETE ON producao.proposicoes
    FOR EACH ROW EXECUTE FUNCTION producao.fn_cdc_log();

CREATE TRIGGER trg_despesas_prep
    BEFORE INSERT OR UPDATE ON producao.despesas
    FOR EACH ROW EXECUTE FUNCTION producao.fn_cdc_prep();

CREATE TRIGGER trg_despesas_log
    AFTER INSERT OR UPDATE OR DELETE ON producao.despesas
    FOR EACH ROW EXECUTE FUNCTION producao.fn_cdc_log();
