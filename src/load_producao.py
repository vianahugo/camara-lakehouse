"""
Carrega a API da Camara no Postgres local, que faz o papel do banco de producao.

As triggers do banco (sql/01_schema.sql) registram cada mudanca no cdc_log.

    python src/load_producao.py deputados [--detalhes]
    python src/load_producao.py proposicoes --dias 7
    python src/load_producao.py despesas --ano 2026 --deputados 0
    python src/load_producao.py todos
"""

import argparse
import hashlib
import logging
import os
from collections import Counter
from datetime import date, timedelta

import psycopg
import requests
from dotenv import load_dotenv

from camara_api import CamaraAPI

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("load")


def conectar():
    return psycopg.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", "camara"),
        user=os.getenv("PGUSER", "camara"),
        password=os.getenv("PGPASSWORD", "camara"),
    )


def upsert(conn, tabela: str, pk: str, colunas: list[str], linhas: list[tuple]):
    """INSERT ... ON CONFLICT DO UPDATE em lote. Update sem mudanca a trigger ignora."""
    if not linhas:
        return 0

    cols = ", ".join(colunas)
    placeholders = ", ".join(["%s"] * len(colunas))
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in colunas if c != pk)

    sql = (
        f"INSERT INTO producao.{tabela} ({cols}) VALUES ({placeholders}) "
        f"ON CONFLICT ({pk}) DO UPDATE SET {updates}"
    )

    with conn.cursor() as cur:
        cur.executemany(sql, linhas)
    conn.commit()
    return len(linhas)


def checar_falhas(falhas: list, total: int, o_que: str):
    """Avisa quem falhou na API; aborta se forem mais de 10% (API fora do ar)."""
    if not falhas:
        return
    logger.warning("%s de %s %s falharam na API: %s", len(falhas), total, o_que, falhas[:20])
    if len(falhas) > total * 0.1:
        raise SystemExit(f"Mais de 10% dos {o_que} falharam. Tente mais tarde.")


def contar_eventos(conn, tabela: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT op, count(*) FROM producao.cdc_log "
            "WHERE table_name = %s AND exported_at IS NULL GROUP BY op",
            (tabela,),
        )
        return dict(cur.fetchall())


# ---------------------------------------------------------------------
# Deputados
# ---------------------------------------------------------------------

COLS_DEPUTADOS = [
    "id", "nome", "nome_civil", "sigla_partido", "sigla_uf",
    "id_legislatura", "situacao", "condicao_eleitoral", "email", "url_foto",
]


def sync_deputados(conn, api: CamaraAPI, detalhes: bool = False):
    logger.info("Buscando deputados em exercicio...")
    linhas = []

    for dep in api.listar_deputados():
        registro = {
            "id": dep.get("id"),
            "nome": dep.get("nome"),
            "nome_civil": None,
            "sigla_partido": dep.get("siglaPartido"),
            "sigla_uf": dep.get("siglaUf"),
            "id_legislatura": dep.get("idLegislatura"),
            "situacao": None,
            "condicao_eleitoral": None,
            "email": dep.get("email"),
            "url_foto": dep.get("urlFoto"),
        }

        if detalhes:
            # uma requisicao a mais por deputado (~3 min no total)
            det = api.detalhe_deputado(dep["id"])
            status = det.get("ultimoStatus") or {}
            registro["nome_civil"] = det.get("nomeCivil")
            registro["situacao"] = status.get("situacao")
            registro["condicao_eleitoral"] = status.get("condicaoEleitoral")

        linhas.append(tuple(registro[c] for c in COLS_DEPUTADOS))

    n = upsert(conn, "deputados", "id", COLS_DEPUTADOS, linhas)
    logger.info("deputados: %s registros processados | eventos CDC pendentes: %s",
                n, contar_eventos(conn, "deputados"))


# ---------------------------------------------------------------------
# Proposicoes
# ---------------------------------------------------------------------

COLS_PROPOSICOES = [
    "id", "sigla_tipo", "numero", "ano", "ementa", "data_apresentacao",
    "cod_situacao", "descricao_situacao", "descricao_tramitacao",
    "sigla_orgao", "despacho", "status_data_hora",
]


def _limpar_ts(valor):
    # a API devolve '2024-03-21T00:00' (sem segundos) ou ''
    if not valor:
        return None
    return valor.replace("T", " ")


def sync_proposicoes(conn, api: CamaraAPI, dias: int = 7, limite: int | None = None):
    fim = date.today()
    inicio = fim - timedelta(days=dias)
    logger.info("Buscando proposicoes com tramitacao entre %s e %s...", inicio, fim)

    ids = []
    basicos = {}
    for prop in api.listar_proposicoes(inicio.isoformat(), fim.isoformat()):
        ids.append(prop["id"])
        basicos[prop["id"]] = prop
        if limite and len(ids) >= limite:
            break

    # A listagem nao traz a situacao: precisa do detalhe de cada proposicao.
    logger.info("%s proposicoes na janela. Buscando o statusProposicao de cada "
                "uma (1 requisicao por proposicao)...", len(ids))

    linhas = []
    falhas = []
    for i, id_prop in enumerate(ids, 1):
        if i % 50 == 0:
            logger.info("  ... %s/%s", i, len(ids))

        try:
            det = api.detalhe_proposicao(id_prop)
        except requests.RequestException as e:
            logger.warning("  proposicao %s: %s", id_prop, type(e).__name__)
            falhas.append(id_prop)
            continue
        status = det.get("statusProposicao") or {}
        base = basicos[id_prop]

        linhas.append((
            id_prop,
            base.get("siglaTipo"),
            base.get("numero"),
            base.get("ano"),
            (base.get("ementa") or "").strip() or None,
            _limpar_ts(det.get("dataApresentacao") or base.get("dataApresentacao")),
            status.get("codSituacao") or None,
            status.get("descricaoSituacao"),
            status.get("descricaoTramitacao"),
            status.get("siglaOrgao"),
            (status.get("despacho") or "").strip() or None,
            _limpar_ts(status.get("dataHora")),
        ))

    n = upsert(conn, "proposicoes", "id", COLS_PROPOSICOES, linhas)
    logger.info("proposicoes: %s registros processados | eventos CDC pendentes: %s",
                n, contar_eventos(conn, "proposicoes"))
    checar_falhas(falhas, len(ids), "proposicoes")


# ---------------------------------------------------------------------
# Despesas
# ---------------------------------------------------------------------

COLS_DESPESAS = [
    # cod_documento guarda a chave "<codDocumento>#<hash>", nao o codigo cru
    "cod_documento", "id_deputado", "ano", "mes", "tipo_despesa",
    "tipo_documento", "data_documento", "num_documento", "valor_documento",
    "valor_liquido", "valor_glosa", "nome_fornecedor", "cnpj_cpf_fornecedor",
    "url_documento",
]


def _num(valor):
    try:
        return float(valor)
    except (TypeError, ValueError):
        return None


def _txt(valor):
    # Identificadores (codDocumento, CPF/CNPJ, numero do documento) ficam como
    # texto: alguns codDocumento sao GUID e CPF pode ter zero a esquerda.
    if valor is None or valor == "":
        return None
    return str(valor).strip()


# codDocumento nao e unico: credito de passagem (remarcacao, reembolso) vem
# como outra linha com o mesmo codigo e valor negativo. A chave usa o
# codigo + um hash destes campos.
CAMPOS_ID_DESPESA = (
    "mes", "dataDocumento", "tipoDespesa", "tipoDocumento", "numDocumento",
    "parcela", "codLote", "numRessarcimento", "valorDocumento", "valorGlosa",
    "valorLiquido", "cnpjCpfFornecedor",
)


def _id_despesa(d: dict, vistos: Counter) -> str:
    """
    Chave da linha: <codDocumento>#<hash do conteudo>.

    Mesmo conteudo gera a mesma chave, entao recarregar nao duplica. Linhas
    identicas em tudo recebem sufixo -2, -3...
    """
    cod = str(d.get("codDocumento") or "").strip()
    base = "|".join(str(d.get(c)) for c in CAMPOS_ID_DESPESA)
    chave = f"{cod}#{hashlib.sha1(base.encode()).hexdigest()[:10]}"
    vistos[chave] += 1
    return chave if vistos[chave] == 1 else f"{chave}-{vistos[chave]}"


def _remover_obsoletas(conn, id_dep: int, ano: int, mes, ids_api: set) -> int:
    """
    Apaga as despesas do deputado/periodo que a API nao devolve mais
    (corrigidas ou removidas na origem). Cada DELETE vira um evento D.

    Chaves antigas (sem '#') sempre saem. Se mais da metade das linhas
    sumir, nao apaga nada: provavelmente a resposta da API veio incompleta.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT cod_documento FROM producao.despesas "
            "WHERE id_deputado = %s AND ano = %s AND (%s::int IS NULL OR mes = %s)",
            (id_dep, ano, mes, mes),
        )
        no_banco = [r[0] for r in cur.fetchall()]

    legadas = [c for c in no_banco if "#" not in c]
    sumiram = [c for c in no_banco if "#" in c and c not in ids_api]

    if sumiram and len(sumiram) > max(10, len(no_banco) // 2):
        logger.warning("  deputado %s: %s de %s linhas sumiram da API; nao apago "
                       "(resposta incompleta?). Rode de novo mais tarde.",
                       id_dep, len(sumiram), len(no_banco))
        sumiram = []

    apagar = legadas + sumiram
    if not apagar:
        return 0
    with conn.cursor() as cur:
        cur.execute("DELETE FROM producao.despesas WHERE cod_documento = ANY(%s)",
                    (apagar,))
    conn.commit()
    return len(apagar)


def buscar_despesas(api: CamaraAPI, id_dep: int, id_leg: int, ano: int, mes):
    """Linhas de despesa de um deputado, no formato de COLS_DESPESAS."""
    linhas = []
    vistos = Counter()
    for d in api.listar_despesas(id_dep, id_leg, ano=ano, mes=mes):
        linhas.append((
            _id_despesa(d, vistos),
            id_dep,
            d.get("ano"),
            d.get("mes"),
            d.get("tipoDespesa"),
            d.get("tipoDocumento"),
            d.get("dataDocumento") or None,
            _txt(d.get("numDocumento")),
            _num(d.get("valorDocumento")),
            _num(d.get("valorLiquido")),
            _num(d.get("valorGlosa")),
            d.get("nomeFornecedor"),
            _txt(d.get("cnpjCpfFornecedor")),
            d.get("urlDocumento"),
        ))
    return linhas


def sync_despesas(conn, api: CamaraAPI, ano: int, mes: int | None = None,
                  n_deputados: int | None = None):
    """Carrega as despesas de um ano (ou mes) dos n primeiros deputados (0 = todos)."""
    with conn.cursor() as cur:
        cur.execute("SELECT id, id_legislatura FROM producao.deputados ORDER BY id")
        deputados = cur.fetchall()

    if not deputados:
        logger.error("Nenhum deputado no banco. Rode `load_producao.py deputados` antes.")
        return

    legislatura_padrao = api.legislatura_atual()
    logger.info("Legislatura corrente: %s", legislatura_padrao)

    if n_deputados:
        deputados = deputados[:n_deputados]

    logger.info("Buscando despesas de %s deputados (ano=%s, mes=%s)...",
                len(deputados), ano, mes or "todos")

    total = 0
    apagadas = 0
    falhas = []

    for i, (id_dep, id_leg) in enumerate(deputados, 1):
        try:
            linhas = buscar_despesas(api, id_dep, id_leg or legislatura_padrao, ano, mes)
        except requests.RequestException as e:
            logger.warning("  deputado %s: %s", id_dep, type(e).__name__)
            falhas.append(id_dep)
            continue

        # grava por deputado: se a carga parar no meio, o que ja foi fica
        total += upsert(conn, "despesas", "cod_documento", COLS_DESPESAS, linhas)

        # resposta vazia pode ser a falha silenciosa do idLegislatura: nao apaga nada
        if linhas:
            ids_api = {linha[0] for linha in linhas}
            apagadas += _remover_obsoletas(conn, id_dep, ano or linhas[0][2], mes, ids_api)

        if i % 25 == 0 or i == len(deputados):
            logger.info("  ... %s/%s deputados | %s despesas gravadas | "
                        "%s obsoletas removidas", i, len(deputados), total, apagadas)

    checar_falhas(falhas, len(deputados), "deputados")

    if total == 0:
        logger.warning(
            "Nenhuma despesa encontrada para ano=%s. Tente o ano anterior "
            "(--ano %s) ou rode `python src/diag_despesas.py` para "
            "diagnosticar o endpoint.", ano, ano - 1
        )
        return

    logger.info("despesas: %s registros processados, %s obsoletas removidas | "
                "eventos CDC pendentes: %s",
                total, apagadas, contar_eventos(conn, "despesas"))


# ---------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description="Carrega dados da Camara no Postgres")
    p.add_argument("alvo", choices=["deputados", "proposicoes", "despesas", "todos"])
    p.add_argument("--detalhes", action="store_true",
                   help="deputados: busca o detalhe de cada um (lento)")
    p.add_argument("--dias", type=int, default=7,
                   help="proposicoes: janela de tramitacao em dias")
    p.add_argument("--limite", type=int, default=0,
                   help="proposicoes: teto de registros por execucao (0 = sem limite)")
    p.add_argument("--ano", type=int, default=date.today().year)
    p.add_argument("--mes", type=int)
    p.add_argument("--deputados", type=int, default=30,
                   help="despesas: quantos deputados varrer (0 = todos)")
    args = p.parse_args()

    api = CamaraAPI()
    with conectar() as conn:
        if args.alvo in ("deputados", "todos"):
            sync_deputados(conn, api, detalhes=args.detalhes)
        if args.alvo in ("proposicoes", "todos"):
            sync_proposicoes(conn, api, dias=args.dias, limite=args.limite)
        if args.alvo in ("despesas", "todos"):
            sync_despesas(conn, api, ano=args.ano, mes=args.mes,
                          n_deputados=args.deputados)

    logger.info("Concluido.")


if __name__ == "__main__":
    main()
