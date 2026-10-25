"""
Cliente da API de Dados Abertos da Camara dos Deputados.
https://dadosabertos.camara.leg.br/swagger/api.html

A API e publica e devolve 429 quando recebe muitas requisicoes seguidas,
entao tudo passa por uma Session com retry e backoff.
"""

import logging
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"

SLEEP_ENTRE_REQS = 0.2  # no maximo ~5 req/s

logger = logging.getLogger(__name__)


def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.5,  # 1.5s, 3s, 6s, 12s, 24s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "camara-lakehouse/1.0",
    })
    return session


class CamaraAPI:
    def __init__(self, base_url: str = BASE_URL):
        self.base_url = base_url.rstrip("/")
        self.session = _build_session()

    def get(self, path: str, **params) -> dict:
        url = path if path.startswith("http") else f"{self.base_url}{path}"
        resp = self.session.get(url, params=params or None, timeout=30)
        resp.raise_for_status()
        time.sleep(SLEEP_ENTRE_REQS)
        return resp.json()

    def get_dados(self, path: str, **params):
        return self.get(path, **params).get("dados")

    def paginar(self, path: str, max_paginas: int | None = None, **params):
        """Percorre as paginas seguindo o link rel='next' que a API devolve."""
        params.setdefault("itens", 100)
        url = f"{self.base_url}{path}"
        pagina = 0

        while url:
            payload = self.get(url, **params) if pagina == 0 else self.get(url)
            for item in payload.get("dados", []):
                yield item

            pagina += 1
            if max_paginas and pagina >= max_paginas:
                logger.info("Parando em %s paginas (limite definido)", pagina)
                break

            url = next(
                (lk["href"] for lk in payload.get("links", []) if lk.get("rel") == "next"),
                None,
            )

    def listar_deputados(self, id_legislatura: int | None = None):
        """Deputados em exercicio (sem parametro, da legislatura atual)."""
        params = {"ordem": "ASC", "ordenarPor": "nome"}
        if id_legislatura:
            params["idLegislatura"] = id_legislatura
        yield from self.paginar("/deputados", **params)

    def detalhe_deputado(self, id_deputado: int) -> dict:
        return self.get_dados(f"/deputados/{id_deputado}")

    def listar_proposicoes(self, data_inicio: str, data_fim: str,
                           max_paginas: int | None = None):
        """Proposicoes que tiveram tramitacao entre data_inicio e data_fim."""
        yield from self.paginar(
            "/proposicoes",
            dataInicio=data_inicio,
            dataFim=data_fim,
            ordem="ASC",
            ordenarPor="id",
            max_paginas=max_paginas,
        )

    def detalhe_proposicao(self, id_prop: int) -> dict:
        """Detalhe com o bloco statusProposicao, que a listagem nao traz."""
        return self.get_dados(f"/proposicoes/{id_prop}")

    def legislatura_atual(self) -> int:
        dados = self.get_dados(
            "/legislaturas", ordem="DESC", ordenarPor="id", itens=1
        )
        return dados[0]["id"]

    def listar_despesas(self, id_deputado: int, id_legislatura: int,
                        ano: int | None = None, mes: int | None = None,
                        max_paginas: int | None = None):
        """
        Despesas da cota parlamentar de um deputado.

        idLegislatura e obrigatorio: sem ele a API ignora o ano e devolve
        lista vazia com status 200 (ver src/diag_despesas.py).
        """
        params = {
            "idLegislatura": id_legislatura,
            "ordem": "DESC",
            "ordenarPor": "ano",
        }
        if ano:
            params["ano"] = ano
        if mes:
            params["mes"] = mes

        yield from self.paginar(
            f"/deputados/{id_deputado}/despesas",
            max_paginas=max_paginas,
            **params,
        )
