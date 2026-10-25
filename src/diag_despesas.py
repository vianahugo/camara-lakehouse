"""
Testa combinacoes de parametros no endpoint /deputados/{id}/despesas.

Foi assim que apareceu o problema do idLegislatura: sem ele, a API
devolve 200 com lista vazia em vez de erro.

    python src/diag_despesas.py
"""

import requests

BASE = "https://dadosabertos.camara.leg.br/api/v2"

TESTES = [
    ("sem nenhum parametro",           {}),
    ("so itens",                       {"itens": 5}),
    ("ano=2026",                       {"ano": 2026, "itens": 5}),
    ("ano=2025",                       {"ano": 2025, "itens": 5}),
    ("ano=2024",                       {"ano": 2024, "itens": 5}),
    ("idLegislatura=57",               {"idLegislatura": 57, "itens": 5}),
    ("idLegislatura=57 + ano=2026",    {"idLegislatura": 57, "ano": 2026, "itens": 5}),
    ("idLegislatura=57 + ano=2025",    {"idLegislatura": 57, "ano": 2025, "itens": 5}),
    ("ano=2025 + mes=6",               {"ano": 2025, "mes": 6, "itens": 5}),
    ("ano=2025 + ordenarPor=ano",      {"ano": 2025, "ordem": "ASC",
                                        "ordenarPor": "ano", "itens": 5}),
    ("ano=2025 + ordenarPor=dataDoc",  {"ano": 2025, "ordem": "DESC",
                                        "ordenarPor": "dataDocumento", "itens": 5}),
]


def pegar_deputados(n=3):
    r = requests.get(f"{BASE}/deputados",
                     params={"ordem": "ASC", "ordenarPor": "nome", "itens": n},
                     timeout=30)
    r.raise_for_status()
    return [(d["id"], d["nome"]) for d in r.json()["dados"]]


def main():
    deputados = pegar_deputados()
    print(f"Testando com {len(deputados)} deputados\n")

    sucessos = []

    for id_dep, nome in deputados:
        print("=" * 72)
        print(f"{nome}  (id={id_dep})")
        print("=" * 72)

        for rotulo, params in TESTES:
            url = f"{BASE}/deputados/{id_dep}/despesas"
            try:
                r = requests.get(url, params=params, timeout=30)
                total = r.headers.get("x-total-count", "?")
                if r.status_code == 200:
                    n = len(r.json().get("dados", []))
                    marca = "  <<< FUNCIONOU" if n > 0 else ""
                    print(f"  [{r.status_code}] {rotulo:32s} "
                          f"itens={n:2d}  x-total-count={total}{marca}")
                    if n > 0:
                        sucessos.append((nome, rotulo, r.url))
                else:
                    print(f"  [{r.status_code}] {rotulo:32s} {r.text[:80]}")
            except Exception as e:
                print(f"  [ERR] {rotulo:32s} {type(e).__name__}: {e}")
        print()

    print("=" * 72)
    if not sucessos:
        print("NENHUMA combinacao trouxe dados.")
        return

    print("COMBINACOES QUE TROUXERAM DADOS:\n")
    vistos = set()
    for nome, rotulo, url in sucessos:
        if rotulo not in vistos:
            print(f"  {rotulo}\n    {url}\n")
            vistos.add(rotulo)

    item = requests.get(sucessos[0][2], timeout=30).json()["dados"][0]
    print("  CAMPOS DE UMA DESPESA:")
    for k, v in item.items():
        print(f"    {k:24s} {str(v)[:50]}")


if __name__ == "__main__":
    main()
