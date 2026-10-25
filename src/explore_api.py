"""
Mostra os campos que a API devolve hoje para deputados, proposicoes e
despesas. Util para conferir schemas/*.json e sql/01_schema.sql quando a
API muda.

    python src/explore_api.py
"""

from datetime import date, timedelta

from camara_api import CamaraAPI


def mostrar(titulo: str, obj: dict, limite_chaves: int = 40):
    print("\n" + "=" * 70)
    print(titulo)
    print("=" * 70)
    if not obj:
        print("  (vazio)")
        return
    for i, (k, v) in enumerate(obj.items()):
        if i >= limite_chaves:
            print(f"  ... e mais {len(obj) - limite_chaves} campos")
            break
        if isinstance(v, dict):
            print(f"  {k:24s} <objeto> -> {list(v.keys())}")
        elif isinstance(v, list):
            print(f"  {k:24s} <lista com {len(v)} itens>")
        else:
            texto = str(v)
            if len(texto) > 60:
                texto = texto[:57] + "..."
            print(f"  {k:24s} {texto}")


def main():
    api = CamaraAPI()

    dep_lista = next(iter(api.listar_deputados()))
    mostrar("DEPUTADOS - listagem (/deputados)", dep_lista)

    dep_detalhe = api.detalhe_deputado(dep_lista["id"])
    mostrar("DEPUTADOS - detalhe (/deputados/{id})", dep_detalhe)
    mostrar("DEPUTADOS - ultimoStatus", dep_detalhe.get("ultimoStatus", {}))

    hoje = date.today()
    inicio = hoje - timedelta(days=7)
    props = list(api.listar_proposicoes(inicio.isoformat(), hoje.isoformat(),
                                        max_paginas=1))
    print(f"\n>> {len(props)} proposicoes tramitaram entre "
          f"{inicio} e {hoje} (primeira pagina)")
    if props:
        mostrar("PROPOSICOES - listagem", props[0])
        det = api.detalhe_proposicao(props[0]["id"])
        mostrar("PROPOSICOES - statusProposicao", det.get("statusProposicao", {}))

    legislatura = api.legislatura_atual()
    print(f"\n>> Legislatura corrente: {legislatura}")

    desp = []
    for ano in (hoje.year, hoje.year - 1):
        desp = list(api.listar_despesas(dep_lista["id"], legislatura,
                                        ano=ano, max_paginas=1))
        print(f">> {len(desp)} despesas | {dep_lista['nome']} | ano {ano}")
        if desp:
            break

    if desp:
        mostrar("DESPESAS - item", desp[0])
    else:
        print("\n   Nenhuma despesa encontrada. Veja src/diag_despesas.py.")


if __name__ == "__main__":
    main()
