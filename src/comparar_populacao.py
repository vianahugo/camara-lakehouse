"""
Explica a diferenca entre o total do banco e o do portal da Camara.

O portal soma todos que exerceram o mandato na legislatura (titulares e
suplentes, inclusive quem ja saiu). O banco so tem os deputados em
exercicio. O script soma as despesas de quem falta e mostra quanto da
diferenca isso explica.

    python src/comparar_populacao.py --ano 2026 --portal 176229417.42
"""

import argparse

from camara_api import CamaraAPI
from load_producao import conectar


def brl(v: float) -> str:
    s = f"{v:,.2f}"
    return "R$ " + s.replace(",", "X").replace(".", ",").replace("X", ".")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--ano", type=int, required=True)
    p.add_argument("--portal", type=float, default=None,
                   help="total do portal no ano, so numeros (ex.: 176229417.42)")
    args = p.parse_args()

    api = CamaraAPI()
    id_leg = api.legislatura_atual()

    print(f"Legislatura {id_leg}: listando todos que exerceram o mandato...")
    todos = {d["id"]: d for d in api.paginar("/deputados", idLegislatura=id_leg,
                                             ordem="ASC", ordenarPor="nome")}

    with conectar() as conn, conn.cursor() as cur:
        cur.execute("SELECT id FROM producao.deputados")
        no_banco = {r[0] for r in cur.fetchall()}
        cur.execute("SELECT coalesce(sum(valor_liquido), 0), count(DISTINCT id_deputado) "
                    "FROM producao.despesas WHERE ano = %s", (args.ano,))
        total_banco, qtd_com_despesa = cur.fetchone()
    total_banco = float(total_banco)

    faltam = sorted(set(todos) - no_banco)
    print(f"  API (legislatura inteira): {len(todos)} deputados")
    print(f"  Banco                    : {len(no_banco)} deputados "
          f"({qtd_com_despesa} com despesa em {args.ano})")
    print(f"  Faltam no banco          : {len(faltam)}")

    if not faltam:
        print("\nA populacao e a mesma. A diferenca com o portal vem de outro lugar.")
        return

    print(f"\nSomando as despesas de {args.ano} dos {len(faltam)} que faltam...")
    linhas = []
    for i, id_dep in enumerate(faltam, 1):
        d = todos[id_dep]
        try:
            desp = list(api.listar_despesas(id_dep, id_leg, ano=args.ano))
        except Exception as e:  # noqa: BLE001
            print(f"  ! {d.get('nome')} ({id_dep}): {e}")
            continue
        total = sum(float(x.get("valorLiquido") or 0) for x in desp)
        linhas.append((total, d.get("nome"), d.get("siglaPartido"), d.get("siglaUf"),
                       len(desp)))
        if i % 20 == 0:
            print(f"  {i}/{len(faltam)}")

    linhas.sort(reverse=True)
    falta_total = sum(l[0] for l in linhas)
    com_gasto = [l for l in linhas if l[4] > 0]

    print()
    print(f"{'deputado':<34} {'part':<8} {'uf':<3} {'linhas':>6} {'total ' + str(args.ano):>18}")
    print("-" * 74)
    for total, nome, part, uf, n in com_gasto[:25]:
        print(f"{nome[:33]:<34} {str(part):<8} {str(uf):<3} {n:>6} {brl(total):>18}")
    if len(com_gasto) > 25:
        print(f"... e mais {len(com_gasto) - 25} com despesa")
    print("-" * 74)

    print()
    print(f"  Banco ({args.ano})                        : {brl(total_banco)}")
    print(f"  Quem falta ({len(com_gasto)} com despesa)        : {brl(falta_total)}")
    print(f"  Banco + quem falta                   : {brl(total_banco + falta_total)}")
    if args.portal is not None:
        gap = args.portal - total_banco
        resto = args.portal - (total_banco + falta_total)
        print(f"  Portal                               : {brl(args.portal)}")
        print(f"  Diferenca banco x portal             : {brl(gap)}")
        if gap:
            print(f"  Explicada por quem falta             : {falta_total / gap:.1%}")
        print(f"  Sobra sem explicacao                 : {brl(resto)}")


if __name__ == "__main__":
    main()
