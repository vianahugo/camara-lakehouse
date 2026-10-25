"""
Confere as despesas da API contra o Postgres.

    python src/validar_despesas.py --ano 2026                              # todos
    python src/validar_despesas.py --ano 2026 --deputado "Felipe Carreras"  # um, mes a mes

O total da API e a soma do valorLiquido de todas as linhas, com os creditos
negativos abatendo, que e o mesmo criterio do portal da Camara. A lista
completa de todos os deputados vai para logs/validar_despesas_<ano>.csv.
So le a API e o banco.
"""

import argparse
import csv
from collections import defaultdict
from pathlib import Path

from camara_api import CamaraAPI
from load_producao import conectar

LOGS_DIR = Path(__file__).resolve().parent.parent / "logs"


def brl(v: float) -> str:
    s = f"{v:,.2f}"
    return "R$ " + s.replace(",", "X").replace(".", ",").replace("X", ".")


def achar_deputado(conn, alvo: str):
    with conn.cursor() as cur:
        if alvo.isdigit():
            cur.execute("SELECT id, nome, id_legislatura FROM producao.deputados "
                        "WHERE id = %s", (int(alvo),))
        else:
            cur.execute("SELECT id, nome, id_legislatura FROM producao.deputados "
                        "WHERE nome ILIKE %s ORDER BY nome", (f"%{alvo}%",))
        achados = cur.fetchall()
    if not achados:
        raise SystemExit(f"Nenhum deputado no banco com '{alvo}'.")
    if len(achados) > 1:
        nomes = "\n  ".join(f"{i} - {n}" for i, n, _ in achados)
        raise SystemExit(f"Mais de um deputado com '{alvo}'. Use o id:\n  {nomes}")
    return achados[0]


def validar_deputado(api: CamaraAPI, conn, alvo: str, ano: int, leg_atual: int):
    id_dep, nome, id_leg = achar_deputado(conn, alvo)

    with conn.cursor() as cur:
        cur.execute("SELECT mes, count(*), coalesce(sum(valor_liquido), 0) "
                    "FROM producao.despesas WHERE id_deputado = %s AND ano = %s "
                    "GROUP BY mes", (id_dep, ano))
        banco = {m: (n, float(tot)) for m, n, tot in cur.fetchall()}

    print(f"Buscando na API: {nome} (id {id_dep}), {ano}...")
    pela_api = defaultdict(lambda: [0, 0.0])
    for d in api.listar_despesas(id_dep, id_leg or leg_atual, ano=ano):
        mes = pela_api[d.get("mes")]
        mes[0] += 1
        mes[1] += float(d.get("valorLiquido") or 0)

    print()
    print(f"{'mes':>3} | {'API':>16} | {'banco':>16} | {'diferenca':>14} | "
          f"{'lin API':>7} | {'lin banco':>9}")
    print("-" * 80)

    tot_api = tot_banco = 0.0
    for mes in sorted(set(pela_api) | set(banco)):
        n_a, v_a = pela_api.get(mes, (0, 0.0))
        n_b, v_b = banco.get(mes, (0, 0.0))
        print(f"{mes:>3} | {brl(v_a):>16} | {brl(v_b):>16} | {brl(v_b - v_a):>14} | "
              f"{n_a:>7} | {n_b:>9}")
        tot_api += v_a
        tot_banco += v_b

    print("-" * 80)
    print(f"{'ano':>3} | {brl(tot_api):>16} | {brl(tot_banco):>16} | "
          f"{brl(tot_banco - tot_api):>14}")
    print("\nO Power BI le a gold, que so muda depois do Job do Databricks.")


def validar_todos(api: CamaraAPI, conn, ano: int, leg_atual: int):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT d.id, d.nome, d.id_legislatura,
                   coalesce(sum(x.valor_liquido), 0), count(x.cod_documento)
            FROM producao.deputados d
            LEFT JOIN producao.despesas x ON x.id_deputado = d.id AND x.ano = %s
            GROUP BY d.id, d.nome, d.id_legislatura
            ORDER BY d.id
        """, (ano,))
        deputados = cur.fetchall()

    print(f"Conferindo {len(deputados)} deputados na API ({ano}), ~10 minutos...")
    linhas, erros = [], []
    for i, (id_dep, nome, id_leg, tot_b, n_b) in enumerate(deputados, 1):
        try:
            desp = list(api.listar_despesas(id_dep, id_leg or leg_atual, ano=ano))
        except Exception as e:  # noqa: BLE001
            erros.append((id_dep, nome, str(e)))
            continue
        tot_a = sum(float(x.get("valorLiquido") or 0) for x in desp)
        linhas.append((id_dep, nome, round(tot_a, 2), round(float(tot_b), 2),
                       round(float(tot_b) - tot_a, 2), len(desp), n_b))
        if i % 50 == 0:
            print(f"  {i}/{len(deputados)}")

    LOGS_DIR.mkdir(exist_ok=True)
    destino = LOGS_DIR / f"validar_despesas_{ano}.csv"
    with open(destino, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["id", "nome", "api", "banco", "diferenca", "linhas_api", "linhas_banco"])
        w.writerows(sorted(linhas, key=lambda l: l[4]))

    divergem = sorted((l for l in linhas if abs(l[4]) >= 0.01), key=lambda l: l[4])
    tot_api = sum(l[2] for l in linhas)
    tot_banco = sum(l[3] for l in linhas)

    print()
    print(f"  Total API   : {brl(tot_api)}")
    print(f"  Total banco : {brl(tot_banco)}")
    print(f"  Diferenca   : {brl(tot_banco - tot_api)}  "
          f"({len(divergem)} de {len(linhas)} deputados divergem)")

    if divergem:
        print()
        print(f"{'id':>7} {'deputado':<30} {'API':>16} {'banco':>16} {'diferenca':>14}")
        print("-" * 88)
        for id_dep, nome, a, b, dif, _, _ in divergem[:25]:
            print(f"{id_dep:>7} {nome[:29]:<30} {brl(a):>16} {brl(b):>16} {brl(dif):>14}")
        if len(divergem) > 25:
            print(f"... e mais {len(divergem) - 25} (veja o CSV)")

    if erros:
        print(f"\n{len(erros)} deputados deram erro na API:")
        for id_dep, nome, e in erros[:10]:
            print(f"  {id_dep} {nome}: {e}")

    print(f"\nLista completa: {destino}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--ano", type=int, required=True)
    p.add_argument("--deputado", help="id ou parte do nome; sem ele, confere todos")
    args = p.parse_args()

    api = CamaraAPI()
    leg_atual = api.legislatura_atual()
    with conectar() as conn:
        if args.deputado:
            validar_deputado(api, conn, args.deputado, args.ano, leg_atual)
        else:
            validar_todos(api, conn, args.ano, leg_atual)


if __name__ == "__main__":
    main()
