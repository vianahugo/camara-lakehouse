#!/usr/bin/env bash
#
# Ciclo local completo: API -> Postgres -> parquet -> Volume -> Job do Databricks.
#
# Roda pelo Agendador de Tarefas (ou cron). Sobe o Postgres se precisar
# e grava a saida em logs/.
#
#   ./run_local.sh          # ciclo diario
#   ./run_local.sh full     # inclui o full-load (so na primeira vez)
#
set -uo pipefail

cd "$(dirname "$0")" || exit 1
RAIZ="$(pwd)"
INICIO_UTC="$(date -u '+%Y-%m-%d %H:%M:%S')"

mkdir -p logs
LOG="logs/run_$(date +%Y-%m-%d).log"

# saida no log e na tela
exec > >(tee -a "$LOG") 2>&1

echo ""
echo "========================================================"
echo "  Execucao iniciada em $(date '+%Y-%m-%d %H:%M:%S')"
echo "  Pasta: $RAIZ"
echo "========================================================"

falhar() {
    echo ""
    echo "!! FALHOU: $1"
    echo "!! Encerrado em $(date '+%H:%M:%S')"
    exit 1
}

# ---------------------------------------------------------------
# 1. Ambiente virtual
# ---------------------------------------------------------------
if [ -f ".venv/Scripts/activate" ]; then
    ACTIVATE=".venv/Scripts/activate"      # Windows
elif [ -f ".venv/bin/activate" ]; then
    ACTIVATE=".venv/bin/activate"          # macOS / Linux
else
    falhar "ambiente virtual nao encontrado. Crie com: python -m venv .venv"
fi

# shellcheck disable=SC1090
source "$ACTIVATE" || falhar "nao consegui ativar o venv"
echo "==> venv ativo: $(python --version 2>&1)"

# ---------------------------------------------------------------
# 2. Postgres
# ---------------------------------------------------------------
echo "==> Verificando o Postgres..."
docker compose up -d >/dev/null 2>&1 \
    || falhar "docker compose falhou. O Docker Desktop esta aberto?"

for i in $(seq 1 30); do
    if docker exec camara-postgres pg_isready -U camara -d camara >/dev/null 2>&1; then
        echo "    Postgres pronto (${i}s)"
        break
    fi
    [ "$i" -eq 30 ] && falhar "Postgres nao respondeu em 30s"
    sleep 1
done

# ---------------------------------------------------------------
# 3. Cargas
# ---------------------------------------------------------------
echo ""
echo "==> [1/4] Carregando da API para o Postgres"
python src/load_producao.py deputados                || falhar "load deputados"
python src/load_producao.py proposicoes --dias 2      || falhar "load proposicoes"
python src/load_producao.py despesas --deputados 0    || falhar "load despesas"

if [ "${1:-}" = "full" ]; then
    echo ""
    echo "==> Exportando full-load"
    python src/export_parquet.py full || falhar "export full"
fi

echo ""
echo "==> [2/4] Exportando eventos de CDC"
python src/export_parquet.py cdc || falhar "export cdc"

echo ""
echo "==> [3/4] Enviando para o Volume"
if [ "${1:-}" = "full" ]; then
    python src/upload_volume.py --schemas || falhar "upload schemas"
fi
python src/upload_volume.py || falhar "upload parquet"

echo ""
echo "==> [4/4] Disparando o Job no Databricks"
python src/disparar_job.py || falhar "disparar job"

# ---------------------------------------------------------------
# 4. Resumo e limpeza
# ---------------------------------------------------------------
echo ""
echo "==> Eventos de CDC exportados nesta execucao:"
RESUMO=$(docker exec camara-postgres psql -U camara -d camara -t -A -c \
  "SELECT '    ' || table_name || ' | ' || op || ' | ' || count(*)
   FROM producao.cdc_log WHERE exported_at >= '$INICIO_UTC'
   GROUP BY table_name, op ORDER BY 1;" 2>/dev/null)
echo "${RESUMO:-    nenhum}"

# mantem 30 dias de log
find logs -name "run_*.log" -type f -mtime +30 -delete 2>/dev/null

echo ""
echo "========================================================"
echo "  Concluido em $(date '+%H:%M:%S')"
echo "  Log: $LOG"
echo "========================================================"
