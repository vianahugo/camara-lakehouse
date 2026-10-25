"""
Envia os parquet de data/ para o Volume do Unity Catalog.

Na Free Edition nao da para usar um bucket proprio, entao o Volume
gerenciado faz o papel do S3. Para o Spark, /Volumes/... e um caminho comum.

    python src/upload_volume.py --schemas   # tambem envia schemas/*.json
    python src/upload_volume.py             # so o que ainda nao esta no Volume
    python src/upload_volume.py --forcar    # reenvia tudo
"""

import argparse
import logging
import os
from pathlib import Path

from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)-7s | %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("upload")

RAIZ = Path(__file__).resolve().parent.parent
DATA_DIR = RAIZ / "data"
SCHEMA_DIR = RAIZ / "schemas"

VOLUME_ROOT = os.getenv("UC_VOLUME_ROOT", "/Volumes/camara/raw/landing").rstrip("/")


def enviar(w: WorkspaceClient, local: Path, remoto: str):
    with open(local, "rb") as fh:
        w.files.upload(remoto, fh, overwrite=True)
    logger.info("  %s -> %s (%.1f KB)", local.name, remoto, local.stat().st_size / 1024)


def arquivos_remotos(w: WorkspaceClient, pasta: str) -> dict:
    """{nome: tamanho} dos arquivos de uma pasta do Volume."""
    try:
        return {e.name: e.file_size
                for e in w.files.list_directory_contents(pasta)
                if not e.is_directory}
    except Exception:
        return {}  # pasta ainda nao existe


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--schemas", action="store_true", help="envia tambem schemas/*.json")
    p.add_argument("--forcar", action="store_true", help="reenvia mesmo o que ja esta no Volume")
    args = p.parse_args()

    if not os.getenv("DATABRICKS_HOST") or not os.getenv("DATABRICKS_TOKEN"):
        raise SystemExit("Defina DATABRICKS_HOST e DATABRICKS_TOKEN no .env.")

    w = WorkspaceClient()
    logger.info("Destino: %s", VOLUME_ROOT)

    enviados = 0
    pulados = 0

    if args.schemas:
        for arquivo in sorted(SCHEMA_DIR.glob("*.json")):
            enviar(w, arquivo, f"{VOLUME_ROOT}/schemas/{arquivo.name}")
            enviados += 1

    # Os parquet tem timestamp no nome e nunca sao reescritos: se o mesmo
    # nome com o mesmo tamanho ja esta no Volume, nao precisa enviar de novo.
    remotos = {}
    for arquivo in sorted(DATA_DIR.rglob("*.parquet")):
        pasta = f"{VOLUME_ROOT}/{arquivo.parent.relative_to(DATA_DIR).as_posix()}"
        if pasta not in remotos:
            remotos[pasta] = arquivos_remotos(w, pasta)

        if not args.forcar and remotos[pasta].get(arquivo.name) == arquivo.stat().st_size:
            pulados += 1
            continue
        enviar(w, arquivo, f"{pasta}/{arquivo.name}")
        enviados += 1

    logger.info("%s arquivo(s) enviado(s), %s ja estavam no Volume.", enviados, pulados)


if __name__ == "__main__":
    main()
