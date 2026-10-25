"""
Dispara o Job do Databricks depois que os arquivos chegam ao Volume.

Assim o Job roda logo apos o upload, em vez de depender de um horario
fixo combinado com o agendamento local.

    python src/disparar_job.py
"""

import logging
import os

from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)-7s | %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("job")


def main():
    job_id = os.getenv("DATABRICKS_JOB_ID")
    if not job_id:
        raise SystemExit("Defina DATABRICKS_JOB_ID no .env.")

    w = WorkspaceClient()
    run = w.jobs.run_now(job_id=int(job_id))
    logger.info("Job %s disparado (run_id %s)", job_id, run.run_id)


if __name__ == "__main__":
    main()
