from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from sein_lobulaire_tasks import (
    cleanup_remote_dir_task,
    extract_ipp_c50_task,
    fetch_csv_task,
    push_pdf_task,
    refresh_count_lobulaire_task,
    run_tnm_extraction_task,
)

DAG_ID = "extraction_sein_count_lobulaire"

REMOTE_HOST = Variable.get("EXTRACTION_SEIN_REMOTE_HOST", default_var="srvlakehouse")
REMOTE_PORT = int(Variable.get("EXTRACTION_SEIN_REMOTE_PORT", default_var="22"))
REMOTE_USER = Variable.get("EXTRACTION_SEIN_REMOTE_USER", default_var="administrateur")
SSH_PASSWORD_VAR_KEY = Variable.get(
    "EXTRACTION_SEIN_SSH_PASSWORD_VAR_KEY",
    default_var="password_serverlakehouse",
)

REMOTE_STAGE_DIR = Variable.get(
    "EXTRACTION_SEIN_REMOTE_STAGE_DIR",
    default_var="/home/administrateur/pdf_llm_sein",
)
REMOTE_OUTPUT_DIR = Variable.get(
    "EXTRACTION_SEIN_REMOTE_OUTPUT_DIR",
    default_var=REMOTE_STAGE_DIR,
)
REMOTE_CSV_NAME = Variable.get(
    "EXTRACTION_SEIN_REMOTE_CSV_NAME",
    default_var="ipp_stage_results.csv",
)
REMOTE_TMP_DIR = Variable.get(
    "EXTRACTION_SEIN_REMOTE_TMP_DIR",
    default_var="/tmp",
)
REMOTE_CSV_PATH = f"{REMOTE_OUTPUT_DIR.rstrip('/')}/{REMOTE_CSV_NAME}"
LOCAL_CSV_PATH = Variable.get(
    "EXTRACTION_SEIN_LOCAL_CSV_PATH",
    default_var="/tmp/extraction_sein/ipp_stage_results.csv",
)


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["sein", "lobulaire", "c50"],
) as dag:
    extract_ipp_c50 = PythonOperator(
        task_id="extract_ipp_c50_from_diagnostic",
        python_callable=extract_ipp_c50_task,
        op_kwargs={
            "date_debut_obs": "2015-01-01",
            "conn_id": "postgres_test",
        },
    )

    push_pdf = PythonOperator(
        task_id="push_pdf_c50_to_lakehouse",
        python_callable=push_pdf_task,
        op_kwargs={
            "remote_host": REMOTE_HOST,
            "remote_port": REMOTE_PORT,
            "remote_user": REMOTE_USER,
            "ssh_password_var_key": SSH_PASSWORD_VAR_KEY,
            "ipp_task_id": "extract_ipp_c50_from_diagnostic",
            "remote_script": "/opt/push_pdf_llm.py",
            "source_dir": "/opt/PDF",
            "stage_dir": REMOTE_STAGE_DIR,
            "link_mode": "symlink",
            "remote_python_bin": "python3",
            "remote_tmp_dir": REMOTE_TMP_DIR,
        },
    )

    run_extraction = PythonOperator(
        task_id="run_tnm_lobulaire_extraction",
        python_callable=run_tnm_extraction_task,
        op_kwargs={
            "remote_host": REMOTE_HOST,
            "remote_port": REMOTE_PORT,
            "remote_user": REMOTE_USER,
            "ssh_password_var_key": SSH_PASSWORD_VAR_KEY,
            "remote_script": "/opt/llm_extract/extract_tnm_stage_by_ipp.py",
            "remote_data_dir": REMOTE_STAGE_DIR,
            "remote_output_dir": REMOTE_OUTPUT_DIR,
            "remote_csv_name": REMOTE_CSV_NAME,
            "remote_tmp_dir": REMOTE_TMP_DIR,
            "ipp_task_id": "extract_ipp_c50_from_diagnostic",
            "require_lobular_anapath": True,
        },
    )

    fetch_csv = PythonOperator(
        task_id="fetch_lobulaire_csv",
        python_callable=fetch_csv_task,
        op_kwargs={
            "remote_host": REMOTE_HOST,
            "remote_port": REMOTE_PORT,
            "remote_user": REMOTE_USER,
            "ssh_password_var_key": SSH_PASSWORD_VAR_KEY,
            "remote_csv_path": REMOTE_CSV_PATH,
            "local_csv_path": LOCAL_CSV_PATH,
        },
    )

    refresh_count = PythonOperator(
        task_id="refresh_count_lobulaire",
        python_callable=refresh_count_lobulaire_task,
        op_kwargs={
            "local_csv_path": LOCAL_CSV_PATH,
            "conn_id": "postgres_test",
            "target_schema": "sein",
            "target_table": "count_lobulaire",
            "start_year": 2015,
        },
    )

    cleanup_remote = PythonOperator(
        task_id="cleanup_remote_stage_dir",
        python_callable=cleanup_remote_dir_task,
        trigger_rule="all_done",
        op_kwargs={
            "remote_host": REMOTE_HOST,
            "remote_port": REMOTE_PORT,
            "remote_user": REMOTE_USER,
            "ssh_password_var_key": SSH_PASSWORD_VAR_KEY,
            "remote_dir": REMOTE_STAGE_DIR,
        },
    )

    extract_ipp_c50 >> push_pdf >> run_extraction >> fetch_csv >> refresh_count >> cleanup_remote
