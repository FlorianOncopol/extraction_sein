from __future__ import annotations

import logging
import json
import os
import re
import shlex
import tempfile
from datetime import datetime
from typing import Optional

import pandas as pd
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

LOGGER = logging.getLogger(__name__)

STAGE_DIGIT_MAPPING = {
    "1": "I",
    "2": "II",
    "3": "III",
    "4": "IV",
}

COUNT_STAGES = [
    "ALL",
    "Stage 0",
    "Stage I",
    "Stage IA",
    "Stage IB",
    "Stage IC",
    "Stage II",
    "Stage IIA",
    "Stage IIB",
    "Stage IIC",
    "Stage III",
    "Stage IIIA",
    "Stage IIIB",
    "Stage IIIC",
    "Stage IV",
    "UNKNOWN",
]


def _get_ssh_client(
    host: str,
    port: int,
    user: str,
    password_var_key: str,
) -> "paramiko.SSHClient":
    import paramiko

    password = Variable.get(password_var_key)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=host,
        port=port,
        username=user,
        password=password,
        timeout=30,
        allow_agent=False,
        look_for_keys=False,
    )
    return client


def _normalize_stage(raw: object) -> str:
    if raw is None or pd.isna(raw):
        return "UNKNOWN"
    value = str(raw).strip()
    if not value or value.lower() in {"null", "nan"}:
        return "UNKNOWN"
    clean = value.split("(")[0].strip().upper()
    clean = re.sub(r"^(STADE|STAGE)\s*", "", clean).strip()
    clean = clean.replace("AJCC", "").strip()
    clean = STAGE_DIGIT_MAPPING.get(clean, clean)
    if not re.fullmatch(r"0|IV|III[ABC]?|II[ABC]?|I[ABC]?", clean):
        return "UNKNOWN"
    return f"Stage {clean}"


def _parse_date(raw: object) -> Optional[pd.Timestamp]:
    if raw is None or pd.isna(raw):
        return None
    value = str(raw).strip()
    if not value or value.lower() in {"null", "nan", "nat"}:
        return None
    if len(value) == 8 and value.isdigit():
        value = f"{value[:4]}-{value[4:6]}-{value[6:8]}"
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed


def _is_lobular_histology(raw: object) -> bool:
    if raw is None or pd.isna(raw):
        return False
    value = str(raw).strip().upper()
    return value in {"LOBULAR", "MIXED_NST_LOBULAR"} or "LOBULAR" in value


def extract_ipp_c50_task(
    date_debut_obs: str = "2015-01-01",
    date_fin_obs: str = "today",
    conn_id: str = "postgres_test",
    **kwargs,
) -> None:
    """
    Extrait les IPP C50 depuis osiris.diagnostic.

    Cette premiere etape est volontairement large: l'histologie lobulaire est
    detectee ensuite dans les comptes rendus anapath par extract_tnm_stage_by_ipp.
    """
    start_date = _date_bound(date_debut_obs, start=True)
    end_date = _date_bound(date_fin_obs, start=False)

    query = """
        SELECT DISTINCT ON (d.ipp_ocr)
            d.diagnostic_id,
            d.ipp_ocr,
            d.date_prelevement::date AS date_prelevement,
            d.code_cim,
            d.libelle_cim,
            d.code_morphologique,
            d.tnm_code,
            d.cancer_type,
            d.cancer_site,
            d.stage_date::date AS stage_date
        FROM osiris.diagnostic d
        WHERE LEFT(UPPER(BTRIM(d.code_cim::text)), 3) = 'C50'
          AND d.date_prelevement IS NOT NULL
          AND d.date_prelevement::date >= %(start_date)s::date
          AND d.date_prelevement::date <= %(end_date)s::date
          AND NULLIF(BTRIM(d.ipp_ocr::text), '') IS NOT NULL
        ORDER BY
            d.ipp_ocr,
            d.date_prelevement DESC NULLS LAST,
            d.stage_date DESC NULLS LAST,
            d.date_diagnostic_updated_at DESC NULLS LAST,
            d.diagnostic_id DESC
    """

    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    try:
        df = pd.read_sql_query(
            query,
            conn,
            params={
                "start_date": start_date,
                "end_date": end_date,
            },
        )
    finally:
        conn.close()

    ipp_records = [
        {
            "ipp": str(row["ipp_ocr"]).strip(),
            "organe": "SEIN",
            "code_cim": None if pd.isna(row["code_cim"]) else str(row["code_cim"]).strip(),
            "date_diag_tkc": None if pd.isna(row["date_prelevement"]) else str(row["date_prelevement"]),
            "date_diag_dcc": None,
            "diagnostic_id": None if pd.isna(row["diagnostic_id"]) else str(row["diagnostic_id"]),
            "libelle_cim": None if pd.isna(row["libelle_cim"]) else str(row["libelle_cim"]),
            "code_morphologique": None if pd.isna(row["code_morphologique"]) else str(row["code_morphologique"]),
            "tnm_code": None if pd.isna(row["tnm_code"]) else str(row["tnm_code"]),
            "cancer_type": None if pd.isna(row["cancer_type"]) else str(row["cancer_type"]),
            "cancer_site": None if pd.isna(row["cancer_site"]) else str(row["cancer_site"]),
            "stage_date": None if pd.isna(row["stage_date"]) else str(row["stage_date"]),
        }
        for _, row in df.iterrows()
        if str(row.get("ipp_ocr", "")).strip()
    ]
    ipp_list = [row["ipp"] for row in ipp_records]

    LOGGER.info("IPP sein C50 extraits: %d", len(ipp_list))
    ti = kwargs["ti"]
    ti.xcom_push(key="ipp_list", value=ipp_list)
    ti.xcom_push(key="ipp_records", value=ipp_records)


def push_pdf_task(
    remote_host: str,
    remote_port: int,
    remote_user: str,
    ssh_password_var_key: str,
    ipp_task_id: str = "extract_ipp_c50_from_diagnostic",
    remote_script: str = "/opt/push_pdf_llm.py",
    source_dir: str = "/opt/PDF",
    stage_dir: str = "/home/administrateur/pdf_llm_sein",
    link_mode: str = "symlink",
    remote_python_bin: str = "python3",
    remote_tmp_dir: str = "/tmp",
    remote_progress_every: int = 200,
    remote_command_timeout: Optional[int] = None,
    **kwargs,
) -> None:
    ti = kwargs["ti"]
    ipp_list: list[str] = ti.xcom_pull(task_ids=ipp_task_id, key="ipp_list") or []
    if not ipp_list:
        LOGGER.warning("Aucun IPP recu en XCom, staging PDF ignore.")
        return

    client = _get_ssh_client(remote_host, remote_port, remote_user, ssh_password_var_key)
    local_ipp_file: Optional[str] = None
    remote_ipp_file: Optional[str] = None
    sftp = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".json",
            prefix="ipp_c50_",
            delete=False,
            encoding="utf-8",
        ) as tmp:
            json.dump({"ipp_list": ipp_list}, tmp, ensure_ascii=False)
            local_ipp_file = tmp.name

        remote_ipp_file = f"{remote_tmp_dir.rstrip('/')}/{os.path.basename(local_ipp_file)}"
        sftp = client.open_sftp()
        sftp.put(local_ipp_file, remote_ipp_file)

        cmd = " ".join(
            [
                shlex.quote(remote_python_bin),
                shlex.quote(remote_script),
                "--ipp-file",
                shlex.quote(remote_ipp_file),
                "--local-dir",
                shlex.quote(source_dir),
                "--stage-dir",
                shlex.quote(stage_dir),
                "--link-mode",
                shlex.quote(link_mode),
                "--clean-stage-dir",
                "--progress-every",
                shlex.quote(str(remote_progress_every)),
            ]
        )
        _run_ssh_command(client, cmd, "push_pdf", remote_command_timeout)
    finally:
        if sftp is not None:
            if remote_ipp_file:
                try:
                    sftp.remove(remote_ipp_file)
                except Exception:
                    pass
            sftp.close()
        if local_ipp_file and os.path.exists(local_ipp_file):
            os.unlink(local_ipp_file)
        client.close()


def run_tnm_extraction_task(
    remote_host: str,
    remote_port: int,
    remote_user: str,
    ssh_password_var_key: str,
    remote_script: str = "/opt/llm_extract/extract_tnm_stage_by_ipp.py",
    remote_data_dir: str = "/home/administrateur/pdf_llm_sein",
    remote_output_dir: Optional[str] = None,
    remote_python_bin: str = "python3",
    remote_csv_name: str = "ipp_stage_results.csv",
    remote_tmp_dir: str = "/tmp",
    ipp_task_id: str = "extract_ipp_c50_from_diagnostic",
    require_lobular_anapath: bool = True,
    remote_command_timeout: Optional[int] = None,
    **kwargs,
) -> None:
    ti = kwargs["ti"]
    ipp_records: list[dict[str, Optional[str]]] = ti.xcom_pull(task_ids=ipp_task_id, key="ipp_records") or []

    client = _get_ssh_client(remote_host, remote_port, remote_user, ssh_password_var_key)
    local_metadata_file: Optional[str] = None
    remote_metadata_file: Optional[str] = None
    sftp = None
    try:
        output_dir = remote_output_dir or remote_data_dir
        output_csv_path = f"{output_dir.rstrip('/')}/{remote_csv_name}"
        if ipp_records:
            with tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".json",
                prefix="ipp_c50_metadata_",
                delete=False,
                encoding="utf-8",
            ) as tmp:
                json.dump({"ipp_records": ipp_records}, tmp, ensure_ascii=False)
                local_metadata_file = tmp.name
            remote_metadata_file = f"{remote_tmp_dir.rstrip('/')}/{os.path.basename(local_metadata_file)}"
            sftp = client.open_sftp()
            sftp.put(local_metadata_file, remote_metadata_file)

        cmd = (
            f"mkdir -p {shlex.quote(output_dir)} && "
            f"rm -f {shlex.quote(output_csv_path)} && "
            f"{shlex.quote(remote_python_bin)} {shlex.quote(remote_script)} "
            f"{shlex.quote(remote_data_dir)} "
            f"--output-dir {shlex.quote(output_dir)} "
            f"--ipp-strategy baseline "
            f"--log-level INFO "
            f"--csv-name {shlex.quote(remote_csv_name)}"
        )
        if remote_metadata_file:
            cmd += f" --ipp-metadata-file {shlex.quote(remote_metadata_file)}"
        if require_lobular_anapath:
            cmd += " --require-lobular-anapath"
        _run_ssh_command(client, cmd, "tnm_extraction", remote_command_timeout)
    finally:
        if sftp is not None:
            if remote_metadata_file:
                try:
                    sftp.remove(remote_metadata_file)
                except Exception:
                    pass
            sftp.close()
        if local_metadata_file and os.path.exists(local_metadata_file):
            os.unlink(local_metadata_file)
        client.close()


def fetch_csv_task(
    remote_host: str,
    remote_port: int,
    remote_user: str,
    remote_csv_path: str,
    local_csv_path: str,
    ssh_password_var_key: str,
    **kwargs,
) -> None:
    local_dir = os.path.dirname(local_csv_path)
    if local_dir:
        os.makedirs(local_dir, exist_ok=True)

    client = _get_ssh_client(remote_host, remote_port, remote_user, ssh_password_var_key)
    try:
        sftp = client.open_sftp()
        try:
            sftp.get(remote_csv_path, local_csv_path)
        finally:
            sftp.close()
    finally:
        client.close()


def cleanup_remote_dir_task(
    remote_host: str,
    remote_port: int,
    remote_user: str,
    remote_dir: str,
    ssh_password_var_key: str,
    remote_command_timeout: Optional[int] = None,
    **kwargs,
) -> None:
    client = _get_ssh_client(remote_host, remote_port, remote_user, ssh_password_var_key)
    try:
        cmd = (
            f"mkdir -p {shlex.quote(remote_dir)} && "
            f"find {shlex.quote(remote_dir)} -mindepth 1 -maxdepth 1 -exec rm -rf -- {{}} +"
        )
        _run_ssh_command(client, cmd, "cleanup_remote_dir", remote_command_timeout)
    finally:
        client.close()


def _run_ssh_command(
    client: "paramiko.SSHClient",
    cmd: str,
    label: str,
    timeout: Optional[int],
) -> None:
    LOGGER.info("Commande SSH %s: %s", label, cmd)
    _, stdout, stderr = client.exec_command(cmd, timeout=timeout, get_pty=True)
    stdout_txt = stdout.read().decode("utf-8", errors="replace")
    stderr_txt = stderr.read().decode("utf-8", errors="replace")
    exit_status = stdout.channel.recv_exit_status()

    if stdout_txt.strip():
        LOGGER.info("STDOUT %s tail:\n%s", label, "\n".join(stdout_txt.strip().splitlines()[-40:]))
    if stderr_txt.strip():
        LOGGER.warning("STDERR %s tail:\n%s", label, "\n".join(stderr_txt.strip().splitlines()[-40:]))
    if exit_status != 0:
        error_excerpt = (stderr_txt or stdout_txt).strip()[:1000]
        raise RuntimeError(f"La commande {label} a termine avec le code {exit_status}. Detail: {error_excerpt}")


def refresh_count_lobulaire_task(
    local_csv_path: str,
    conn_id: str = "postgres_test",
    target_schema: str = "sein",
    target_table: str = "count_lobulaire",
    start_year: int = 2015,
    **kwargs,
) -> None:
    """
    Reconstruit la table de comptage des cancers du sein C50 lobulaires.

    PostgreSQL ne gere pas les noms en trois parties: avec une connexion sur la
    base oncpole_test, la table cible est sein.count_lobulaire.
    """
    df = pd.read_csv(local_csv_path, dtype=str)
    if df.empty:
        LOGGER.warning("CSV vide: aucun comptage lobulaire a charger.")
        return

    df["ipp"] = df.get("ipp", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    df = df[df["ipp"] != ""].copy()
    if df.empty:
        LOGGER.warning("CSV sans IPP valide: aucun comptage lobulaire a charger.")
        return

    df["histology_type"] = df.get("histology_type", pd.Series(dtype=str))
    df = df[df["histology_type"].apply(_is_lobular_histology)].copy()
    if df.empty:
        LOGGER.info("Aucun IPP avec histologie lobulaire dans le CSV.")
        _replace_count_table(pd.DataFrame(columns=["annee", "stage", "cancer_lobulaire_count"]), conn_id, target_schema, target_table)
        return

    df["stage_norm"] = df.get("stage", pd.Series(dtype=str)).apply(_normalize_stage)
    metadata_df = _fetch_c50_metadata(conn_id, df["ipp"].drop_duplicates().tolist())
    df = df.merge(metadata_df, on="ipp", how="inner")
    df["date_diag"] = pd.to_datetime(
        df["date_diag_tkc"].combine_first(df["date_diag_dcc"]).apply(_parse_date),
        errors="coerce",
    )
    df = df[df["date_diag"].notna()].copy()
    df["annee"] = df["date_diag"].dt.year.astype(int)
    df = df[df["annee"] >= int(start_year)].copy()

    count_rows = _build_dense_counts(df, start_year=start_year)
    _replace_count_table(count_rows, conn_id, target_schema, target_table)
    LOGGER.info("Table %s.%s reconstruite: %d lignes.", target_schema, target_table, len(count_rows))


def _date_bound(value: str, start: bool) -> str:
    token = str(value).strip().lower()
    if token in {"today", "now", "current_date"}:
        return datetime.today().strftime("%Y-%m-%d")
    if len(token) == 4 and token.isdigit():
        return f"{token}-01-01" if start else f"{token}-12-31"
    return token


def _fetch_c50_metadata(conn_id: str, ipps: list[str]) -> pd.DataFrame:
    if not ipps:
        return pd.DataFrame(columns=["ipp", "date_diag_tkc", "date_diag_dcc"])

    query = """
        WITH ranked AS (
            SELECT
                d.ipp_ocr::text AS ipp,
                d.date_prelevement::date AS date_diag_tkc,
                NULL::date AS date_diag_dcc,
                ROW_NUMBER() OVER (
                    PARTITION BY d.ipp_ocr::text
                    ORDER BY
                        d.date_prelevement DESC NULLS LAST,
                        d.stage_date DESC NULLS LAST,
                        d.date_diagnostic_updated_at DESC NULLS LAST,
                        d.diagnostic_id DESC
                ) AS rn
            FROM osiris.diagnostic d
            WHERE d.ipp_ocr::text = ANY(%s)
              AND LEFT(UPPER(BTRIM(d.code_cim::text)), 3) = 'C50'
              AND d.date_prelevement IS NOT NULL
              AND d.date_prelevement::date >= DATE '2015-01-01'
        )
        SELECT ipp, date_diag_tkc, date_diag_dcc
        FROM ranked
        WHERE rn = 1
    """
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    try:
        return pd.read_sql_query(query, conn, params=(ipps,))
    finally:
        conn.close()


def _build_dense_counts(df: pd.DataFrame, start_year: int) -> pd.DataFrame:
    current_year = datetime.today().year
    years = list(range(int(start_year), current_year + 1))

    grouped = (
        df.groupby(["annee", "stage_norm"])["ipp"]
        .nunique()
        .reset_index(name="cancer_lobulaire_count")
        .rename(columns={"stage_norm": "stage"})
    )
    totals = (
        df.groupby("annee")["ipp"]
        .nunique()
        .reset_index(name="cancer_lobulaire_count")
    )
    totals["stage"] = "ALL"
    grouped = pd.concat([grouped, totals], ignore_index=True)

    skeleton = pd.MultiIndex.from_product([years, COUNT_STAGES], names=["annee", "stage"]).to_frame(index=False)
    dense = skeleton.merge(grouped, on=["annee", "stage"], how="left")
    dense["cancer_lobulaire_count"] = dense["cancer_lobulaire_count"].fillna(0).astype(int)
    return dense


def _replace_count_table(df: pd.DataFrame, conn_id: str, schema: str, table: str) -> None:
    from psycopg2.extras import execute_values

    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    full_table = f"{_quote_ident(schema)}.{_quote_ident(table)}"
    rows = [
        (int(row["annee"]), str(row["stage"]), int(row["cancer_lobulaire_count"]))
        for _, row in df.iterrows()
    ]
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(schema)}")
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {full_table} (
                    annee integer NOT NULL,
                    stage text NOT NULL,
                    cancer_lobulaire_count integer NOT NULL,
                    PRIMARY KEY (annee, stage)
                )
                """
            )
            cur.execute(f"ALTER TABLE {full_table} DROP COLUMN IF EXISTS last_update")
            cur.execute(f"TRUNCATE TABLE {full_table}")
            if rows:
                execute_values(
                    cur,
                    f"""
                    INSERT INTO {full_table}
                        (annee, stage, cancer_lobulaire_count)
                    VALUES %s
                    """,
                    rows,
                    page_size=500,
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'
