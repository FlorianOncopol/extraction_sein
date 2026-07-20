from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Optional

import pandas as pd
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
    first_visit_max_date: str = "today",
    **kwargs,
) -> None:
    """
    Extrait les IPP sein C50 depuis datamart_oeci_survie.v_statut_vital.

    Cette premiere etape est volontairement large: l'histologie lobulaire est
    detectee ensuite dans les comptes rendus anapath par extract_tnm_stage_by_ipp.
    """
    start_date = _date_bound(date_debut_obs, start=True)
    end_date = _date_bound(date_fin_obs, start=False)
    first_visit_limit = _date_bound(first_visit_max_date, start=False)

    query = """
        SELECT DISTINCT ON (v.ipp_ocr)
            v.ipp_ocr,
            v.organe,
            v.code_cim,
            v.date_diag_tkc::date AS date_diag_tkc,
            v.date_diag_dcc::date AS date_diag_dcc
        FROM datamart_oeci_survie.v_statut_vital v
        JOIN LATERAL (
            SELECT MIN(vis.visit_start_date::date) AS first_visit_start_date
            FROM osiris.visit vis
            WHERE vis.ipp_ocr::text = v.ipp_ocr::text
              AND vis.visit_start_date IS NOT NULL
              AND vis.visit_start_date::date >= COALESCE(v.date_diag_tkc, v.date_diag_dcc)::date
              AND vis.visit_start_date::date <= %(first_visit_limit)s::date
        ) fv ON fv.first_visit_start_date IS NOT NULL
        WHERE UPPER(BTRIM(v.organe::text)) = 'SEIN'
          AND LEFT(UPPER(BTRIM(v.code_cim::text)), 3) = 'C50'
          AND COALESCE(v.date_diag_tkc, v.date_diag_dcc) IS NOT NULL
          AND COALESCE(v.date_diag_tkc, v.date_diag_dcc)::date >= %(start_date)s::date
          AND COALESCE(v.date_diag_tkc, v.date_diag_dcc)::date <= %(end_date)s::date
          AND NULLIF(BTRIM(v.ipp_ocr::text), '') IS NOT NULL
        ORDER BY
            v.ipp_ocr,
            COALESCE(v.date_diag_tkc, v.date_diag_dcc) DESC NULLS LAST,
            v.date_diag_tkc DESC NULLS LAST,
            v.date_diag_dcc DESC NULLS LAST
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
                "first_visit_limit": first_visit_limit,
            },
        )
    finally:
        conn.close()

    ipp_records = [
        {
            "ipp": str(row["ipp_ocr"]).strip(),
            "organe": None if pd.isna(row["organe"]) else str(row["organe"]).strip(),
            "code_cim": None if pd.isna(row["code_cim"]) else str(row["code_cim"]).strip(),
            "date_diag_tkc": None if pd.isna(row["date_diag_tkc"]) else str(row["date_diag_tkc"]),
            "date_diag_dcc": None if pd.isna(row["date_diag_dcc"]) else str(row["date_diag_dcc"]),
        }
        for _, row in df.iterrows()
        if str(row.get("ipp_ocr", "")).strip()
    ]
    ipp_list = [row["ipp"] for row in ipp_records]

    LOGGER.info("IPP sein C50 extraits: %d", len(ipp_list))
    ti = kwargs["ti"]
    ti.xcom_push(key="ipp_list", value=ipp_list)
    ti.xcom_push(key="ipp_records", value=ipp_records)


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
                v.ipp_ocr::text AS ipp,
                v.date_diag_tkc::date AS date_diag_tkc,
                v.date_diag_dcc::date AS date_diag_dcc,
                ROW_NUMBER() OVER (
                    PARTITION BY v.ipp_ocr::text
                    ORDER BY COALESCE(v.date_diag_tkc, v.date_diag_dcc) DESC NULLS LAST
                ) AS rn
            FROM datamart_oeci_survie.v_statut_vital v
            WHERE v.ipp_ocr::text = ANY(%s)
              AND UPPER(BTRIM(v.organe::text)) = 'SEIN'
              AND LEFT(UPPER(BTRIM(v.code_cim::text)), 3) = 'C50'
              AND COALESCE(v.date_diag_tkc, v.date_diag_dcc) IS NOT NULL
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
