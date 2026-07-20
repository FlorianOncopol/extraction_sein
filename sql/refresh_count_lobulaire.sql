CREATE SCHEMA IF NOT EXISTS sein;

CREATE TABLE IF NOT EXISTS sein.count_lobulaire (
    annee integer NOT NULL,
    stage text NOT NULL,
    cancer_lobulaire_count integer NOT NULL,
    PRIMARY KEY (annee, stage)
);

ALTER TABLE sein.count_lobulaire DROP COLUMN IF EXISTS last_update;

TRUNCATE TABLE sein.count_lobulaire;

WITH stage_dim(stage) AS (
    VALUES
        ('ALL'),
        ('Stage 0'),
        ('Stage I'),
        ('Stage IA'),
        ('Stage IB'),
        ('Stage IC'),
        ('Stage II'),
        ('Stage IIA'),
        ('Stage IIB'),
        ('Stage IIC'),
        ('Stage III'),
        ('Stage IIIA'),
        ('Stage IIIB'),
        ('Stage IIIC'),
        ('Stage IV'),
        ('UNKNOWN')
),
year_dim(annee) AS (
    SELECT generate_series(2015, EXTRACT(YEAR FROM CURRENT_DATE)::integer)
),
lobulaire AS (
    SELECT DISTINCT
        s.ipp,
        EXTRACT(YEAR FROM d.date_prelevement)::integer AS annee,
        COALESCE(NULLIF(BTRIM(s.stage), ''), 'UNKNOWN') AS stage
    FROM datamart_oeci_survie.ipp_stade s
    JOIN osiris.diagnostic d
      ON d.ipp_ocr::text = s.ipp::text
    WHERE LEFT(UPPER(BTRIM(d.code_cim::text)), 3) = 'C50'
      AND d.date_prelevement IS NOT NULL
      AND d.date_prelevement::date >= DATE '2015-01-01'
      AND d.date_prelevement::date <= CURRENT_DATE
      AND (
          UPPER(COALESCE(s.histology_type, '')) = 'LOBULAR'
          OR UPPER(COALESCE(s.histology_type, '')) = 'MIXED_NST_LOBULAR'
          OR UPPER(COALESCE(s.histology_type, '')) LIKE '%LOBULAR%'
      )
),
counts_by_stage AS (
    SELECT annee, stage, COUNT(DISTINCT ipp)::integer AS cancer_lobulaire_count
    FROM lobulaire
    GROUP BY annee, stage
),
counts_total AS (
    SELECT annee, 'ALL'::text AS stage, COUNT(DISTINCT ipp)::integer AS cancer_lobulaire_count
    FROM lobulaire
    GROUP BY annee
),
counts_all AS (
    SELECT * FROM counts_by_stage
    UNION ALL
    SELECT * FROM counts_total
)
INSERT INTO sein.count_lobulaire (
    annee,
    stage,
    cancer_lobulaire_count
)
SELECT
    y.annee,
    sd.stage,
    COALESCE(c.cancer_lobulaire_count, 0) AS cancer_lobulaire_count
FROM year_dim y
CROSS JOIN stage_dim sd
LEFT JOIN counts_all c
    ON c.annee = y.annee
   AND c.stage = sd.stage
ORDER BY y.annee, sd.stage;
