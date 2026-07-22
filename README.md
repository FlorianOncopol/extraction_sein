# Extraction sein lobulaire

Ce dossier contient la base specifique au comptage des cancers du sein C50 avec histologie lobulaire.

Flux attendu:

1. `extract_ipp_c50_task` extrait les IPP avec `code_cim` commencant par `C50` depuis `osiris.diagnostic`, avec `date_prelevement >= 2015-01-01`.
2. Le pipeline de copie PDF reprend la logique du projet precedent pour envoyer les PDF/JSON du patient vers le serveur d'extraction.
3. `extract_tnm_stage_by_ipp.py` lit d'abord uniquement les documents anapath/pathology de chaque IPP pour detecter `carcinome lobulaire` via `histology_type=LOBULAR` ou `MIXED_NST_LOBULAR`.
4. Si aucun anapath lobulaire n'est trouve, l'IPP est ignore avant le scan TNM complet.
5. Si l'histologie lobulaire est confirmee, le script scanne les documents de cet IPP et extrait le stade avec les regles sein.
6. `refresh_count_lobulaire_task` lit le CSV produit, garde uniquement les IPP C50 lobulaires, normalise le stade et reconstruit `sein.count_lobulaire`.

Dans PostgreSQL, `oncpole_test.sein.count_lobulaire` signifie: base `oncpole_test`, schema `sein`, table `count_lobulaire`.

Colonnes de la table finale:

- `annee`
- `stage`
- `cancer_lobulaire_count`

La ligne `stage = 'ALL'` donne le total annuel des cancers C50 lobulaires. Les autres lignes donnent la repartition annuelle par stade de 2015 a l'annee courante.

Le DAG `dag_count_lobulaire.py` orchestre l'alimentation complete de `sein.count_lobulaire`.

Variables Airflow utiles:

- `EXTRACTION_SEIN_REMOTE_HOST`: defaut `srvlakehouse`
- `EXTRACTION_SEIN_REMOTE_PORT`: defaut `22`
- `EXTRACTION_SEIN_REMOTE_USER`: defaut `administrateur`
- `EXTRACTION_SEIN_SSH_PASSWORD_VAR_KEY`: optionnel, nom d'une autre Variable Airflow contenant le mot de passe SSH

Le script SQL `sql/refresh_count_lobulaire.sql` permet de reconstruire la meme table depuis `datamart_oeci_survie.ipp_stade` en rejoignant `osiris.diagnostic` pour le filtre C50 et l'annee de `date_prelevement`.
