# Extraction sein lobulaire

Ce dossier contient la base specifique au comptage des cancers du sein C50 avec histologie lobulaire.

Flux attendu:

1. `extract_ipp_c50_task` extrait les IPP `SEIN` avec `code_cim` commencant par `C50` depuis `datamart_oeci_survie.v_statut_vital`, a partir de 2015.
2. Le pipeline de copie PDF reprend la logique du projet precedent pour envoyer les PDF/JSON du patient vers le serveur d'extraction.
3. `extract_tnm_stage_by_ipp.py` lit les comptes rendus, privilegie les documents anapath, detecte `carcinome lobulaire` via `histology_type=LOBULAR` ou `MIXED_NST_LOBULAR`, et extrait le stade.
4. `refresh_count_lobulaire_task` lit le CSV produit, garde uniquement les IPP C50 lobulaires, normalise le stade et reconstruit `sein.count_lobulaire`.

Dans PostgreSQL, `oncpole_test.sein.count_lobulaire` signifie: base `oncpole_test`, schema `sein`, table `count_lobulaire`.

Colonnes de la table finale:

- `annee`
- `stage`
- `cancer_lobulaire_count`

La ligne `stage = 'ALL'` donne le total annuel des cancers C50 lobulaires. Les autres lignes donnent la repartition annuelle par stade de 2015 a l'annee courante.

Le script SQL `sql/refresh_count_lobulaire.sql` permet de reconstruire la meme table directement depuis `datamart_oeci_survie.ipp_stade` une fois cette table alimentee.
