sales_data_pipeline

Pipeline de données end-to-end simulant une plateforme e-commerce — de la génération de données synthétiques jusqu'aux dashboards KPI.

Stack : FastAPI · Apache Airflow · Snowflake · dbt · Power BI


Architecture

FastAPI (Railway)
      │
      │  REST API — orders / customers / products
      ▼
Apache Airflow
      │
      │  Ingestion quotidienne vers Snowflake (VARIANT)
      ▼
Snowflake — RAW schema
      │
      │  dbt run
      ▼
STAGING schema          MARTS schema
stg_orders          →   fct_sales
stg_customers       →   fct_sales_incremental
stg_products        →   kpi_daily_revenue
                        kpi_region_revenue
      │
      ▼
Power BI Dashboard


Structure du projet

sales_data_pipeline/
├── api/
│   ├── main.py                            # API FastAPI — données synthétiques avec bruit
│   ├── Procfile                           # Config déploiement Railway
│   └── runtime.txt
│
├── airflow/
│   └── dags/
│       └── sales_end_to_end_pipeline.py   # DAG complet : init → ingestion → dbt
│
└── dbt_projectt/
    └── models/
        ├── raw/                           # Vues pass-through sur les tables sources
        │   ├── raw_orders.sql
        │   ├── raw_customers.sql
        │   └── raw_products.sql
        ├── staging/                       # Nettoyage et validation
        │   ├── stg_orders.sql
        │   ├── stg_customers.sql
        │   └── stg_products.sql
        └── marts/                         # Tables métier prêtes à consommer
            ├── fct_sales.sql
            ├── kpi_daily_revenue.sql
            └── kpi_region_revenue.sql


Data quality — par conception

L'API injecte intentionnellement du bruit (probabilité 20% par enregistrement) pour simuler des données réelles imparfaites. La couche staging le traite explicitement :

ModèleRègle appliquéestg_ordersquantité ≤ 0 ou > 1000 → NULL · date invalide → NULLstg_productsprix ≤ 0 → NULL · flag valid_price = falsestg_customersrégion inconnue → 'Unknown'


Modèles dbt

ModèleMatérialisationDescriptionraw_*viewPass-through direct sur les tables VARIANT Snowflakestg_ordersviewCommandes nettoyées — validation quantité et datestg_customersviewClients nettoyés — normalisation des régionsstg_productsviewProduits nettoyés — garde sur les prixfct_salestableJointure orders × products avec revenue = quantité × prixfct_sales_incrementalincrementalIdem fct_sales, dédupliqué sur order_idkpi_daily_revenueviewRevenu total journalier + nombre de commandeskpi_region_revenueviewRevenu agrégé par région client


Schéma Snowflake

Les données sont stockées en VARIANT dans la couche RAW — JSON ingéré via PARSE_JSON(). Les modèles staging extraient les colonnes typées avec la syntaxe data:field::type de Snowflake.

SALES_DB
├── RAW
│   ├── RAW_ORDERS     (data VARIANT, ingestion_time TIMESTAMP)
│   ├── RAW_CUSTOMERS  (data VARIANT, ingestion_time TIMESTAMP)
│   └── RAW_PRODUCTS   (data VARIANT, ingestion_time TIMESTAMP)
├── STAGING            (géré par dbt)
└── MARTS              (géré par dbt)


DAG Airflow

sales_end_to_end_pipeline — planifié @daily

init_snowflake
      │
      ├── fetch_orders
      ├── fetch_customers
      └── fetch_products
               │
            dbt_raw
               │
          dbt_staging
               │
           dbt_marts

La tâche init_snowflake est idempotente — utilise CREATE IF NOT EXISTS pour toutes les ressources (base, schémas, warehouse, tables).


Dashboard Power BI

Connecté directement au schéma MARTS de Snowflake. Deux pages principales :


Revenu quotidien — graphe linéaire sur kpi_daily_revenue (order_date × total_revenue)
Revenu par région — graphe en barres sur kpi_region_revenue (region × revenue)


Connexion : connecteur Snowflake → Import / DirectQuery sur SALES_DB.MARTS


Lancer le projet

1. API (déjà déployée sur Railway)

https://web-production-cf3ff.up.railway.app

Endpoints : /orders?limit=N · /customers?limit=N · /products

2. Airflow

bashpip install apache-airflow snowflake-connector-python requests

# Copier le DAG dans le dossier Airflow
cp airflow/dags/sales_end_to_end_pipeline.py ~/airflow/dags/

airflow standalone

3. dbt

bashcd dbt_projectt
pip install dbt-snowflake

# Configurer profiles.yml avec tes credentials Snowflake
dbt debug
dbt run
dbt test


Concepts clés illustrés


Ingestion semi-structurée — pattern Snowflake VARIANT + PARSE_JSON
Transformation en couches — séparation Raw / Staging / Marts (approche medallion)
Modèles incrementaux — fct_sales_incremental dédupliqué sur clé unique
Qualité des données — logique CASE défensive en staging, tests dbt (not_null, unique)
Orchestration — DAG Airflow avec ingestion parallèle + couches dbt séquentielles
Déploiement — FastAPI sur Railway, dbt ciblant Snowflake cloud



Auteur:ABOU ELOUAFA Zakaria
