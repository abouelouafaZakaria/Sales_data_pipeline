# sales_data_pipeline

Pipeline de données end-to-end simulant une plateforme e-commerce — de la génération de données synthétiques jusqu'aux dashboards KPI.

**Stack :** FastAPI · Apache Airflow · Snowflake · dbt · Power BI

---

## Architecture

```
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
```

---

## Structure du projet

```
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
```

---

## Data quality — par conception

L'API injecte intentionnellement du bruit (probabilité 20% par enregistrement) pour simuler des données réelles imparfaites. La couche staging le traite explicitement :

| Modèle | Règle appliquée |
|---|---|
| `stg_orders` | quantité ≤ 0 ou > 1000 → `NULL` · date invalide → `NULL` |
| `stg_products` | prix ≤ 0 → `NULL` · flag `valid_price = false` |
| `stg_customers` | région inconnue → `'Unknown'` |

---

## Modèles dbt

| Modèle | Matérialisation | Description |
|---|---|---|
| `raw_*` | view | Pass-through direct sur les tables VARIANT Snowflake |
| `stg_orders` | view | Commandes nettoyées — validation quantité et date |
| `stg_customers` | view | Clients nettoyés — normalisation des régions |
| `stg_products` | view | Produits nettoyés — garde sur les prix |
| `fct_sales` | table | Jointure orders × products avec revenue = quantité × prix |
| `fct_sales_incremental` | incremental | Idem fct_sales, dédupliqué sur `order_id` |
| `kpi_daily_revenue` | view | Revenu total journalier + nombre de commandes |
| `kpi_region_revenue` | view | Revenu agrégé par région client |

---

## Schéma Snowflake

Les données sont stockées en `VARIANT` dans la couche RAW — JSON ingéré via `PARSE_JSON()`. Les modèles staging extraient les colonnes typées avec la syntaxe `data:field::type` de Snowflake.

```
SALES_DB
├── RAW
│   ├── RAW_ORDERS     (data VARIANT, ingestion_time TIMESTAMP)
│   ├── RAW_CUSTOMERS  (data VARIANT, ingestion_time TIMESTAMP)
│   └── RAW_PRODUCTS   (data VARIANT, ingestion_time TIMESTAMP)
├── STAGING            (géré par dbt)
└── MARTS              (géré par dbt)
```

---

## DAG Airflow

`sales_end_to_end_pipeline` — planifié `@daily`

```
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
```

La tâche `init_snowflake` est idempotente — utilise `CREATE IF NOT EXISTS` pour toutes les ressources (base, schémas, warehouse, tables).

---

## Dashboard Power BI

Connecté directement au schéma MARTS de Snowflake. Deux pages principales :

- **Revenu quotidien** — graphe linéaire sur `kpi_daily_revenue` (order_date × total_revenue)
- **Revenu par région** — graphe en barres sur `kpi_region_revenue` (region × revenue)

Connexion : connecteur Snowflake → Import / DirectQuery sur `SALES_DB.MARTS`

---

## Lancer le projet

### 1. API (déjà déployée sur Railway)

```
https://web-production-cf3ff.up.railway.app
```

Endpoints : `/orders?limit=N` · `/customers?limit=N` · `/products`

### 2. Airflow

```bash
pip install apache-airflow snowflake-connector-python requests

# Copier le DAG dans le dossier Airflow
cp airflow/dags/sales_end_to_end_pipeline.py ~/airflow/dags/

airflow standalone
```

### 3. dbt

```bash
cd dbt_projectt
pip install dbt-snowflake

# Configurer profiles.yml avec tes credentials Snowflake
dbt debug
dbt run
dbt test
```

---

## Concepts clés illustrés

- **Ingestion semi-structurée** — pattern Snowflake VARIANT + PARSE_JSON
- **Transformation en couches** — séparation Raw / Staging / Marts (approche medallion)
- **Modèles incrementaux** — `fct_sales_incremental` dédupliqué sur clé unique
- **Qualité des données** — logique CASE défensive en staging, tests dbt (not_null, unique)
- **Orchestration** — DAG Airflow avec ingestion parallèle + couches dbt séquentielles
- **Déploiement** — FastAPI sur Railway, dbt ciblant Snowflake cloud

---

## Auteur

ABOU ELOUAFA Zakaria
[LinkedIn](www.linkedin.com/in/abou-elouafa-zakaria-285a72224) 
