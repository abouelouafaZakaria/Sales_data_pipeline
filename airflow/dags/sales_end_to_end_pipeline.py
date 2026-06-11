from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import requests
import json
import snowflake.connector

# -----------------------
# CONFIG
# -----------------------

API_BASE = "https://web-production-cf3ff.up.railway.app"

def get_snowflake_config():
    return {
        "user": Variable.get("SNOWFLAKE_USER"),
        "password": Variable.get("SNOWFLAKE_PASSWORD"),
        "account": Variable.get("SNOWFLAKE_ACCOUNT"),
        "warehouse": "SALES_WH",
        "database": "SALES_DB",
        "schema": "RAW"
    }

DBT_PATH = "/home/pain2/sales_data_pipeline/dbt_projectt"


# -----------------------
# ALERTS
# -----------------------

def on_failure_callback(context):
    task_id = context["task_instance"].task_id
    dag_id = context["task_instance"].dag_id
    execution_date = context["execution_date"]
    log_url = context["task_instance"].log_url

    subject = f"[Airflow] FAILED — {dag_id} · {task_id}"
    body = f"""
    <h3>Tâche échouée</h3>
    <ul>
        <li><b>DAG :</b> {dag_id}</li>
        <li><b>Tâche :</b> {task_id}</li>
        <li><b>Date d'exécution :</b> {execution_date}</li>
        <li><b>Logs :</b> <a href="{log_url}">{log_url}</a></li>
    </ul>
    """
    send_email(to="z.abouelouafa@gmail.com", subject=subject, html_content=body)


def on_retry_callback(context):
    task_id = context["task_instance"].task_id
    dag_id = context["task_instance"].dag_id
    try_number = context["task_instance"].try_number
    execution_date = context["execution_date"]

    subject = f"[Airflow] RETRY {try_number} — {dag_id} · {task_id}"
    body = f"""
    <h3>Tâche en cours de retry</h3>
    <ul>
        <li><b>DAG :</b> {dag_id}</li>
        <li><b>Tâche :</b> {task_id}</li>
        <li><b>Tentative :</b> {try_number}</li>
        <li><b>Date d'exécution :</b> {execution_date}</li>
    </ul>
    """
    send_email(to="z.abouelouafa@gmail.com", subject=subject, html_content=body)


# -----------------------
# INIT SNOWFLAKE
# -----------------------

def init_snowflake():
    conn = snowflake.connector.connect(**get_snowflake_config())
    cur = conn.cursor()

    cur.execute("CREATE DATABASE IF NOT EXISTS SALES_DB")
    cur.execute("CREATE SCHEMA IF NOT EXISTS SALES_DB.RAW")
    cur.execute("CREATE SCHEMA IF NOT EXISTS SALES_DB.STAGING")
    cur.execute("CREATE SCHEMA IF NOT EXISTS SALES_DB.MARTS")
    cur.execute("""
        CREATE WAREHOUSE IF NOT EXISTS SALES_WH
        WITH WAREHOUSE_SIZE = 'XSMALL'
        AUTO_SUSPEND = 60
        AUTO_RESUME = TRUE
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS SALES_DB.RAW.RAW_ORDERS (
            data VARIANT,
            ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS SALES_DB.RAW.RAW_CUSTOMERS (
            data VARIANT,
            ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS SALES_DB.RAW.RAW_PRODUCTS (
            data VARIANT,
            ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """)

    conn.commit()
    cur.close()
    conn.close()


# -----------------------
# INGESTION
# -----------------------

def fetch_orders():
    data = requests.get(f"{API_BASE}/orders?limit=50").json()

    conn = snowflake.connector.connect(**get_snowflake_config())
    cur = conn.cursor()

    for row in data:
        cur.execute("""
            INSERT INTO SALES_DB.RAW.RAW_ORDERS (data)
            SELECT PARSE_JSON(%s)
        """, (json.dumps(row),))

    conn.commit()
    cur.close()
    conn.close()


def fetch_customers():
    data = requests.get(f"{API_BASE}/customers?limit=50").json()

    conn = snowflake.connector.connect(**get_snowflake_config())
    cur = conn.cursor()

    for row in data:
        cur.execute("""
            INSERT INTO SALES_DB.RAW.RAW_CUSTOMERS (data)
            SELECT PARSE_JSON(%s)
        """, (json.dumps(row),))

    conn.commit()
    cur.close()
    conn.close()


def fetch_products():
    data = requests.get(f"{API_BASE}/products?limit=50").json()

    conn = snowflake.connector.connect(**get_snowflake_config())
    cur = conn.cursor()

    for row in data:
        cur.execute("""
            INSERT INTO SALES_DB.RAW.RAW_PRODUCTS (data)
            SELECT PARSE_JSON(%s)
        """, (json.dumps(row),))

    conn.commit()
    cur.close()
    conn.close()


# -----------------------
# DAG
# -----------------------

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
    "on_retry_callback": on_retry_callback,
    "email_on_failure": False,   # géré manuellement via on_failure_callback
    "email_on_retry": False,     # géré manuellement via on_retry_callback
}

with DAG(
    dag_id="sales_end_to_end_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    # ---------------- INIT ----------------
    init_snowflake_task = PythonOperator(
        task_id="init_snowflake",
        python_callable=init_snowflake
    )

    # ---------------- INGESTION ----------------
    orders = PythonOperator(
        task_id="fetch_orders",
        python_callable=fetch_orders
    )

    customers = PythonOperator(
        task_id="fetch_customers",
        python_callable=fetch_customers
    )

    products = PythonOperator(
        task_id="fetch_products",
        python_callable=fetch_products
    )

    # ---------------- DBT ----------------
    dbt_raw = BashOperator(
        task_id="dbt_raw",
        bash_command=f"cd {DBT_PATH} && dbt run --select path:models/raw"
    )

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=f"cd {DBT_PATH} && dbt run --select path:models/staging"
    )

    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command=f"cd {DBT_PATH} && dbt run --select path:models/marts"
    )

    # ---------------- PIPELINE ORDER ----------------

    init_snowflake_task >> [orders, customers, products] >> dbt_raw >> dbt_staging >> dbt_marts
