from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.sql import SqlSensor
import random
import time


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="olympic_medals_hw_dag",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["homework", "olympic", "medals"],
) as dag:

    # ---------------------------------------------------------
    # TASK 1: Создание таблицы Postgres (вместо MySQL)
    # ---------------------------------------------------------
    create_medals_stats_table = PostgresOperator(
        task_id="create_medals_stats_table",
        postgres_conn_id="hw_postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS medals_stats (
                id SERIAL PRIMARY KEY,
                medal_type VARCHAR(50),
                value INT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """,
    )

    # ---------------------------------------------------------
    # TASK 2: Выбор случайной медали
    # ---------------------------------------------------------
    def choose_medal_func(**context):
        medal = random.choice(["gold", "silver", "bronze"])
        print(f"Chosen medal: {medal}")
        return medal

    choose_medal = PythonOperator(
        task_id="choose_medal",
        python_callable=choose_medal_func,
    )

    # ---------------------------------------------------------
    # TASK 3: Ветка по медали
    # ---------------------------------------------------------
    def branch_by_medal_func(**context):
        ti = context["ti"]
        medal = ti.xcom_pull(task_ids="choose_medal")
        print("Branching for:", medal)
        return f"process_{medal}"

    branch_by_medal = BranchPythonOperator(
        task_id="branch_by_medal",
        python_callable=branch_by_medal_func,
    )

    # ---------------------------------------------------------
    # TASK 4: Обработка медалей
    # ---------------------------------------------------------
    def process_gold_func():
        print("Processing GOLD medal...")
        time.sleep(1)

    def process_silver_func():
        print("Processing SILVER medal...")
        time.sleep(1)

    def process_bronze_func():
        print("Processing BRONZE medal...")
        time.sleep(1)

    process_gold = PythonOperator(
        task_id="process_gold",
        python_callable=process_gold_func,
    )

    process_silver = PythonOperator(
        task_id="process_silver",
        python_callable=process_silver_func,
    )

    process_bronze = PythonOperator(
        task_id="process_bronze",
        python_callable=process_bronze_func,
    )

    # ---------------------------------------------------------
    # TASK 5: Искусственная задержка перед проверкой
    # ---------------------------------------------------------
    def delay_after_insert_func():
        print("Delaying after insert...")
        time.sleep(5)

    delay_after_insert = PythonOperator(
        task_id="delay_after_insert",
        python_callable=delay_after_insert_func,
    )

    # ---------------------------------------------------------
    # TASK 6: SQL Sensor. Проверка, что запись свежая
    # ---------------------------------------------------------
    check_latest_medal_record_is_fresh = SqlSensor(
        task_id="check_latest_medal_record_is_fresh",
        conn_id="hw_postgres",
        sql="""
            SELECT 1
            FROM medals_stats
            WHERE created_at > NOW() - INTERVAL '10 minutes';
        """,
        poke_interval=5,
        timeout=60,
        mode="poke",
    )

    # -------------------- DAG dependencies ---------------------

    create_medals_stats_table >> choose_medal >> branch_by_medal
    branch_by_medal >> process_gold >> delay_after_insert
    branch_by_medal >> process_silver >> delay_after_insert
    branch_by_medal >> process_bronze >> delay_after_insert
    delay_after_insert >> check_latest_medal_record_is_fresh