from datetime import datetime, timedelta
import random
import time

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule

# ВАЖНО: проверь, что в Airflow есть соединение с таким conn_id
MYSQL_CONN_ID = "mysql_default"

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
    tags=["olympic", "homework", "medals"],
) as dag:

    # 1. Создаём таблицу для статистики медалей
    create_table = MySqlOperator(
        task_id="create_medals_stats_table",
        mysql_conn_id=MYSQL_CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS olympic_dataset.medal_stats (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    # 2. Випадковий вибір ['Bronze', 'Silver', 'Gold']
    def choose_medal_type(**context):
        medal = random.choice(["Bronze", "Silver", "Gold"])
        # кладём в XCom
        return medal

    choose_medal = PythonOperator(
        task_id="choose_medal",
        python_callable=choose_medal_type,
    )

    # 3. Branch — в зависимости от выбранной медали
    def branch_by_medal(**context):
        medal = context["ti"].xcom_pull(task_ids="choose_medal")
        if medal == "Bronze":
            return "process_bronze"
        elif medal == "Silver":
            return "process_silver"
        else:
            return "process_gold"

    branch_task = BranchPythonOperator(
        task_id="branch_by_medal",
        python_callable=branch_by_medal,
        provide_context=True,
    )

    # 4. Три задачи — считаем количество и пишем в таблицу

    def insert_medal_count(medal_type: str):
        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        count_sql = """
            SELECT COUNT(*) AS cnt
            FROM olympic_dataset.athlete_event_results
            WHERE medal = %s;
        """
        records = hook.get_first(count_sql, parameters=(medal_type,))
        cnt = records[0] if records else 0

        insert_sql = """
            INSERT INTO olympic_dataset.medal_stats (medal_type, count, created_at)
            VALUES (%s, %s, NOW());
        """
        hook.run(insert_sql, parameters=(medal_type, cnt))

    def make_medal_task(task_id: str, medal_type: str):
        return PythonOperator(
            task_id=task_id,
            python_callable=insert_medal_count,
            op_kwargs={"medal_type": medal_type},
        )

    process_bronze = make_medal_task("process_bronze", "Bronze")
    process_silver = make_medal_task("process_silver", "Silver")
    process_gold = make_medal_task("process_gold", "Gold")

    # 5. Задержка
    # Вариант 1: сделай тут 20 сек, чтобы сенсор прошёл
    # Вариант 2: временно меняешь на 35 сек и показываешь, что сенсор падает
    def delay_task_fn(delay_seconds: int = 20):
        time.sleep(delay_seconds)

    delay_task = PythonOperator(
        task_id="delay_after_insert",
        python_callable=delay_task_fn,
        op_kwargs={"delay_seconds": 20},  # меняй на 35 для failed-скрина
        trigger_rule=TriggerRule.ONE_SUCCESS,  # достаточно успеха одного из трёх
    )

    # 6. Сенсор: самый новый запис не старше 30 сек
    medal_sensor = SqlSensor(
        task_id="check_latest_medal_record_is_fresh",
        conn_id=MYSQL_CONN_ID,
        sql="""
        SELECT
            CASE
                WHEN TIMESTAMPDIFF(
                    SECOND,
                    MAX(created_at),
                    NOW()
                ) <= 30
                THEN 1
                ELSE 0
            END AS is_fresh
        FROM olympic_dataset.medal_stats;
        """,
        mode="poke",           # можно оставить по умолчанию
        poke_interval=10,      # каждые 10 сек проверка
        timeout=60,            # общее время ожидания
    )

    # Зависимости
    create_table >> choose_medal >> branch_task
    branch_task >> [process_bronze, process_silver, process_gold] >> delay_task
    delay_task >> medal_sensor