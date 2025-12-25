from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook # Импортируем сам Hook

# --- КОНФИГУРАЦИЯ ---
# Используйте ваше рабочее имя подключения (или 'psql_test')
POSTGRES_CONN_ID = "psql_test"

def test_postgres_connection_fn(conn_id: str):
    """
    Функция, которая пытается получить Hook и выполнить простую команду.
    Если Hook не может получить соединение, задача завершится ошибкой.
    """
    print(f"Попытка установить соединение с ID: {conn_id}")

    # 1. Получение Hook'а
    hook = PostgresHook(postgres_conn_id=conn_id)

    # 2. Получение объекта соединения (реальное подключение к БД)
    # Это вызовет ошибку, если пароль или хост неверны
    conn = hook.get_conn() 
    print("Соединение успешно установлено.")

    # 3. Выполнение простейшего запроса (для гарантии, что Hook работает)
    with conn.cursor() as cursor:
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        print(f"Тестовый запрос SELECT 1 успешно выполнен. Результат: {result}")
    
    conn.close()
    print("Соединение успешно закрыто. Тест пройден.")


with DAG(
    dag_id="connection_test_dag",
    schedule=None,
    start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
    catchup=False,
    tags=["test", "infra"],
) as dag:
    
    test_connection = PythonOperator(
        task_id="test_postgres_connection",
        python_callable=test_postgres_connection_fn,
        op_kwargs={"conn_id": POSTGRES_CONN_ID},
    )