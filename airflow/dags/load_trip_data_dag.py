from __future__ import annotations

import pendulum
import requests
import pandas as pd
import io
import tempfile
from airflow.sdk import DAG, task
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


POSTGRES_CONN_ID = "stage_db"
POSTGRES_TARGET_TABLE = "stage.f_yellow_trips"
URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
YELLOW_TRIPDATA = "yellow_tripdata_"


with DAG(
    dag_id="nyc_tlc_load_trip_data",
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=True,
    max_active_runs=2,
    tags=["nyc_tlc"],
) as dag:
    
    @task
    def get_link_task(**kwargs):
        month = kwargs["ds"][:-3]
        data_link = URL + YELLOW_TRIPDATA + month + ".parquet"
        print(data_link)
        return data_link

    @task.sensor(poke_interval=10, timeout=30, soft_fail=True)
    def check_link_task(data_link):
        try:
            r = requests.head(data_link, timeout=5)
            return r.status_code == 200
        except Exception:
            return False

    # check_link_task = HttpSensor(
    #     task_id="check_data_link",
    #     http_conn_id="http_empty",
    #     endpoint=link,
    #     method="HEAD",
    #     response_check=lambda response: response.status_code == 200,
    #     poke_interval=10,
    #     timeout=30,
    # )

    @task(
        task_id="load_data",
        retries=3,
        retry_delay=pendulum.duration(minutes=1),
        execution_timeout=pendulum.duration(minutes=10),
        pool="heavy_tasks_pool",
    )
    def process_data_task(data_link):
        response = requests.get(data_link)
        parquet_buffer = io.BytesIO(response.content)
        df = pd.read_parquet(parquet_buffer)

        print("Data frame loaded")

        for col in df.select_dtypes("float64").columns:
            df[col] = pd.to_numeric(df[col], downcast="float")
        df["passenger_count"] = df["passenger_count"].astype("Int8")
        df["RatecodeID"] = df["RatecodeID"].astype("Int8")
        df["payment_type"] = df["payment_type"].astype("Int8")

        print("Data frame converted")

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.csv') as f:
            df.to_csv(f, sep='\t', header=False, index=False, na_rep='')
            f.flush()
            cols = [col for col in df.columns]
            cols_str = ", ".join(cols)
            pg_hook.copy_expert(
                sql=f"COPY {POSTGRES_TARGET_TABLE} ({cols_str}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t')",
                filename=f.name
            )


    link = get_link_task()
    check_link_task(link) >> process_data_task(link)