from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from bs4 import BeautifulSoup


HTTP_CONN_ID = "nyc_tlc_api"
URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
ENDPOINT = "/site/tlc/about/tlc-trip-record-data.page"


def scrape_data_links():
    http_hook = HttpHook(method='GET', http_conn_id=HTTP_CONN_ID)
    response = http_hook.run(
        endpoint=ENDPOINT,
        extra_options={
            'headers': {
                'User-Agent': "Chrome/100.0.4896.88", 
                'Accept-Encoding': "gzip, deflate"                   
            }
        }
    )
    html_content = response.text
    soup = BeautifulSoup(html_content, 'html.parser')

    data_links = []
    for link in soup.find_all('a', href=True):
        href = link['href']
        if 'yellow_tripdata' in href and href.endswith('.parquet'):
            data_links.append(href)

    print(f"Найдено {len(data_links)} ссылок на файлы parquet")
    for link in data_links:
        print(f"Найдена ссылка: {link}")


with DAG(
    dag_id="nyc_tlc_data_discovery",
    schedule=None,
    start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
    catchup=False,
    tags=["nyc_tlc"],
) as dag:
    
    discover_files = PythonOperator(
        task_id="discover_data_links",
        python_callable=scrape_data_links,
        retries=3,
        retry_delay=pendulum.duration(minutes=1),
        execution_timeout=pendulum.duration(minutes=1),
    )