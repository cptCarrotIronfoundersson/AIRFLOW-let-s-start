import json

import pendulum
import requests
from typing import List
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from airflow.decorators import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2022, 12, 12, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def my_very_own_etl():
    @task()
    def extract() -> List[dict]:
        request = requests.get("https://api.thecatapi.com/v1/images/search?limit=10")
        request.raise_for_status()
        return request.json()

    @task()
    def transform(data: List[dict]) -> List[str]:
        return [i['url'] for i in data]

    @task()
    def load(data: List[str]):
        for url in data:
            r = requests.get(url)
            r.raise_for_status()
            hook = S3Hook(aws_conn_id="S3_ETL_CONN")
            extra = hook.get_connection(hook.aws_conn_id).get_extra()
            hook.load_bytes(r.content, key=url.split('/')[-1], bucket_name=json.loads(extra)['bucket_name'])

    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load(transformed_data)


my_very_own_etl()
