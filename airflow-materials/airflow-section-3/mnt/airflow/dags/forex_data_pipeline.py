from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.email import EmailOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookHook

from datetime import datetime, timedelta

import csv
import requests
import json

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@admin.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def download_rates():
    BASE_URL = "https://gist.githubusercontent.com/marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b/raw/"
    ENDPOINTS = {
        'USD': 'api_forex_exchange_usd.json',
        'EUR': 'api_forex_exchange_eur.json'
    }
    with open('/opt/airflow/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')
        for idx, row in enumerate(reader):
            base = row['base']
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get(f"{BASE_URL}{ENDPOINTS[base]}").json()
            outdata = {'base': base, 'rates': {}, 'last_update': indata['date']}
            for pair in with_pairs:
                outdata['rates'][pair] = indata['rates'][pair]
            with open('/opt/airflow/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')

def _get_message() -> str:
    return "Slack message"


with DAG("forex_data_pipeline", 
        start_date=datetime(2021, 11, 25), 
        schedule_interval="@daily", 
        default_args=default_args,
        catchup=False) as dag:

    is_forex_rates_available_task = HttpSensor(
        task_id="is_forex_rates_available_task",
        http_conn_id="forex_api",
        endpoint="marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b",
        response_check=lambda response: "rates" in response.text,
        poke_interval=5,
        timeout=20
    )

    is_forex_currencies_file_available_task = FileSensor(
        task_id="is_forex_currencies_file_available_task",
        fs_conn_id="forex_path",
        filepath="forex_currencies.csv",
        poke_interval=5,
        timeout=20
    )

    download_rates_task = PythonOperator(
        task_id="download_rates_task",
        python_callable=download_rates
    )

    save_rates_to_hdfs_task = BashOperator(
        task_id="save_rates_to_hdfs_task",
        bash_command="""
            hdfs dfs -mkdir -p /forex && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/forex_rates.json /forex
        """
    )

    create_hive_rates_table = HiveOperator(
        task_id="create_hive_rates_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS forex_rates(
                base STRING,
                last_update DATE,
                eur DOUBLE,
                usd DOUBLE,
                nzd DOUBLE,
                gbp DOUBLE,
                jpy DOUBLE,
                cad DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
    )

    process_rates_spark_task = SparkSubmitOperator(
        task_id="process_rates_spark_task",
        application="/opt/airflow/dags/scripts/forex_processing.py",
        conn_id="spark_conn",
        verbose=False
    )

    # send_email_task = EmailOperator(
    #     task_id="send_email_task",
    #     to="airflow@airflow.com",
    #     subject="Test",
    #     html_content="<h3>Test</h3>"
    # )

    # send_slack_task = SlackWebhookHook(
    #     task_id="send_slack_task",
    #     http_conn_id="slack_conn",
    #     message=_get_message(),
    #     channel="#test"
    # )

    is_forex_rates_available_task >> is_forex_currencies_file_available_task >> download_rates_task >> save_rates_to_hdfs_task
    save_rates_to_hdfs_task >> create_hive_rates_table >> process_rates_spark_task