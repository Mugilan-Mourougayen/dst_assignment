import logging
from datetime import datetime
import json
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

dag = DAG(
    'opensky_dag_one',
    schedule_interval='0 1 * * *',
    start_date=datetime(2023, 1, 11),
    catchup=True
)

def read_from_url():
    url = "https://opensky-network.org/api/flights/departure?airport=LFPG&begin=1669852800&end=1669939200"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for non-200 status codes
        dataset = response.json()
        # Process the dataset as needed
        return dataset
    except requests.exceptions.RequestException as e:
        logging.error(f"Error in API request: {e}")
    except (KeyError, ValueError) as e:
        logging.error(f"Error in parsing API response: {e}")

    return {}

def write_to_json(data):
    try:
        with open('/home/mugil/airflow/task_one.json', 'w') as file:
            json.dump(data, file)
    except IOError as e:
        logging.error(f"Error in writing to JSON file: {e}")

read_task = PythonOperator(
    task_id='read_from_url',
    python_callable=read_from_url,
    dag=dag
)

write_task = PythonOperator(
    task_id='write_to_json',
    python_callable=write_to_json,
    op_args=[read_task.output],
    dag=dag
)

read_task >> write_task
