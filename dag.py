from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from datetime import datetime

def trigger_metaflow_run():
    url = "http://metaflow-service.poc-dap-six-index-1.svc.cluster.local:8080/api/flows/LsegPipeline/runs"
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, headers=headers, json={"run_id": "airflow-trigger"})
    print("Status:", response.status_code)
    print("Response:", response.text)

with DAG(
    dag_id='trigger_metaflow',
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False,
    tags=['metaflow']
) as dag:
    PythonOperator(task_id='trigger', python_callable=trigger_metaflow_run)