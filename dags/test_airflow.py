from datetime import datetime, timedelta

from airflow import DAG
#from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
import scrapper
import credentials_db

dag_args = {
    "depends_on_past": False,
    "email": [credentials_db.email],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    dag_id="scrapper_recipes",
    description="DAG of scrapping recipes web page",
    default_args=dag_args,
    schedule=timedelta(minutes=10),
    start_date=datetime(2025,1,30),
    catchup=False,
    tags=["First_DAG"]
)

task1 = PythonOperator(
    task_id = "getLastUpdateDate",
    python_callable= scrapper.getLastUpdateDate,
    dag = dag
)

task2 =  PythonOperator(
    task_id = "getRecipesAndStorage",
    python_callable= scrapper.getRecipesAndStorage,
    dag = dag
)

task1 >> task2



