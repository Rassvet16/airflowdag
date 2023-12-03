from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator


with DAG(dag_id="first", start_date=datetime(2022, 1, 1),catchup=False, schedule_interval="0 0 * * *") as dag:
    result_firs_task = BashOperator(task_id='firs_task', bash_command=f"python3 /opt/airflow/dags/test.py")
    # Задачи представлены в виде операторов
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    @task()
    def airflow_task():
        print("airflow")

    # Еще одна задача BashOperator
    bash_task = BashOperator(task_id="bash_task", bash_command="echo executing bash task")
    @task()
    def bb():
        print("bb")
    # Задаем зависимости между задачами для последовательного выполнения
    result_firs_task >> hello >> airflow_task() >> bash_task >> bb()