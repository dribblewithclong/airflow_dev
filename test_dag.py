from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

default_args = {
    "retries": 20,
    "retry_delay": timedelta(minutes=4),
    "start_date": datetime(2023, 7, 31, 0, 0, 0),
    "owner": 'j_ai_biec',
    "email_on_retry": False,
    "email_on_failure": True,
}
dag = DAG(
    dag_id='2',
    description='test DAG',
    schedule_interval=None,
    max_active_runs=1,
    concurrency=1,
    default_args=default_args,
    catchup=False,
)

with dag:
    @task
    def main():
        import subprocess

        result1 = subprocess.run(["echo $PWD"], shell=True, capture_output=True, text=True)

        result2 = subprocess.run(["touch ~/.dbt/profiles.yml"], shell=True, capture_output=True, text=True)

        print(result1.stdout)
        print(result2.stdout)

    # @task.bash
    # def main2():
    #     return """
    #     mkdir -p /opt/airflow/dbt_jobs &&
    #     cp -f -r /opt/airflow/dags/y4a_de_repo/dbt_test/bi_proc /opt/airflow/dbt_jobs/ &&
    #     cd /opt/airflow/dbt_jobs/bi_proc &&
    #     dbt run
    #     rm -r /opt/airflow/dbt_jobs/bi_proc
    #     """
    dbt_run_task = BashOperator(
        task_id='dbt_run_task',
        bash_command="""
        mkdir -p /opt/airflow/dbt_jobs &&
        cp -f -r /opt/airflow/dags/y4a_de_repo/dbt_test/bi_proc /opt/airflow/dbt_jobs/ &&
        cd /opt/airflow/dbt_jobs/bi_proc &&
        dbt run
        rm -r /opt/airflow/dbt_jobs/bi_proc
        """,
    )

    @task.bash
    def main3():
        return "dbt debug"
    
    main() >> dbt_run_task
