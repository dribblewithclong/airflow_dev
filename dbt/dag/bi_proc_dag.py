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
    dag_id='test_dbt_bi',
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

    dbt_b2r_cate_view = BashOperator(
        task_id='cate_view',
        bash_command="""
        mkdir -p /opt/airflow/dbt_jobs &&
        cp -f -r /opt/airflow/dags/airflow_iykyk/dbt/bi_proc /opt/airflow/dbt_jobs/ &&
        cd /opt/airflow/dbt_jobs/bi_proc &&
        dbt run --select +main_tb_y4a_amz_avc_b2r_btr_cate
        dbt run --select +main_tb_y4a_amz_avc_b2r_y4a_view
        rm -r /opt/airflow/dbt_jobs/bi_proc
        """,
    )

    dbt_b2r_promo = BashOperator(
        task_id='promo',
        bash_command="""
        mkdir -p /opt/airflow/dbt_jobs &&
        cp -f -r /opt/airflow/dags/airflow_iykyk/dbt/bi_proc /opt/airflow/dbt_jobs/ &&
        cd /opt/airflow/dbt_jobs/bi_proc &&
        dbt run --select +main_tb_b2r_promo
        rm -r /opt/airflow/dbt_jobs/bi_proc
        """,
    )

    dbt_b2r_car_limit = BashOperator(
        task_id='car_limit',
        bash_command="""
        mkdir -p /opt/airflow/dbt_jobs &&
        cp -f -r /opt/airflow/dags/airflow_iykyk/dbt/bi_proc /opt/airflow/dbt_jobs/ &&
        cd /opt/airflow/dbt_jobs/bi_proc &&
        dbt run --select +main_test_car_limit
        rm -r /opt/airflow/dbt_jobs/bi_proc
        """,
    )
    
    main() >> dbt_b2r_cate_view
    main() >> dbt_b2r_promo >> dbt_b2r_car_limit
