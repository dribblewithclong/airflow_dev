from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import sys, os 
sys.path.append(os.path.dirname(__file__))
import airflow_function

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
    def trigger_bi_proc():
        domain='http://172.30.15.30:8090'
        airflow_username = 'airflow_de_bi'
        airflow_password = '123456'
        a = airflow_function.Airflow(domain, airflow_username, airflow_password)
        dagId = 'dag_PBi_trigger_signal_API'
        taskId = 'B2R_-_dbt_b2r_bi_de_level_1'
        a.trigger_task(dagId, taskId)
    
    dbt_b2r_cate_view = BashOperator(
        task_id='cate_view',
        bash_command="""
        mkdir -p /opt/airflow/dbt_jobs &&
        cp -f -r /opt/airflow/dags/airflow_iykyk/dbt/bi_proc /opt/airflow/dbt_jobs/ &&
        cd /opt/airflow/dbt_jobs/bi_proc &&
        dbt run --select +main_tb_y4a_amz_avc_b2r_btr_cate
        dbt run --select main_tb_y4a_amz_avc_b2r_y4a_view
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
    
    [dbt_b2r_cate_view, dbt_b2r_promo >> dbt_b2r_car_limit] >> trigger_bi_proc()
