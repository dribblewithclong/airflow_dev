from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

import sys, os 
sys.path.append(os.path.dirname(__file__))

default_args = {
    "retries": 20,
    "retry_delay": timedelta(minutes=4),
    "start_date": datetime(2023, 7, 31, 0, 0, 0),
    "owner": 'trungntn',
    "email_on_retry": False,
    "email_on_failure": True,
}
dag = DAG(
    dag_id='yams_dbt',
    description='test DAG',
    schedule_interval="0 6 * * *",
    max_active_runs=1,
    concurrency=1,
    default_args=default_args,
    catchup=False,
)

with dag:    
    yams_task = BashOperator(
        task_id='yams_task',
        bash_command="""
        mkdir -p /opt/airflow/dbt_jobs &&
        cp -f -r /opt/airflow/dags/airflow_iykyk/dbt/bi_proc /opt/airflow/dbt_jobs/ &&
        cd /opt/airflow/dbt_jobs/bi_proc &&
        dbt run --select tb_y4a_amz_ads_target_keyword_yams
        rm -r /opt/airflow/dbt_jobs/bi_proc
        """,
    )

    aid = BashOperator(
        task_id='aid',
        bash_command="""
        mkdir -p /opt/airflow/dbt_jobs &&
        cp -f -r /opt/airflow/dags/airflow_iykyk/dbt/bi_proc /opt/airflow/dbt_jobs/ &&
        cd /opt/airflow/dbt_jobs/bi_proc &&
        dbt run --select tb_y4a_amz_ads_adid_yams
        rm -r /opt/airflow/dbt_jobs/bi_proc
        """,
    )

    yams_task >> aid
