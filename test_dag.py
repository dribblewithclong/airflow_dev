from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task

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

        result2 = subprocess.run(["dbt debug"], shell=True, capture_output=True, text=True)

        print(result1.stdout)
        print(result2.stdout)
    
    @task
    def main15():
        import os
        import yaml

        def create_and_append_yaml(file_path, new_content):
            # Check if the file exists
            if not os.path.exists(file_path):
                # Create an empty file if it doesn't exist
                with open(file_path, 'w') as file:
                    yaml.dump({}, file)  # Dump an empty dictionary to initialize the file
            
            # Read the existing content of the YAML file
            with open(file_path, 'r') as file:
                existing_content = yaml.safe_load(file) or {}
            
            # Update the existing content with new content
            if isinstance(existing_content, dict):
                existing_content.update(new_content)
            else:
                raise TypeError("Existing content is not a dictionary")

            # Write the updated content back to the file
            with open(file_path, 'w') as file:
                yaml.dump(existing_content, file)
        
        create_and_append_yaml(
            file_path='~/.dbt/profiles.yml',
            new_content={
                'bi_proc': {
                    'outputs': {
                        'dev': {
                            'dbname': 'y4a_datamart',
                            'host': '172.30.105.111',
                            'pass': 'VNAs!asdlkj1230sa',
                            'port': 5432,
                            'schema': 'y4a_cdm',
                            'threads': 1,
                            'type': 'postgres',
                            'user': 'y4a_dd_trungntn'
                        }
                    },
                    'target': 'dev'
                }
            }
        )

    @task.bash
    def main2():
        return """
        mkdir -p /opt/airflow/dbt_jobs/bi_proc &&
        cd /opt/airflow/dbt_jobs/bi_proc &&
        dbt debug
        """

    @task.bash
    def main3():
        return "dbt debug"
    
    main() >> main2() >> main3()
