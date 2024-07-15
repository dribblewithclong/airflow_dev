import requests

domain='https://airflow-processing.yes4all.com/dd'
auth = ('airflow-dd', 'ABAB!d108gh2')
dag_run_id = 'manual__2024-07-01T04:49:03.435876+00:00'
dagId = 'DUMMY_1'
payload = {
"dry_run": False,
"task_id": "crawl_first",

"dag_run_id": dag_run_id,
"include_upstream": False,
"include_downstream": False,
"include_future": False,
"include_past": False,
"new_state": "failed"

}

url = f"{domain}/api/v1/dags/{dagId}/updateTaskInstancesState"
header = {"Content-Type": "application/json"}

response = requests.post(url, headers=header, json=payload, auth=auth)
print(response)
print(response.text)

