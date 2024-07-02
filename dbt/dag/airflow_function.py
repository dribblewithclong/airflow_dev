
from datetime import datetime, timezone
import requests


class Airflow:
    def __init__(self, domain: str, username: str, password: str) -> None:
        self.domain = domain
        self.header = {"Content-Type": "application/json"}
        self.auth = (username, password)
        self.runDate = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def trigger(self, dagId: str):
        print(f"Triggering {dagId}...")
        url = f"{self.domain}/api/v1/dags/{dagId}/dagRuns"
        payload = {"dag_run_id": f"api__{self.runDate}"}

        response = requests.post(url, headers=self.header, json=payload, auth=self.auth)
        js = response.json()
        print(js)
        return js

    def get_a_dag_run(self, dagId: str, dagRunId: str):
        url = f"{self.domain}/api/v1/dags/{dagId}/dagRuns/{dagRunId}"
        response = requests.get(url, headers=self.header, auth=self.auth)
        return response.json()

    def get_dag_list(self, only_active= True, offset = 0):
        url = f"{self.domain}/api/v1/dags"
        params = {
            'only_active': only_active, 
            'paused': False,
            'offset': offset
            }
        response = requests.get(url, headers=self.header, auth=self.auth, params= params)
        js = response.json()
        return js

    def update_variable(self, variable: str, value: str):
        path = f"/api/v1/variables/{variable}"
        url = self.domain + path
        payload = {"key": variable, "value": value}
        response = requests.patch(
            url, auth=self.auth, headers=self.header, json=payload
        )
        return response.json()

    def get_dagRuns(self, dagId, limit=1):
        url = f"{self.domain}/api/v1/dags/{dagId}/dagRuns"
        params = {"limit": limit, "order_by": "-execution_date"}
        js = requests.get(url, params=params, auth=self.auth).json()
        return js

    def get_task_instances(self, dagId, dagRunId):
        url = f"{self.domain}/api/v1/dags/{dagId}/dagRuns/{dagRunId}/taskInstances"
        js = requests.get(url, headers=self.header, auth=self.auth).json()
        return js
    
    def update_roles(self,role,actionList:list):
        url = f"{self.domain}/api/v1/roles/{role}"
        actions = []
        for e in actionList:
            action,resource = e 
            actions.append({"action":{"name":action} , "resource":{"name":resource}})
        
        payload = {
            "actions":actions,
            "name":role
        }
        response = requests.patch(url, headers= self.header, auth= self.auth, json= payload)
        return response.json()
    
    def trigger_task(self, dag_id, task_id, include_downstream=False):
        js = self.get_dagRuns(dag_id)
        last_dag_run_id = js['dag_runs'][0]['dag_run_id']

        payload = {
        "dag_run_id": last_dag_run_id,
        "dry_run": False,
        "include_parentdag": False,
        "include_past": False,
        "include_subdags": False,
        "include_upstream": False,
        "include_downstream": include_downstream,
        "only_failed": False,
        "reset_dag_runs": True,
        "task_ids": [
        task_id
        ]
        }

        url = f"{self.domain}/api/v1/dags/{dag_id}/clearTaskInstances"
        header = {"Content-Type": "application/json"}

        response = requests.post(url, headers=header, json=payload, auth=self.auth)

    def update_state_task(self, dag_id, task_id, state_update, include_downstream=False):
        """
        success | failed | skipped
        """
        js = self.get_dagRuns(dag_id)
        last_dag_run_id = js['dag_runs'][0]['dag_run_id']

        payload = {
        "dry_run": False,
        "task_id": task_id,
        "dag_run_id": last_dag_run_id,
        "include_upstream": False,
        "include_downstream": include_downstream,
        "include_future": False,
        "include_past": False,
        "new_state": state_update

        }

        url = f"{self.domain}/api/v1/dags/{dag_id}/updateTaskInstancesState"
        header = {"Content-Type": "application/json"}

        response = requests.post(url, headers=header, json=payload, auth=self.auth)


if __name__ == '__main__':
    a = Airflow('https://airflow-processing.yes4all.com/dd', 'airflow-dd', 'ABAB!d108gh2')
    js = a.update_state_task('DUMMY_SENSOR_V1', 'b2r_ready', 'failed')