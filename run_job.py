"""
Script to trigger a Databricks job run using REST API.
"""

import requests
import os
from dotenv import load_dotenv

load_dotenv()

access_token = os.getenv("ACCESS_TOKEN")
server_h = os.getenv("SERVER_HOSTNAME")
job_id = os.getenv("JOB_ID")

url = f"https://{server_h}/api/2.0/jobs/run-now"

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
}

data = {"job_id": job_id}

response = requests.post(url, headers=headers, json=data)

if response.status_code == 200:
    print("Job run successfully triggered.")
    run_id = response.json().get("run_id")
    print(f"Run ID: {run_id}")
else:
    print(f"Error: {response.status_code}, {response.text}")
