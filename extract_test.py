# COMMAND ----------

import requests
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
import json
import base64

display(dbutils.fs.ls('dbfs:/'))

# COMMAND ----------

load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")

# COMMAND ----------

headers = {'Authorization': 'Bearer %s' % access_token}
url = "https://"+server_h+"/api/2.0"

def perform_query(path, headers, data={}):
    session = requests.Session()
    resp = session.request(
    "POST",
    url + path,
    data=json.dumps(data),
    verify=True,
    headers=headers,
    )
    return resp.json()

def mkdirs(path, headers):
    _data = {'path': path}
    return perform_query('/dbfs/mkdirs', headers=headers, data=_data)

def create(path, overwrite, headers):
    _data = {'path': path, 'overwrite': overwrite}
    return perform_query('/dbfs/create', headers=headers, data=_data)

def add_block(handle, data, headers):
    _data = {'handle': handle, 'data': data}
    return perform_query('/dbfs/add-block', headers=headers, data=_data)

def close(handle, headers):
    _data = {'handle': handle}
    return perform_query('/dbfs/close', headers=headers, data=_data)

def put_file_from_url(url, dbfs_path, overwrite, headers):
    response = requests.get(url)
    if response.status_code == 200:
        content = response.content
        handle = create(dbfs_path, overwrite, headers=headers)['handle']
        print(f"Uploading file: {dbfs_path}")
        for i in range(0, len(content), 2**20):
            add_block(handle, 
                      base64.standard_b64encode(content[i:i+2**20]).decode(), 
                      headers=headers)
        close(handle, headers=headers)
        print(f"File {dbfs_path} uploaded successfully.")
    else:
        print(f"Error downloading file from {url}. Status code: {response.status_code}")

# COMMAND ----------

FILESTORE_PATH = "dbfs:/FileStore/mini_project11"
STUDENT_LIFESTYLE_URL = "https://raw.githubusercontent.com/ailina-aniwan/Ailina_Aniwan_Mini_Project_11/refs/heads/main/data/student_lifestyle_dataset.csv"
STUDENT_LIFESTYLE_DBFS_PATH = FILESTORE_PATH + "/student_lifestyle_dataset.csv"

mkdirs(path=FILESTORE_PATH, headers=headers)
put_file_from_url(
    STUDENT_LIFESTYLE_URL,
    STUDENT_LIFESTYLE_DBFS_PATH,
    overwrite=True,
    headers=headers,
)

# COMMAND ----------

spark = SparkSession.builder.appName("Test Extract").getOrCreate()
student_lifestyle_df = spark.read.csv(
    STUDENT_LIFESTYLE_DBFS_PATH,
    header=True,
    inferSchema=True,
)

student_lifestyle_df.show()
print(f"Number of rows: {student_lifestyle_df.count()}")

# COMMAND ----------

student_lifestyle_df.write.format("delta").mode("overwrite").saveAsTable("student_lifestyle_delta")

delta_table_df = spark.read.table("student_lifestyle_delta")
delta_table_df.show()
print(f"Number of rows in Delta Table: {delta_table_df.count()}")

# COMMAND ----------

display(dbutils.fs.ls(FILESTORE_PATH))

# COMMAND ----------

query_result = spark.sql("""
    SELECT Stress_Level, AVG(GPA) AS Avg_GPA, COUNT(*) AS Count
    FROM student_lifestyle_delta
    GROUP BY Stress_Level
    ORDER BY Avg_GPA DESC
""")
query_result.show()

# COMMAND ----------

import requests

job_id = os.getenv("JOB_ID")
trigger_url = f'https://{server_h}/api/2.0/jobs/run-now'

headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json',
}

data = {'job_id': job_id}

response = requests.post(trigger_url, headers=headers, json=data)

if response.status_code == 200:
    print('Job run successfully triggered.')
else:
    print(f'Error: {response.status_code}, {response.text}')
