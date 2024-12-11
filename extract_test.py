# COMMAND ----------

import requests
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
import base64

# Mock display and dbutils for local development
try:
    display
    dbutils
except NameError:
    def display(obj):
        print(obj)
    class MockDbutils:
        def __init__(self):
            self.fs = self.MockFS()
        class MockFS:
            def ls(self, path):
                return [{"path": path}]
    dbutils = MockDbutils()

display(dbutils.fs.ls('dbfs:/'))

# COMMAND ----------

# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")

# COMMAND ----------

# Define helper functions for Databricks REST API interaction
headers = {'Authorization': f'Bearer {access_token}'}
base_url = f"https://{server_h}/api/2.0"

def perform_query(path, headers, data={}):
    """Perform a POST request to the Databricks REST API."""
    session = requests.Session()
    response = session.post(
        base_url + path,
        json=data,
        headers=headers,
        verify=True,
    )
    response.raise_for_status()  # Raise HTTPError for bad responses
    return response.json()

def mkdirs(path, headers):
    """Create directories on DBFS."""
    _data = {'path': path}
    return perform_query('/dbfs/mkdirs', headers=headers, data=_data)

def create(path, overwrite, headers):
    """Create a new file in DBFS."""
    _data = {'path': path, 'overwrite': overwrite}
    return perform_query('/dbfs/create', headers=headers, data=_data)

def add_block(handle, data, headers):
    """Add a block of data to a DBFS file."""
    _data = {'handle': handle, 'data': data}
    return perform_query('/dbfs/add-block', headers=headers, data=_data)

def close(handle, headers):
    """Close a file in DBFS."""
    _data = {'handle': handle}
    return perform_query('/dbfs/close', headers=headers, data=_data)

def put_file_from_url(url, dbfs_path, overwrite, headers):
    """Download a file from a URL and upload it to DBFS."""
    response = requests.get(url)
    response.raise_for_status()  # Raise HTTPError if the response is bad
    content = response.content
    handle = create(dbfs_path, overwrite, headers=headers)['handle']
    print(f"Uploading file: {dbfs_path}")
    for i in range(0, len(content), 2**20):
        add_block(handle, 
                  base64.standard_b64encode(content[i:i+2**20]).decode(), 
                  headers=headers)
    close(handle, headers=headers)
    print(f"File {dbfs_path} uploaded successfully.")

# COMMAND ----------

# Define file paths and dataset URLs
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

# Initialize Spark session
spark = SparkSession.builder.appName("Test Extract").getOrCreate()
student_lifestyle_df = spark.read.csv(
    STUDENT_LIFESTYLE_DBFS_PATH,
    header=True,
    inferSchema=True,
)

student_lifestyle_df.show()
print(f"Number of rows: {student_lifestyle_df.count()}")

# COMMAND ----------

# Write data to Delta table
student_lifestyle_df.write.format("delta").mode("overwrite").saveAsTable("student_lifestyle_delta")

delta_table_df = spark.read.table("student_lifestyle_delta")
delta_table_df.show()
print(f"Number of rows in Delta Table: {delta_table_df.count()}")

# COMMAND ----------

# Display file listing
display(dbutils.fs.ls(FILESTORE_PATH))

# COMMAND ----------

# Perform SQL query and show results
query_result = spark.sql("""
    SELECT Stress_Level, AVG(GPA) AS Avg_GPA, COUNT(*) AS Count
    FROM student_lifestyle_delta
    GROUP BY Stress_Level
    ORDER BY Avg_GPA DESC
""")
query_result.show()

# COMMAND ----------

# Trigger a Databricks job using REST API
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
