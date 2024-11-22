"""
Test Databricks functionality.
"""

import requests
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/mini_project11"
url = f"https://{server_h}/api/2.0"


# Function to check if a file path exists in Databricks DBFS and validate authentication
def check_filestore_path(path, headers):
    """
    Check if a given file path exists in the Databricks FileStore.

    Args:
        path (str): The file path to check.
        headers (dict): Authentication headers for the Databricks API.

    Returns:
        bool: True if the path exists, False otherwise.
    """
    try:
        response = requests.get(f"{url}/dbfs/get-status?path={path}", headers=headers)
        response.raise_for_status()
        return "path" in response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    return False


# Test if the specified FILESTORE_PATH exists
def test_databricks():
    """
    Test Databricks connectivity and the existence of the file path.
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    path_exists = check_filestore_path(FILESTORE_PATH, headers)
    assert path_exists, f"Path {FILESTORE_PATH} does not exist or is inaccessible."


if __name__ == "__main__":
    test_databricks()
