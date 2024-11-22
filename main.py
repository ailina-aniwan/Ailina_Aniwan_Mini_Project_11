"""
Main CLI or app entry point for the student lifestyle data pipeline.
"""

from mylib.extract import extract
from mylib.transform_load import load
from mylib.query_viz import query_transform, viz
import os


if __name__ == "__main__":
    current_directory = os.getcwd()
    print(f"Current working directory: {current_directory}")

    extract()
    print("Data extraction completed.")

    load()
    print("Data load and transformation completed.")

    query_transform()
    print("Data querying completed.")

    viz()
    print("Data visualization completed.")
