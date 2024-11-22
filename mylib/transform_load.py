"""
Transform and load functions for the student lifestyle dataset.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id


def load(dataset="dbfs:/FileStore/mini_project11/student_lifestyle_dataset.csv"):
    spark = SparkSession.builder.appName(
        "Student Lifestyle Data Processing"
    ).getOrCreate()

    # Load CSV into a PySpark DataFrame, inferring schema and adding headers
    student_lifestyle_df = spark.read.csv(dataset, header=True, inferSchema=True)

    # Add unique IDs to the DataFrame
    student_lifestyle_df = student_lifestyle_df.withColumn(
        "id", monotonically_increasing_id()
    )

    # Transform into a Delta Lake table and store it
    student_lifestyle_df.write.format("delta").mode("overwrite").saveAsTable(
        "student_lifestyle_delta"
    )

    num_rows = student_lifestyle_df.count()
    print(f"Number of rows in the student lifestyle dataset: {num_rows}")

    return "Finished transform and load"


if __name__ == "__main__":
    load()
