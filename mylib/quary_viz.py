"""
Query and visualization file for the student lifestyle dataset.
"""

from pyspark.sql import SparkSession
import matplotlib.pyplot as plt


# Sample query
def query_transform():
    """
    Run a predefined SQL query on a Spark Delta Table.

    Returns:
        DataFrame: Result of the SQL query.
    """
    spark = SparkSession.builder.appName("Query").getOrCreate()
    query = """
        SELECT Stress_Level, AVG(GPA) AS Avg_GPA, AVG(Social_Hours_Per_Day) AS Avg_Social_Hours
        FROM student_lifestyle_delta
        GROUP BY Stress_Level
        ORDER BY Avg_GPA DESC
    """
    query_result = spark.sql(query)
    return query_result


# Visualization for the project
def viz():
    query = query_transform()
    count = query.count()
    if count > 0:
        print(f"Data validation passed. {count} rows available.")
    else:
        print("No data available. Please investigate.")

    # Convert query result to Pandas for visualization
    query_df = query.toPandas()

    # Boxplot for GPA by Stress Level
    plt.figure(figsize=(10, 6))
    query_df.boxplot(column="Avg_GPA", by="Stress_Level")
    plt.xlabel("Stress Level")
    plt.ylabel("Average GPA")
    plt.suptitle("")
    plt.title("Average GPA by Stress Level")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    plt.savefig("gpa_by_stress_level.png")
    plt.show()

    # Bar chart for average social hours by stress level
    plt.figure(figsize=(10, 6))
    plt.bar(query_df["Stress_Level"], query_df["Avg_Social_Hours"], color="blue")
    plt.xlabel("Stress Level")
    plt.ylabel("Average Social Hours")
    plt.title("Average Social Hours by Stress Level")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("social_hours_by_stress_level.png")
    plt.show()


if __name__ == "__main__":
    query_transform()
    viz()
