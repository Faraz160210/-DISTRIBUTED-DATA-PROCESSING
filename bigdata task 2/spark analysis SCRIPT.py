from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

def analyze_dataset(file_path, output_path="output/results.parquet", app_name="DatasetAnalysis"):
    """
    Analyzes a large dataset using Apache Spark, performing filtering, grouping, and aggregations.

    Args:
        file_path (str): The path to the dataset file.
        output_path (str): The path to save the analysis results.
        app_name (str): The name of the Spark application.
    """
    try:
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        df = spark.read.csv(file_path, header=True, inferSchema=True)

        print("Schema:")
        df.printSchema()
        print("Total Rows:", df.count())

        filtered_df = df.filter(df["column_name"] > 100)
        print("Rows after filtering:", filtered_df.count())

        grouped_df = df.groupBy("category_column").agg(
            F.count("*").alias("count"),
            F.avg("numeric_column").alias("average"),
            F.sum("another_numeric_column").alias("total")
        )

        print("Grouped and aggregated data:")
        grouped_df.show()

        modified_df = df.withColumn("new_column", df["numeric_column"] * 2)
        print("Modified Data with new column:")
        modified_df.show(5)

        ordered_df = grouped_df.orderBy(F.desc("count"))
        print("Ordered data:")
        ordered_df.show()

        ordered_df.write.parquet(output_path)
        print(f"Analysis results saved to: {output_path}")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python src/analyze_data.py <path_to_dataset>")
        sys.exit(1)
    file_path = sys.argv[1]
    analyze_dataset(file_path)
