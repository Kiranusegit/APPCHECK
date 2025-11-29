import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct
from pyspark.sql.types import *

# Create a SparkSession with appropriate configurations
spark = (SparkSession.builder.appName("SAMPLE_PIPELINE")
      .config("spark.jars.packages", "com.databricks:spark-csv_2.11:1.5.0")
      .getOrCreate())

# Set the SparkContext as active
sc = spark.sparkContext

# Define the schema for the data
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("age", IntegerType(), True),
    StructField("name", StringType(), True),
])

# Step 1: Start with a dataset of (id, age, name) tuples
input_data = [(1, 25, "Alice"), (2, 30, "Bob"), (3, 35, "Charlie")]
df = spark.createDataFrame(input_data, schema)

# Step 2: Read data from a CSV file and transform it into a new dataset
csv_file_path = os.path.join(os.getcwd(), "sample.csv")
csv_data = spark.read.format("csv").option("header", "true").load(csv_file_path)

# Convert the data to the correct schema
transformed_data = csv_data.selectExpr("CAST(id AS INT)", "CAST(age AS INT)", "name")

# Step 3: Write the transformed data back to a CSV file as a new dataset
output_file_path = os.path.join(os.getcwd(), "output.csv")
transformed_data.write.format("csv").option("header", "true").save(output_file_path)