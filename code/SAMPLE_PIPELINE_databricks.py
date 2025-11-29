import os
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import *
from pyspark.sql.functions import col
import json
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" --conf spark.hadoop.fs.s3a.access.key=XXXXX  --conf spark.hadoop.fs.s3a.secret.key=XXXXXXX spark-shell'
spark = SparkSession \
    .builder\
    .appName("SAMPLE_PIPELINE")\
    .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.3')\
    .getOrCreate()

# Step 1 - Start node from the pipeline input
start_df = spark.read \
              .format("com.databricks.spark.csv") \
              .option("header", "true") \
              .option("inferschema", "true") \
              .load("s3://bucketname/file_path")

# Step 2 - Read data from S3 bucket and transform it into desired format
read_df = spark.read \
               .format("com.databricks.spark.csv") \
               .option("header", "true") \
               .option("inferschema", "true") \
               .load("s3://bucketname/file_path")
read_df = read_df.withColumn(col("DATE"), functions.to_date(col("DATE")))
read_df = read_df.withColumn(col("TIME"), functions.to_timestamp(col("TIME")))

# Step 3 - Transform data using PySpark DataFrame operations
transformed_df = read_df\
                 .select(col("*")) \
                 .filter(read_df['ID'] != "") \
                 .groupBy('ID') \
                 .agg(functions.count('*').alias('COUNT'))

# Step 4 - Write data to S3 bucket in CSV format
write_df = transformed_df\
            .coalesce(1)\
            .write\
            .format("com.databricks.spark.csv")\
            .option("header", "true")\
            .option("inferschema", "true")\
            .mode('overwrite') \
            .save("s3://bucketname/file_path")

#Step 5 - Transform data using PySpark DataFrame operations
transformed_df2 = read_df\
                   .select(col("*")) \
                   .filter(read_df['ID'] != "") \
                   .groupBy('ID') \
                   .agg(functions.count('*').alias('COUNT')) \
                   .withColumn(col("DATE"), functions.to_date(col("DATE"))) \
                   .withColumn(col("TIME"), functions.to_timestamp(col("TIME")))

#Step 6 - Write data to S3 bucket in JSON format
write_df = transformed_df2\
            .coalesce(1)\
            .write\
            .format("com.databricks.spark.csv")\
            .option("header", "true")\
            .option("inferschema", "true")\
            .mode('overwrite') \
            .save("s3://bucketname/file_path")

# Step 7 - Add any final transformations and data processing steps
final_df = transformed_df2.withColumn(col("DATE"), functions.to_date(col("DATE"))) \
                          .withColumn(col("TIME"), functions.to_timestamp(col("TIME")))

# Step 8 - Write final output to S3 bucket in JSON format
final_df.coalesce(1)\
        .write\
        .format("com.databricks.spark.csv")\
        .option("header", "true")\
        .option("inferschema", "true")\
        .mode('overwrite') \
        .save("s3://bucketname/file_path")\
				.show()