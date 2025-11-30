# Import the necessary libraries
from pyspark.sql import SparkSession, functions, types

# Set up a SparkSession with appropriate configurations
spark = SparkSession \
    .builder \
    .appName("SMP_PIPELINE") \
    .getOrCreate()

# Define a case insensitive not equal to condition for city
cond = F.col("city").isNot(F.lit("NEW YORK")).alias("not_new_york")

# Filter the rows where city is not equal to "New York"
filtered_df = df.filter(cond)

# Join the filtered dataframe with another dataframe based on a common column
filtered_df = spark.createDataFrame([('1', 'Alice', 'NYC'), ('2', 'Bob', 'Paris'), 
                                       ('3', 'Charlie', 'LA')], ['id', 'name', 'city'])
joined_df = filtered_df.join(filtered_df, on=['city'])

# Aggregate the data based on a specific column
agg_df = joined_df.groupBy('city').agg({"name": "count"})

# Write the aggregated data to a file in parquet format
agg_df.write.parquet("aggregated_data", mode="overwrite")