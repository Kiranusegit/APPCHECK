```python
# Import necessary libraries
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType

# Create a SparkSession with appropriate configurations
spark = SparkSession \
    .builder \
    .appName("PIPELIN2") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3") \
    .getOrCreate()

# Implement all pipeline steps in the correct order
# Step 1: Read Data
read_data_0 = spark.read.format("jdbc") \
    .option("url", "TEST") \
    .option("dbtable", "TEST") \
    .option("user", "TEST") \
    .option("password", "TEST") \
    .load()

# Step 2: Filter
filter_1 = read_data_0.where(F.col("condition") == "filter condition")

# Step 3: Filter
filter_2 = filter_1.where(F.col("condition10") == True)

# Step 4: Aggregation
aggregation_3 = filter_2.groupBy("col1").agg({"agg_expr": "count(col1)"})

# Step 5: Join
join_5 = spark.read.format("jdbc") \
    .option("url", "TEST") \
    .option("dbtable", "TEST") \
    .option("user", "TEST") \
    .option("password", "TEST") \
    .load()\
    .alias("r").join(aggregation_3.alias("a"), on=["Join Condition111sadsasdasd"])

# Step 6: Filter
filter_7 = join_5.where(F.col("condition100") == True)

# Step 7: Filter
filter_8 = filter_7.where(F.col("condition100") == "filter condition100")

# Step 8: Write Data
write_data_6 = filter_8.write \
    .format("json") \
    .option("header", True) \
    .mode("overwrite") \
    .option("path", "path target") \
    .save()

# Include proper error handling and logging
log = spark.sparkContext._jvm.java.util.logging.Logger

try:
    # Implement the pipeline steps here
    pass
except Exception as e:
    log.error(e)

# Use best practices for PySpark optimization
# Optimize the code by reducing shuffle and sort operations
# Use broadcast joins when appropriate
# Avoid using too many SparkContexts