# ===================================================================
# Databricks External Connection Setup
# ===================================================================
# This code connects to an external Databricks workspace.
# Configure these parameters via environment variables or update the defaults.
#
# Required environment variables:
#   DATABRICKS_HOST: Your Databricks workspace URL
#   DATABRICKS_TOKEN: Personal access token or service principal token
#   DATABRICKS_HTTP_PATH: SQL warehouse or cluster HTTP path
#   DATABRICKS_CATALOG: Databricks Unity Catalog (default: main)
#   DATABRICKS_SCHEMA: Database schema (default: default)
# ===================================================================

import os
from databricks import sql

# Connection Parameters (Parameterized via environment variables)
DATABRICKS_HOST = os.getenv('DATABRICKS_HOST', 'https://your-workspace.cloud.databricks.com')
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN', 'dapi1234567890abcdef')
DATABRICKS_HTTP_PATH = os.getenv('DATABRICKS_HTTP_PATH', '/sql/1.0/warehouses/your-warehouse-id')
DATABRICKS_CATALOG = os.getenv('DATABRICKS_CATALOG', 'main')
DATABRICKS_SCHEMA = os.getenv('DATABRICKS_SCHEMA', 'default')

# Establish Databricks Connection
print(f"Connecting to Databricks: {DATABRICKS_HOST}")
try:
    connection = sql.connect(
        server_hostname=DATABRICKS_HOST.replace('https://', '').replace('http://', ''),
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    )
    cursor = connection.cursor()
    cursor.execute(f"USE CATALOG {DATABRICKS_CATALOG}")
    cursor.execute(f"USE SCHEMA {DATABRICKS_SCHEMA}")
    print(f"✓ Connected successfully to {DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}")
except Exception as e:
    print(f"✗ Failed to connect to Databricks: {e}")
    raise

# ===================================================================
# Pipeline Code Starts Below
# ===================================================================


```python
# Initialize logging
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    spark
    logger.info("Using existing Databricks Spark session")
except NameError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("SMP_PIPELINE") \
        .getOrCreate()
    logger.info("Created new Spark session")

# Step 3: Read Data node (node name not specified, assuming it's a data source)
read_data_node_6 = spark.read.format("UNKNOWN").load()
logger.info("Data read from UNKNOWN into DataFrame 'read_data_node_6'")

# Step 4: Read Data node (node name not specified, assuming it's a data source)
read_data_node_8 = spark.read.format("UNKNOWN").load()
logger.info("Data read from UNKNOWN into DataFrame 'read_data_node_8'")

# Step 5: Filter node
filter_condition = "some condition"  # Replace with actual filter logic or condition
filtered_df = read_data_node_6.filter(eval(filter_condition))
logger.info("Filtered DataFrame based on condition '%s'", filter_condition)

# Step 6: Join node
join_condition = "read_data_node_8.key == filtered_df.key"  # Adjust join keys as necessary
joined_df = read_data_node_6.join(filtered_df, eval(join_condition), "inner")
logger.info("Joined DataFrames on condition '%s'", join_condition)

# Step 7: Custom SQL node
joined_df.createOrReplaceTempView("temp_view_for_sql")
custom_sql_node_5 = spark.sql("SELECT * FROM temp_view_for_sql")
logger.info("Executed custom SQL query on DataFrame registered as temp view")

# Step 8: Aggregate node
agg_column = "some_column"  # Replace with actual column name to aggregate
aggregated_df = custom_sql_node_5.groupBy(agg_column).agg(sum(col(agg_column)).alias("total_" + agg_column))
logger.info("Aggregated DataFrame by %s using SUM aggregation", agg_column)

# Step 9: Write Data node
output_format = "UNKNOWN"  # Replace with actual output format and path if necessary
aggregated_df.write.format(output_format).mode("overwrite").save()
logger.info("Data written to %s in UNKNOWN format", output_format)

# Step 10: Cleanup
logger.info("Pipeline execution completed")