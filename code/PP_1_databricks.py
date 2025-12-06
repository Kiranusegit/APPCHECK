```python
import os
from databricks import sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, sum, avg, count, max, min
import logging
from pyspark.sql import Row

# Initialize logger
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

try:
    spark
    logger.info("Using existing Databricks Spark session")
except NameError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("PP_1") \
        .getOrCreate()
    logger.info("Created new Spark session")

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

# Step 3: Read Customer Data
try:
    customer_data = spark.read.format("UNKNOWN").load("path/to/customer_data")
    customer_data.createOrReplaceTempView("customer_view")
    read_customer_data_1 = spark.sql("SELECT * FROM customer_view")
    logger.info("Customer data successfully read and registered as view")
except Exception as e:
    logger.error(f"Failed to read customer data: {e}")

# Step 4: Read Transaction Data
try:
    transaction_data = spark.read.format("UNKNOWN").load("path/to/transaction_data")
    transaction_data.createOrReplaceTempView("transaction_view")
    read_transaction_data_2 = spark.sql("SELECT * FROM transaction_view")
    logger.info("Transaction data successfully read and registered as view")
except Exception as e:
    logger.error(f"Failed to read transaction data: {e}")

# Step 5: Join Data
try:
    join_condition = "CustomerID = TransactionID"
    joined_data = read_customer_data_1.join(read_transaction_data_2, join_condition, "inner")
    joined_data.createOrReplaceTempView("joined_view")
    join_data_3 = spark.sql("SELECT * FROM joined_view")
    logger.info("Data successfully joined")
except Exception as e:
    logger.error(f"Failed to join data: {e}")

# Step 6: Filter Transactions
try:
    filtered_transactions = joined_data.filter(col("Amount") > 0)
    filtered_transactions.createOrReplaceTempView("filtered_view")
    filter_transactions_4 = spark.sql("SELECT * FROM filtered_view")
    logger.info("Transactions successfully filtered")
except Exception as e:
    logger.error(f"Failed to filter transactions: {e}")

# Step 7: Write to Snowflake
try:
    write_to_snowflake_5 = filter_transactions_4.write \
        .format("snowflake") \
        .options(**{
            "sfUrl": os.getenv("SNOWFLAKE_URL"),
            "sfUser": os.getenv("SNOWFLAKE_USER"),
            "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
            "dbName": "your_database",
            "schemaName": "your_schema",
            "tableName": "Cust_Trans"
        }) \
        .mode("overwrite") \
        .save()
    logger.info("Data successfully written to Snowflake")
except Exception as e:
    logger.error(f"Failed to write data to Snowflake: {e}")

# Step 8: Cleanup
logger.info("Pipeline execution completed")