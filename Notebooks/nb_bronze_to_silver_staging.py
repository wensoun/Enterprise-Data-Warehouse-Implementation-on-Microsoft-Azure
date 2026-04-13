# Databricks notebook source
# MAGIC %md
# # Notebook: Bronze to Silver Staging
# ## Purpose: Read CDC Parquet files from Bronze layer and write to staging tables

# COMMAND ----------

# MAGIC %md
# ### 1. Configuration and Imports

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime

# Configuration
storage_account = "adlsedwprod01"
bronze_container = "bronze"
synapse_server = "synapse-edw-prod-01.sql.azuresynapse.net"
synapse_database = "sqldwedw"

print(f"Starting Bronze to Silver ETL process...")
print(f"Processing time: {datetime.datetime.now()}")

# COMMAND ----------

# MAGIC %md
# ### 2. Table Configuration

# COMMAND ----------

tables_config = [
    {
        "table_name": "Product",
        "bronze_path": f"abfss://{bronze_container}@{storage_account}.dfs.core.windows.net/Product/",
        "staging_table": "stg_Product_CDC",
        "is_dimension": True,
        "track_columns": ["Name", "ListPrice", "StandardCost", "Color"]
    },
    {
        "table_name": "Customer",
        "bronze_path": f"abfss://{bronze_container}@{storage_account}.dfs.core.windows.net/Customer/",
        "staging_table": "stg_Customer_CDC",
        "is_dimension": True,
        "track_columns": ["CustomerType", "TerritoryID"]
    },
    {
        "table_name": "SalesOrderDetail",
        "bronze_path": f"abfss://{bronze_container}@{storage_account}.dfs.core.windows.net/SalesOrderDetail/",
        "staging_table": "stg_SalesOrderDetail_CDC",
        "is_dimension": False,
        "track_columns": []
    }
]

# COMMAND ----------

# MAGIC %md
# ### 3. Function to Read Latest CDC Files

# COMMAND ----------

def read_latest_cdc_files(bronze_path, table_name):
    """
    Reads the latest CDC Parquet files from Bronze layer
    """
    from datetime import datetime
    
    today = datetime.now()
    date_path = f"{bronze_path}{today.year}/{today.month:02d}/{today.day:02d}/"
    
    print(f"Reading CDC data for {table_name} from: {date_path}")
    
    try:
        df = spark.read.parquet(date_path + "*.parquet")
        df = df.withColumn("_processing_date", lit(datetime.now()))
        df = df.withColumn("_source_file", input_file_name())
        
        record_count = df.count()
        print(f"Read {record_count} records for {table_name}")
        
        return df
    except Exception as e:
        print(f"No new files found for {table_name}: {e}")
        return None

# COMMAND ----------

# MAGIC %md
# ### 4. Function to Process CDC to Staging

# COMMAND ----------

def process_cdc_to_staging(df, table_config):
    """
    Processes CDC data and writes to staging table in Synapse
    """
    if df is None or df.count() == 0:
        print(f"No data to process for {table_config['table_name']}")
        return
    
    # Add operation type description
    df = df.withColumn(
        "operation_type",
        when(col("__$operation") == 1, "DELETE")
        .when(col("__$operation") == 2, "INSERT")
        .when(col("__$operation") == 3, "UPDATE_BEFORE")
        .when(col("__$operation") == 4, "UPDATE_AFTER")
        .otherwise("UNKNOWN")
    )
    
    # Filter relevant operations
    if table_config["is_dimension"]:
        df_processed = df.filter(col("__$operation").isin([2, 4]))
        print(f"Dimension: Processing {df_processed.count()} changes")
    else:
        df_processed = df.filter(col("__$operation").isin([2]))
        print(f"Fact: Processing {df_processed.count()} new records")
    
    # Add hash for change detection
    if table_config["is_dimension"] and table_config["track_columns"]:
        hash_columns = [col(c).cast("string") for c in table_config["track_columns"]]
        df_processed = df_processed.withColumn(
            "_row_hash",
            sha2(concat_ws("|", *hash_columns), 256)
        )
    
    # Write to staging
    staging_table = table_config["staging_table"]
    
    jdbc_url = f"jdbc:sqlserver://{synapse_server}:1433;database={synapse_database};encrypt=true;trustServerCertificate=false"
    
    write_properties = {
        "user": "sqladminuser",
        "password": dbutils.secrets.get(scope="azure", key="synapse-password"),
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    
    df_processed.write \
        .mode("overwrite") \
        .jdbc(url=jdbc_url, table=staging_table, properties=write_properties)
    
    print(f"Successfully wrote to staging table: {staging_table}")
    display(df_processed.limit(5))

# COMMAND ----------

# MAGIC %md
# ### 5. Main Execution

# COMMAND ----------

print("=" * 60)
print("STARTING BRONZE TO SILVER STAGING PROCESS")
print("=" * 60)

for table_config in tables_config:
    print(f"\n--- Processing: {table_config['table_name']} ---")
    
    df_cdc = read_latest_cdc_files(
        table_config["bronze_path"], 
        table_config["table_name"]
    )
    
    process_cdc_to_staging(df_cdc, table_config)
    
    print(f"Completed: {table_config['table_name']}")

print("\n" + "=" * 60)
print("BRONZE TO SILVER STAGING COMPLETE")
print("=" * 60)