# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# =====================================================
# 1️⃣ Widgets
# =====================================================

import json
from pyspark.sql.functions import current_timestamp

start_time = spark.sql("SELECT current_timestamp()").collect()[0][0]

dbutils.widgets.text(
    "table_metadata",
    "{'table_id': '1', 'table_name': 'customers', 'source_system': 'sqlserver', "
    "'source_schema': 'banking', 'source_table': 'customers', "
    "'source_path': '', 'bronze_schema': 'bronze', "
    "'silver_schema': 'silver', 'active_flag': 'True', "
    "'load_order': '1', 'created_at': '2026-02-18 13:23:37.053711'}"
)

dbutils.widgets.text(
    "table_parameters",
    "{'load_type': 'MERGE', "
    "'primary_key': 'customer_id', "
    "'watermark_column': 'updated_at'}"
)

dbutils.widgets.text(
    "run_id",
    "472519629468310"
)
run_id=dbutils.widgets.get("run_id")

# Parse JSON safely
table_metadata = json.loads(dbutils.widgets.get("table_metadata").replace("'", '"'))
table_parameters = json.loads(dbutils.widgets.get("table_parameters").replace("'", '"'))

print("Table Metadata:", table_metadata)
print("Table Parameters:", table_parameters)
print(f"Run ID: {run_id}")

# COMMAND ----------

# =====================================================
# 2️⃣ Extract Variables
# =====================================================

table_id = int(table_metadata["table_id"])
table_name = table_metadata["table_name"]
source_system = table_metadata["source_system"].lower()
source_schema = table_metadata["source_schema"]
source_table = table_metadata["source_table"]
source_path = table_metadata["source_path"]
bronze_schema = table_metadata["bronze_schema"]

load_type = table_parameters.get("load_type")
watermark_column = table_parameters.get("watermark_column")

bronze_table_fqn = f"banking.{bronze_schema}.{table_name}"

print(f"Target Bronze Table: {bronze_table_fqn}")

# COMMAND ----------

# DBTITLE 1,Metadata Entry
# =====================================================
# Make and entry to audit table
# =====================================================
entry_exists = spark.sql(f"""
    SELECT 1
    FROM banking.metadata.pipeline_runs
    WHERE run_id = {run_id} AND table_id = {table_id}
""").count() > 0

if entry_exists:
    spark.sql(f"""
        UPDATE banking.metadata.pipeline_runs
        SET
            layer = 'Silver',
            start_time = TIMESTAMP('{start_time}'),
            end_time = NULL,
            status = 'INPROGRESS',
            number_of_records = NULL,
            error_message = NULL
        WHERE run_id = {run_id} AND table_id = {table_id}
    """)
else:
    spark.sql(f"""
        INSERT INTO banking.metadata.pipeline_runs
        VALUES (
            {run_id},
            {table_id},
            'Silver',
            TIMESTAMP('{start_time}'),
            NULL,  -- end time
            'INPROGRESS',
            NULL, --number of records
            NULL -- error message
        )
    """)

# COMMAND ----------

# =====================================================
# 3️⃣ Get Last Watermark (For Filtering Only)
# =====================================================

last_watermark = None

if load_type in ["APPEND", "MERGE"] and watermark_column:
    watermark_df = spark.sql(f"""
        SELECT last_watermark_value
        FROM banking.metadata.table_watermarks
        WHERE table_id = {table_id}
    """)
    
    if watermark_df.count() > 0:
        last_watermark = watermark_df.first()["last_watermark_value"]

print("Last Watermark:", last_watermark)

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists banking.bronze

# COMMAND ----------

# =====================================================
# 4️⃣ Read Source
# =====================================================

try:

    # =====================================================
    # SQL SERVER SOURCE
    # =====================================================

    if source_system == "sqlserver":

        # 🔐 Read SQL Server connection details from Databricks Secret
        secret_json = dbutils.secrets.get(
            scope="banking-scope",
            key="sqlserver-connection-json"
        )

        # Convert JSON secret into Python dictionary
        config = json.loads(secret_json)

        # =====================================================
        # Azure SQL JDBC Connection URL
        # =====================================================

        jdbc_url = (
            f"jdbc:sqlserver://{config['host']}:{config['port']};"
            f"databaseName={config['database']};"
            f"encrypt=true;"
            f"trustServerCertificate=false;"
            f"hostNameInCertificate=*.database.windows.net;"
            f"loginTimeout=30;"
        )

        # JDBC connection properties
        jdbc_properties = {
            "user": config["user"],
            "password": config["password"],
            "driver": config["driver"]
        }

        # Print connection details for debugging
        # Password is intentionally NOT printed
        print("Connecting to Azure SQL...")
        print("Host:", config["host"])
        print("Port:", config["port"])
        print("Database:", config["database"])
        print("User:", config["user"])
        print("Driver:", config["driver"])

        # =====================================================
        # Build Source Query
        # =====================================================

        # Incremental load using watermark
        if load_type in ["APPEND", "MERGE"] and last_watermark:

            query = f"""
            (
                SELECT *
                FROM {source_schema}.{source_table}
                WHERE {watermark_column} > '{last_watermark}'
            ) AS src
            """

        # Full load
        else:

            query = f"""
            (
                SELECT *
                FROM {source_schema}.{source_table}
            ) AS src
            """

        print("Reading source table:")
        print(f"{source_schema}.{source_table}")

        # =====================================================
        # Read Azure SQL using JDBC
        # =====================================================

        source_df = spark.read.jdbc(
            url=jdbc_url,
            table=query,
            properties=jdbc_properties
        )

        # =====================================================
        # TEMPORARY JDBC CONNECTION TEST
        # =====================================================
        # This forces Spark to actually connect to Azure SQL.
        # If this fails, the problem is JDBC/network/authentication.

        print("Testing actual Azure SQL JDBC read...")

        source_df.show(5)

        print("Azure SQL JDBC read successful! ✅")


    # =====================================================
    # BLOB SOURCE
    # =====================================================

    elif source_system == "blob":

        source_df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option(
                "cloudFiles.schemaLocation",
                f"/Volumes/banking/source/volume/_schema/{table_name}"
            )
            .option("header", "true")
            .load(source_path)
        )


    # =====================================================
    # Unsupported Source
    # =====================================================

    else:

        raise ValueError(
            f"Unsupported source_system: {source_system}"
        )


    # =====================================================
    # 5️⃣ Add insert_timestamp
    # =====================================================

    source_df = source_df.withColumn(
        "insert_timestamp",
        current_timestamp()
    )


    # =====================================================
    # 6️⃣ Write to Bronze
    # =====================================================

    # -----------------------------------------------------
    # BLOB → BRONZE (Streaming)
    # -----------------------------------------------------

    if source_system == "blob":

        (
            source_df.writeStream
            .format("delta")
            .option(
                "checkpointLocation",
                f"/Volumes/banking/source/volume/_checkpoints/{table_name}"
            )
            .outputMode("append")
            .trigger(availableNow=True)
            .toTable(bronze_table_fqn)
        )

        # Streaming DataFrame cannot be counted directly
        records_read = None


    # -----------------------------------------------------
    # SQL SERVER → BRONZE (Batch)
    # -----------------------------------------------------

    else:

        (
            source_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(bronze_table_fqn)
        )

        records_read = source_df.count()


    print("Source → Bronze Load Completed Successfully. ✅")
    print("Watermark will be updated after Silver load.")


# =====================================================
# ERROR HANDLING
# =====================================================

except Exception as e:

    end_time = spark.sql(
        "SELECT current_timestamp()"
    ).collect()[0][0]

    error_message = str(e)

    print("❌ Source → Bronze Load Failed")
    print("Error:", error_message)

    # Update pipeline run status
    spark.sql(f"""
        UPDATE banking.metadata.pipeline_runs
        SET
            end_time = TIMESTAMP('{end_time}'),
            status = 'FAILED',
            error_message = {
                'NULL'
                if not error_message
                else "'" + error_message.replace("'", "") + "'"
            }
        WHERE table_id = {table_id}
          AND run_id = {run_id}
    """)

    # Re-raise original exception
    raise