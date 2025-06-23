# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, unix_timestamp, to_date, sum as spark_sum, count,
    avg, max as spark_max, min as spark_min, round
)
import sys # system library for command line arguments
import logging 

# -----------------------------------------
# Logging Setup
# -----------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s" # Format for log messages
)
logger = logging.getLogger(__name__) # Set up a logger for this module

# -----------------------------------------
# Spark Session Initialization
# -----------------------------------------
spark = SparkSession.builder \
    .appName("User and Transaction Analysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN") # Reduce log verbosity


# -----------------------------------------
# Read Command Line Arguments
# -----------------------------------------
if len(sys.argv) != 3: # Check for correct number of arguments
    logger.error("Usage: user_transaction_metrics.py <input_path> <output_path>")
    sys.exit(1) # Exit if arguments are not provided correctly

input_path = sys.argv[1] # Path to input data
output_path = sys.argv[2] # Path to save output data

logger.info(f"Input path: {input_path}")
logger.info(f"Output path: {output_path}")

# -----------------------------------------
# Load Raw Datasets
# -----------------------------------------
try:
    rentals = spark.read.option("header", "true").csv(f"{input_path}/rental_transactions/") 
    users = spark.read.option("header", "true").csv(f"{input_path}/users/")
    logger.info("Successfully loaded users and transactions datasets.")
except Exception as e:
    logger.error(f"Error loading data: {e}")
    sys.exit(1) 

# -----------------------------------------
# Data Type Casting and Enrichment
# -----------------------------------------
rentals = rentals \
    .withColumn("total_amount", col("total_amount").cast("double")) \
    .withColumn("rental_start_time", unix_timestamp("rental_start_time", "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("rental_end_time", unix_timestamp("rental_end_time", "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("rental_duration_hours", (col("rental_end_time") - col("rental_start_time")) / 3600) \
    .withColumn("rental_date", to_date((col("rental_start_time").cast("timestamp"))))

# -----------------------------------------
# Join with Users
# -----------------------------------------
rental_user_df = rentals.join(users, on="user_id", how="left") # Left join to include all rentals


# -----------------------------------------
# DAILY KPIs
# -----------------------------------------
daily_kpis = rental_user_df.groupBy("rental_date") \
    .agg(
        count("*").alias("daily_transaction_count"),
        spark_sum("total_amount").alias("daily_revenue")
    )

# -----------------------------------------
# USER KPIs
# -----------------------------------------
user_kpis = rental_user_df.groupBy("user_id", "first_name", "last_name", "email") \
    .agg(
        count("*").alias("total_transactions"),
        round(spark_sum("total_amount"), 2).alias("total_spent"),
        round(avg("total_amount"), 2).alias("avg_transaction_value"),
        spark_max("total_amount").alias("max_transaction"),
        spark_min("total_amount").alias("min_transaction"),
        round(spark_sum("rental_duration_hours"), 2).alias("total_rental_hours")
    )

# -----------------------------------------
# Save to S3 as Parquet
# -----------------------------------------
try:
    daily_kpis.write.mode("overwrite").parquet(f"{output_path}/daily_metrics/") # Save daily KPIs
    user_kpis.write.mode("overwrite").parquet(f"{output_path}/user_metrics/") # Save user KPIs
    logger.info("Successfully wrote user and daily metrics to S3.")
except Exception as e:
    logger.error(f"Failed to write output: {e}")
    sys.exit(1) # Exit if writing fails

spark.stop() # Stop the Spark session