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
# Define required columns for 'rentals' for casting and feature engineering
required_rental_cols = ["total_amount", "rental_start_time", "rental_end_time", "user_id"]
if not set(required_rental_cols).issubset(rentals.columns):
    missing_cols = list(set(required_rental_cols) - set(rentals.columns))
    logger.error(f"Missing required columns in 'rentals' DataFrame: {missing_cols}")
    sys.exit(1)

rentals = rentals \
    .withColumn("total_amount", col("total_amount").cast("double")) \
    .withColumn("rental_start_time", unix_timestamp("rental_start_time", "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("rental_end_time", unix_timestamp("rental_end_time", "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("rental_duration_hours", (col("rental_end_time") - col("rental_start_time")) / 3600) \
    .withColumn("rental_date", to_date((col("rental_start_time").cast("timestamp"))))

# -----------------------------------------
# Join with Users
# -----------------------------------------
# Define required columns for 'users' for joining
required_user_cols = ["user_id", "first_name", "last_name", "email"]
if not set(required_user_cols).issubset(users.columns):
    missing_cols = list(set(required_user_cols) - set(users.columns))
    logger.error(f"Missing required columns in 'users' DataFrame: {missing_cols}")
    sys.exit(1)

# Ensure 'user_id' is present in rentals before join
if "user_id" not in rentals.columns:
    logger.error("Missing 'user_id' column in 'rentals' DataFrame, cannot perform join.")
    sys.exit(1)

rental_user_df = rentals.join(users, on="user_id", how="left") # Left join to include all rentals

# -----------------------------------------
# DAILY KPIs
# -----------------------------------------
# Validate columns for daily_kpis aggregation
required_daily_kpi_cols = ["rental_date", "total_amount"]
if not set(required_daily_kpi_cols).issubset(rental_user_df.columns):
    missing_cols = list(set(required_daily_kpi_cols) - set(rental_user_df.columns))
    logger.error(f"Missing required columns for daily KPIs aggregation: {missing_cols}")
    sys.exit(1)

daily_kpis = rental_user_df.groupBy("rental_date") \
    .agg(
        count("*").alias("daily_transaction_count"),
        spark_sum("total_amount").alias("daily_revenue")
    )

# -----------------------------------------
# USER KPIs
# -----------------------------------------
# Validate columns for user_kpis aggregation
required_user_kpi_cols = ["user_id", "first_name", "last_name", "email", "total_amount", "rental_duration_hours"]
if not set(required_user_kpi_cols).issubset(rental_user_df.columns):
    missing_cols = list(set(required_user_kpi_cols) - set(rental_user_df.columns))
    logger.error(f"Missing required columns for user KPIs aggregation: {missing_cols}")
    sys.exit(1)

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