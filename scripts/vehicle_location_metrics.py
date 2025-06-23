# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, unix_timestamp, sum as spark_sum, count, avg,
    max as spark_max, min as spark_min, round, countDistinct
)
import sys
import logging

# -----------------------------------------
# Logging Setup
# -----------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# -----------------------------------------
# Spark Session Initialization
# -----------------------------------------
spark = SparkSession.builder \
    .appName("Vehicle and Location Performance Metrics") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# -----------------------------------------
# Read Command Line Arguments
# -----------------------------------------
if len(sys.argv) != 3: # Check for correct number of arguments not more than 3
    logger.error("Usage: vehicle_location_metrics.py <input_path> <output_path>")
    sys.exit(1)

input_path = sys.argv[1]
output_path = sys.argv[2]

logger.info(f"Input path: {input_path}")
logger.info(f"Output path: {output_path}")

# -----------------------------------------
# Load Raw Datasets
# -----------------------------------------
try:
    rentals = spark.read.option("header", "true").csv(f"{input_path}/rental_transactions/")
    vehicles = spark.read.option("header", "true").csv(f"{input_path}/vehicles/")
    locations = spark.read.option("header", "true").csv(f"{input_path}/locations/")
    logger.info("Successfully loaded rental, vehicle, and location datasets.")
except Exception as e:
    logger.error(f"Error loading input data: {e}")
    sys.exit(1)

# -----------------------------------------
# Type Casting and Feature Engineering
# -----------------------------------------
rentals = rentals \
    .withColumn("total_amount", col("total_amount").cast("double")) \
    .withColumn("rental_start_time", unix_timestamp("rental_start_time", "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("rental_end_time", unix_timestamp("rental_end_time", "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("rental_duration_hours", (col("rental_end_time") - col("rental_start_time")) / 3600) \
    .withColumn("pickup_location", col("pickup_location").cast("int"))

# -----------------------------------------
# Join with Vehicles
# -----------------------------------------
vehicles = vehicles.withColumn("vehicle_id", col("vehicle_id").cast("string"))
joined_df = rentals.join(vehicles.select("vehicle_id", "vehicle_type"), on="vehicle_id", how="left")

# -----------------------------------------
# Aggregate Metrics by Pickup Location and Vehicle Type
# -----------------------------------------
vehicle_metrics = joined_df.groupBy("pickup_location", "vehicle_type") \
    .agg(
        round(spark_sum("total_amount"), 2).alias("total_revenue"), 
        count("*").alias("total_transactions"),
        round(avg("total_amount"), 2).alias("avg_transaction"),
        spark_max("total_amount").alias("max_transaction"),
        spark_min("total_amount").alias("min_transaction"),
        countDistinct("vehicle_id").alias("unique_vehicles_used"),
        round(avg("rental_duration_hours"), 2).alias("avg_rental_duration_hours"),
        round(spark_sum("rental_duration_hours"), 2).alias("total_rental_hours")
    )

# -----------------------------------------
# Join with Locations for Human-Readable Info
# -----------------------------------------
locations = locations.withColumn("location_id", col("location_id").cast("int"))
final_df = vehicle_metrics.join(locations, vehicle_metrics.pickup_location == locations.location_id, "left")

# Select Final Columns
final_df = final_df.select(
    "pickup_location", "location_name", "city", "state",
    "vehicle_type", "total_revenue", "total_transactions",
    "avg_transaction", "max_transaction", "min_transaction",
    "unique_vehicles_used", "avg_rental_duration_hours", "total_rental_hours"
)

# -----------------------------------------
# Save Output to S3 in Parquet Format
# -----------------------------------------
try:
    final_df.write.mode("overwrite").parquet(f"{output_path}/vehicle_location_metrics/")
    logger.info("Successfully wrote enriched vehicle-location metrics to S3.")
except Exception as e:
    logger.error(f"Error writing output: {e}")
    sys.exit(1)

spark.stop()

