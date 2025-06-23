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