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