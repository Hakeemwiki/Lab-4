# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, unix_timestamp, to_date, sum as spark_sum, count,
    avg, max as spark_max, min as spark_min, round
)
import sys
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
