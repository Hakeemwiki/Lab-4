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
