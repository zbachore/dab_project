import os, sys
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = (
    SparkSession.builder
    .appName("local-spark")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

print("Spark version:", spark.version)
spark.stop()
