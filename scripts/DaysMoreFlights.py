
import sys
sys.path.insert(0,'..')
from env_wrapper import EnvVariables

from pyspark.sql.functions import col
from pyspark.sql import SparkSession

env = EnvVariables()
print("\n\n\n\n", env.getFileName(), "\n\n\n\n")

# Build pyspark
spark = (SparkSession.builder.master("local[4]")
            .appName("GetDaysMoreFlights")
            .getOrCreate())

df = spark.read.option("header",True).csv(env.getFileName())

# Average values
result_df = df.groupBy(col("flightDate")).count()

# Write to file
result_df.sort("flightDate").write.option("header",True).csv("GetDaysMoreFlights")

# spark-submit DaysMoreFlights.py
