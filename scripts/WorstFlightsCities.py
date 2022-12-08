
import sys
sys.path.insert(0,'..')
from env_wrapper import EnvVariables

from pyspark.sql.functions import col, bround, ceil
from pyspark.sql import SparkSession

env = EnvVariables()
print("\n\n\n\n", env.getFileName(), "\n\n\n\n")

# Build pyspark
spark = (SparkSession.builder.master("local[4]")
            .appName("WorstFlightsCities")
            .getOrCreate()) 

df = spark.read.option("header",True).csv(env.getFileName())

# Average values
result_df = df.withColumn("totalFare", col("totalFare").cast("float")).groupBy("startingAirport","destinationAirport").max("totalFare")

# Write to file
result_df.sort("startingAirport").write.option("header",True).csv("WorstFlightsCities")

# spark-submit WorstFlightsCities.py