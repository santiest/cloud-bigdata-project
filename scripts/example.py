
import sys
sys.path.insert(0,'..')
from env_wrapper import EnvVariables
from schema import DatasetSchema

from pyspark.sql.functions import col
from pyspark.sql import SparkSession

env = EnvVariables()
print("\n\n\n\n", env.getFileName(), "\n\n\n\n")

# Build pyspark
spark = (SparkSession.builder.master("local[4]")
            .appName("GetMostVisitedAirportsEXAMPLE")
            .getOrCreate())

df = spark.read.option("header",True).schema(DatasetSchema().schema).csv(env.getFileName())

# Average values
result_df = df.groupBy(col("destinationAirport")).count()

# Write to file
result_df.sort("count").write.option("header",True).csv("output")

# spark-submit example.py

