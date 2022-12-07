import sys
sys.path.insert(0,'..')
from env_wrapper import EnvVariables
from schema import DatasetSchema

from pyspark.sql.functions import col, round
from pyspark.sql import SparkSession

env = EnvVariables()
print("\n\n\n\n", env.getFileName(), "\n\n\n\n")

# Build pyspark
spark = (SparkSession.builder.master("local[4]")
            .appName("DaysFromPurchaseToFlight")
            .getOrCreate())
df = spark.read.option("header",True).schema(DatasetSchema().schema).csv(env.getFileName())

# Values

df1 = df.withColumn('ElapsedTime', df.flightDate - df.searchDate)
result_df = df1.groupBy(col("ElapsedTime")).count()
# Write to file
result_df.sort(col("count").desc()).write.option("header",True).csv("DaysFromPurchaseToFlight")

# spark-submit DaysFromPurchaseToFlight.py

