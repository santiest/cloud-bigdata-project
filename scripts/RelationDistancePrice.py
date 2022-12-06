
import sys
sys.path.insert(0,'..')
from env_wrapper import EnvVariables

from pyspark.sql.functions import col, bround, ceil
from pyspark.sql import SparkSession

env = EnvVariables()
print("\n\n\n\n", env.getFileName(), "\n\n\n\n")

# Build pyspark
spark = (SparkSession.builder.master("local[4]")
            .appName("RelationDistancePrice")
            .getOrCreate()) 

df = spark.read.option("header",True).csv(env.getFileName())

# Average values
result_df = df.withColumn("totalFare", col("totalFare").cast("Integer")).groupBy(bround("totalTravelDistance", -2)).avg("totalFare")

# Write to file
result_df.sort("bround(totalTravelDistance, -2)").write.option("header",True).csv("RelationDistancePrice")

# spark-submit RelationDistancePrice.py