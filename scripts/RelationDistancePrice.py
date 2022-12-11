
import sys
sys.path.insert(0,'..')
from env_wrapper import EnvVariables
from schema import DatasetSchema

from pyspark.sql.functions import col, bround, ceil
from pyspark.sql import SparkSession
from pyspark import SparkFiles

# Build pyspark
spark = (SparkSession.builder.master("local[*]")
            .appName("RelationDistancePrice")
            .getOrCreate()) 

spark.sparkContext.addPyFile(SparkFiles.get("env_wrapper.py"))
spark.sparkContext.addPyFile(SparkFiles.get("schema.py"))

env = EnvVariables()
print("\n\n\n\n", env.getFileName(), "\n\n\n\n")

df = spark.read.option("header",True).schema(DatasetSchema().schema).csv(env.getFileName())

# Average values
result_df = df.withColumn("totalFare", col("totalFare").cast("Integer")).groupBy(bround("totalTravelDistance", -2)).avg("totalFare")

# Write to file
result_df.sort("bround(totalTravelDistance, -2)").write.option("header",True).mode("overwrite").csv(env.getOutputDir() + spark.sparkContext.appName)

# spark-submit RelationDistancePrice.py