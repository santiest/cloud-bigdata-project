
import sys
sys.path.insert(0,'..')
from env_wrapper import EnvVariables
from schema import DatasetSchema

from pyspark.sql.functions import col, min
from pyspark.sql import SparkSession
from pyspark import SparkFiles

# Build pyspark
spark = (SparkSession.builder.master("local[*]")
            .appName("BestFlightsCities")
            .getOrCreate()) 

spark.sparkContext.addPyFile(SparkFiles.get("env_wrapper.py"))
spark.sparkContext.addPyFile(SparkFiles.get("schema.py"))

env = EnvVariables()
print("\n\n\n\n", env.getFileName(), "\n\n\n\n")

df = spark.read.option("header",True).schema(DatasetSchema().schema).csv(env.getFileName())

# Preparation of the data
result_df = df.withColumn("totalFare", col("totalFare").cast("float"))

# Groupiing of the data and minimun price
result_df = result_df.groupBy("startingAirport","destinationAirport").agg(min("totalFare"))

# Write to file
result_df.sort("startingAirport").write.option("header",True).mode("overwrite").csv(env.getOutputDir() + spark.sparkContext.appName)

# spark-submit BestFlightsCities.py for cloud
# spark-submit --py-files ../env_wrapper.py,../schema.py BestFlightsCities.py in local console