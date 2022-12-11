
import sys
sys.path.insert(0,'..')
from env_wrapper import EnvVariables
from schema import DatasetSchema

from pyspark.sql.functions import col, bround, ceil, split
from pyspark.sql import SparkSession
from pyspark import SparkFiles

# Build pyspark
spark = (SparkSession.builder.master("local[*]")
            .appName("RelationDistanceTimeAirline")
            .getOrCreate()) 

spark.sparkContext.addPyFile(SparkFiles.get("env_wrapper.py"))
spark.sparkContext.addPyFile(SparkFiles.get("schema.py"))

env = EnvVariables()
print("\n\n\n\n", env.getFileName(), "\n\n\n\n")

df = spark.read.option("header",True).schema(DatasetSchema().schema).csv(env.getFileName())

# Average values
result_df1 = df.withColumn('Airline', split(df.segmentsAirlineName, '\|').getItem(0)).withColumn("totalTravelDistance", col("totalTravelDistance").cast("Float")).groupBy("Airline").avg("totalTravelDistance")
result_df2 = df.withColumn('Airline', split(df.segmentsAirlineName, '\|').getItem(0)).withColumn("hour", split("travelDuration", 'H').getItem(0)).withColumn("hour", split("hour", 'T').getItem(1))
result_df2 = result_df2.withColumn("Minutes", split("travelDuration", 'H').getItem(1)).withColumn("Minutes", split("Minutes", 'M').getItem(0))
result_df2 = result_df2.withColumn("hour", col("hour").cast("Integer")).withColumn("Minutes", col("Minutes").cast("Integer")).withColumn("travelDurationSplit", col("Minutes") +(col("hour")*60))
result_df2 = result_df2.groupBy("Airline").avg("travelDurationSplit")
result_df = result_df1.join(result_df2, ["Airline"])
result_df = result_df.filter(col("avg(totalTravelDistance)")!= "0")
# Write to file
result_df.write.option("header",True).csv("RelationDistanceTimeAirline").mode("overwrite").csv(env.getOutputDir() + spark.sparkContext.appName)

# spark-submit RelationDistanceTimeAirline.py