import sys
sys.path.insert(0,'..')
from env_wrapper import EnvVariables
from schema import DatasetSchema

from pyspark.sql.functions import col, round, split
from pyspark.sql import SparkSession

env = EnvVariables()
print("\n\n\n\n", env.getFileName(), "\n\n\n\n")

# Build pyspark
spark = (SparkSession.builder.master("local[4]")
            .appName("AverageElapsedDays")
            .getOrCreate())
df = spark.read.option("header",True).schema(DatasetSchema().schema).csv(env.getFileName())

# Values

df1 = df.withColumn('ElapsedTime', df.flightDate - df.searchDate)
df2 = df1.withColumn('time', split(split(df1.ElapsedTime, ' ').getItem(1), '\'').getItem(1).cast("int"))

result_df = df2.groupBy(col("startingAirport")).avg('time')
result_df = result_df.select('*', round("avg(time)", 2).alias("Avg_time(Days)"))
result_df = result_df.select("startingAirport", "Avg_time(Days)")
# Write to file
result_df.sort(col("startingAirport")).write.option("header",True).csv("AverageElapsedDays")

# spark-submit AverageElapsedDays.py

