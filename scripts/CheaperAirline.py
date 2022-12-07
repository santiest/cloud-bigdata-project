
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
            .appName("GetAveragePriceOfAirline")
            .getOrCreate())
df = spark.read.option("header",True).schema(DatasetSchema().schema).csv(env.getFileName())

# Average values
df = df.filter((df.startingAirport == sys.argv[1]) & (df.destinationAirport == sys.argv[2]))

result_df = df.withColumn('Airline', split(df.segmentsAirlineName, '\|').getItem(0))

result_df1 = result_df.groupBy('Airline').avg('totalFare')
result_df2 = result_df1.select('*', round("avg(totalFare)",2).alias('Avg_Price'))
result_df2 = result_df2.select('Airline', 'Avg_Price')

# Write to file
result_df2.sort("Avg_Price").write.option("header",True).csv("CheaperAirline")

# spark-submit CheaperAirline.py ATL BOS

# Hay que pasarle el nombre de el aeropuerto de inicio y el de destino