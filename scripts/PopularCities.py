# Este programa calcula el numero de vuelos con destino a cada ciudad 
import sys
sys.path.insert(0,'..')
from env_wrapper import EnvVariables
from schema import DatasetSchema

from pyspark.sql.functions import col, split, floor
from pyspark.sql import SparkSession

env = EnvVariables()
print("\n\n\n\n", env.getFileName(), "\n\n\n\n")

# Build pyspark
spark = (SparkSession.builder.master("local[4]")
            .appName("PopularCities")
            .getOrCreate())

df = spark.read.option("header",True).schema(DatasetSchema().schema).csv(env.getFileName())

# Filtra las columnas que se desean
result_df = df.select(col("destinationAirport"))
#Agrupa por aerpuerto de destino y cuento
result_df1 = result_df.groupBy('destinationAirport').count()

# Ordeno los datos y los escribo en un fichero
result_df1.sort(col('count').desc()).write.option("header",True).csv("PopularCities")

# spark-submit PopularCities.py
