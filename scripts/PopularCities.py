# Este programa calcula el numero de vuelos con destino a cada ciudad 
import sys
sys.path.insert(0,'..')
from env_wrapper import EnvVariables
from schema import DatasetSchema
from pyspark import SparkFiles

from pyspark.sql.functions import col, split
from pyspark.sql import SparkSession

# Build pyspark
spark = (SparkSession.builder.master("local[*]")
            .appName("PopularCities")
            .getOrCreate())

spark.sparkContext.addPyFile(SparkFiles.get("env_wrapper.py"))
spark.sparkContext.addPyFile(SparkFiles.get("schema.py"))

env = EnvVariables()
print("\n\n\n\n", env.getFileName(), "\n\n\n\n")


df = spark.read.option("header",True).schema(DatasetSchema().schema).csv(env.getFileName())
# Filtra las columnas que se desean
result_df = df.select(col("destinationAirport"))
#Agrupa por aerpuerto de destino y cuento
result_df1 = result_df.groupBy('destinationAirport').count()
# Ordeno los datos y los escribo en un fichero
result_df1.sort(col('count').desc()).write.option("header",True).mode("overwrite").csv(env.getOutputDir() + spark.sparkContext.appName)
# spark-submit PopularCities.py
