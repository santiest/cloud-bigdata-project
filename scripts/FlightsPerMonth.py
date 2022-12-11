# Este programa calcula el numero de vuelos de cada aerolinea para un destino y origen pedido por cada mes. 

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
            .appName("FlightsPerMonth")
            .getOrCreate())

df = spark.read.option("header",True).schema(DatasetSchema().schema).csv(env.getFileName())

# Filtro por origen y destino y por las columnas necesarias
result_df = df.filter((df.startingAirport == sys.argv[1]) & (df.destinationAirport  == sys.argv[2]))
result_df1 = result_df.select('flightDate', 'segmentsAirlineName')

# Saco la aerolinea
result_df1 = result_df1.withColumn('Airline', split('segmentsAirlineName', '\|').getItem(0))

# Saco el mes,  YYYY-MM-DD
result_df1 = result_df1.withColumn('Month', split('flightDate', '-').getItem(1))

# Calculo el numero de vuelos que hay por cada mes
result_df2 = result_df1.groupBy('Month', 'Airline').count()

# Ordeno los datos y los escribo en un fichero
result_df2.sort('Airline', 'Month').write.option("header",True).csv("FlightsPerMonth")

# spark-submit FlightsPerMonth.py
