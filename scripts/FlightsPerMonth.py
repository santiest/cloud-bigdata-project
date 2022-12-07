# Este programa calcula el numero de vuelos con el mismo destino y origen pedido por cada mes. 

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
result_df = df.filter((df.startingAirport == sys.argv[1]) & (df.destinationAirport  == sys.argv[2])).select('flightDate')

# Saco el mes,  YYYY-MM-DD
result_df1 = result_df.withColumn('month', split('flightDate', '-').getItem(1))

# Calculo la media de los sitios que sobran por cada hora los redondeo
result_df2 = result_df1.groupBy('month').count()

# Ordeno los datos y los escribo en un fichero
result_df2.sort('month').write.option("header",True).csv("output")

# spark-submit FlightsPerMonth.py
