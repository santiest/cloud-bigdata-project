# Este programa calcula la media de precios por mes dado un aeropuerto de inicio y uno de destino

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
            .appName("AveragePricePerMonth")
            .getOrCreate())

df = spark.read.option("header",True).schema(DatasetSchema().schema).csv(env.getFileName())

# Filtro por origen y destino y por las columnas necesarias
result_df = df.filter((df.startingAirport == sys.argv[1]) & (df.destinationAirport  == sys.argv[2]))
result_df1 = result_df.select('totalFare','segmentsDepartureTimeRaw')

# Saco el mes
result_df2 = result_df1.withColumn('auxmonth', split(result_df1.segmentsDepartureTimeRaw, 'T').getItem(0))
result_df3 = result_df2.withColumn('month', split(result_df2.auxmonth, '-').getItem(1)).drop('auxmonth','segmentsDepartureTimeRaw')

# Calculo el precio medio de los vuelos por mes
result_df3 = result_df3.groupBy('month').avg('totalFare')
result_df3 = result_df3.select('month', floor("avg(totalFare)").alias('Average_Price'))

# Ordeno los datos y los escribo en un fichero en orden descendiente en los precios medios
result_df3.sort(result_df3.Average_Price.desc()).write.option("header",True).csv("AveragePricePerMonth")

# spark-submit AveragePricePerMonth.py ATL BOS

# Hay que indicar que aeropuerto de inicio es y el de destino

