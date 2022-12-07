# Este programa calcula la media de asientos libres de los vuelos con el mismo destino 
# y origen pedido. La salida consiste en las horas y la media de asientos.

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
            .appName("SeatsRemainingPerHour")
            .getOrCreate())

df = spark.read.option("header",True).schema(DatasetSchema().schema).csv(env.getFileName())

# Filtro por origen y destino y por las columnas necesarias
result_df = df.filter((df.startingAirport == sys.argv[1]) & (df.destinationAirport  == sys.argv[2]))
result_df1 = result_df.select('seatsRemaining','segmentsDepartureTimeRaw')

# Saco la hora de partida,  YYYY-MM-DDThh:mm:ss.000±[hh]:00
result_df2 = result_df1.withColumn('auxhour', split(result_df1.segmentsDepartureTimeRaw, 'T').getItem(1))
result_df3 = result_df2.withColumn('hour', split(result_df2.auxhour, ':').getItem(0)).drop('auxhour','segmentsDepartureTimeRaw')

# Calculo la media de los sitios que sobran por cada hora los redondeo
result_df3 = result_df3.groupBy('hour').avg('seatsRemaining')
result_df3 = result_df3.select('hour', floor("avg(seatsRemaining)").alias('media'))

# Ordeno los datos y los escribo en un fichero
result_df3.sort(result_df3.media.desc()).write.option("header",True).csv("output")

# spark-submit SeatsRemainingPerHour.py