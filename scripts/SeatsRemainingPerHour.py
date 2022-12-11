# Este programa calcula la media de asientos libres de los vuelos de cada aerolinea con el mismo destino 
# y origen pedido. La salida consiste en las horas y la media de asientos.

import sys
sys.path.insert(0,'..')
from env_wrapper import EnvVariables
from schema import DatasetSchema
from pyspark import SparkFiles

from pyspark.sql.functions import col, split, floor
from pyspark.sql import SparkSession

# Build pyspark
spark = (SparkSession.builder.master("local[*]")
            .appName("SeatsRemainingPerHour")
            .getOrCreate())

spark.sparkContext.addPyFile(SparkFiles.get("env_wrapper.py"))
spark.sparkContext.addPyFile(SparkFiles.get("schema.py"))

env = EnvVariables()
print("\n\n\n\n", env.getFileName(), "\n\n\n\n")



df = spark.read.option("header",True).schema(DatasetSchema().schema).csv(env.getFileName())

# Filtro por origen y destino y por las columnas necesarias
result_df = df.filter((df.startingAirport == sys.argv[1]) & (df.destinationAirport  == sys.argv[2]))
result_df1 = result_df.select('seatsRemaining','segmentsDepartureTimeRaw', 'segmentsAirlineName')

# Saco la aerolinea
result_df1 = result_df1.withColumn('Airline', split(df.segmentsAirlineName, '\|').getItem(0))

# Saco la hora de partida,  YYYY-MM-DDThh:mm:ss.000±[hh]:00
result_df2 = result_df1.withColumn('auxhour', split(result_df1.segmentsDepartureTimeRaw, 'T').getItem(1))
result_df3 = result_df2.withColumn('Hour', split(result_df2.auxhour, ':').getItem(0)).drop('auxhour','segmentsDepartureTimeRaw', 'segmentsAirlineName')

# Calculo la media de los sitios que sobran por cada hora los redondeo
result_df3 = result_df3.groupBy('Airline', 'Hour').avg('seatsRemaining')
result_df3 = result_df3.select('Airline','Hour', floor("avg(seatsRemaining)").alias('avgSeats'))

# Ordeno los datos y los escribo en un fichero
result_df3.sort('Airline', 'Hour').write.option("header",True).mode("overwrite").csv(env.getOutputDir() + spark.sparkContext.appName)

# spark-submit SeatsRemainingPerHour.py