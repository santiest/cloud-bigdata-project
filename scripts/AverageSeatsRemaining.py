# Este programa calcula la media de asientos libres de los vuelos de cada aerolinea con el mismo destino 
# y origen pedido.

import sys
sys.path.insert(0,'..')
from env_wrapper import EnvVariables
from schema import DatasetSchema
from pyspark import SparkFiles

from pyspark.sql.functions import col, split, floor
from pyspark.sql import SparkSession

# Build pyspark
spark = (SparkSession.builder.master("local[*]")
            .appName("AverageSeatsRemaining")
            .getOrCreate())

spark.sparkContext.addPyFile(SparkFiles.get("env_wrapper.py"))
spark.sparkContext.addPyFile(SparkFiles.get("schema.py"))

env = EnvVariables()
print("\n\n\n\n", env.getFileName(), "\n\n\n\n")



df = spark.read.option("header",True).schema(DatasetSchema().schema).csv(env.getFileName())

# Filtro por origen y destino y por las columnas necesarias
result_df = df.filter((df.startingAirport == sys.argv[1]) & (df.destinationAirport  == sys.argv[2]))
result_df1 = result_df.select('seatsRemaining', 'segmentsAirlineName')

# Saco la aerolinea
result_df1 = result_df1.withColumn('Airline', split(df.segmentsAirlineName, '\|').getItem(0))


# Calculo la media de los sitios que sobran en cada aerolinea
result_df2 = result_df1.groupBy('Airline').avg('seatsRemaining')
result_df3 = result_df2.select('Airline', floor("avg(seatsRemaining)").alias('avgSeats'))

# Ordeno los datos y los escribo en un fichero
result_df3.sort('Airline').write.option("header",True).mode("overwrite").csv(env.getOutputDir() + spark.sparkContext.appName)

# spark-submit AerageSeatsRemaining.py origen destino