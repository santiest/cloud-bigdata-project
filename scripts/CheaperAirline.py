# Este script calcula que aerolinea es de promedio mas barata al hacer el viaje entre dos ciudades
import sys
sys.path.insert(0,'..')
from env_wrapper import EnvVariables
from schema import DatasetSchema
from pyspark import SparkFiles

from pyspark.sql.functions import col, round, split
from pyspark.sql import SparkSession

# Build pyspark
spark = (SparkSession.builder.master("local[*]")
            .appName("CheaperAirline")
            .getOrCreate())

spark.sparkContext.addPyFile(SparkFiles.get("env_wrapper.py"))
spark.sparkContext.addPyFile(SparkFiles.get("schema.py"))

env = EnvVariables()
print("\n\n\n\n", env.getFileName(), "\n\n\n\n")


df = spark.read.option("header",True).schema(DatasetSchema().schema).csv(env.getFileName())
# Filtra para quedarme solo con las filas que tengan el aeropuerto de inicio y destino pedido
df = df.filter((df.startingAirport == sys.argv[1]) & (df.destinationAirport == sys.argv[2]))
# Saco el nombre de al aerolinea
result_df = df.withColumn('Airline', split(df.segmentsAirlineName, '\|').getItem(0))
# Agrupo por el nombre de la aerolinea y calculo la media de precios
result_df1 = result_df.groupBy('Airline').avg('totalFare')
# Redondeo para dejar solo dos decimales en la media
result_df2 = result_df1.select('*', round("avg(totalFare)",2).alias('Avg_Price'))
# Dejo solo las columnas que necesito
result_df2 = result_df2.select('Airline', 'Avg_Price')
# Ordena y escribe la salida en un archivo
result_df2.sort("Avg_Price").write.option("header",True).mode("overwrite").csv(env.getOutputDir() + spark.sparkContext.appName)

# spark-submit CheaperAirline.py ATL BOS

# Hay que pasarle el nombre de el aeropuerto de inicio y el de destino