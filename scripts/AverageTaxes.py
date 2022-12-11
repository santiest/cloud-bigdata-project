# Este script calcula los impuestos medios de un cada aeropuerto
import sys
sys.path.insert(0,'..')
from env_wrapper import EnvVariables
from schema import DatasetSchema
from pyspark import SparkFiles

from pyspark.sql.functions import col, round
from pyspark.sql import SparkSession

# Build pyspark
spark = (SparkSession.builder.master("local[*]")
            .appName("AverageTaxes")
            .getOrCreate())
            
spark.sparkContext.addPyFile(SparkFiles.get("env_wrapper.py"))
spark.sparkContext.addPyFile(SparkFiles.get("schema.py"))

env = EnvVariables()
print("\n\n\n\n", env.getFileName(), "\n\n\n\n")


df = spark.read.option("header",True).schema(DatasetSchema().schema).csv(env.getFileName())
# Introduce una columna con el valor de los impuestos
df2 = df.withColumn('taxes',df.totalFare - df.baseFare)
# Redondea a dos decimales
df2 = df2.select("*",round("taxes",2))
# Agrupa por el mismo aeropuerto de inicio
result_df = df2.groupBy("startingAirport").avg("taxes")
#Redondea a dos decimales
result_df = result_df.select('*', round("avg(taxes)", 2).alias("Avg_taxes"))
# Dejo solo las columnas de aeropuerto de inicio e impuestos medios
result_df = result_df.select("startingAirport", "Avg_taxes")
# Ordena y escribe la salida en un archivo
result_df.sort("startingAirport").write.option("header",True).mode("overwrite").csv(env.getOutputDir() + spark.sparkContext.appName)
# spark-submit AverageTaxes.py

