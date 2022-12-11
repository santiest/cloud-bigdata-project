# Este script calcula el numero de dias medios que transcurren entre la toma de los datos y la fecha de vuelo
# de cada aeropuerto
import sys
sys.path.insert(0,'..')
from env_wrapper import EnvVariables
from schema import DatasetSchema
from pyspark import SparkFiles

from pyspark.sql.functions import col, round, split
from pyspark.sql import SparkSession

# Build pyspark
spark = (SparkSession.builder.master("local[*]")
            .appName("AverageElapsedDays")
            .getOrCreate())

spark.sparkContext.addPyFile(SparkFiles.get("env_wrapper.py"))
spark.sparkContext.addPyFile(SparkFiles.get("schema.py"))

env = EnvVariables()
print("\n\n\n\n", env.getFileName(), "\n\n\n\n")


df = spark.read.option("header",True).schema(DatasetSchema().schema).csv(env.getFileName())
# Saca la diferencia de dias que hay entre las dos fechas
df1 = df.withColumn('ElapsedTime', df.flightDate - df.searchDate)
df2 = df1.withColumn('time', split(split(df1.ElapsedTime, ' ').getItem(1), '\'').getItem(1).cast("int"))
#Agrupa por aeropuerto de inicio y calcula la media
result_df = df2.groupBy(col("startingAirport")).avg('time')
# Redondea a dos decimales la media
result_df = result_df.select('*', round("avg(time)", 2).alias("Avg_time(Days)"))
# Deja solo las filas que son necesarias
result_df = result_df.select("startingAirport", "Avg_time(Days)")
# Ordena y escribe la salida en un archivo
result_df.sort(col("startingAirport")).write.option("header",True).mode("overwrite").csv(env.getOutputDir() + spark.sparkContext.appName)

# spark-submit AverageElapsedDays.py

