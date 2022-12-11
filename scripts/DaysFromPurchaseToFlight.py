# Este script cuenta el numero de vecesz que se repite que hay un vuelo con la misma diferencia de dias
# entre la fecha de adquisicion de los datos y la fecha de vuelo
import sys
sys.path.insert(0,'..')
from env_wrapper import EnvVariables
from schema import DatasetSchema
from pyspark import SparkFiles

from pyspark.sql.functions import col, round
from pyspark.sql import SparkSession

# Build pyspark
spark = (SparkSession.builder.master("local[*]")
            .appName("DaysFromPurchaseToFlight")
            .getOrCreate())

spark.sparkContext.addPyFile(SparkFiles.get("env_wrapper.py"))
spark.sparkContext.addPyFile(SparkFiles.get("schema.py"))

env = EnvVariables()
print("\n\n\n\n", env.getFileName(), "\n\n\n\n")


df = spark.read.option("header",True).schema(DatasetSchema().schema).csv(env.getFileName())
# Saca la diferencia de dias que hay entre las dos fechas
df1 = df.withColumn('ElapsedTime', df.flightDate - df.searchDate)
# Agrupa por tiempo y cuenta
result_df = df1.groupBy(col("ElapsedTime")).count()
# Ordena y escribe la salida en un archivo
result_df.sort(col("count").desc()).write.option("header",True).mode("overwrite").csv(env.getOutputDir() + spark.sparkContext.appName)

# spark-submit DaysFromPurchaseToFlight.py

