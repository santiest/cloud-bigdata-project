# Este script calcula que aerolinea realiza mas viajes
import sys
sys.path.insert(0,'..')
from env_wrapper import EnvVariables
from schema import DatasetSchema
from pyspark import SparkFiles

from pyspark.sql.functions import col, round, split
from pyspark.sql import SparkSession

# Build pyspark
spark = (SparkSession.builder.master("local[*]")
            .appName("PopularAirline")
            .getOrCreate())

spark.sparkContext.addPyFile(SparkFiles.get("env_wrapper.py"))
spark.sparkContext.addPyFile(SparkFiles.get("schema.py"))

env = EnvVariables()
print("\n\n\n\n", env.getFileName(), "\n\n\n\n")


df = spark.read.option("header",True).schema(DatasetSchema().schema).csv(env.getFileName())
# Filtro las columnas que se van a necesitar
df.select('segmentsAirlineName')
# Extraigo el nombre de la aerolinea en otra columna
result_df = df.withColumn('Airline', split(df.segmentsAirlineName, '\|').getItem(0))
# Agrupo por la aerolinea y cuento
result_df1 = result_df.groupBy('Airline').count()
# Ordeno los datos y los escribo en un fichero
result_df1.sort(col("count").desc()).write.option("header",True).mode("overwrite").csv(env.getOutputDir() + spark.sparkContext.appName)

# spark-submit PopularAirline.py 
