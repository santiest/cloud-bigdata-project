# Este script calcula la relacion entre el precio medio de los viajes con escala y sin escala
import sys
sys.path.insert(0,'..')
from env_wrapper import EnvVariables
from schema import DatasetSchema
from pyspark import SparkFiles

from pyspark.sql.functions import col, round, split
from pyspark.sql import SparkSession

# Build pyspark
spark = (SparkSession.builder.master("local[*]")
            .appName("RelationNonStopPrice")
            .getOrCreate())

spark.sparkContext.addPyFile(SparkFiles.get("env_wrapper.py"))
spark.sparkContext.addPyFile(SparkFiles.get("schema.py"))

env = EnvVariables()
print("\n\n\n\n", env.getFileName(), "\n\n\n\n")


df = spark.read.option("header",True).schema(DatasetSchema().schema).csv(env.getFileName())

# Filtro las columnas que se van a necesitar
df.select('startingAirport', 'destinationAirport', 'isNonStop', 'totalFare')
# Agrupa por aeropuerto de inicio, de destino y si es o no con escalas
result_df = df.groupBy('startingAirport', 'destinationAirport', 'isNonStop').avg('totalFare')
# Redondea a dos decimales
result_df = result_df.select('*', round("avg(totalFare)",2).alias('Avg_Price')).drop("avg(totalFare)")
# Ordeno los datos y los escribo en un fichero
result_df.sort(col('startingAirport'), col('destinationAirport')).write.option("header",True).mode("overwrite").csv(env.getOutputDir() + spark.sparkContext.appName)

# spark-submit RelationNonStopPrice.py 
