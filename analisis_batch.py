from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

spark = SparkSession.builder.appName("AnalisisBatchUNAD").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1. Cargar el CSV
df = spark.read.csv("delitos_informaticos.csv", header=True, inferSchema=True)

print("=== 1. ESQUEMA DEL DATASET ===")
df.printSchema()

print("=== 2. MUESTRA DE LOS PRIMEROS REGISTROS ===")
df.show(5)

# 3. Limpieza de nulos
total_inicial = df.count()
df_limpio = df.dropna()
total_limpio = df_limpio.count()
print(f"=== 3. LIMPIEZA: Registros iniciales: {total_inicial} | Registros limpios: {total_limpio} ===")

# 4. Agrupación exploratoria (EDA)
col_1 = df_limpio.columns[0]
col_2 = df_limpio.columns[1]

print(f"=== 4. TOP 10 REGISTROS AGRUPADOS POR: {col_1} ===")
df_limpio.groupBy(col_1).count().orderBy(desc("count")).show(10, truncate=False)

print(f"=== 5. TOP 10 REGISTROS AGRUPADOS POR: {col_2} ===")
df_limpio.groupBy(col_2).count().orderBy(desc("count")).show(10, truncate=False)

spark.stop()
