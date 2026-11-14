# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Crear la tabla Catalog.Schema.Table desde Volumen (capa Silver de noticias de Amankay)
# MAGIC Archivo local en Volumen: `/Volumes/workspace/amankay/raw/reporte_noticias_2026-10-29.xlsx`
# MAGIC
# MAGIC El archivo se lee directamente con `pandas` desde el volumen de Unity Catalog
# MAGIC y luego se convierte a un DataFrame de Spark.

# COMMAND ----------

# MAGIC %pip install openpyxl --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Widgets de configuración (eliminamos S3 ya que se usará el volumen local)
dbutils.widgets.text("ruta_volumen", "/Volumes/workspace/amankay/raw/reporte_noticias_2026-10-29.xlsx", "Ruta del archivo en Volumen")
dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "amankay", "Schema")
dbutils.widgets.text("table", "silver_noticias", "Table")

RUTA_VOLUMEN = dbutils.widgets.get("ruta_volumen")
CATALOG, SCHEMA, TABLA = (dbutils.widgets.get(w) for w in ("catalog", "schema", "table"))
NOMBRE = f"{CATALOG}.{SCHEMA}.{TABLA}"

print("Origen local:", RUTA_VOLUMEN)
print("Destino tabla:", NOMBRE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lectura del archivo Excel desde el Volumen de Unity Catalog
# MAGIC No se requieren credenciales de AWS ni llamadas externas a S3.

# COMMAND ----------

import pandas as pd

def leer_excel_volumen(ruta_archivo):
    # Lectura directa del archivo Excel usando pandas y openpyxl
    pdf = pd.read_excel(ruta_archivo, engine="openpyxl")
    
    # Normalización de fechas y textos
    for c in ("fecha",):
        pdf[c] = pd.to_datetime(pdf[c]).dt.strftime("%Y-%m-%d")
    pdf["fecha_carga_silver"] = pd.to_datetime(pdf["fecha_carga_silver"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    pdf["tiene_departamento"] = pdf["tiene_departamento"].astype(str).str.lower()
    
    # Conversión a lista de filas para asegurar nulos limpios
    columnas = list(pdf.columns)
    filas = [[None if pd.isna(v) else str(v) for v in fila] for fila in pdf.itertuples(index=False, name=None)]
    return columnas, filas

columnas, filas = leer_excel_volumen(RUTA_VOLUMEN)
print("Filas leídas desde el Volumen:", len(filas))   # esperado: 5160

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

esquema = StructType([StructField(c, StringType(), True) for c in columnas])
df = spark.createDataFrame(filas, schema=esquema)

df = (df.withColumn("fecha", F.to_date("fecha"))
        .withColumn("tiene_departamento", F.col("tiene_departamento") == "true")
        .withColumn("fecha_carga_silver", F.to_timestamp("fecha_carga_silver")))

print("Filas en Spark:", df.count())   # esperado: 5160
df.printSchema()

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
df.write.mode("overwrite").saveAsTable(NOMBRE)
display(spark.sql(f"SELECT COUNT(*) AS filas FROM {NOMBRE}"))