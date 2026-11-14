# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Crear la tabla Catalog.Schema.Table desde S3 (capa Silver de noticias de Amankay)
# MAGIC Archivo en S3 (Excel): `s3://pipeline-amankay-v1/silver/reporte/2026-10-29/reporte_noticias_2026-10-29.xlsx`
# MAGIC
# MAGIC El archivo se lee con `boto3` + `pandas` (Excel) y luego se convierte a un DataFrame de Spark.
# MAGIC Esta forma **no necesita configurar S3A en el clúster**, por lo que también funciona en cómputo serverless
# MAGIC siempre que el workspace tenga salida a Internet.

# COMMAND ----------

# MAGIC %pip install openpyxl boto3 --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("bucket", "pipeline-amankay-v1", "Bucket de S3")
dbutils.widgets.text("key", "silver/reporte/2026-10-29/reporte_noticias_2026-10-29.xlsx", "Ruta (key) del archivo")
dbutils.widgets.text("region", "us-east-1", "Región de AWS")
dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "amankay", "Schema")
dbutils.widgets.text("table", "silver_noticias", "Table")

BUCKET = dbutils.widgets.get("bucket")
KEY = dbutils.widgets.get("key")
REGION = dbutils.widgets.get("region")
CATALOG, SCHEMA, TABLA = (dbutils.widgets.get(w) for w in ("catalog", "schema", "table"))
NOMBRE = f"{CATALOG}.{SCHEMA}.{TABLA}"
print("Origen :", f"s3://{BUCKET}/{KEY}")
print("Destino:", NOMBRE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Credenciales de AWS Academy (Learner Lab)
# MAGIC Copia `aws_access_key_id`, `aws_secret_access_key` y `aws_session_token` desde **AWS Details → AWS CLI**
# MAGIC y guárdalas en un *secret scope* llamado `awsacademy` (claves `access_key`, `secret_key`, `session_token`).
# MAGIC Son **temporales**: si se vence la sesión del laboratorio, vuelve a copiarlas.
# MAGIC No las escribas en el notebook ni las subas a GitHub.

# COMMAND ----------

import io
import boto3
import pandas as pd

s3 = boto3.client(
    "s3",
    aws_access_key_id=dbutils.secrets.get("awsacademy", "access_key"),
    aws_secret_access_key=dbutils.secrets.get("awsacademy", "secret_key"),
    aws_session_token=dbutils.secrets.get("awsacademy", "session_token"),
    region_name=REGION,
)

# Verifica que el archivo existe en S3 y muestra su tamaño
meta = s3.head_object(Bucket=BUCKET, Key=KEY)
print("Archivo encontrado en S3. Tamaño (bytes):", meta["ContentLength"])

# COMMAND ----------

# Lee el Excel desde S3 y lo convierte a un DataFrame de Spark.
# Todo se pasa primero como texto (None en vacíos) y luego se castea en Spark,
# para evitar errores de tipos mezclados al convertir desde pandas.
def leer_excel_s3(cliente_s3, bucket, key):
    cuerpo = cliente_s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    pdf = pd.read_excel(io.BytesIO(cuerpo), engine="openpyxl")
    for c in ("fecha",):
        pdf[c] = pd.to_datetime(pdf[c]).dt.strftime("%Y-%m-%d")
    pdf["fecha_carga_silver"] = pd.to_datetime(pdf["fecha_carga_silver"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    pdf["tiene_departamento"] = pdf["tiene_departamento"].astype(str).str.lower()
    # Se arma una lista de filas para que los vacios sean None reales (no "nan")
    # sin depender de la version de pandas.
    columnas = list(pdf.columns)
    filas = [[None if pd.isna(v) else str(v) for v in fila] for fila in pdf.itertuples(index=False, name=None)]
    return columnas, filas

columnas, filas = leer_excel_s3(s3, BUCKET, KEY)
print("Filas leídas desde S3:", len(filas))   # esperado: 5160

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Si Databricks no puede salir a S3 (error de conexión o de red)
# MAGIC 1. Descarga el `.xlsx` desde S3 a tu computador y súbelo con **Catalog → Add data → Upload to a volume**.
# MAGIC 2. Lee el archivo con `pd.read_excel("/Volumes/workspace/amankay/raw/reporte_noticias_2026-10-29.xlsx")`
# MAGIC    y continúa con las celdas de conversión y `saveAsTable`.
# MAGIC Genie solo trabaja con tablas de Unity Catalog, por eso la tabla se crea con `saveAsTable`.