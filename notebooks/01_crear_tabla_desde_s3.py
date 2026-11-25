# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Importar el reporte Gold de S3 a Unity Catalog
# MAGIC Selecciona `sts` para usar Databricks Secrets o `volumen` para leer el mismo
# MAGIC origen mediante un volumen externo de Unity Catalog. Cada ejecución reemplaza
# MAGIC la instantánea de la tabla de destino después de validar el archivo.

# COMMAND ----------

# MAGIC %pip install boto3 openpyxl

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import re
from contextlib import closing
from io import BytesIO
from pathlib import PurePosixPath

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

dbutils.widgets.dropdown("modo", "sts", ["sts", "volumen"], "Modo de acceso a S3")
for nombre, valor in {
    "BUCKET_DEFECTO": "",
    "REGION": "",
    "clave_s3": "",
    "secret_scope": "amankay-s3",
    "volumen_externo": "",
    "catalog": "workspace",
    "schema": "amankay",
    "table": "gold_noticias",
    "separador_csv": ",",
    "encoding_csv": "utf-8-sig",
}.items():
    dbutils.widgets.text(nombre, valor, nombre)

modo = dbutils.widgets.get("modo")
clave = dbutils.widgets.get("clave_s3").strip()
catalog, schema, tabla = (dbutils.widgets.get(w).strip() for w in ("catalog", "schema", "table"))
if any(not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", parte) for parte in (catalog, schema, tabla)):
    raise ValueError("Usa letras, dígitos y guiones bajos en los identificadores de destino.")

namespace = f"`{catalog}`.`{schema}`"
destino = f"{namespace}.`{tabla}`"
formato = PurePosixPath(clave).suffix.lower()
if not clave or clave.startswith("/") or any(p in {"", ".", ".."} for p in clave.split("/")):
    raise ValueError("Indica en clave_s3 la ruta relativa a la raíz del bucket o del volumen.")
if formato not in (".csv", ".xlsx"):
    raise ValueError("El archivo Gold debe ser CSV o XLSX.")

# COMMAND ----------

if modo == "sts":
    bucket = dbutils.widgets.get("BUCKET_DEFECTO").strip()
    region = dbutils.widgets.get("REGION").strip()
    scope = dbutils.widgets.get("secret_scope").strip()
    if not bucket or not region or not scope:
        raise ValueError("Completa BUCKET_DEFECTO, REGION y secret_scope.")

    with closing(boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=dbutils.secrets.get(scope=scope, key="ACCESS_KEY"),
        aws_secret_access_key=dbutils.secrets.get(scope=scope, key="SECRET_KEY"),
        aws_session_token=dbutils.secrets.get(scope=scope, key="SESSION_TOKEN"),
    )) as s3:
        try:
            objeto = s3.get_object(Bucket=bucket, Key=clave)
            with closing(objeto["Body"]) as cuerpo:
                contenido = BytesIO(cuerpo.read())
        except ClientError as error:
            codigo = error.response["Error"]["Code"]
            if codigo in {"ExpiredToken", "InvalidToken", "InvalidAccessKeyId", "SignatureDoesNotMatch"}:
                detalle = "Actualiza ACCESS_KEY, SECRET_KEY y SESSION_TOKEN de la misma sesión."
            elif codigo in {"AccessDenied", "403"}:
                detalle = "Revisa s3:GetObject y, si aplica, kms:Decrypt."
            else:
                detalle = "Revisa el bucket, la región y la clave del objeto."
            raise RuntimeError(f"S3 devolvió {codigo}. {detalle}") from None
    origen = f"s3://{bucket}/{clave}"
else:
    volumen = dbutils.widgets.get("volumen_externo").strip().rstrip("/")
    if not re.fullmatch(r"/Volumes/[^/]+/[^/]+/[^/]+", volumen):
        raise ValueError("Indica el volumen externo como /Volumes/<catalog>/<schema>/<volumen>.")
    origen = contenido = f"{volumen}/{clave}"

if formato == ".csv":
    reporte = pd.read_csv(
        contenido,
        dtype=str,
        keep_default_na=False,
        sep=dbutils.widgets.get("separador_csv"),
        encoding=dbutils.widgets.get("encoding_csv"),
    )
else:
    reporte = pd.read_excel(contenido, engine="openpyxl", dtype=str, keep_default_na=False)

requeridas = {"fecha", "medio", "titulo", "tema", "departamento_mencionado"}
faltantes = requeridas.difference(reporte.columns)
if faltantes:
    raise ValueError(f"Faltan columnas para las consultas de noticias: {', '.join(sorted(faltantes))}.")
if reporte.empty:
    raise ValueError("El archivo Gold está vacío. Se conserva la tabla existente.")

# COMMAND ----------

columnas = list(reporte.columns)
filas = [
    tuple(None if pd.isna(valor) or valor == "" else str(valor) for valor in fila)
    for fila in reporte.itertuples(index=False, name=None)
]
esquema = StructType([StructField(columna, StringType(), True) for columna in columnas])
df = spark.createDataFrame(filas, schema=esquema)
df = df.withColumn("fecha", F.expr("try_cast(fecha AS DATE)"))
for columna in ("fecha_carga_silver", "fecha_carga_gold"):
    if columna in columnas:
        df = df.withColumn(columna, F.expr(f"try_cast({columna} AS TIMESTAMP)"))
if "tiene_departamento" in columnas:
    df = df.withColumn("tiene_departamento", F.expr("try_cast(tiene_departamento AS BOOLEAN)"))

fechas_invalidas = df.filter(F.col("fecha").isNull()).count()
if fechas_invalidas:
    raise ValueError(f"Hay {fechas_invalidas} fechas vacías o inválidas. Usa fechas ISO (AAAA-MM-DD).")

print(f"Origen: {origen}")
print(f"Destino: {destino}")
if catalog.casefold() not in {item.name.casefold() for item in spark.catalog.listCatalogs()}:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
if not spark.catalog.databaseExists(namespace):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(destino)
display(spark.sql(f"SELECT COUNT(*) AS filas, MIN(fecha) AS desde, MAX(fecha) AS hasta FROM {destino}"))
