# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Importar reportes Gold de S3 a Unity Catalog
# MAGIC Selecciona `sts` para ingresar credenciales temporales o `volumen` para leer el mismo
# MAGIC origen mediante un volumen externo o administrado de Unity Catalog. Cada ejecución reemplaza
# MAGIC las tablas que corresponden a los archivos del lote después de validar su contenido.

# COMMAND ----------

# MAGIC %pip install boto3 openpyxl

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import re
import warnings
from contextlib import closing
from getpass import GetPassWarning, getpass
from io import BytesIO
from pathlib import Path, PurePosixPath

import boto3
import pandas as pd
from botocore.exceptions import (
    ClientError, ConnectionClosedError, ConnectTimeoutError, EndpointConnectionError, ReadTimeoutError,
)
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

dbutils.widgets.dropdown("modo", "sts", ["sts", "volumen"], "Modo de acceso a S3")
dbutils.widgets.dropdown("entrada_sts", "manual", ["manual", "secrets"], "Credenciales STS")
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

# COMMAND ----------

modo = dbutils.widgets.get("modo")
clave = dbutils.widgets.get("clave_s3").strip()
catalog, schema, tabla = (dbutils.widgets.get(w).strip() for w in ("catalog", "schema", "table"))
if any(not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", parte) for parte in (catalog, schema, tabla)):
    raise ValueError("Usa letras, dígitos y guiones bajos en los identificadores de destino.")

namespace = f"`{catalog}`.`{schema}`"
if clave.startswith("/") or "://" in clave:
    raise ValueError("Indica una ruta relativa al bucket o volumen.")
clave = clave.removesuffix("/*").rstrip("/")
if clave and any(p in {"", ".", ".."} for p in clave.split("/")):
    raise ValueError("Indica una ruta relativa al bucket o volumen.")

# COMMAND ----------

def leer_credenciales_sts():
    nombres = ("ACCESS_KEY", "SECRET_KEY", "SESSION_TOKEN")
    if dbutils.widgets.get("entrada_sts") == "secrets":
        scope = dbutils.widgets.get("secret_scope").strip()
        return tuple(dbutils.secrets.get(scope=scope, key=nombre).strip() for nombre in nombres)

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", GetPassWarning)
            return tuple(getpass(f"{nombre}: ").strip() for nombre in nombres)
    except Exception as error:
        raise RuntimeError(
            "La entrada enmascarada no está disponible en esta ejecución. "
            "Selecciona entrada_sts=secrets y configura secret_scope."
        ) from error


def es_reporte(ruta):
    return PurePosixPath(ruta).suffix.lower() in {".csv", ".xlsx"}


def elegir_archivos(rutas, prefijo, seleccionar_fecha):
    archivos = sorted(ruta for ruta in rutas if es_reporte(ruta))
    if seleccionar_fecha:
        por_fecha = {}
        for ruta in archivos:
            fecha, separador, _ = ruta[len(prefijo):].partition("/")
            if separador and re.fullmatch(r"\d{4}-\d{2}-\d{2}", fecha):
                por_fecha.setdefault(fecha, []).append(ruta)
        archivos = por_fecha[max(por_fecha)] if por_fecha else []
    if not archivos:
        raise ValueError(f"No hay CSV ni XLSX para cargar en {prefijo}.")
    return archivos


def leer_reporte(ruta, contenido):
    if PurePosixPath(ruta).suffix.lower() == ".csv":
        return pd.read_csv(
            contenido, dtype=str, keep_default_na=False,
            sep=dbutils.widgets.get("separador_csv"),
            encoding=dbutils.widgets.get("encoding_csv"),
        )
    return pd.read_excel(contenido, engine="openpyxl", dtype=str, keep_default_na=False)


reportes = {}
if modo == "sts":
    bucket = dbutils.widgets.get("BUCKET_DEFECTO").strip()
    region = dbutils.widgets.get("REGION").strip()
    if not bucket or not region:
        raise ValueError("Completa BUCKET_DEFECTO y REGION.")

    def crear_cliente_s3():
        acceso, secreto, token = leer_credenciales_sts()
        if not all((acceso, secreto, token)):
            raise ValueError("Completa ACCESS_KEY, SECRET_KEY y SESSION_TOKEN.")
        return boto3.client(
            "s3", region_name=region,
            aws_access_key_id=acceso, aws_secret_access_key=secreto, aws_session_token=token,
        )

    operacion = "listar"
    with closing(crear_cliente_s3()) as s3:
        try:
            if es_reporte(clave):
                archivos = [clave]
            else:
                prefijo = f"{clave}/" if clave else "gold/reporte/"
                pags = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefijo)
                rutas = (objeto["Key"] for pag in pags for objeto in pag.get("Contents", []))
                archivos = elegir_archivos(rutas, prefijo, seleccionar_fecha=not clave)
            for archivo in archivos:
                operacion = f"leer {archivo}"
                objeto = s3.get_object(Bucket=bucket, Key=archivo)
                with closing(objeto["Body"]) as cuerpo:
                    reportes[f"s3://{bucket}/{archivo}"] = leer_reporte(archivo, BytesIO(cuerpo.read()))
        except (ConnectionClosedError, EndpointConnectionError, ConnectTimeoutError, ReadTimeoutError) as error:
            raise RuntimeError(
                f"No se recibió una respuesta de S3 al intentar {operacion} en {bucket} ({region}). "
                "Verifica la región y el acceso de red del cómputo al endpoint S3. "
                "En Free Edition puedes cargar la carpeta en un volumen administrado y usar modo=volumen."
            ) from error
        except ClientError as error:
            codigo = error.response["Error"]["Code"]
            if codigo in {"ExpiredToken", "InvalidToken", "InvalidAccessKeyId", "SignatureDoesNotMatch"}:
                detalle = "Actualiza ACCESS_KEY, SECRET_KEY y SESSION_TOKEN de la misma sesión."
            elif codigo in {"AccessDenied", "403"}:
                detalle = ("Revisa s3:ListBucket sobre el bucket y el prefijo." if operacion == "listar"
                           else "Revisa s3:GetObject y, si aplica, kms:Decrypt.")
            else:
                detalle = "Revisa el bucket, la región y la ruta."
            raise RuntimeError(f"S3 devolvió {codigo} al intentar {operacion}. {detalle}") from None
else:
    volumen = dbutils.widgets.get("volumen_externo").strip().rstrip("/")
    if not re.fullmatch(r"/Volumes/[^/]+/[^/]+/[^/]+", volumen):
        raise ValueError("Indica el volumen como /Volumes/<catalog>/<schema>/<volumen>.")
    ruta = Path(volumen) / clave
    archivos = [str(ruta)] if es_reporte(clave) else elegir_archivos(
        (str(archivo) for archivo in ruta.rglob("*") if archivo.is_file()),
        str(ruta), seleccionar_fecha=False,
    )
    for archivo in archivos:
        reportes[archivo] = leer_reporte(archivo, archivo)

# COMMAND ----------

columnas_noticias = {"fecha", "medio", "titulo", "tema", "departamento_mencionado"}
tablas = {}
for origen, reporte in reportes.items():
    if reporte.empty:
        raise ValueError(f"El archivo Gold está vacío: {origen}.")
    es_noticias = columnas_noticias.issubset(reporte.columns)
    if es_noticias:
        nombre = tabla.lower()
    else:
        sufijo = re.sub(r"[^a-z0-9_]+", "_", PurePosixPath(origen).stem.lower()).strip("_")
        nombre = f"{tabla.lower()}_{sufijo}_{PurePosixPath(origen).suffix[1:].lower()}"
        if nombre in tablas:
            raise ValueError(f"Dos archivos generan el mismo nombre de tabla: {nombre}.")
    if "_archivo_origen" in reporte.columns:
        raise ValueError(f"La columna _archivo_origen está reservada: {origen}.")
    tablas.setdefault(nombre, []).append(reporte.assign(_archivo_origen=origen))

def convertir_a_spark(partes, es_noticias):
    reporte = pd.concat(partes, ignore_index=True)
    columnas = list(reporte.columns)
    filas = [
        tuple(None if pd.isna(valor) or valor == "" else str(valor) for valor in fila)
        for fila in reporte.itertuples(index=False, name=None)
    ]
    esquema = StructType([StructField(columna, StringType(), True) for columna in columnas])
    df = spark.createDataFrame(filas, schema=esquema)
    if es_noticias:
        df = df.withColumn("fecha", F.expr("try_cast(fecha AS DATE)"))
        invalidas = df.filter(F.col("fecha").isNull()).count()
        if invalidas:
            raise ValueError(f"Hay {invalidas} fechas de noticias vacías o inválidas. Usa fechas ISO.")
        for columna in ("fecha_carga_silver", "fecha_carga_gold"):
            if columna in columnas:
                df = df.withColumn(columna, F.expr(f"try_cast({columna} AS TIMESTAMP)"))
        if "tiene_departamento" in columnas:
            df = df.withColumn("tiene_departamento", F.expr("try_cast(tiene_departamento AS BOOLEAN)"))
    return df


datos = {nombre: convertir_a_spark(partes, nombre == tabla.lower()) for nombre, partes in tablas.items()}
if catalog.casefold() not in {item.name.casefold() for item in spark.catalog.listCatalogs()}:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
if not spark.catalog.databaseExists(namespace):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")
for nombre, df in datos.items():
    destino = f"{namespace}.`{nombre}`"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(destino)
    print(f"Destino: {destino}")
    display(spark.sql(f"SELECT _archivo_origen, COUNT(*) AS filas FROM {destino} GROUP BY _archivo_origen"))
if tabla.lower() not in datos:
    print("La carpeta no tenía archivos con las columnas de noticias. Revisa las tablas de reporte publicadas.")
