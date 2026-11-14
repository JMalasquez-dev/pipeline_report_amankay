# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Crear la tabla Catalog.Schema.Table desde S3 (capa Silver de noticias de Amankay)
# MAGIC Archivo: `reporte_noticias_2026-10-29.csv` (conversión del `.xlsx` de la capa Silver).
# MAGIC **Antes de ejecutar:** sube el CSV a tu bucket de S3 (AWS Academy) y completa los widgets.

# COMMAND ----------

dbutils.widgets.text("bucket_path", "s3a://TU-BUCKET/silver/reporte_noticias_2026-10-29.csv", "Ruta del CSV en S3")
dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "amankay", "Schema")
dbutils.widgets.text("table", "silver_noticias", "Table")

RUTA = dbutils.widgets.get("bucket_path")
CATALOG, SCHEMA, TABLA = (dbutils.widgets.get(w) for w in ("catalog", "schema", "table"))
NOMBRE = f"{CATALOG}.{SCHEMA}.{TABLA}"
print("Tabla destino:", NOMBRE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Credenciales de AWS Academy (Learner Lab)
# MAGIC Copia `aws_access_key_id`, `aws_secret_access_key` y `aws_session_token` desde **AWS Details → AWS CLI**.
# MAGIC Son **temporales** (duran unas horas): si se vence la sesión, vuelve a copiarlas.
# MAGIC Recomendado: guárdalas en un *secret scope*; no las escribas en el notebook ni las subas a GitHub.
# MAGIC Esta configuración requiere un clúster clásico (no *serverless*); si tu workspace solo tiene serverless, usa la alternativa de la última celda.

# COMMAND ----------

ACCESS_KEY = dbutils.secrets.get("awsacademy", "access_key")
SECRET_KEY = dbutils.secrets.get("awsacademy", "secret_key")
SESSION_TOKEN = dbutils.secrets.get("awsacademy", "session_token")

hconf = sc._jsc.hadoopConfiguration()
hconf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
hconf.set("fs.s3a.access.key", ACCESS_KEY)
hconf.set("fs.s3a.secret.key", SECRET_KEY)
hconf.set("fs.s3a.session.token", SESSION_TOKEN)

# Verifica que Databricks "ve" el archivo en S3
display(dbutils.fs.ls(RUTA.rsplit("/", 1)[0] + "/"))

# COMMAND ----------

from pyspark.sql import functions as F

df = (spark.read
      .option("header", True)
      .option("multiLine", True)
      .option("escape", '"')
      .csv(RUTA))

df = (df.withColumn("fecha", F.to_date("fecha"))
        .withColumn("tiene_departamento", F.col("tiene_departamento") == "true")
        .withColumn("fecha_carga_silver", F.to_timestamp("fecha_carga_silver")))

print("Filas leídas desde S3:", df.count())   # esperado: 5160
df.printSchema()

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
df.write.mode("overwrite").saveAsTable(NOMBRE)
display(spark.sql(f"SELECT COUNT(*) AS filas FROM {NOMBRE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alternativa si el clúster no permite credenciales S3A (por ejemplo, serverless)
# MAGIC 1. Descarga el CSV desde S3 y súbelo con **Catalog → Add data → Upload to a volume**.
# MAGIC 2. Cambia `RUTA` por la ruta del volumen, por ejemplo `/Volumes/workspace/amankay/raw/reporte_noticias_2026-10-29.csv`.
# MAGIC 3. Ejecuta de nuevo las celdas de lectura y de creación de la tabla.
# MAGIC Genie solo trabaja con tablas de Unity Catalog, por eso la tabla se crea con `saveAsTable`.
