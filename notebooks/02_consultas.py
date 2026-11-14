# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Consultas sobre `silver_noticias` (cada una nació de un prompt en Genie Agents)
# MAGIC Cambia el catálogo y el schema si usaste otros nombres en el notebook 01.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG workspace;
# MAGIC USE SCHEMA amankay;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q1 FILTRO: últimas 10 notas de sismo que mencionan a Lima
# MAGIC SELECT fecha, medio, titulo
# MAGIC FROM silver_noticias
# MAGIC WHERE tema = 'sismo' AND departamento_mencionado = 'LIMA'
# MAGIC ORDER BY fecha DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q2 AGRUPACIÓN: número de notas por mes y tema
# MAGIC SELECT month(fecha) AS mes, tema, COUNT(*) AS notas
# MAGIC FROM silver_noticias
# MAGIC GROUP BY month(fecha), tema
# MAGIC ORDER BY mes, notas DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q3 RANKING: los 10 departamentos más mencionados
# MAGIC SELECT departamento_mencionado AS departamento, COUNT(*) AS notas,
# MAGIC        RANK() OVER (ORDER BY COUNT(*) DESC) AS ranking
# MAGIC FROM silver_noticias
# MAGIC WHERE departamento_mencionado IS NOT NULL
# MAGIC GROUP BY departamento_mencionado
# MAGIC ORDER BY ranking
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q4 COMPARATIVA: notas por tema, primer vs segundo semestre
# MAGIC WITH base AS (
# MAGIC   SELECT tema,
# MAGIC          SUM(CASE WHEN month(fecha) <= 6 THEN 1 ELSE 0 END) AS semestre_1,
# MAGIC          SUM(CASE WHEN month(fecha) >  6 THEN 1 ELSE 0 END) AS semestre_2
# MAGIC   FROM silver_noticias
# MAGIC   GROUP BY tema
# MAGIC )
# MAGIC SELECT tema, semestre_1, semestre_2,
# MAGIC        ROUND(100.0 * (semestre_2 - semestre_1) / semestre_1, 1) AS variacion_pct
# MAGIC FROM base
# MAGIC ORDER BY variacion_pct DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q5 RANKING + PARTICIPACIÓN: 10 medios con más notas y porcentaje acumulado
# MAGIC WITH medios AS (
# MAGIC   SELECT medio, COUNT(*) AS notas FROM silver_noticias GROUP BY medio
# MAGIC )
# MAGIC SELECT medio, notas,
# MAGIC        ROUND(100.0 * notas / SUM(notas) OVER (), 1) AS pct,
# MAGIC        ROUND(100.0 * SUM(notas) OVER (ORDER BY notas DESC, medio ROWS UNBOUNDED PRECEDING) / SUM(notas) OVER (), 1) AS pct_acumulado
# MAGIC FROM medios
# MAGIC ORDER BY notas DESC, medio
# MAGIC LIMIT 10;

# COMMAND ----------

# Q6 (Python): tema dominante de cada mes
from pyspark.sql import functions as F, Window

noticias = spark.table("silver_noticias")
por_mes_tema = (noticias
                .groupBy(F.month("fecha").alias("mes"), "tema")
                .agg(F.count("*").alias("notas")))
ventana = Window.partitionBy("mes").orderBy(F.col("notas").desc())
dominante = (por_mes_tema
             .withColumn("puesto", F.row_number().over(ventana))
             .filter("puesto = 1")
             .select("mes", "tema", "notas")
             .orderBy("mes"))
display(dominante)
