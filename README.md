# Amankay · FIA · Databricks + S3 + Genie Agents

Trabajo con la capa Silver de noticias (`reporte_noticias_2026-10-29`, 5 160 filas, año 2025).

```
data/reporte_noticias_2026-10-29.csv     # CSV para subir a S3 (conversión del .xlsx)
notebooks/01_crear_tabla_desde_s3.py     # lee S3 y crea Catalog.Schema.Table
notebooks/02_consultas.py                # 5 consultas SQL + 1 en Python
queries/q1..q5_*.sql                     # las mismas consultas, un archivo por consulta
genie_prompts.md                         # prompts usados en Genie Agents
```

Pasos: (1) subir el CSV a S3; (2) importar el repositorio en Databricks con *Git folder*;
(3) ejecutar el notebook 01; (4) crear el Genie space; (5) ejecutar el notebook 02.
No subas credenciales de AWS al repositorio: usa *secrets*.

    aws configure export-credentials --format env