# Amankay · Databricks, Amazon S3 Gold y Genie Agents

El notebook importa un CSV o XLSX de noticias ya procesado en S3 Gold a una tabla Delta administrada en Unity Catalog. El destino predeterminado es `workspace.amankay.gold_noticias`. Cada ejecución exitosa reemplaza la instantánea de esa tabla. El objeto original se lee desde S3.

## 1. Configura el acceso a S3

**Con tus cinco parámetros, usa el modo `sts`.** Importa el repositorio como Git folder o importa los notebooks en Databricks. Desde una terminal con la CLI de Databricks autenticada en tu workspace, ejecuta estos comandos. Escribe cada valor en el prompt interactivo.

```text
databricks secrets create-scope amankay-s3
databricks secrets put-secret amankay-s3 ACCESS_KEY
databricks secrets put-secret amankay-s3 SECRET_KEY
databricks secrets put-secret amankay-s3 SESSION_TOKEN
```

En el notebook [01](notebooks/01_crear_tabla_desde_s3.py), usa estos widgets.

| Widget | Valor |
| --- | --- |
| `modo` | `sts` |
| `BUCKET_DEFECTO` | Nombre del bucket, sin `s3://` |
| `REGION` | Región real del bucket, por ejemplo `us-east-1` |
| `clave_s3` | Clave exacta desde la raíz del bucket, por ejemplo `gold/report/reporte_noticias.xlsx` |
| `secret_scope` | `amankay-s3` si usaste los comandos anteriores |

La identidad temporal debe tener `s3:GetObject` sobre el objeto y `kms:Decrypt` si el archivo usa una clave KMS que lo requiere. El cómputo del notebook necesita acceso de red a S3 y la identidad que lo ejecuta necesita permiso `READ` sobre el scope. Cuando caduque la sesión AWS, actualiza los tres secretos juntos antes de volver a ejecutar. [Secret scopes y permisos en Databricks](https://docs.databricks.com/aws/en/security/secrets/workspace-secret-scopes)

### Pantalla «Create a new credential» de tu captura

El campo **IAM role (ARN)** requiere un rol IAM de AWS con acceso al prefijo Gold. Si tu evaluación pide demostrar una conexión gobernada por Unity Catalog, crea también esta ruta y ejecuta el notebook en modo `volumen`.

1. En AWS, pide al administrador el rol para Gold. En **Catalog → Connect → Credentials**, crea una **Storage Credential** de tipo **AWS IAM Role** con ese ARN. Actualiza la política de confianza del rol con el External ID generado por Databricks y la autorización de autoasunción que indica la guía. Valida la credencial. [Credencial de almacenamiento y ubicación externa](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3/s3-external-location-manual)
2. En **External Locations**, registra el prefijo Gold real, por ejemplo `s3://<bucket>/gold/report/`, con esa credencial. Prueba la conexión.
3. En un catálogo y esquema existentes, registra un volumen externo en esa ubicación. Por ejemplo, desde SQL:

   ```sql
   CREATE EXTERNAL VOLUME IF NOT EXISTS workspace.amankay.s3_gold
   LOCATION 's3://<bucket>/gold/report/';
   ```

   Para crearlo necesitas `USE CATALOG`, `USE SCHEMA`, `CREATE VOLUME` sobre el esquema y `CREATE EXTERNAL VOLUME` en la ubicación externa. Quien ejecute el notebook necesita `READ VOLUME` y los permisos de uso del catálogo y esquema. [Creación de volúmenes en Unity Catalog](https://docs.databricks.com/aws/en/volumes/utility-commands)
4. En el notebook 01 configura `modo=volumen`, `volumen_externo=/Volumes/workspace/amankay/s3_gold` y `clave_s3=reporte_noticias.xlsx` si el archivo está en la raíz del volumen. Usa la ruta relativa al volumen si está en una subcarpeta.

## 2. Publica y verifica Catalog → Schema → Table

Ejecuta el notebook [01](notebooks/01_crear_tabla_desde_s3.py) en cómputo con Unity Catalog. En clústeres clásicos usa Databricks Runtime 15.2 o posterior para que el notebook 02 acepte parámetros en las celdas SQL. Configura los widgets después de que se creen y vuelve a ejecutar el notebook. Para CSV, ajusta `separador_csv` y `encoding_csv` si hace falta. Excel se lee desde la primera hoja. El archivo debe tener `fecha` (formato ISO), `medio`, `titulo`, `tema` y `departamento_mencionado`. El archivo se procesa en memoria en el driver.

Usa un catálogo con almacenamiento administrado disponible. Si el catálogo o esquema se van a crear, la identidad necesita `CREATE CATALOG` o `CREATE SCHEMA` respectivamente. Para la carga necesita `USE CATALOG`, `USE SCHEMA`, y `CREATE TABLE` para crear la tabla. En recargas, usa la identidad propietaria de la tabla o una con los permisos necesarios para reemplazar su contenido y esquema.

La salida del notebook muestra la ruta de origen, la tabla de destino, el número de filas y el rango de fechas. Verifica la tabla en Catalog Explorer y ejecuta el [notebook 02](notebooks/02_consultas.py). Si cambiaste el destino, actualiza su widget `tabla_gold`. Los SQL individuales en `queries/` usan el nombre predeterminado. Para la evidencia, captura la ruta Gold de S3, la ejecución del notebook y la tabla en Catalog Explorer.

## 3. Usa la tabla Gold en Genie Agents

Abre **Genie Agents**, crea un agente, selecciona un SQL warehouse Pro o Serverless con permiso **CAN USE** y agrega `workspace.amankay.gold_noticias` o la tabla configurada. La identidad que conversa necesita `USE CATALOG`, `USE SCHEMA` y `SELECT`. [Configuración de Genie Agents](https://docs.databricks.com/aws/en/genie-agents/set-up)

En una misma conversación, ejecuta las tres proyecciones de [genie_prompts.md](genie_prompts.md). Para la evidencia, captura la tabla seleccionada en el agente y las tres respuestas con su SQL y el período histórico usado.
