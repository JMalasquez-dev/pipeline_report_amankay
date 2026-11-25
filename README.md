# Amankay · Databricks, Amazon S3 Gold y Genie Agents

El notebook importa un CSV o XLSX de noticias ya procesado en S3 Gold a una tabla Delta administrada en Unity Catalog. El destino predeterminado es `workspace.amankay.gold_noticias`. Cada ejecución exitosa reemplaza la instantánea de esa tabla. El objeto original se lee desde S3.

## 1. Configura el acceso a S3

**Para ingresar tus cinco parámetros, usa `modo=sts` y `entrada_sts=manual`.** Importa el repositorio como Git folder o importa los notebooks en Databricks. Ejecuta la celda que crea los widgets y completa `BUCKET_DEFECTO`, `REGION` y `clave_s3`. Al ejecutar la celda de lectura de S3, ingresa `ACCESS_KEY`, `SECRET_KEY` y `SESSION_TOKEN` de la misma sesión cuando se soliciten. La entrada requiere que tu sesión interactiva admita prompts enmascarados.

Si la sesión no admite esos prompts o quieres ejecutar como job, usa `entrada_sts=secrets`. Desde una terminal con la CLI de Databricks autenticada en tu workspace, ejecuta estos comandos y escribe cada valor en el prompt interactivo.

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
| `entrada_sts` | `manual` para ingresar las credenciales al ejecutar o `secrets` para leerlas del scope |
| `secret_scope` | `amankay-s3` si seleccionaste `secrets` y usaste los comandos anteriores |

La identidad temporal debe tener `s3:GetObject` sobre el objeto y `kms:Decrypt` si el archivo usa una clave KMS que lo requiere. El cómputo del notebook necesita acceso de red a S3 y la identidad que lo ejecuta necesita permiso `READ` sobre el scope. Cuando caduque la sesión AWS, ingresa los tres valores de la nueva sesión o actualiza los tres secretos antes de volver a ejecutar. [Secret scopes y permisos en Databricks](https://docs.databricks.com/aws/en/security/secrets/workspace-secret-scopes)

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

### AWS Academy sin permiso para crear roles IAM

Si AWS devuelve `iam:CreateRole` denegado, la credencial de almacenamiento requiere un rol configurado por un administrador. Mientras esperas ese acceso, puedes probar la tabla Gold y Genie en Databricks Free Edition con un volumen administrado. Free Edition restringe las conexiones salientes, por lo que el modo `sts` depende de que el cómputo pueda alcanzar el endpoint de S3. [Restricciones de Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)

1. Descarga el CSV o XLSX Gold desde la consola de Amazon S3 mientras el laboratorio esté activo.
2. Ejecuta este SQL en Databricks y carga el archivo con **New → Add or upload data → Upload files to a volume**. Si ya tienes un volumen administrado, úsalo directamente.

   ```sql
   CREATE SCHEMA IF NOT EXISTS workspace.amankay;
   CREATE VOLUME IF NOT EXISTS workspace.amankay.gold_archivos;
   ```

3. En el notebook 01, ejecuta la celda que crea los widgets. Selecciona `modo=volumen`, indica `volumen_externo=/Volumes/workspace/amankay/gold_archivos` y configura `clave_s3` con el nombre del archivo tal como aparece en el volumen, incluida su extensión. Luego ejecuta las celdas restantes.
4. Presenta este flujo como descarga desde S3 y carga manual a Unity Catalog. La conexión directa gobernada de S3 queda pendiente hasta disponer del rol IAM.

Si ves el error de `clave_s3`, verifica que el valor esté presente y sea relativo. En modo `sts` se mide desde la raíz del bucket, por ejemplo `gold/report/reporte.xlsx`. En modo `volumen` se mide desde la raíz del volumen, por ejemplo `reporte.xlsx`. En ambos casos se omiten `s3://` y la barra inicial.

## 2. Publica y verifica Catalog → Schema → Table

Ejecuta el notebook [01](notebooks/01_crear_tabla_desde_s3.py) en cómputo con Unity Catalog. En clústeres clásicos usa Databricks Runtime 15.2 o posterior para que el notebook 02 acepte parámetros en las celdas SQL. Configura los widgets después de que se creen y vuelve a ejecutar el notebook. Para CSV, ajusta `separador_csv` y `encoding_csv` si hace falta. Excel se lee desde la primera hoja. El archivo debe tener `fecha` (formato ISO), `medio`, `titulo`, `tema` y `departamento_mencionado`. El archivo se procesa en memoria en el driver.

Usa un catálogo con almacenamiento administrado disponible. Si el catálogo o esquema se van a crear, la identidad necesita `CREATE CATALOG` o `CREATE SCHEMA` respectivamente. Para la carga necesita `USE CATALOG`, `USE SCHEMA`, y `CREATE TABLE` para crear la tabla. En recargas, usa la identidad propietaria de la tabla o una con los permisos necesarios para reemplazar su contenido y esquema.

La salida del notebook muestra la ruta de origen, la tabla de destino, el número de filas y el rango de fechas. Verifica la tabla en Catalog Explorer y ejecuta el [notebook 02](notebooks/02_consultas.py). Si cambiaste el destino, actualiza su widget `tabla_gold`. Los SQL individuales en `queries/` usan el nombre predeterminado. Para la evidencia, captura la ruta Gold de S3, la ejecución del notebook y la tabla en Catalog Explorer.

## 3. Usa la tabla Gold en Genie Agents

Abre **Genie Agents**, crea un agente, selecciona un SQL warehouse Pro o Serverless con permiso **CAN USE** y agrega `workspace.amankay.gold_noticias` o la tabla configurada. La identidad que conversa necesita `USE CATALOG`, `USE SCHEMA` y `SELECT`. [Configuración de Genie Agents](https://docs.databricks.com/aws/en/genie-agents/set-up)

En una misma conversación, ejecuta las tres proyecciones de [genie_prompts.md](genie_prompts.md). Para la evidencia, captura la tabla seleccionada en el agente y las tres respuestas con su SQL y el período histórico usado.
