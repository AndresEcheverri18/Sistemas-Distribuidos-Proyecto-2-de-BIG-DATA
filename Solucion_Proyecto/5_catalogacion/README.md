# Punto 5 – Catalogación de tablas SQL (AWS Glue y EMR/Hive)

Catalogar es registrar **metadatos**: nombre de la tabla, columnas, tipos, formato y ubicación en S3.
Así los motores SQL pueden consultar los archivos Parquet como si fueran tablas. Los datos **no se
mueven ni se copian**: siguen en S3 (*schema-on-read*).

Se catalogó la zona **trusted** con las dos herramientas que menciona el enunciado:

| Herramienta | Base de datos | Cómo define el esquema | Archivo |
|---|---|---|---|
| **AWS Glue Crawler** | `covid_trusted` | **Lo infiere** leyendo los Parquet | (consola o CLI, abajo) |
| **EMR / Hive** | `covid_hive` | **Se declara** con `CREATE EXTERNAL TABLE` | `catalogo_hive.sql` |

Las dos quedan en el **mismo AWS Glue Data Catalog**, porque el EMR se creó usando Glue como
metastore de Hive y Spark. Por eso las tablas de `covid_hive` también se ven en la consola de Glue y
en Athena, y las de `covid_trusted` se ven en Hive y en SparkSQL.

```
                ┌──────────── AWS Glue Data Catalog ────────────┐
 Crawler ─────► │ covid_trusted: casos, departamentos, municipios│ ◄──── Athena
 Hive DDL ────► │ covid_hive:    casos, departamentos, municipios│ ◄──── Hive / SparkSQL (EMR)
                └────────────────────┬──────────────────────────┘
                                     │ LOCATION
                          s3://<bucket>/trusted/{casos,departamentos,municipios}/*.parquet
```

---

## 5.1 Catálogo con AWS Glue Crawler

### Consola

1. **AWS Glue → Data Catalog → Databases → Add database** → nombre `covid_trusted`.
2. **Crawlers → Create crawler**:

   | Paso | Valor |
   |---|---|
   | Name | `covid-trusted-crawler` |
   | Data sources | **S3**, agregar 3 rutas: `s3://<bucket>/trusted/casos/`, `s3://<bucket>/trusted/departamentos/`, `s3://<bucket>/trusted/municipios/` |
   | IAM role | **LabRole** |
   | Target database | `covid_trusted` |
   | Crawler schedule | On demand |

3. **Run crawler**. Al terminar dirá *3 tables created*.
4. **Tables**: abrir `casos` y revisar el esquema inferido (19 columnas, tipos `int`, `date`, `string`).

¿Por qué 3 rutas y no solo `trusted/`? Con una ruta por tabla el crawler nombra cada tabla con la
última carpeta (`casos`, `departamentos`, `municipios`). Así no hay riesgo de que mezcle esquemas.

### CLI equivalente

```bash
aws glue create-database --database-input '{"Name":"covid_trusted"}'

aws glue create-crawler --name covid-trusted-crawler --role LabRole --database-name covid_trusted \
  --targets "{\"S3Targets\":[{\"Path\":\"s3://$BUCKET/trusted/casos/\"},{\"Path\":\"s3://$BUCKET/trusted/departamentos/\"},{\"Path\":\"s3://$BUCKET/trusted/municipios/\"}]}"

aws glue start-crawler --name covid-trusted-crawler
aws glue get-crawler --name covid-trusted-crawler --query "Crawler.State"     # esperar READY
aws glue get-tables --database-name covid_trusted --query "TableList[].Name"
```

---

## 5.2 Catálogo con EMR / Hive

En el nodo master del EMR:

```bash
ssh -i labsuser.pem hadoop@<master-dns>
aws s3 cp s3://$BUCKET/scripts/catalogo_hive.sql .
hive --hivevar BUCKET=$BUCKET -f catalogo_hive.sql
```

También se puede pegar el contenido en el editor **Hive de Hue** (`http://<master-dns>:8888`). Hue
pide el valor de la variable `BUCKET`.

Salida esperada (validada contra los Parquet reales):

```
SHOW TABLES;            → casos, departamentos, municipios
DESCRIBE FORMATTED casos → Table Type: EXTERNAL_TABLE · SerDe: ParquetHiveSerDe · Location: s3://<bucket>/trusted/casos
SELECT COUNT(*) ...     → 158768
SELECT * FROM departamentos LIMIT 5 → 5 Antioquia Andina 6677930 / 8 Atlántico Caribe 2722128 / ...
SELECT * FROM municipios LIMIT 5    → 5001 5 MEDELLÍN Municipio 6.246631 -75.581775 / ...
```

Comprobación cruzada (evidencia de que es un catálogo común): en la consola de Glue aparece la BD
`covid_hive`; en Athena, la tabla `covid_hive.casos` responde `SELECT COUNT(*)`; y en Hive,
`USE covid_trusted; SHOW TABLES;` muestra las tablas creadas por el crawler.

---

## 5.3 Explicación del código `catalogo_hive.sql`

| Sentencia | Explicación |
|---|---|
| `CREATE DATABASE IF NOT EXISTS covid_hive ... LOCATION 's3://${hivevar:BUCKET}/hive/covid_hive.db/'` | Crea la BD en el catálogo. `${hivevar:BUCKET}` es una **variable de Hive** que se reemplaza por el valor de `--hivevar BUCKET=...`, para no escribir el bucket "quemado" en el código. `LOCATION` evita que la BD apunte al HDFS temporal del cluster |
| `USE covid_hive` | Las siguientes tablas se crean en esa BD |
| `CREATE EXTERNAL TABLE IF NOT EXISTS casos (...)` | **EXTERNAL**: Hive solo guarda el metadato. Un `DROP TABLE` no borra los archivos de S3, que son propiedad del ETL |
| Lista de columnas y tipos | Exactamente las 19 columnas que escribe el ETL (`COLUMNAS_CASOS`), con los mismos tipos (`INT`, `DATE`, `STRING`, `DOUBLE`). Hive lee el Parquet **por nombre de columna**, así que los nombres deben coincidir |
| `STORED AS PARQUET` | Le dice a Hive que use el SerDe/InputFormat de Parquet |
| `LOCATION 's3://${hivevar:BUCKET}/trusted/casos/'` | Carpeta donde están los archivos. Todo archivo que caiga ahí es parte de la tabla: si el ETL vuelve a correr, la tabla se actualiza sola |
| `COMMENT '...'` | Documentación que queda guardada en el catálogo |
| `SHOW TABLES; DESCRIBE FORMATTED; SELECT ...` | Verificación y evidencias |

---

## 5.4 Crawler vs Hive DDL (pregunta típica de sustentación)

| | Glue Crawler | Hive DDL |
|---|---|---|
| Esquema | Inferido automáticamente | Declarado a mano |
| Ventaja | Rápido; detecta columnas nuevas y particiones | Control total de nombres y tipos; queda versionado en el repositorio |
| Riesgo | Puede inferir tipos inesperados con CSV sucios | Si el ETL cambia el esquema, hay que actualizar el DDL |
| Dónde queda | Glue Data Catalog | Glue Data Catalog (porque el EMR lo usa como metastore) |

---

## Evidencias para el informe (Punto 5)

1. Consola Glue: crawler `covid-trusted-crawler` con estado *Ready* y "3 tables created".
2. Consola Glue → Tables: esquema de `covid_trusted.casos`.
3. Terminal EMR: ejecución de `hive -f catalogo_hive.sql` con `SHOW TABLES` y `COUNT(*)`.
4. Consola Glue → Databases: aparecen **ambas** BDs (`covid_trusted` y `covid_hive`).
5. Athena: `SELECT COUNT(*) FROM covid_hive.casos` (prueba del catálogo compartido).
