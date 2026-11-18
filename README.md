# Proyecto 2 – Big Data (SI3007) · Solución completa

**Caso de estudio:** COVID-19 en Colombia (marzo – julio 2020)
**Plataforma:** AWS Academy (Learner Lab) – región `us-east-1`

Este README es la **puerta de entrada**. Explica la idea completa, la arquitectura, el orden de
ejecución y cómo se conectan todos los archivos. Cada carpeta `N_...` tiene su propio
`README.md` con el detalle **línea por línea** del código de ese punto.

---

## 1. El proyecto en un minuto (para abrir la sustentación)

> Construimos un **datalake en AWS S3** que integra datos de COVID-19 de **3 fuentes distintas**:
> **archivos** en una EC2 (100 mil casos), una **base de datos RDS MariaDB** (departamentos con región y
> población) y **URLs de Internet** (API de datos abiertos datos.gov.co: casos más recientes y
> municipios con coordenadas).
> Un script en la EC2, programado con **cron**, hace la **ingesta automática** a la zona **raw**.
> Un **Job de AWS Glue** (Spark) limpia y homologa los datos hacia la zona **trusted** en Parquet.
> Las tablas se **catalogan** con un **Crawler de Glue** y con **DDL de Hive en EMR** (ambos quedan
> en el Glue Data Catalog). Respondemos **6 preguntas de negocio** con el **mismo archivo SQL** en
> **Athena, Hive y SparkSQL**, y también con la **API de DataFrames de PySpark**, que guarda los
> resultados en la zona **refined**. Al final, una **API REST** (API Gateway + Lambda + Athena) expone
> las respuestas y una app **Streamlit** con gráficas **matplotlib** y un mapa las visualiza.

---

## 2. Estructura del repositorio (un directorio por punto del enunciado)

| Punto del enunciado | Carpeta | Archivos | Servicio AWS / herramienta |
|---|---|---|---|
| 1. Caso de estudio y preguntas | `1_caso_estudio/` | `README.md` | – |
| 2. Fuentes: RDS MariaDB, archivos EC2, URLs | `2_fuentes_datos/` | `mariadb_departamentos.sql` | RDS MariaDB, EC2, datos.gov.co |
| 3. Ingesta automática a S3 | `3_ingesta/` | `ingesta.py` | EC2 + cron, S3 |
| 4. Preparación raw → trusted | `4_preparacion/` | `etl_raw_a_trusted.py` | AWS Glue (Job Spark) y EMR Spark |
| 5. Catalogación | `5_catalogacion/` | `catalogo_hive.sql` | Glue Crawler, EMR/Hive |
| 6. Consultas SQL | `6_consultas_sql/` | `consultas.sql`, `consultas_sparksql.py` | Athena, Hive, SparkSQL |
| 7. Analítica con PySpark | `7_analitica_pyspark/` | `analitica_pyspark.py` | EMR Spark |
| 8. Visualización + API Gateway + Streamlit | `8_visualizacion_api/` | `lambda_api.py`, `app_streamlit.py` | API Gateway, Lambda, Athena, EC2 |
| – | raíz | `requirements.txt` | librerías Python de la EC2 |
| Informe | raíz | `SI3007-262-proyecto2-diligenciado.docx` | documento del enunciado ya diligenciado |

En total son **9 archivos de código**, **9 README** y el informe. Nada más.

**Sobre el informe `.docx`:** es una copia del enunciado, que no se modificó, con las 6 preguntas y la
sección *Desarrollo* (Puntos 1 a 8) ya escritas: descripciones, arquitectura, hallazgos y extractos
del código real. Le falta lo que solo ustedes pueden poner:
- los nombres de los integrantes;
- la URL del repositorio GitHub;
- los **pantallazos**, cada uno marcado en **amarillo** como `[PANTALLAZO: ...]` en el lugar exacto
  donde va. Cada carpeta tiene en su README la lista "Evidencias para el informe" con lo que debe
  verse en cada captura.

---

## 3. Arquitectura

```
  FUENTES (Punto 2)
  ┌────────────────────────┐  ┌──────────────────────────┐  ┌──────────────────────────────┐
  │ EC2  ~/datos/*.csv     │  │ RDS MariaDB  covid_db    │  │ URL datos.gov.co (API Socrata)│
  │ 100K casos COVID       │  │ tabla departamentos      │  │ - casos 2 al 15 jul 2020     │
  │ (6 mar – 1 jul 2020)   │  │ (región, población)      │  │ - municipios DIVIPOLA lat/lon│
  └───────────┬────────────┘  └────────────┬─────────────┘  └──────────────┬───────────────┘
              └──────────────┐             │             ┌─────────────────┘
                             ▼             ▼             ▼
  INGESTA (Punto 3)     ┌──────────────────────────────────────┐
                        │ EC2: cron ──► ingesta.py             │   boto3 / pymysql / requests
                        └──────────────────┬───────────────────┘
                                           ▼
  DATALAKE S3           s3://<bucket>/raw/        (bronze: copia fiel de las fuentes, CSV)
                                           │
  PREPARACIÓN (Punto 4)   etl_raw_a_trusted.py  ──  AWS Glue Job (Spark)  ó  EMR spark-submit
                                           ▼
                        s3://<bucket>/trusted/    (silver: limpio, tipado, Parquet)
                                 │                         │
  CATALOGACIÓN (Punto 5)   Glue Crawler ► BD covid_trusted   Hive DDL (EMR) ► BD covid_hive
                                 └──────────► AWS Glue Data Catalog ◄──────┘  (metastore común)
                                           │
  CONSULTAS SQL (Punto 6)   consultas.sql ─► Athena  |  Hive (EMR)  |  SparkSQL (EMR)
  ANALÍTICA (Punto 7)       analitica_pyspark.py (EMR) ─► s3://<bucket>/refined/ (gold: CSV + Parquet)
                                           │
  CONSUMO (Punto 8)   Streamlit (EC2, matplotlib + mapa) ─HTTP─► API Gateway ─► Lambda ─► Athena
```

### Zonas del datalake (estructura exacta en S3)

```
s3://<bucket>/
├── raw/                                   ← Punto 3 (ingesta.py). Nunca se modifica, solo se agrega.
│   ├── ec2_archivos/casos_covid/fecha_ingesta=AAAA-MM-DD/Casos_positivos_..._100K.csv
│   ├── mariadb/departamentos/fecha_ingesta=AAAA-MM-DD/departamentos.csv
│   └── url/
│       ├── casos_covid_api/fecha_ingesta=AAAA-MM-DD/casos_covid_api.csv
│       └── divipola_municipios/fecha_ingesta=AAAA-MM-DD/divipola_municipios.csv
├── trusted/                               ← Punto 4 (etl_raw_a_trusted.py). Parquet.
│   ├── casos/            (158.768 filas)
│   ├── departamentos/    (33 filas)
│   └── municipios/       (1.122 filas)
├── refined/                               ← Punto 7 (analitica_pyspark.py). CSV + Parquet.
│   ├── p1_top_departamentos/{csv,parquet}/
│   ├── ...                                   (una carpeta por pregunta)
│   └── p6_top_municipios/{csv,parquet}/
├── scripts/                               ← copia de los .py/.sql (Glue lee el script desde aquí)
├── hive/covid_hive.db/                    ← ubicación de la BD creada con Hive (Punto 5)
└── athena-results/                        ← resultados de Athena (consola y Lambda)
```

---

## 4. Guía de ejecución paso a paso (runbook)

> Orden obligatorio: cada paso usa lo que produjo el anterior.
> Los comandos `aws ...` se ejecutan en la **terminal del Learner Lab** o en **AWS CloudShell**
> (ya tienen credenciales). Los comandos de la EC2 y del EMR se ejecutan por SSH en esas máquinas.

| Paso | Qué se hace | Dónde | Detalle |
|---|---|---|---|
| 0 | Crear el bucket S3 del datalake | Learner Lab / CloudShell | abajo |
| 1 | Definir caso y preguntas | – | `1_caso_estudio/README.md` |
| 2 | Crear EC2 + RDS MariaDB, cargar la tabla, copiar el CSV a la EC2 | Consola + EC2 | `2_fuentes_datos/README.md` |
| 3 | Ejecutar la ingesta y programarla con cron | EC2 | `3_ingesta/README.md` |
| 4 | Crear y ejecutar el Job de Glue (y opcional en EMR) | Consola Glue / EMR | `4_preparacion/README.md` |
| 5 | Crawler de Glue + DDL de Hive | Consola Glue / EMR | `5_catalogacion/README.md` |
| 6 | Consultas en Athena, Hive, SparkSQL | Athena / EMR | `6_consultas_sql/README.md` |
| 7 | Analítica PySpark → refined | EMR | `7_analitica_pyspark/README.md` |
| 8 | Lambda + API Gateway + Streamlit | Consola + EC2 | `8_visualizacion_api/README.md` |

### Paso 0 – Bucket del datalake

```bash
export BUCKET=<su-usuario>-covid-datalake      # nombre único global, en minúsculas
aws s3 mb s3://$BUCKET --region us-east-1
```

Subir los scripts que usarán Glue y EMR (desde la carpeta `Solucion_Proyecto/`):

```bash
aws s3 cp 4_preparacion/etl_raw_a_trusted.py      s3://$BUCKET/scripts/
aws s3 cp 5_catalogacion/catalogo_hive.sql         s3://$BUCKET/scripts/
aws s3 cp 6_consultas_sql/consultas.sql            s3://$BUCKET/scripts/
aws s3 cp 6_consultas_sql/consultas_sparksql.py    s3://$BUCKET/scripts/
aws s3 cp 7_analitica_pyspark/analitica_pyspark.py s3://$BUCKET/scripts/
```

**Cluster EMR:** se usa el del curso (guía `bigdata/00-lab-aws-emr/Install-AWS-EMR-7.10.0.pdf`):
EMR 7.10.0 con Hadoop, Hive, Spark, Hue y JupyterHub, y con **"Use for Hive table metadata"** y
**"Use for Spark table metadata"** (Glue Data Catalog) activados. Eso es lo que hace que lo que se
catalogue en Hive aparezca en Glue/Athena y viceversa.

---

## 5. Mapa global: qué corre dónde y quién lo llama

| Archivo | Se ejecuta en | Lo invoca | Lee de | Escribe en |
|---|---|---|---|---|
| `mariadb_departamentos.sql` | RDS MariaDB | usted, con el cliente `mariadb` desde la EC2 | – | tabla `covid_db.departamentos` |
| `ingesta.py` | EC2 | **cron** (automático) o usted a mano | carpeta `~/datos`, MariaDB, URLs | `s3://.../raw/` |
| `etl_raw_a_trusted.py` | AWS Glue (Job) **o** EMR | botón *Run* / trigger de Glue, o `spark-submit` | `raw/` | `trusted/` |
| `catalogo_hive.sql` | EMR (Hive) | `hive -f` | `trusted/` (solo metadatos) | Glue Data Catalog (BD `covid_hive`) |
| Crawler de Glue (consola) | AWS Glue | *Run crawler* | `trusted/` | Glue Data Catalog (BD `covid_trusted`) |
| `consultas.sql` | Athena / Hive / Spark / Lambda | usted, `hive -f`, `consultas_sparksql.py`, `lambda_api.py` | tablas catalogadas | pantalla / `athena-results/` |
| `consultas_sparksql.py` | EMR | `spark-submit` | `trusted/` o catálogo + `consultas.sql` | pantalla |
| `analitica_pyspark.py` | EMR | `spark-submit` | `trusted/` | `refined/` |
| `lambda_api.py` | AWS Lambda | **API Gateway** en cada petición HTTP | `consultas.sql` (en el .zip) + Athena | respuesta JSON |
| `app_streamlit.py` | EC2 (puerto 8501) | `streamlit run` / el navegador del usuario | API Gateway (HTTP) | página web |

**Idea clave para la sustentación:** `consultas.sql` es la **única fuente de verdad** de la lógica de
negocio en SQL. Lo ejecutan cuatro consumidores distintos (Athena, Hive, SparkSQL y la API) y
`analitica_pyspark.py` responde lo mismo con DataFrames. Comprobamos que **todas las respuestas
son idénticas** (ver sección 6).

---

## 6. Cómo se validó la solución (pruebas reales antes de entregar)

Todo el código se ejecutó de verdad con los datos reales, en un entorno local con **Spark 3.5.3 y
Java 17** (la misma versión de Spark de EMR 7.x):

1. `ingesta.py` descargó de verdad la API de datos.gov.co (**59.067** casos en 2 páginas de 50.000 y
   **1.122** municipios). S3 y MariaDB se reemplazaron por dobles de prueba que escriben en disco.
2. `etl_raw_a_trusted.py`: 159.067 casos en raw → **158.768** en trusted (**299 descartados** por ID
   repetido, ver hallazgo en el Punto 4). Se comprobó que solo lee la **última** `fecha_ingesta`.
3. `catalogo_hive.sql` se ejecutó contra los Parquet con un metastore Hive: tipos correctos, 158.768 filas.
4. `consultas.sql` en SparkSQL (vistas temporales y catálogo Hive) **==** `analitica_pyspark.py`
   (DataFrames) **==** SQLite, que divide enteros sin decimales igual que Athena, así que también
   sirvió para validar los `CAST ... AS DOUBLE`: **6 de 6 preguntas idénticas**.
5. `lambda_api.py` respondió a través de un servidor HTTP que imita API Gateway, con un Athena simulado,
   y `app_streamlit.py` se ejecutó sin navegador (`streamlit.testing.AppTest`): las 6 preguntas se
   muestran sin errores, con tabla, gráfica y mapa.

Lo que **no** se pudo probar localmente, porque requiere la cuenta AWS, es la ejecución dentro de los
servicios (Glue, Athena, EMR, Lambda y API Gateway). Para eso cada README trae los comandos exactos.

---

## 7. Resultados obtenidos (para comparar con los suyos en AWS)

| Pregunta | Respuesta principal |
|---|---|
| P1 Top departamentos | Bogotá D.C. 52.165 · Atlántico 35.248 · Valle 15.184 · Bolívar 12.516 · Antioquia 10.910 |
| P2 Tasa por 100 mil hab. | **Amazonas 3.051** (brote de Leticia) · Atlántico 1.295 · Bogotá 674 |
| P3 Regiones | Andina 73.459 casos (letalidad 3,23 %) · **Caribe letalidad 6,37 %** (la más alta) |
| P4 Edad y sexo | Más casos entre 20-39 años; la letalidad sube con la edad: 80+ hombres **46,2 %** |
| P5 Top días | 10-jul-2020 (6.801 casos) – 9 de los 10 días pico salen de los datos de la **URL** |
| P6 Top municipios | Bogotá · Barranquilla · Cali · Cartagena · Soledad · Medellín (con lat/lon) |

Las tablas completas están en `6_consultas_sql/README.md`. Los números en AWS deben coincidir,
salvo diferencias mínimas si el INS actualiza registros históricos en la API.

---

## 8. Preguntas probables de la sustentación (y cómo responderlas)

**¿Por qué tres fuentes y por qué esas?**
Simulan un escenario empresarial típico. Los *hechos* (casos) llegan como **archivos** exportados de
un sistema (EC2). Los *datos maestros* (departamentos, población, región) viven en una **base de datos
transaccional** (RDS MariaDB). Los datos externos llegan de **datos abiertos por URL** (API Socrata).

**¿Qué es raw, trusted y refined?**
Son las zonas del datalake (medallion: bronze, silver, gold).
- **raw:** copia fiel de la fuente; no se modifica y conserva el histórico por `fecha_ingesta`.
- **trusted:** datos limpios, tipados, homologados y sin duplicados, en Parquet.
- **refined:** respuestas de negocio listas para consumir.

**¿Por qué Parquet en trusted?**
Es columnar y comprimido (snappy) y guarda el esquema. Athena y Spark leen solo las columnas que
necesitan, así que se escanean menos datos y se paga menos en Athena: los 28 MB de CSV de casos
(archivo EC2 + URL) quedan en 2,2 MB de Parquet.

**¿Cómo es automática la ingesta?**
Con **cron** en la EC2: `ingesta.py` corre solo según el horario (ej. cada 6 horas) y deja un log.
Cada corrida crea una carpeta `fecha_ingesta=` nueva.

**¿Cuál es la diferencia entre el Crawler de Glue y Hive?**
- El **crawler** *infiere* el esquema leyendo los archivos.
- En **Hive** el esquema se *declara* con `CREATE EXTERNAL TABLE`.

Ambos guardan la definición en el **Glue Data Catalog**, porque el EMR lo usa como metastore. Por eso
Athena ve también las tablas creadas en Hive. Es *schema-on-read*: los datos quedan en S3 y la tabla
solo es metadato.

**¿Qué significa EXTERNAL TABLE?**
Que Hive no es dueño de los datos: si se borra la tabla, los archivos en S3 **no** se borran.

**¿Por qué el mismo SQL corre en Athena, Hive y Spark?**
Porque se escribió con reglas de portabilidad:
- `CAST(... AS DOUBLE)` antes de dividir, porque Athena (Trino) divide enteros sin decimales.
- `ORDER BY` solo con alias de la salida, porque Hive lo exige.
- Las columnas derivadas (`grupo_edad`, `dias_diagnostico`) se calculan en el ETL, porque las
  funciones de fecha cambian entre motores.

**¿Por qué Streamlit no está "detrás" de API Gateway directamente?**
Streamlit necesita **WebSockets** (`/_stcore/stream`) y el proxy HTTP de API Gateway no los soporta.
Por eso API Gateway expone la **API de datos** (Lambda + Athena) y Streamlit es el cliente que la
consume. Así cumplimos "aplicación expuesta mediante API Gateway con Streamlit".

**¿Qué limpieza hace el ETL?**
Ver la tabla de reglas en `4_preparacion/README.md`: homologación de 2 esquemas, fechas en 2 formatos,
deduplicación por ID, departamento derivado del municipio, edad en años, grupos de edad, indicador de
fallecido por COVID y días hasta el diagnóstico.

**¿Qué hallazgos de calidad encontraron?**
- 299 IDs repetidos con datos distintos en el archivo fuente.
- 1.449 casos con fecha de muerte pero estado "N/A": según el INS son muertes por **otras** causas,
  así que no se cuentan como fallecidos por COVID.
- 2 municipios con códigos DIVIPOLA antiguos que no cruzan con la DIVIPOLA actual.
- El archivo trata distritos como Barranquilla (8001) como si fueran "departamentos"; se corrigen
  derivando el departamento del código del municipio (8001 → 8 Atlántico).

**¿Por qué Glue y EMR para lo mismo?**
El enunciado pide "AWS Glue y Apache Spark". Un Job de Glue **es** Spark administrado y sin
servidores (*serverless*). El mismo script corre en Glue (sin administrar cluster) y en EMR (cluster
propio con Hadoop/Hive/Spark). Así se muestra que el código es portable.

---

## 9. Glosario rápido

| Término | Significado en este proyecto |
|---|---|
| Datalake | Almacenamiento central en S3 con datos en su formato original y por zonas |
| Medallion | Nombre de las zonas bronze/silver/gold = raw/trusted/refined |
| Glue Data Catalog | Catálogo de metadatos (BD, tablas, esquemas, ubicación en S3) compartido por Athena, Glue, EMR |
| Crawler | Proceso de Glue que recorre S3, infiere esquemas y crea tablas en el catálogo |
| Glue Job | Script Spark que AWS ejecuta sin que usted administre servidores |
| EMR | Cluster administrado de Hadoop/Spark/Hive |
| Schema-on-read | El esquema se aplica al leer; los archivos no cambian |
| Athena | Motor SQL serverless (Trino/Presto) que consulta S3 usando el Glue Data Catalog |
| SparkSQL | Módulo de Spark para ejecutar SQL sobre DataFrames/tablas |
| DataFrame | Tabla distribuida de Spark con columnas tipadas |
| API Gateway | Servicio que publica una URL HTTPS y la conecta con un backend (Lambda) |
| Lambda | Función que AWS ejecuta bajo demanda, sin servidor |
| DIVIPOLA | Codificación oficial DANE de departamentos (2 dígitos) y municipios (5 dígitos) |
| Letalidad | % de casos que fallecen = fallecidos / casos × 100 |

---

## 10. Limpieza de recursos (evitar consumo de créditos)

- **Terminar el cluster EMR** cuando termine: no se puede pausar (ver guía del curso).
- Detener la EC2 y la RDS cuando no las use.
- El Job de Glue, el Crawler, Athena, Lambda y API Gateway solo cobran cuando se ejecutan.
