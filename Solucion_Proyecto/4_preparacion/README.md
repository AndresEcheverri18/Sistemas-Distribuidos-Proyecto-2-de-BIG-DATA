# Punto 4 – Preparación de datos: raw → trusted (AWS Glue y Apache Spark)

**Archivo:** `etl_raw_a_trusted.py` (PySpark) · **Dónde corre:** **AWS Glue Job** (principal) y
**EMR** con `spark-submit` (mismo código).

## 4.1 Qué hace (visión general)

```
raw/ec2_archivos/casos_covid/   (CSV, columnas "ID de caso", fechas 6/3/2020)  ─┐ homologar ─┐
raw/url/casos_covid_api/        (CSV, columnas id_de_caso, fechas 2020-07-02)  ─┘ y unir    ─┼─► reglas de calidad ─► trusted/casos/          (Parquet)
raw/mariadb/departamentos/      (CSV)  ───────────────────────── tipar y depurar ─────────────────────────────────► trusted/departamentos/  (Parquet)
raw/url/divipola_municipios/    (CSV, lat "6,24")  ─────────────── tipar y depurar ────────────────────────────────► trusted/municipios/     (Parquet)
```

**Glue y Spark a la vez:** un *Job de AWS Glue* de tipo *Spark* es Apache Spark administrado y sin
servidores. El script usa `SparkSession` estándar (no `GlueContext`), así que el **mismo archivo**
corre en Glue y en el cluster EMR. Así se cumple "herramientas a utilizar: AWS Glue y Apache Spark".

---

## 4.2 Cómo ejecutarlo

### Opción A – AWS Glue Job (consola)

1. Subir el script: `aws s3 cp etl_raw_a_trusted.py s3://$BUCKET/scripts/`
2. Consola **AWS Glue → ETL jobs → Script editor** → *Engine:* **Spark** → *Upload script* → elegir
   `etl_raw_a_trusted.py`.
3. Pestaña **Job details**:

   | Campo | Valor |
   |---|---|
   | Name | `covid-etl-raw-trusted` |
   | IAM Role | **LabRole** |
   | Type / Glue version | Spark / **Glue 4.0** |
   | Worker type / Number of workers | G.1X / **2** |
   | Script path | `s3://<bucket>/scripts/` |
   | Job parameters | Key `--datalake` · Value `s3://<bucket>` |

4. **Save** → **Run**. En *Runs* el estado debe quedar en **Succeeded** (unos 2 minutos).
5. Los `print()` del script quedan en **CloudWatch → Log groups → `/aws-glue/jobs/output`**.

### Opción A' – AWS Glue Job (CLI, equivalente)

```bash
aws glue create-job --name covid-etl-raw-trusted --role LabRole \
  --glue-version 4.0 --worker-type G.1X --number-of-workers 2 \
  --command Name=glueetl,ScriptLocation=s3://$BUCKET/scripts/etl_raw_a_trusted.py,PythonVersion=3 \
  --default-arguments "{\"--datalake\":\"s3://$BUCKET\"}"

aws glue start-job-run --job-name covid-etl-raw-trusted
aws glue get-job-runs --job-name covid-etl-raw-trusted --query "JobRuns[0].[JobRunState,ExecutionTime]"
```

### Opción B – Apache Spark en EMR (mismo script)

En el nodo master (`ssh -i labsuser.pem hadoop@<master-dns>`):

```bash
spark-submit --deploy-mode cluster s3://$BUCKET/scripts/etl_raw_a_trusted.py --datalake s3://$BUCKET
# o como Step del cluster (consola EMR → Steps → Add step → Spark application) con los mismos argumentos
```

### Verificar

```bash
aws s3 ls s3://$BUCKET/trusted/ --recursive --human-readable
```

### Salida real de la prueba

```
1. Leyendo zona RAW (ultima carga de cada fuente)
   .../raw/ec2_archivos/casos_covid -> ultima fecha_ingesta = 2026-11-17
   .../raw/url/casos_covid_api -> ultima fecha_ingesta = 2026-11-17
   .../raw/mariadb/departamentos -> ultima fecha_ingesta = 2026-11-17
   .../raw/url/divipola_municipios -> ultima fecha_ingesta = 2026-11-17
2. Homologando y uniendo las 2 fuentes de casos
3. Aplicando reglas de calidad y escribiendo zona TRUSTED (Parquet)
   escrito .../trusted/casos
   escrito .../trusted/departamentos
   escrito .../trusted/municipios
4. Resumen: casos raw = 159067 | casos trusted = 158768 | descartados = 299
+-----------+-----+
|     fuente|count|
+-----------+-----+
|ec2_archivo|99701|
|    url_api|59067|
+-----------+-----+
root
 |-- id_caso: integer          |-- tipo_contagio: string
 |-- fecha_reporte: date       |-- estado: string
 |-- fecha_notificacion: date  |-- recuperado: string
 |-- cod_departamento: integer |-- fallecido: integer
 |-- cod_municipio: integer    |-- fecha_inicio_sintomas: date
 |-- municipio: string         |-- fecha_diagnostico: date
 |-- edad: integer             |-- fecha_recuperacion: date
 |-- grupo_edad: string        |-- fecha_muerte: date
 |-- sexo: string              |-- dias_diagnostico: integer
                               |-- fuente: string
```

---

## 4.3 Reglas de preparación (lo que debe saber explicar)

| # | Regla | Por qué | Código |
|---|---|---|---|
| 1 | Leer **solo la última carga** de cada fuente | raw guarda histórico; el ETL debe procesar la foto más reciente | `leer_ultima_ingesta()` |
| 2 | **Homologar esquemas**: dos fuentes con nombres distintos se llevan a un esquema común | El archivo dice `"ID de caso"` y la API `id_de_caso`; son el mismo dato | `MAPEO_CASOS_EC2`, `MAPEO_CASOS_URL`, `homologar_casos()` |
| 3 | **Fechas** de texto a `DATE`, con un formato distinto por fuente | Archivo: `6/3/2020 0:00:00` (d/M/yyyy); API: `2020-07-02 00:00:00` (yyyy-MM-dd) | `homologar_casos()` |
| 4 | **Linaje**: columna `fuente` = `ec2_archivo` / `url_api` | Saber de dónde vino cada registro | `homologar_casos()` |
| 5 | Tipos numéricos (`cast("int")`) | Todo llega como texto desde CSV | `preparar_casos()` |
| 6 | Descartar registros sin ID, sin fecha de reporte o sin municipio | No se pueden ubicar ni contar | `preparar_casos()` |
| 7 | **Deduplicar por `id_caso`** | Un caso no puede contarse dos veces, aunque venga en las dos fuentes | `dropDuplicates(["id_caso"])` |
| 8 | **Departamento derivado del municipio**: `cod_departamento = floor(cod_municipio / 1000)` | El archivo trata a Barranquilla (8001), Cartagena (13001) y Santa Marta (47001) como "departamentos". Con esto 8001 → 8 Atlántico y 13001 → 13 Bolívar, y cruza con MariaDB | `preparar_casos()` |
| 9 | **Edad en años** (unidad 2 = meses y 3 = días → 0 años) y válida en 0–120 | Un bebé de 5 meses venía como `edad=5` | `preparar_casos()` |
| 10 | **Grupo de edad** `00-09`, `10-19`, …, `80+` | Se calcula una vez aquí para que el SQL sea igual en todos los motores | `preparar_casos()` |
| 11 | Normalizar textos: `sexo` en M/F; categorías con mayúscula inicial; vacío o `N/A` → `Sin dato` | Había `fallecido` y `Fallecido` como valores distintos | `categoria()` |
| 12 | **`fallecido` = 1/0** solo si estado o recuperado = `Fallecido` | Permite `SUM(fallecido)`. Las filas con fecha de muerte y estado `N/A` son muertes por **otras causas** (INS) | `preparar_casos()` |
| 13 | **`dias_diagnostico`** = diagnóstico − inicio de síntomas (0–90) | Mide la oportunidad del diagnóstico; los valores negativos o absurdos → null | `preparar_casos()` |
| 14 | Lat/lon `"6,246631"` → `6.246631` (double) | La API usa coma decimal | `preparar_municipios()` |
| 15 | Escribir en **Parquet** con `mode("overwrite")` | Formato columnar y tipado; reprocesar reemplaza la versión anterior | `escribir()` |

### Hallazgos de calidad encontrados

- **299 IDs repetidos con datos diferentes** en el archivo de la EC2. Por ejemplo, el ID 10251
  aparece para una mujer de 51 años de Malambo y para un hombre de 19 años de Cali. Es una
  inconsistencia de la fuente. Como el ID debe ser único, se conserva un registro por ID: por eso
  quedan 99.701 de 100.000.
- **1.449 casos** con fecha de muerte pero estado "N/A". No se cuentan como muertes por COVID.
- **2 municipios** (52517 "MAGUI (PAYAN)" y 94663 "MAPIRIPANA (CD)", así vienen en el archivo) con
  códigos que no existen en la DIVIPOLA actual. No afectan las preguntas por departamento; en la P6 quedan fuera del cruce.

---

## 4.4 Explicación del código `etl_raw_a_trusted.py`

### Árbol de llamadas

```
Glue (Run) / spark-submit
└── main()
    ├── argparse.parse_known_args()        ← toma --datalake e ignora los argumentos propios de Glue
    ├── crear_spark()
    ├── leer_ultima_ingesta(spark, ruta) ×4        (casos EC2, casos URL, departamentos, municipios)
    ├── homologar_casos(casos_ec2, MAPEO_CASOS_EC2, "d/M/yyyy",   "ec2_archivo")
    ├── homologar_casos(casos_url, MAPEO_CASOS_URL, "yyyy-MM-dd", "url_api")
    │   └── .unionByName(...)              ← une las 2 fuentes por NOMBRE de columna
    ├── preparar_casos(casos)
    │   └── categoria(columna) ×3          (tipo_contagio, estado, recuperado)
    ├── escribir(casos, trusted/casos)
    ├── escribir(preparar_departamentos(departamentos), trusted/departamentos)
    └── escribir(preparar_municipios(municipios), trusted/municipios)
        └── coordenada() ×2                (función interna: latitud y longitud)
```

### Constantes

- **`MAPEO_CASOS_EC2` / `MAPEO_CASOS_URL`**: diccionarios `{nombre_destino: nombre_en_origen}`. Son
  la "tabla de homologación": la misma clave (`id_caso`) apunta a la columna correspondiente de cada
  fuente (`"ID de caso"` y `"id_de_caso"`).
- **`COLUMNAS_FECHA`**: las 6 columnas que se convierten de texto a fecha.
- **`FORMATO_FECHA_EC2 = "d/M/yyyy"`** y **`FORMATO_FECHA_URL = "yyyy-MM-dd"`**: patrones de fecha de Java.
  `d` y `M` sin repetir aceptan 1 o 2 dígitos (`6/3/2020` y `27/12/2020`).
- **`COLUMNAS_CASOS`**: el esquema final (orden de columnas) de `trusted/casos`. Es exactamente el
  mismo que se declara en `5_catalogacion/catalogo_hive.sql`.

### Funciones

**`crear_spark()`**
- `SparkSession.builder...getOrCreate()`: en Glue y EMR la sesión ya existe y la reutiliza; en local
  la crea.
- `spark.sql.ansi.enabled = false`: si un texto no se puede convertir (ej. `cast("abc" as int)`)
  devuelve `null` en vez de abortar el job.
- `spark.sql.legacy.timeParserPolicy = CORRECTED`: usa el parser de fechas moderno y devuelve `null`
  si una fecha no cuadra con el patrón.
- `mapreduce.fileoutputcommitter.marksuccessfuljobs = false`: Spark no crea el archivo vacío
  `_SUCCESS`, que el crawler de Glue podría intentar leer como dato.

**`leer_ultima_ingesta(spark, ruta)`**
- `spark.read.option("header", True).csv(ruta)`: lee **todas** las subcarpetas
  `fecha_ingesta=...`. Spark hace *partition discovery*: convierte el nombre de la carpeta en una
  columna `fecha_ingesta`. Todas las demás columnas quedan como texto (no se infiere el esquema).
- `df.agg(F.max("fecha_ingesta")).first()[0]`: busca la fecha más reciente. Es una acción, así que
  Spark ejecuta en ese momento.
- `filter(...== ultima).drop("fecha_ingesta")`: deja solo esa carga y quita la columna auxiliar.
- *Se probó:* con una carga vieja (2026-11-10) con datos falsos, el ETL la ignoró.

**`categoria(columna)`**: devuelve una **expresión** de columna (no un DataFrame).
`F.when(vacío o N/A → "Sin dato").otherwise(F.initcap(F.lower(...)))`. Así `fallecido` y `FALLECIDO`
quedan como `Fallecido`.

**`homologar_casos(df, mapeo, formato_fecha, fuente)`**
1. `df.select([F.col(origen).alias(destino) for destino, origen in mapeo.items()])`: una *list
   comprehension* que elige las 15 columnas útiles y las renombra al esquema común. Las columnas que
   no están en el mapeo (país, etnia…) se descartan.
2. Para cada fecha: `F.split(col, " ").getItem(0)` se queda con la parte antes del espacio
   (`"6/3/2020"`) y `F.to_date(..., formato)` la convierte a `DATE`.
3. `F.lit(fuente)` agrega una columna constante con el origen.

Se llama **dos veces** y después `unionByName` las une: se apilan las filas emparejando las columnas
por nombre y no por posición, lo que es más seguro.

**`preparar_casos(df)`**: aplica las reglas 5 a 13 en orden.
- `.cast("int")` en las columnas numéricas.
- `.filter(... isNotNull ...)` y `.dropDuplicates(["id_caso"])`.
- `F.floor(cod_municipio / 1000)`: 11001 / 1000 = 11,001 → floor = 11 (Bogotá).
- Edad: `when(unidad_edad in (2,3) → 0).otherwise(edad)`, luego `when(between(0,120))`, y lo que no
  cumple queda null.
- `grupo_edad`: `decada = floor(edad/10)*10`. Si `decada >= 80` → `"80+"`. Si no,
  `lpad(decada,2,"0") + "-" + lpad(decada+9,2,"0")`, por ejemplo 5 años → `"00-09"`. El cero a la
  izquierda hace que el orden alfabético sea correcto.
- `fallecido`: `when(recuperado == "Fallecido" | estado == "Fallecido", 1).otherwise(0)`.
- `dias_diagnostico`: `F.datediff(fin, inicio)` en días, y solo si está entre 0 y 90.
- `select(COLUMNAS_CASOS)`: fija el orden final de columnas.

**`preparar_departamentos(df)`**: castea código y población a `int`, quita espacios con `trim` y
deduplica por código.

**`preparar_municipios(df)`**: `cod_mpio "05001"` → `int 5001`. Las coordenadas usan la función
interna `coordenada()`: `regexp_replace(",", ".")` y luego `cast("double")`.

**`escribir(df, ruta)`**: `coalesce(1)` junta los datos en **un solo archivo** Parquet, porque son
datos pequeños (en producción con GB se dejan muchos archivos o se particiona).
`mode("overwrite")` reemplaza la versión anterior.

**`main()`**
- `parse_known_args()` y no `parse_args()`: Glue agrega sus propios argumentos (`--JOB_ID`,
  `--JOB_RUN_ID`, `--TempDir`…) y con `parse_args()` el script fallaría al no reconocerlos.
- `casos.count()` antes de limpiar y `preparar_casos(casos).cache()` después: `cache()` guarda el
  resultado en memoria para no recalcularlo en el `count()`, el `write` y el `groupBy` del resumen.
- Imprime un resumen de calidad (raw, trusted, descartados) y el esquema final: evidencias para el
  informe.

### Concepto clave: transformaciones *lazy* vs acciones

Casi todo el código (`select`, `withColumn`, `filter`, `join`) son **transformaciones**: Spark solo
arma un plan de ejecución. El trabajo distribuido ocurre en las **acciones**: `count()`, `first()`,
`write`, `show()`. Por eso el ETL "se demora" en los `count()` y en las escrituras, no en las
líneas de transformación.

---

## Evidencias para el informe (Punto 4)

1. Consola Glue: configuración del Job (LabRole, Glue 4.0, parámetro `--datalake`).
2. Consola Glue → Runs: ejecución **Succeeded** con su duración.
3. CloudWatch (`/aws-glue/jobs/output`): el resumen `casos raw = ... | trusted = ...` y el esquema.
4. EMR: `spark-submit` del mismo script terminando OK (evidencia de "Apache Spark").
5. Consola S3: `trusted/casos/`, `trusted/departamentos/` y `trusted/municipios/` con sus `.parquet`.
6. Extracto de código: `homologar_casos()` y la tabla de reglas 4.3.
