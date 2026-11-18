# Punto 6 – Consultas analíticas con SQL (Athena, EMR/Hive y SparkSQL)

**Archivos:**
- `consultas.sql`: las 6 preguntas de negocio en SQL, **un solo archivo para los 3 motores**.
- `consultas_sparksql.py`: ejecuta ese mismo archivo con `spark.sql()`.

```
                  consultas.sql  (única fuente de verdad del SQL de negocio)
          ┌──────────────┬──────────────┼───────────────┬────────────────────┐
          ▼              ▼              ▼               ▼                    │
     AWS Athena      Hive (EMR)     SparkSQL (EMR)   Lambda (Punto 8) ──► Athena
   (covid_trusted)  (covid_hive)  consultas_sparksql.py
```

---

## 6.1 Cómo ejecutarlo en cada motor

### A) AWS Athena

1. Consola **Athena → Query editor → Settings → Manage** → *Query result location*:
   `s3://<bucket>/athena-results/` (solo la primera vez).
2. *Data source:* `AwsDataCatalog` · *Database:* **`covid_trusted`**.
3. Copiar **una consulta a la vez** de `consultas.sql` (desde `SELECT` hasta `;`) y dar **Run**.
4. Observar en cada ejecución *Run time* y *Data scanned*. Gracias a Parquet se escanean pocos KB/MB.

Opcional: las mismas consultas funcionan con *Database:* `covid_hive`, las tablas creadas por Hive.

### B) EMR / Hive

```bash
aws s3 cp s3://$BUCKET/scripts/consultas.sql .
hive --database covid_hive -f consultas.sql          # tablas creadas con Hive DDL
hive --database covid_trusted -f consultas.sql       # tablas creadas por el crawler (mismo catálogo)
```

También en el editor Hive de **Hue** (puerto 8888), seleccionando la base de datos a la izquierda.

### C) SparkSQL

```bash
aws s3 cp s3://$BUCKET/scripts/consultas_sparksql.py .
spark-submit consultas_sparksql.py --datalake s3://$BUCKET                           # vistas sobre Parquet
spark-submit consultas_sparksql.py --datalake s3://$BUCKET --database covid_trusted  # tablas del Glue Catalog
```

`consultas.sql` debe estar en la **misma carpeta** que el `.py`. Por defecto `spark-submit` usa *client
mode*: el driver corre en el master y lee el archivo local.

---

## 6.2 Explicación de cada consulta (`consultas.sql`)

Tablas usadas: `casos c` (hechos), `departamentos d` y `municipios m` (dimensiones). Es un **modelo
estrella**: los JOIN se hacen por los códigos DIVIPOLA.

### P1 – Top 10 departamentos con más casos
```sql
SELECT d.departamento, COUNT(*) AS total_casos, SUM(c.fallecido) AS total_fallecidos
FROM casos c JOIN departamentos d ON c.cod_departamento = d.cod_departamento
GROUP BY d.departamento ORDER BY total_casos DESC LIMIT 10;
```
- `JOIN` trae el **nombre** del departamento desde MariaDB, porque el caso solo tiene el código.
- `COUNT(*)` cuenta las filas, es decir los casos, de cada grupo.
- `SUM(c.fallecido)` cuenta las muertes: `fallecido` vale 1 o 0, así que la suma es el conteo.
- `ORDER BY ... DESC LIMIT 10` deja los 10 mayores.

### P2 – Tasa de contagio por 100.000 habitantes
```sql
ROUND(CAST(COUNT(*) AS DOUBLE) * 100000 / d.poblacion, 2) AS casos_por_100k
```
- La tasa es casos / población × 100.000, para comparar departamentos de distinto tamaño.
- **`CAST(... AS DOUBLE)`** es necesario porque en **Athena (Trino)** `entero / entero` da un entero
  truncado: `2411*100000/79020` daría `3051` en vez de `3051.13`.
- `d.poblacion` va también en el `GROUP BY`, porque toda columna no agregada debe agruparse.

### P3 – Indicadores por región natural
```sql
ROUND(CAST(SUM(c.fallecido) AS DOUBLE) * 100 / COUNT(*), 2) AS letalidad_pct,
ROUND(AVG(c.dias_diagnostico), 1)                           AS prom_dias_diagnostico
```
- **Letalidad** = fallecidos / casos × 100.
- `AVG` ignora los `NULL`: los casos sin fecha de síntomas no afectan el promedio.

### P4 – Edad y sexo
```sql
WHERE c.sexo IN ('M', 'F') GROUP BY c.grupo_edad, c.sexo ORDER BY grupo_edad, sexo;
```
- Agrupación por **dos** columnas: 9 grupos × 2 sexos = 18 filas.
- `grupo_edad` ya viene calculado del ETL (`"00-09"`…`"80+"`), así que el SQL es igual en todos los
  motores.
- `ORDER BY grupo_edad, sexo` usa los **alias de salida**, sin el prefijo `c.`. **Hive** exige esto
  después de un `GROUP BY`.

### P5 – Top 10 días
```sql
SELECT c.fecha_reporte, COUNT(*) AS total_casos, SUM(c.fallecido) AS total_fallecidos
FROM casos c GROUP BY c.fecha_reporte ORDER BY total_casos DESC LIMIT 10;
```
- Agrupa por fecha. Integra los datos del archivo (hasta el 1-jul) con los de la URL (2–15 jul).

### P6 – Top 10 municipios con ubicación
```sql
FROM casos c
JOIN municipios m    ON c.cod_municipio = m.cod_municipio
JOIN departamentos d ON c.cod_departamento = d.cod_departamento
```
- **Dos JOIN**, es decir **tres fuentes en una consulta**: casos (EC2 + URL), municipios (URL
  DIVIPOLA) y departamentos (MariaDB).
- `m.latitud` y `m.longitud` alimentan el mapa de Streamlit.

### Reglas de portabilidad aplicadas (por qué el mismo SQL corre en los 3 motores)

| Diferencia entre motores | Solución usada |
|---|---|
| Athena divide enteros sin decimales | `CAST(... AS DOUBLE)` antes de dividir |
| Hive no acepta `ORDER BY c.columna` después de `GROUP BY` | `ORDER BY` con alias de salida |
| Funciones de fecha distintas (`date_diff` vs `datediff`, formatos) | `grupo_edad` y `dias_diagnostico` calculados en el ETL |
| Athena ejecuta una sentencia a la vez | Cada consulta termina en `;` y se copia por separado |
| La API necesita identificar cada consulta | Comentario con formato `-- P1 \| título` |

---

## 6.3 Explicación del código `consultas_sparksql.py`

### Árbol de llamadas

```
spark-submit
└── main()
    ├── argparse: --datalake (obligatorio), --database (opcional), --sql (por defecto: consultas.sql junto al .py)
    ├── si --database:  SparkSession con enableHiveSupport() → spark.sql("USE covid_trusted")
    │   si no:          spark.read.parquet(trusted/<tabla>).createOrReplaceTempView(<tabla>) ×3
    └── for (id, titulo, sql) in leer_consultas(ruta_sql):
            spark.sql(sql).show(truncate=False)
```

**`leer_consultas(ruta_sql)`**: convierte el archivo SQL en una lista de `(id, título, sql)`.
1. Lee todo el archivo y lo parte por `;`, así que cada bloque es una consulta.
2. Recorre las líneas del bloque:
   - si coincide con la expresión regular `--\s*(P\d+)\s*\|\s*(.+)`, de ahí salen el id (`P1`) y el
     título;
   - si es otro comentario o una línea vacía, la ignora;
   - si no, es SQL y la guarda.
3. Solo agrega el bloque si tiene id **y** SQL. Así el encabezado del archivo, que es puro
   comentario, se descarta.

*La misma función existe en `8_visualizacion_api/lambda_api.py`, que devuelve un diccionario en vez de
una lista. Se repite a propósito para que cada componente sea autónomo y no dependa del otro.*

**Dos modos de acceso a las tablas:**
- **Vistas temporales** (por defecto): `createOrReplaceTempView("casos")` registra el DataFrame con
  ese nombre **solo en esta sesión**. No depende de ningún catálogo y funciona en cualquier lugar.
- **Catálogo** (`--database`): `enableHiveSupport()` conecta Spark al metastore, que en EMR es el
  **Glue Data Catalog**. Demuestra que SparkSQL usa las tablas catalogadas en el Punto 5.

---

## 6.4 Resultados esperados (validados: Spark DataFrames = SparkSQL = SQLite)

**P1**

| departamento | total_casos | total_fallecidos |
|---|---|---|
| Bogotá D.C. | 52165 | 1768 |
| Atlántico | 35248 | 2268 |
| Valle del Cauca | 15184 | 785 |
| Bolívar | 12516 | 520 |
| Antioquia | 10910 | 242 |
| Nariño | 4691 | 186 |
| Cundinamarca | 4508 | 159 |
| Magdalena | 2953 | 246 |
| Sucre | 2595 | 212 |
| Amazonas | 2411 | 106 |

**P2**

| departamento | poblacion | total_casos | casos_por_100k |
|---|---|---|---|
| Amazonas | 79020 | 2411 | 3051.13 |
| Atlántico | 2722128 | 35248 | 1294.87 |
| Bogotá D.C. | 7743955 | 52165 | 673.62 |
| Bolívar | 2180976 | 12516 | 573.87 |
| Chocó | 544764 | 2200 | 403.84 |
| Valle del Cauca | 4532152 | 15184 | 335.03 |
| Nariño | 1627589 | 4691 | 288.22 |
| Sucre | 949252 | 2595 | 273.37 |
| Magdalena | 1427026 | 2953 | 206.93 |
| Antioquia | 6677930 | 10910 | 163.37 |

**P3**

| region | total_casos | total_fallecidos | letalidad_pct | prom_dias_diagnostico |
|---|---|---|---|---|
| Andina | 73459 | 2376 | 3.23 | 9.9 |
| Caribe | 57662 | 3671 | 6.37 | 11.6 |
| Pacífica | 22869 | 1120 | 4.9 | 10.6 |
| Amazonía | 2782 | 126 | 4.53 | 10.8 |
| Orinoquía | 1972 | 25 | 1.27 | 10.3 |
| Insular | 24 | 0 | 0.0 | 9.5 |

**P4**

| grupo_edad | sexo | total_casos | total_fallecidos | letalidad_pct |
|---|---|---|---|---|
| 00-09 | F | 3026 | 6 | 0.2 |
| 00-09 | M | 2988 | 7 | 0.23 |
| 10-19 | F | 4952 | 3 | 0.06 |
| 10-19 | M | 5674 | 8 | 0.14 |
| 20-29 | F | 16076 | 42 | 0.26 |
| 20-29 | M | 18806 | 61 | 0.32 |
| 30-39 | F | 16839 | 96 | 0.57 |
| 30-39 | M | 20464 | 156 | 0.76 |
| 40-49 | F | 11870 | 152 | 1.28 |
| 40-49 | M | 13640 | 379 | 2.78 |
| 50-59 | F | 9581 | 327 | 3.41 |
| 50-59 | M | 11107 | 741 | 6.67 |
| 60-69 | F | 5809 | 643 | 11.07 |
| 60-69 | M | 6833 | 1128 | 16.51 |
| 70-79 | F | 3059 | 649 | 21.22 |
| 70-79 | M | 3779 | 1179 | 31.2 |
| 80+ | F | 2124 | 752 | 35.4 |
| 80+ | M | 2141 | 989 | 46.19 |

**P5**

| fecha_reporte | total_casos | total_fallecidos |
|---|---|---|
| 2020-07-10 | 6801 | 225 |
| 2020-07-14 | 5622 | 230 |
| 2020-07-09 | 5337 | 294 |
| 2020-07-15 | 5271 | 263 |
| 2020-07-12 | 5084 | 202 |
| 2020-07-11 | 4586 | 208 |
| 2020-07-07 | 4213 | 188 |
| 2020-06-27 | 4145 | 185 |
| 2020-07-08 | 4144 | 219 |
| 2020-07-04 | 3891 | 184 |

**P6**

| municipio | departamento | latitud | longitud | total_casos |
|---|---|---|---|---|
| BOGOTÁ, D.C. | Bogotá D.C. | 4.649251 | -74.106992 | 52165 |
| BARRANQUILLA | Atlántico | 10.977961 | -74.815546 | 19442 |
| SANTIAGO DE CALI | Valle del Cauca | 3.413686 | -76.52133 | 11480 |
| CARTAGENA DE INDIAS | Bolívar | 10.385126 | -75.496269 | 11115 |
| SOLEDAD | Atlántico | 10.909921 | -74.786054 | 8615 |
| MEDELLÍN | Antioquia | 6.246631 | -75.581775 | 5794 |
| LETICIA | Amazonas | -4.19895 | -69.941721 | 2279 |
| SINCELEJO | Sucre | 9.302322 | -75.395445 | 2053 |
| SOACHA | Cundinamarca | 4.579268 | -74.215463 | 1762 |
| BUENAVENTURA | Valle del Cauca | 3.875708 | -77.01074 | 1715 |

---

## Evidencias para el informe (Punto 6)

1. **Athena**: pantallazo de cada una de las 6 consultas con su resultado, *Run time* y *Data scanned*.
2. **Hive**: terminal con `hive --database covid_hive -f consultas.sql`, o Hue con al menos 2 consultas.
3. **SparkSQL**: salida de `spark-submit consultas_sparksql.py` (ambos modos si es posible).
4. Comentario en el informe: los tres motores dan **los mismos números** con el **mismo archivo SQL**.
