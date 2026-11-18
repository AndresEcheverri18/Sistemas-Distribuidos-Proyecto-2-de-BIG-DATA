# Punto 7 – Procesamiento analítico descriptivo con PySpark (API de DataFrames)

**Archivo:** `analitica_pyspark.py` · **Dónde corre:** EMR (`spark-submit`) · **Escribe en:** `refined/`

## 7.1 Qué hace

Responde **las mismas 6 preguntas** del Punto 6, pero **sin SQL**, usando la API de DataFrames de
PySpark (`join`, `groupBy`, `agg`, `withColumn`, `orderBy`, `limit`). Guarda cada respuesta en la
zona **refined** (gold) en **CSV** (para personas y Excel) y en **Parquet** (para otros procesos
analíticos):

```
trusted/casos ─┐
trusted/departamentos ─┼─► p1_...() ... p6_...() ─► refined/p1_top_departamentos/{csv,parquet}
trusted/municipios ─┘                               ...
                                                    refined/p6_top_municipios/{csv,parquet}
```

Esto replica el formato del Lab 3-3 del curso: las preguntas se responden con DataFrames **y** con
SparkSQL (Punto 6), y las salidas se guardan en `.csv` y `.parquet`.

---

## 7.2 Cómo ejecutarlo

En el nodo master del EMR:

```bash
aws s3 cp s3://$BUCKET/scripts/analitica_pyspark.py .
spark-submit analitica_pyspark.py --datalake s3://$BUCKET
aws s3 ls s3://$BUCKET/refined/ --recursive --human-readable
aws s3 cp s3://$BUCKET/refined/p1_top_departamentos/csv/ ./p1_csv --recursive && cat p1_csv/*.csv   # ver un CSV
```

Alternativa con notebook: en **JupyterHub del EMR** (puerto 9443, kernel PySpark) se puede pegar el
contenido de las funciones `p1_...` a `p6_...`. En EMR `spark` ya existe, así que no hace falta
`main()`. Sirve para tener evidencias celda por celda.

Salida real (extracto):

```
Casos en trusted: 158768

===== p1_top_departamentos: P1 - Top 10 departamentos con mas casos de COVID-19. =====
+---------------+-----------+----------------+
|departamento   |total_casos|total_fallecidos|
+---------------+-----------+----------------+
|Bogotá D.C.    |52165      |1768            |
|Atlántico      |35248      |2268            |
...
   guardado en s3://<bucket>/refined/p1_top_departamentos/csv y .../parquet
```

Las tablas completas son idénticas a las de `6_consultas_sql/README.md` (sección 6.4); se verificó
fila por fila.

---

## 7.3 Explicación del código `analitica_pyspark.py`

### Árbol de llamadas

```
spark-submit
└── main()
    ├── argparse: --datalake
    ├── SparkSession + (sin archivo _SUCCESS)
    ├── casos = spark.read.parquet(trusted/casos).cache()      ← se usa 6 veces: se guarda en memoria
    ├── departamentos, municipios = spark.read.parquet(...)
    └── for nombre, funcion in PREGUNTAS.items():               ← diccionario nombre → función
            resultado = funcion(casos, departamentos, municipios)
            ├── p1_top_departamentos(...)
            ├── p2_tasa_por_100k(...)
            ├── p3_regiones(...)        └── letalidad("total_fallecidos", "total_casos")
            ├── p4_edad_sexo(...)       └── letalidad(...)
            ├── p5_top_dias(...)
            └── p6_top_municipios(...)
            resultado.show()                     ← evidencia en pantalla
            resultado.write...csv(refined/<nombre>/csv)
            resultado.write...parquet(refined/<nombre>/parquet)
```

**Patrón de diseño:** todas las funciones `pN_...` reciben **los mismos 3 parámetros** y devuelven
un DataFrame. Por eso `main()` las recorre con un solo `for` desde el diccionario `PREGUNTAS`.
Agregar una pregunta nueva = escribir una función y agregar una línea al diccionario.

### Equivalencia DataFrame ↔ SQL (para explicar en la sustentación)

| SQL (Punto 6) | PySpark DataFrame (Punto 7) |
|---|---|
| `FROM casos c JOIN departamentos d ON c.cod_departamento = d.cod_departamento` | `casos.join(departamentos, "cod_departamento")` |
| `GROUP BY d.departamento` | `.groupBy("departamento")` |
| `COUNT(*) AS total_casos` | `F.count("*").alias("total_casos")` |
| `SUM(c.fallecido)` | `F.sum("fallecido")` |
| `AVG(c.dias_diagnostico)` | `F.avg("dias_diagnostico")` |
| `ROUND(x, 2)` | `F.round(x, 2)` |
| columna calculada en el `SELECT` | `.withColumn("casos_por_100k", ...)` |
| `WHERE c.sexo IN ('M','F')` | `.filter(F.col("sexo").isin("M", "F"))` |
| `ORDER BY total_casos DESC` | `.orderBy(F.desc("total_casos"))` |
| `LIMIT 10` | `.limit(10)` |

Las dos formas generan **el mismo plan de ejecución** en Spark (optimizador Catalyst). La diferencia
es solo de sintaxis.

### Función por función

**`letalidad(fallecidos, casos)`**: devuelve la *expresión*
`round(fallecidos * 100 / casos, 2)`. En Spark `/` siempre devuelve decimales, así que aquí no hace
falta `CAST`. *La usan `p3_regiones` y `p4_edad_sexo`.*

**`p1_top_departamentos`**: `join` por `cod_departamento` (las dos tablas tienen esa columna, y al
pasar el nombre como texto Spark deja **una sola** columna en el resultado). Después `groupBy`
departamento, `count` y `sum(fallecido)`, se ordena descendente y `limit(10)`.

**`p2_tasa_por_100k`**: igual que P1, pero agrupa también por `poblacion` para poder usarla después
del `agg`. Con `withColumn` se calcula `total_casos * 100000 / poblacion`.

**`p3_regiones`**: agrupa por `region` y calcula 3 agregados (`count`, `sum`, `avg`). La letalidad se
agrega después con `withColumn`, y el `select` final deja las columnas en el mismo orden que el SQL.

**`p4_edad_sexo`**: `filter` de sexo válido, `groupBy` por dos columnas y orden ascendente por
`grupo_edad` y `sexo`.

**`p5_top_dias`**: `groupBy("fecha_reporte")`, el top 10 por casos.

**`p6_top_municipios`**:
- `casos.drop("municipio")`: el caso trae el nombre del municipio como venía en la fuente, y
  `municipios` trae el nombre **oficial DIVIPOLA**. Se borra el primero para que no haya dos columnas
  `municipio` (ambigüedad) y quede el oficial.
- `municipios.select(...)` y `departamentos.select(...)` eligen solo las columnas necesarias, para no
  duplicar `cod_departamento`.
- Dos `join` y luego `groupBy` por municipio, departamento, latitud y longitud.

**`main()`**
- `.cache()` en `casos`: las 6 preguntas leen la misma tabla, así que se carga a memoria una sola vez.
- `funcion.__doc__`: imprime el *docstring* de la función como título de la pregunta.
- `.coalesce(1)`: el resultado es pequeño, así que se escribe un solo archivo por formato.
- `.write.mode("overwrite").option("header", True).csv(...)`: CSV con encabezado.
- `.write.mode("overwrite").parquet(...)`: Parquet que conserva los tipos (bigint, double, date).

---

## Evidencias para el informe (Punto 7)

1. Terminal EMR: `spark-submit analitica_pyspark.py ...` mostrando las 6 tablas.
2. Consola S3: carpeta `refined/` con las 6 preguntas y sus subcarpetas `csv/` y `parquet/`.
3. Contenido de un CSV de refined (abierto en S3 o con `aws s3 cp ... -`).
4. Extracto de código: una función, por ejemplo `p2_tasa_por_100k`, al lado de su SQL equivalente.
