-- =====================================================================
-- PUNTO 6 - Consultas analiticas descriptivas en SQL (preguntas de negocio)
--
-- UN SOLO archivo SQL portable que se ejecuta igual en:
--   * AWS Athena   (base de datos covid_trusted, creada por el crawler de Glue)
--   * EMR / Hive   hive --database covid_hive -f consultas.sql
--   * SparkSQL     spark-submit consultas_sparksql.py --datalake s3://<bucket>
--   * La API (Lambda + API Gateway) lee este archivo y lo ejecuta en Athena
--
-- Reglas de portabilidad (Athena/Trino, Hive y Spark usan dialectos distintos):
--   - CAST(... AS DOUBLE) antes de dividir (Athena divide enteros sin decimales)
--   - ORDER BY solo con alias de la salida (exigencia de Hive)
--   - columnas derivadas ya calculadas en el ETL (grupo_edad, dias_diagnostico...)
-- Formato: la linea "-- Pn | titulo" identifica cada pregunta y cada consulta
-- termina en punto y coma. No usar punto y coma dentro de los comentarios.
-- =====================================================================

-- P1 | Top 10 departamentos con mas casos de COVID-19
SELECT d.departamento,
       COUNT(*)         AS total_casos,
       SUM(c.fallecido) AS total_fallecidos
FROM casos c
JOIN departamentos d ON c.cod_departamento = d.cod_departamento
GROUP BY d.departamento
ORDER BY total_casos DESC
LIMIT 10;

-- P2 | Top 10 departamentos con mayor tasa de contagio por cada 100.000 habitantes
SELECT d.departamento,
       d.poblacion,
       COUNT(*) AS total_casos,
       ROUND(CAST(COUNT(*) AS DOUBLE) * 100000 / d.poblacion, 2) AS casos_por_100k
FROM casos c
JOIN departamentos d ON c.cod_departamento = d.cod_departamento
GROUP BY d.departamento, d.poblacion
ORDER BY casos_por_100k DESC
LIMIT 10;

-- P3 | Casos, fallecidos, letalidad y dias promedio hasta el diagnostico por region natural
SELECT d.region,
       COUNT(*)         AS total_casos,
       SUM(c.fallecido) AS total_fallecidos,
       ROUND(CAST(SUM(c.fallecido) AS DOUBLE) * 100 / COUNT(*), 2) AS letalidad_pct,
       ROUND(AVG(c.dias_diagnostico), 1) AS prom_dias_diagnostico
FROM casos c
JOIN departamentos d ON c.cod_departamento = d.cod_departamento
GROUP BY d.region
ORDER BY total_casos DESC;

-- P4 | Distribucion de casos y letalidad por grupo de edad y sexo
SELECT c.grupo_edad,
       c.sexo,
       COUNT(*)         AS total_casos,
       SUM(c.fallecido) AS total_fallecidos,
       ROUND(CAST(SUM(c.fallecido) AS DOUBLE) * 100 / COUNT(*), 2) AS letalidad_pct
FROM casos c
WHERE c.sexo IN ('M', 'F')
GROUP BY c.grupo_edad, c.sexo
ORDER BY grupo_edad, sexo;

-- P5 | Top 10 dias con mas casos reportados (archivo EC2 + datos recientes de la URL)
SELECT c.fecha_reporte,
       COUNT(*)         AS total_casos,
       SUM(c.fallecido) AS total_fallecidos
FROM casos c
GROUP BY c.fecha_reporte
ORDER BY total_casos DESC
LIMIT 10;

-- P6 | Top 10 municipios con mas casos y su ubicacion geografica
SELECT m.municipio,
       d.departamento,
       m.latitud,
       m.longitud,
       COUNT(*) AS total_casos
FROM casos c
JOIN municipios m    ON c.cod_municipio = m.cod_municipio
JOIN departamentos d ON c.cod_departamento = d.cod_departamento
GROUP BY m.municipio, d.departamento, m.latitud, m.longitud
ORDER BY total_casos DESC
LIMIT 10;
