-- =====================================================================
-- PUNTO 5 - Catalogacion con EMR / Hive
-- Crea la base de datos covid_hive y declara (DDL) las 3 tablas de la zona
-- trusted como TABLAS EXTERNAS sobre los archivos Parquet que estan en S3.
--
-- Como el cluster EMR usa el AWS Glue Data Catalog como metastore de Hive,
-- estas tablas tambien quedan visibles en la consola de Glue y en Athena.
--
-- Ejecutar en el nodo master del EMR:
--   hive --hivevar BUCKET=<bucket> -f catalogo_hive.sql
-- =====================================================================

CREATE DATABASE IF NOT EXISTS covid_hive
COMMENT 'Caso COVID-19 Colombia - zona trusted catalogada con Hive'
LOCATION 's3://${hivevar:BUCKET}/hive/covid_hive.db/';

USE covid_hive;

-- Tabla de hechos: un registro por caso positivo de COVID-19
CREATE EXTERNAL TABLE IF NOT EXISTS casos (
    id_caso               INT,
    fecha_reporte         DATE,
    fecha_notificacion    DATE,
    cod_departamento      INT,
    cod_municipio         INT,
    municipio             STRING,
    edad                  INT,
    grupo_edad            STRING,
    sexo                  STRING,
    tipo_contagio         STRING,
    estado                STRING,
    recuperado            STRING,
    fallecido             INT,
    fecha_inicio_sintomas DATE,
    fecha_diagnostico     DATE,
    fecha_recuperacion    DATE,
    fecha_muerte          DATE,
    dias_diagnostico      INT,
    fuente                STRING
)
COMMENT 'Casos COVID-19 (archivo EC2 + API datos.gov.co) limpios'
STORED AS PARQUET
LOCATION 's3://${hivevar:BUCKET}/trusted/casos/';

-- Dimension: departamentos (origen RDS MariaDB)
CREATE EXTERNAL TABLE IF NOT EXISTS departamentos (
    cod_departamento INT,
    departamento     STRING,
    region           STRING,
    poblacion        INT
)
COMMENT 'Departamentos con region y poblacion (origen RDS MariaDB)'
STORED AS PARQUET
LOCATION 's3://${hivevar:BUCKET}/trusted/departamentos/';

-- Dimension: municipios (origen URL DIVIPOLA)
CREATE EXTERNAL TABLE IF NOT EXISTS municipios (
    cod_municipio    INT,
    cod_departamento INT,
    municipio        STRING,
    tipo_municipio   STRING,
    latitud          DOUBLE,
    longitud         DOUBLE
)
COMMENT 'Municipios DIVIPOLA con coordenadas (origen URL datos.gov.co)'
STORED AS PARQUET
LOCATION 's3://${hivevar:BUCKET}/trusted/municipios/';

-- Verificacion (evidencias)
SHOW TABLES;
DESCRIBE FORMATTED casos;
SELECT COUNT(*) AS total_casos FROM casos;
SELECT * FROM departamentos LIMIT 5;
SELECT * FROM municipios LIMIT 5;
