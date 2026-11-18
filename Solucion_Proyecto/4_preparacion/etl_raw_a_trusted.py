"""
PUNTO 4 - Preparacion de datos: zona RAW -> zona TRUSTED (medallion: bronze -> silver).

Script PySpark que se ejecuta como Job de AWS Glue (Glue = Spark administrado) o
con Apache Spark en el cluster EMR (spark-submit). Es el MISMO codigo en ambos.

Lee de raw/ la ultima carga de cada fuente, limpia, tipa y homologa los datos y
escribe 3 tablas en formato Parquet en trusted/:
    trusted/casos/          (hechos: casos COVID de archivo EC2 + API URL unidos)
    trusted/departamentos/  (dimension: viene de RDS MariaDB)
    trusted/municipios/     (dimension: viene de la URL DIVIPOLA, con lat/lon)

Uso:
    Glue  : parametro del job   --datalake s3://<bucket>
    EMR   : spark-submit etl_raw_a_trusted.py --datalake s3://<bucket>
    Local : spark-submit etl_raw_a_trusted.py --datalake /ruta/a/datalake
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Homologacion de esquemas: las 2 fuentes de casos traen los MISMOS datos pero
# con nombres de columna y formato de fecha diferentes. Se mapean a un esquema
# comun {nombre_destino: nombre_en_origen}.
# ---------------------------------------------------------------------------
MAPEO_CASOS_EC2 = {
    "id_caso": "ID de caso",
    "fecha_reporte": "fecha reporte web",
    "fecha_notificacion": "Fecha de notificación",
    "cod_municipio": "Código DIVIPOLA municipio",
    "municipio": "Nombre municipio",
    "edad": "Edad",
    "unidad_edad": "Unidad de medida de edad",
    "sexo": "Sexo",
    "tipo_contagio": "Tipo de contagio",
    "estado": "Estado",
    "recuperado": "Recuperado",
    "fecha_inicio_sintomas": "Fecha de inicio de síntomas",
    "fecha_diagnostico": "Fecha de diagnóstico",
    "fecha_recuperacion": "Fecha de recuperación",
    "fecha_muerte": "Fecha de muerte",
}
MAPEO_CASOS_URL = {
    "id_caso": "id_de_caso",
    "fecha_reporte": "fecha_reporte_web",
    "fecha_notificacion": "fecha_de_notificaci_n",
    "cod_municipio": "ciudad_municipio",
    "municipio": "ciudad_municipio_nom",
    "edad": "edad",
    "unidad_edad": "unidad_medida",
    "sexo": "sexo",
    "tipo_contagio": "fuente_tipo_contagio",
    "estado": "estado",
    "recuperado": "recuperado",
    "fecha_inicio_sintomas": "fecha_inicio_sintomas",
    "fecha_diagnostico": "fecha_diagnostico",
    "fecha_recuperacion": "fecha_recuperado",
    "fecha_muerte": "fecha_muerte",
}
COLUMNAS_FECHA = ["fecha_reporte", "fecha_notificacion", "fecha_inicio_sintomas",
                  "fecha_diagnostico", "fecha_recuperacion", "fecha_muerte"]
FORMATO_FECHA_EC2 = "d/M/yyyy"     # ej: 6/3/2020 0:00:00
FORMATO_FECHA_URL = "yyyy-MM-dd"   # ej: 2020-07-02 00:00:00

# Columnas (y orden) de la tabla final trusted/casos
COLUMNAS_CASOS = ["id_caso", "fecha_reporte", "fecha_notificacion", "cod_departamento",
                  "cod_municipio", "municipio", "edad", "grupo_edad", "sexo", "tipo_contagio",
                  "estado", "recuperado", "fallecido", "fecha_inicio_sintomas",
                  "fecha_diagnostico", "fecha_recuperacion", "fecha_muerte",
                  "dias_diagnostico", "fuente"]


def crear_spark():
    """Crea (o reutiliza, en Glue/EMR) la sesion de Spark con configuracion segura."""
    spark = SparkSession.builder.appName("etl_raw_a_trusted").getOrCreate()
    spark.conf.set("spark.sql.ansi.enabled", "false")                  # valores invalidos -> null
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")   # fechas invalidas -> null
    # no generar el archivo vacio _SUCCESS (asi el crawler de Glue no lo lee)
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    return spark


def leer_ultima_ingesta(spark, ruta):
    """Lee un dataset de raw/ (CSV con encabezado, todo como texto) y deja solo la carga
    mas reciente. Spark convierte la carpeta fecha_ingesta=AAAA-MM-DD en una columna."""
    df = spark.read.option("header", True).csv(ruta)
    ultima = df.agg(F.max("fecha_ingesta")).first()[0]
    print(f"   {ruta} -> ultima fecha_ingesta = {ultima}")
    return df.filter(F.col("fecha_ingesta") == ultima).drop("fecha_ingesta")


def categoria(columna):
    """Normaliza un texto categorico: quita espacios, 'Primera Mayuscula',
    y vacios / 'N/A' se convierten en 'Sin dato'."""
    limpio = F.trim(columna)
    return (F.when(limpio.isNull() | (limpio == "") | (F.upper(limpio) == "N/A"), "Sin dato")
             .otherwise(F.initcap(F.lower(limpio))))


def homologar_casos(df, mapeo, formato_fecha, fuente):
    """Renombra las columnas de una fuente al esquema comun, convierte las fechas
    (texto -> DATE, solo la parte de la fecha) y marca la fuente de origen (linaje)."""
    df = df.select([F.col(origen).alias(destino) for destino, origen in mapeo.items()])
    for columna in COLUMNAS_FECHA:
        df = df.withColumn(columna, F.to_date(F.split(F.col(columna), " ").getItem(0), formato_fecha))
    return df.withColumn("fuente", F.lit(fuente))


def preparar_casos(df):
    """Reglas de calidad y columnas derivadas de la tabla de hechos 'casos'."""
    df = (df
          .withColumn("id_caso", F.col("id_caso").cast("int"))
          .withColumn("cod_municipio", F.col("cod_municipio").cast("int"))
          .withColumn("edad", F.col("edad").cast("int"))
          .withColumn("unidad_edad", F.col("unidad_edad").cast("int"))
          # 1) registros sin llave, sin fecha o sin municipio no sirven -> se descartan
          .filter(F.col("id_caso").isNotNull() & F.col("fecha_reporte").isNotNull()
                  & F.col("cod_municipio").isNotNull())
          # 2) un caso aparece una sola vez aunque venga en las 2 fuentes
          .dropDuplicates(["id_caso"]))

    # 3) departamento real a partir del municipio (Barranquilla 8001 -> 8 Atlantico,
    #    Cartagena 13001 -> 13 Bolivar). Asi se cruza con la tabla de MariaDB.
    df = df.withColumn("cod_departamento", F.floor(F.col("cod_municipio") / 1000).cast("int"))

    # 4) edad siempre en anios (unidad 2 = meses y 3 = dias -> 0 anios) y en rango 0-120
    edad = F.when(F.col("unidad_edad").isin(2, 3), F.lit(0)).otherwise(F.col("edad"))
    df = df.withColumn("edad", F.when(edad.between(0, 120), edad).cast("int"))
    decada = F.floor(F.col("edad") / 10) * 10
    df = df.withColumn(
        "grupo_edad",
        F.when(F.col("edad").isNull(), "Sin dato")
         .when(decada >= 80, "80+")
         .otherwise(F.concat(F.lpad(decada.cast("string"), 2, "0"), F.lit("-"),
                             F.lpad((decada + 9).cast("string"), 2, "0"))))

    # 5) textos normalizados
    df = (df
          .withColumn("municipio", F.upper(F.trim(F.col("municipio"))))
          .withColumn("sexo", F.when(F.upper(F.trim(F.col("sexo"))).isin("M", "F"),
                                     F.upper(F.trim(F.col("sexo")))).otherwise("Sin dato"))
          .withColumn("tipo_contagio", categoria(F.col("tipo_contagio")))
          .withColumn("estado", categoria(F.col("estado")))
          .withColumn("recuperado", categoria(F.col("recuperado"))))

    # 6) indicador numerico de muerte POR COVID (1/0) -> permite SUM() para contar muertes.
    #    Ojo: hay filas con fecha_muerte pero estado 'N/A': segun el INS son muertes por
    #    OTRAS causas (p.ej. personas recuperadas que murieron meses despues) -> no cuentan.
    df = df.withColumn(
        "fallecido",
        F.when((F.col("recuperado") == "Fallecido") | (F.col("estado") == "Fallecido"), 1)
         .otherwise(0))

    # 7) dias entre inicio de sintomas y diagnostico (oportunidad del diagnostico, 0-90)
    dias = F.datediff(F.col("fecha_diagnostico"), F.col("fecha_inicio_sintomas"))
    df = df.withColumn("dias_diagnostico", F.when(dias.between(0, 90), dias))

    return df.select(COLUMNAS_CASOS)


def preparar_departamentos(df):
    """Tipa y depura la tabla maestra que vino de RDS MariaDB."""
    return (df.select(F.col("cod_departamento").cast("int").alias("cod_departamento"),
                      F.trim("departamento").alias("departamento"),
                      F.trim("region").alias("region"),
                      F.col("poblacion").cast("int").alias("poblacion"))
              .filter(F.col("cod_departamento").isNotNull())
              .dropDuplicates(["cod_departamento"]))


def preparar_municipios(df):
    """Tipa la DIVIPOLA descargada por URL. Lat/lon vienen con coma decimal ('6,24')."""
    def coordenada(columna):
        return F.regexp_replace(F.col(columna), ",", ".").cast("double")
    return (df.select(F.col("cod_mpio").cast("int").alias("cod_municipio"),
                      F.col("cod_dpto").cast("int").alias("cod_departamento"),
                      F.upper(F.trim("nom_mpio")).alias("municipio"),
                      F.trim("tipo_municipio").alias("tipo_municipio"),
                      coordenada("latitud").alias("latitud"),
                      coordenada("longitud").alias("longitud"))
              .filter(F.col("cod_municipio").isNotNull())
              .dropDuplicates(["cod_municipio"]))


def escribir(df, ruta):
    """Escribe la tabla en Parquet (columnar, comprimido, con esquema) reemplazando la anterior."""
    df.coalesce(1).write.mode("overwrite").parquet(ruta)
    print(f"   escrito {ruta}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--datalake", required=True, help="s3://<bucket> o ruta local")
    args, _ = parser.parse_known_args()   # Glue agrega sus propios argumentos: se ignoran
    raw, trusted = f"{args.datalake}/raw", f"{args.datalake}/trusted"
    spark = crear_spark()

    print("1. Leyendo zona RAW (ultima carga de cada fuente)")
    casos_ec2 = leer_ultima_ingesta(spark, f"{raw}/ec2_archivos/casos_covid")
    casos_url = leer_ultima_ingesta(spark, f"{raw}/url/casos_covid_api")
    departamentos = leer_ultima_ingesta(spark, f"{raw}/mariadb/departamentos")
    municipios = leer_ultima_ingesta(spark, f"{raw}/url/divipola_municipios")

    print("2. Homologando y uniendo las 2 fuentes de casos")
    casos = (homologar_casos(casos_ec2, MAPEO_CASOS_EC2, FORMATO_FECHA_EC2, "ec2_archivo")
             .unionByName(homologar_casos(casos_url, MAPEO_CASOS_URL, FORMATO_FECHA_URL, "url_api")))
    total_raw = casos.count()

    print("3. Aplicando reglas de calidad y escribiendo zona TRUSTED (Parquet)")
    casos = preparar_casos(casos).cache()
    total_trusted = casos.count()
    escribir(casos, f"{trusted}/casos")
    escribir(preparar_departamentos(departamentos), f"{trusted}/departamentos")
    escribir(preparar_municipios(municipios), f"{trusted}/municipios")

    print(f"4. Resumen: casos raw = {total_raw} | casos trusted = {total_trusted} | "
          f"descartados = {total_raw - total_trusted}")
    casos.groupBy("fuente").count().show()
    casos.printSchema()


if __name__ == "__main__":
    main()
