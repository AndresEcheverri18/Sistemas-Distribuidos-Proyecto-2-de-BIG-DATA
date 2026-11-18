"""
PUNTO 7 - Procesamiento analitico descriptivo con PySpark (API de DataFrames).

Responde las 6 preguntas de negocio SIN SQL (con transformaciones join, groupBy,
agg, orderBy...) sobre la zona trusted, y guarda cada respuesta en la zona
REFINED (gold) en 2 formatos: CSV y Parquet.
    refined/<pregunta>/csv/      refined/<pregunta>/parquet/

Uso:
    EMR   : spark-submit analitica_pyspark.py --datalake s3://<bucket>
    Local : spark-submit analitica_pyspark.py --datalake /ruta/a/datalake
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def letalidad(fallecidos, casos):
    """% de casos que fallecieron, redondeado a 2 decimales."""
    return F.round(F.col(fallecidos) * 100 / F.col(casos), 2)


def p1_top_departamentos(casos, departamentos, municipios):
    """P1 - Top 10 departamentos con mas casos de COVID-19."""
    return (casos.join(departamentos, "cod_departamento")
            .groupBy("departamento")
            .agg(F.count("*").alias("total_casos"), F.sum("fallecido").alias("total_fallecidos"))
            .orderBy(F.desc("total_casos"))
            .limit(10))


def p2_tasa_por_100k(casos, departamentos, municipios):
    """P2 - Top 10 departamentos con mayor tasa de contagio por cada 100.000 habitantes."""
    return (casos.join(departamentos, "cod_departamento")
            .groupBy("departamento", "poblacion")
            .agg(F.count("*").alias("total_casos"))
            .withColumn("casos_por_100k",
                        F.round(F.col("total_casos") * 100000 / F.col("poblacion"), 2))
            .orderBy(F.desc("casos_por_100k"))
            .limit(10))


def p3_regiones(casos, departamentos, municipios):
    """P3 - Casos, fallecidos, letalidad y dias promedio hasta el diagnostico por region."""
    return (casos.join(departamentos, "cod_departamento")
            .groupBy("region")
            .agg(F.count("*").alias("total_casos"),
                 F.sum("fallecido").alias("total_fallecidos"),
                 F.round(F.avg("dias_diagnostico"), 1).alias("prom_dias_diagnostico"))
            .withColumn("letalidad_pct", letalidad("total_fallecidos", "total_casos"))
            .select("region", "total_casos", "total_fallecidos", "letalidad_pct",
                    "prom_dias_diagnostico")
            .orderBy(F.desc("total_casos")))


def p4_edad_sexo(casos, departamentos, municipios):
    """P4 - Distribucion de casos y letalidad por grupo de edad y sexo."""
    return (casos.filter(F.col("sexo").isin("M", "F"))
            .groupBy("grupo_edad", "sexo")
            .agg(F.count("*").alias("total_casos"), F.sum("fallecido").alias("total_fallecidos"))
            .withColumn("letalidad_pct", letalidad("total_fallecidos", "total_casos"))
            .orderBy("grupo_edad", "sexo"))


def p5_top_dias(casos, departamentos, municipios):
    """P5 - Top 10 dias con mas casos reportados."""
    return (casos.groupBy("fecha_reporte")
            .agg(F.count("*").alias("total_casos"), F.sum("fallecido").alias("total_fallecidos"))
            .orderBy(F.desc("total_casos"))
            .limit(10))


def p6_top_municipios(casos, departamentos, municipios):
    """P6 - Top 10 municipios con mas casos y su ubicacion (lat/lon de la URL DIVIPOLA)."""
    ubicacion = municipios.select("cod_municipio", "municipio", "latitud", "longitud")
    return (casos.drop("municipio")                      # se usa el nombre oficial DIVIPOLA
            .join(ubicacion, "cod_municipio")
            .join(departamentos.select("cod_departamento", "departamento"), "cod_departamento")
            .groupBy("municipio", "departamento", "latitud", "longitud")
            .agg(F.count("*").alias("total_casos"))
            .orderBy(F.desc("total_casos"))
            .limit(10))


# Nombre de la carpeta de salida en refined/ -> funcion que responde la pregunta
PREGUNTAS = {
    "p1_top_departamentos": p1_top_departamentos,
    "p2_tasa_por_100k": p2_tasa_por_100k,
    "p3_regiones": p3_regiones,
    "p4_edad_sexo": p4_edad_sexo,
    "p5_top_dias": p5_top_dias,
    "p6_top_municipios": p6_top_municipios,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--datalake", required=True, help="s3://<bucket> o ruta local")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("analitica_pyspark").getOrCreate()
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    trusted = f"{args.datalake}/trusted"
    casos = spark.read.parquet(f"{trusted}/casos").cache()
    departamentos = spark.read.parquet(f"{trusted}/departamentos")
    municipios = spark.read.parquet(f"{trusted}/municipios")
    print(f"Casos en trusted: {casos.count()}")

    for nombre, funcion in PREGUNTAS.items():
        resultado = funcion(casos, departamentos, municipios).coalesce(1)
        print(f"\n===== {nombre}: {funcion.__doc__.strip()} =====")
        resultado.show(truncate=False)
        destino = f"{args.datalake}/refined/{nombre}"
        resultado.write.mode("overwrite").option("header", True).csv(f"{destino}/csv")
        resultado.write.mode("overwrite").parquet(f"{destino}/parquet")
        print(f"   guardado en {destino}/csv y {destino}/parquet")


if __name__ == "__main__":
    main()
