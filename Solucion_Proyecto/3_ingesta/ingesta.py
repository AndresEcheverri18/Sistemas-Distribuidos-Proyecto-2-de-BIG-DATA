#!/usr/bin/env python3
"""
PUNTO 3 - Ingesta automatica hacia el Datalake (AWS S3, zona RAW).

Lleva al bucket S3 los datos de las 3 fuentes del caso de estudio:
  1. ARCHIVOS en la instancia EC2    -> s3://<bucket>/raw/ec2_archivos/casos_covid/...
  2. BASE DE DATOS RDS MariaDB       -> s3://<bucket>/raw/mariadb/departamentos/...
  3. URLs en Internet (datos.gov.co) -> s3://<bucket>/raw/url/<dataset>/...

Cada carga queda en una subcarpeta "fecha_ingesta=AAAA-MM-DD" para conservar el
historico (los datos de la zona raw nunca se modifican, solo se agregan).

Se ejecuta en la EC2 y se programa con cron para que sea AUTOMATICA (ver README).
Uso manual:
    python ingesta.py --bucket <bucket> --db-host <endpoint-rds> --db-pass <clave>
Los parametros tambien se pueden pasar como variables de entorno
(BUCKET, DIR_ARCHIVOS, DB_HOST, DB_USER, DB_PASS, DB_NAME).
"""
import argparse
import csv
import glob
import io
import os
from datetime import date, datetime

import boto3
import pymysql
import requests

# Fuentes URL (API Socrata del portal de datos abiertos de Colombia).
# Cada entrada: nombre del dataset en el datalake -> URL + parametros de la consulta.
FUENTES_URL = {
    # Casos COVID-19 reportados del 2 al 15 de julio de 2020 (continuan los del archivo EC2,
    # que llegan hasta el 1 de julio de 2020).
    "casos_covid_api": {
        "url": "https://www.datos.gov.co/resource/gt2j-8ykr.csv",
        "params": {
            "$where": "fecha_reporte_web between '2020-07-02T00:00:00' and '2020-07-15T23:59:59'",
            "$order": "id_de_caso",
        },
    },
    # Division politica (DIVIPOLA): municipios de Colombia con latitud y longitud.
    "divipola_municipios": {
        "url": "https://www.datos.gov.co/resource/gdxc-w37w.csv",
        "params": {"$order": "cod_mpio"},
    },
}

TABLAS_MARIADB = ["departamentos"]  # tablas de covid_db que se copian al datalake
TAM_PAGINA = 50000                   # filas por peticion a la API (paginacion)
DIR_TEMPORAL = "/tmp/ingesta"        # carpeta local donde se generan los CSV antes de subirlos


def log(mensaje):
    """Imprime el mensaje con fecha y hora (queda en el log de cron)."""
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {mensaje}", flush=True)


def subir_a_s3(s3, bucket, ruta_local, fuente, dataset):
    """Sube un archivo local a la zona raw: raw/<fuente>/<dataset>/fecha_ingesta=<hoy>/<archivo>."""
    llave = (f"raw/{fuente}/{dataset}/fecha_ingesta={date.today().isoformat()}/"
             f"{os.path.basename(ruta_local)}")
    s3.upload_file(ruta_local, bucket, llave)
    log(f"   subido -> s3://{bucket}/{llave}")


def ingestar_archivos_ec2(s3, bucket, dir_archivos):
    """FUENTE 1: sube todos los CSV que esten en la carpeta de datos de la EC2."""
    log(f"FUENTE 1 - Archivos EC2 en {dir_archivos}")
    archivos = sorted(glob.glob(os.path.join(dir_archivos, "*.csv")))
    if not archivos:
        raise FileNotFoundError(f"No hay archivos .csv en {dir_archivos}")
    for ruta in archivos:
        subir_a_s3(s3, bucket, ruta, "ec2_archivos", "casos_covid")


def ingestar_mariadb(s3, bucket, host, usuario, clave, base_datos):
    """FUENTE 2: exporta cada tabla de MariaDB a CSV (SELECT *) y la sube a S3."""
    log(f"FUENTE 2 - RDS MariaDB {host}/{base_datos}")
    conexion = pymysql.connect(host=host, user=usuario, password=clave,
                               database=base_datos, port=3306, charset="utf8mb4")
    try:
        with conexion.cursor() as cursor:
            for tabla in TABLAS_MARIADB:
                cursor.execute(f"SELECT * FROM {tabla}")
                columnas = [descripcion[0] for descripcion in cursor.description]
                filas = cursor.fetchall()
                ruta = os.path.join(DIR_TEMPORAL, f"{tabla}.csv")
                with open(ruta, "w", newline="", encoding="utf-8") as archivo:
                    escritor = csv.writer(archivo)
                    escritor.writerow(columnas)  # encabezado
                    escritor.writerows(filas)
                log(f"   tabla {tabla}: {len(filas)} filas")
                subir_a_s3(s3, bucket, ruta, "mariadb", tabla)
    finally:
        conexion.close()


def descargar_url(url, params, ruta_salida):
    """Descarga un dataset de la API Socrata en paginas de TAM_PAGINA filas y lo une en un CSV."""
    total, offset = 0, 0
    with open(ruta_salida, "w", newline="", encoding="utf-8") as archivo:
        escritor = csv.writer(archivo)
        while True:
            pagina = dict(params, **{"$limit": TAM_PAGINA, "$offset": offset})
            respuesta = requests.get(url, params=pagina, timeout=300)
            respuesta.raise_for_status()           # error HTTP -> excepcion
            respuesta.encoding = "utf-8"
            filas = list(csv.reader(io.StringIO(respuesta.text)))
            encabezado, datos = filas[0], filas[1:]
            if offset == 0:
                escritor.writerow(encabezado)      # el encabezado se escribe una sola vez
            escritor.writerows(datos)
            total += len(datos)
            if len(datos) < TAM_PAGINA:            # ultima pagina
                break
            offset += TAM_PAGINA
    return total


def ingestar_urls(s3, bucket):
    """FUENTE 3: descarga cada dataset de FUENTES_URL y lo sube a S3."""
    for dataset, fuente in FUENTES_URL.items():
        log(f"FUENTE 3 - URL {fuente['url']}")
        ruta = os.path.join(DIR_TEMPORAL, f"{dataset}.csv")
        filas = descargar_url(fuente["url"], fuente["params"], ruta)
        log(f"   dataset {dataset}: {filas} filas descargadas")
        subir_a_s3(s3, bucket, ruta, "url", dataset)


def main():
    parser = argparse.ArgumentParser(description="Ingesta automatica de 3 fuentes hacia S3 raw")
    parser.add_argument("--bucket", default=os.getenv("BUCKET"))
    parser.add_argument("--dir-archivos", default=os.getenv("DIR_ARCHIVOS", "/home/ubuntu/datos"))
    parser.add_argument("--db-host", default=os.getenv("DB_HOST"))
    parser.add_argument("--db-user", default=os.getenv("DB_USER", "admin"))
    parser.add_argument("--db-pass", default=os.getenv("DB_PASS"))
    parser.add_argument("--db-name", default=os.getenv("DB_NAME", "covid_db"))
    args = parser.parse_args()
    if not args.bucket or not args.db_host or not args.db_pass:
        parser.error("faltan --bucket, --db-host o --db-pass (o sus variables de entorno)")

    os.makedirs(DIR_TEMPORAL, exist_ok=True)
    s3 = boto3.client("s3")  # credenciales: rol LabInstanceProfile de la EC2

    log(f"=== INICIO INGESTA -> s3://{args.bucket}/raw/ ===")
    ingestar_archivos_ec2(s3, args.bucket, args.dir_archivos)
    ingestar_mariadb(s3, args.bucket, args.db_host, args.db_user, args.db_pass, args.db_name)
    ingestar_urls(s3, args.bucket)
    log("=== FIN INGESTA OK ===")


if __name__ == "__main__":
    main()
