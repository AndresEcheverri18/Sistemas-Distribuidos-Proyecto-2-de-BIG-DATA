"""
PUNTO 8 - API REST expuesta con AWS API Gateway (backend: AWS Lambda + Athena).

Endpoints (API Gateway HTTP API -> esta Lambda):
    GET /                 -> lista de preguntas de negocio disponibles
    GET /?pregunta=P1     -> ejecuta en Athena la consulta P1 y devuelve el resultado en JSON

Las consultas NO estan escritas aqui: se leen del mismo archivo consultas.sql del
punto 6, que se empaqueta junto a esta funcion en el .zip de la Lambda.

Variables de entorno de la Lambda:
    DATABASE    base de datos del catalogo de Glue (covid_trusted)
    OUTPUT_S3   carpeta S3 donde Athena guarda resultados (s3://<bucket>/athena-results/)
"""
import json
import os
import re
import time

import boto3

athena = boto3.client("athena")
DATABASE = os.environ.get("DATABASE", "covid_trusted")
OUTPUT_S3 = os.environ.get("OUTPUT_S3")
TIPOS_ENTEROS = {"tinyint", "smallint", "integer", "int", "bigint"}
TIPOS_DECIMALES = {"double", "float", "real", "decimal"}


def leer_consultas(ruta_sql):
    """Devuelve {id: (titulo, sql)} a partir de las lineas '-- Pn | titulo' de consultas.sql."""
    with open(ruta_sql, encoding="utf-8") as archivo:
        contenido = archivo.read()
    consultas = {}
    for bloque in contenido.split(";"):
        id_pregunta, titulo, lineas_sql = None, None, []
        for linea in bloque.splitlines():
            marca = re.match(r"\s*--\s*(P\d+)\s*\|\s*(.+)", linea)
            if marca:
                id_pregunta, titulo = marca.group(1), marca.group(2).strip()
            elif linea.strip() and not linea.strip().startswith("--"):
                lineas_sql.append(linea)
        if id_pregunta and lineas_sql:
            consultas[id_pregunta] = (titulo, "\n".join(lineas_sql))
    return consultas


# Se lee una sola vez, cuando la Lambda arranca (cold start)
CONSULTAS = leer_consultas(os.path.join(os.path.dirname(os.path.abspath(__file__)), "consultas.sql"))


def ejecutar_athena(sql):
    """Lanza la consulta en Athena, espera a que termine y devuelve (columnas, filas)."""
    ejecucion = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": OUTPUT_S3})
    id_ejecucion = ejecucion["QueryExecutionId"]

    while True:                                   # Athena es asincrono: se consulta el estado
        estado = athena.get_query_execution(QueryExecutionId=id_ejecucion)["QueryExecution"]["Status"]
        if estado["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.5)
    if estado["State"] != "SUCCEEDED":
        raise RuntimeError(estado.get("StateChangeReason", estado["State"]))

    resultado = athena.get_query_results(QueryExecutionId=id_ejecucion)["ResultSet"]
    info_columnas = resultado["ResultSetMetadata"]["ColumnInfo"]
    columnas = [c["Name"] for c in info_columnas]
    filas = []
    for fila in resultado["Rows"][1:]:            # la primera fila es el encabezado
        valores = [dato.get("VarCharValue") for dato in fila["Data"]]
        filas.append({col["Name"]: convertir(valor, col["Type"])
                      for col, valor in zip(info_columnas, valores)})
    return columnas, filas


def convertir(valor, tipo):
    """Athena devuelve todo como texto: se convierte a numero segun el tipo de la columna."""
    if valor is None:
        return None
    if tipo in TIPOS_ENTEROS:
        return int(valor)
    if tipo in TIPOS_DECIMALES:
        return float(valor)
    return valor


def respuesta(codigo, cuerpo):
    """Formato de respuesta que espera API Gateway (proxy de Lambda)."""
    return {"statusCode": codigo,
            "headers": {"Content-Type": "application/json",
                        "Access-Control-Allow-Origin": "*"},
            "body": json.dumps(cuerpo, ensure_ascii=False, default=str)}


def lambda_handler(event, context):
    """Punto de entrada: API Gateway invoca esta funcion en cada peticion HTTP."""
    parametros = event.get("queryStringParameters") or {}
    pregunta = (parametros.get("pregunta") or "").upper()

    if not pregunta:                               # GET /  -> catalogo de preguntas
        lista = [{"id": i, "titulo": titulo} for i, (titulo, _) in CONSULTAS.items()]
        return respuesta(200, {"preguntas": lista})
    if pregunta not in CONSULTAS:
        return respuesta(404, {"error": f"Pregunta {pregunta} no existe",
                               "disponibles": list(CONSULTAS)})

    titulo, sql = CONSULTAS[pregunta]
    try:
        columnas, filas = ejecutar_athena(sql)
    except Exception as error:                     # error de Athena -> HTTP 500
        return respuesta(500, {"error": str(error)})
    return respuesta(200, {"pregunta": pregunta, "titulo": titulo,
                           "columnas": columnas, "filas": filas})
