"""
PUNTO 8 - Aplicacion de visualizacion con Streamlit + matplotlib.

Consume la API publicada en AWS API Gateway (lambda_api.py), muestra la respuesta
de cada pregunta de negocio en una tabla, una grafica matplotlib y, para la P6,
un mapa con la ubicacion de los municipios.

Uso (en la EC2):
    export API_URL=https://<id>.execute-api.us-east-1.amazonaws.com
    streamlit run app_streamlit.py --server.port 8501 --server.address 0.0.0.0
"""
import os

import matplotlib.pyplot as plt
import pandas as pd
import requests
import streamlit as st

# Como graficar cada pregunta: tipo de grafica, columna de categorias (x) y de valores (y)
GRAFICAS = {
    "P1": {"tipo": "barh", "x": "departamento", "y": "total_casos"},
    "P2": {"tipo": "barh", "x": "departamento", "y": "casos_por_100k"},
    "P3": {"tipo": "bar", "x": "region", "y": "letalidad_pct"},
    "P4": {"tipo": "agrupada", "x": "grupo_edad", "y": "total_casos", "serie": "sexo"},
    "P5": {"tipo": "bar", "x": "fecha_reporte", "y": "total_casos"},
    "P6": {"tipo": "barh", "x": "municipio", "y": "total_casos"},
}


@st.cache_data(ttl=300)
def llamar_api(api_url, pregunta=None):
    """GET a la API Gateway. Sin pregunta -> catalogo; con pregunta -> resultado de Athena."""
    parametros = {"pregunta": pregunta} if pregunta else None
    respuesta = requests.get(api_url, params=parametros, timeout=60)
    respuesta.raise_for_status()
    return respuesta.json()


def graficar(df, config):
    """Construye la figura matplotlib segun la configuracion de GRAFICAS."""
    fig, ax = plt.subplots(figsize=(10, 5))
    if config["tipo"] == "agrupada":                 # una barra por sexo dentro de cada grupo
        tabla = df.pivot(index=config["x"], columns=config["serie"], values=config["y"])
        tabla.plot(kind="bar", ax=ax)
    elif config["tipo"] == "barh":                   # horizontal: el mayor queda arriba
        datos = df.iloc[::-1]
        ax.barh(datos[config["x"]].astype(str), datos[config["y"]], color="tab:blue")
    else:
        ax.bar(df[config["x"]].astype(str), df[config["y"]], color="tab:blue")
        ax.tick_params(axis="x", rotation=45)
    ax.set_xlabel(config["y"] if config["tipo"] == "barh" else config["x"])
    ax.set_ylabel(config["x"] if config["tipo"] == "barh" else config["y"])
    ax.grid(axis="x" if config["tipo"] == "barh" else "y", alpha=0.3)
    fig.tight_layout()
    return fig


def main():
    st.set_page_config(page_title="COVID-19 Colombia", layout="wide")
    st.title("COVID-19 en Colombia - Proyecto 2 Big Data")
    st.caption("Datalake S3 -> Glue/Spark -> Athena -> Lambda -> API Gateway -> Streamlit")

    api_url = st.sidebar.text_input("URL de la API Gateway", os.getenv("API_URL", ""))
    if not api_url:
        st.info("Escriba la URL de la API Gateway en la barra lateral.")
        return

    preguntas = llamar_api(api_url)["preguntas"]
    opciones = {f"{p['id']} - {p['titulo']}": p["id"] for p in preguntas}
    seleccion = st.sidebar.radio("Pregunta de negocio", list(opciones))
    id_pregunta = opciones[seleccion]

    datos = llamar_api(api_url, id_pregunta)
    df = pd.DataFrame(datos["filas"], columns=datos["columnas"])
    st.subheader(f"{datos['pregunta']}: {datos['titulo']}")

    col_tabla, col_grafica = st.columns([2, 3])
    col_tabla.dataframe(df, hide_index=True)
    col_grafica.pyplot(graficar(df, GRAFICAS[id_pregunta]))

    if id_pregunta == "P6":                          # mapa con lat/lon de la URL DIVIPOLA
        st.subheader("Ubicacion de los municipios")
        st.map(df.rename(columns={"latitud": "lat", "longitud": "lon"}),
               size="total_casos")


if __name__ == "__main__":
    main()
