# Punto 1 – Caso de estudio y preguntas de negocio

## 1.1 Contexto del caso

**Organización simulada:** una Secretaría de Salud que necesita analizar el comportamiento de la
pandemia de COVID-19 en Colombia durante su primera ola (marzo – julio de 2020) para priorizar
recursos (camas UCI, pruebas, campañas) por territorio y por grupo poblacional.

**Problema:** la información está **dispersa** en tres sistemas que no se hablan entre sí:

| Fuente | Qué contiene | Por qué está ahí (escenario realista) |
|---|---|---|
| **Archivos en una EC2** | 100.000 casos positivos reportados del 6-mar al 1-jul-2020 (CSV del INS) | Exportaciones periódicas que el sistema de vigilancia deja en un servidor de archivos |
| **Base de datos RDS MariaDB** | Tabla maestra de los 33 departamentos: código DIVIPOLA, región natural y población 2020 | Datos maestros administrados por un sistema transaccional (OLTP) |
| **URLs en Internet** (datos.gov.co) | (a) casos del 2 al 15-jul-2020 (59.067) vía API · (b) DIVIPOLA: 1.122 municipios con latitud y longitud | Datos abiertos del Gobierno, la fuente más actualizada |

**Solución:** un datalake en AWS S3 que integra las tres fuentes, prepara los datos y los expone para
análisis SQL, PySpark, visualización y una API.

### Dataset base

- **Casos positivos de COVID-19 en Colombia**: Instituto Nacional de Salud (INS), publicado en
  <https://www.datos.gov.co/Salud-y-Protecci-n-Social/Casos-positivos-de-COVID-19-en-Colombia/gt2j-8ykr>.
  La muestra de 100K está en el repositorio del curso: `si3007-262/datasets/covid19/`.
  - API usada: `https://www.datos.gov.co/resource/gt2j-8ykr.csv`
- **DIVIPOLA – Códigos de municipios** (DANE): `https://www.datos.gov.co/resource/gdxc-w37w.csv`
- **Población por departamento**: proyección DANE 2020. Valores aproximados con fines académicos,
  simulados en MariaDB como lo pide el enunciado.

---

## 1.2 Preguntas de negocio (6, el enunciado pide mínimo 5)

| # | Pregunta | Fuentes que necesita | Por qué le sirve al negocio |
|---|---|---|---|
| **P1** | ¿Cuáles son los **10 departamentos con más casos** de COVID-19 y cuántos fallecidos tuvieron? | casos (EC2 + URL) + departamentos (MariaDB) | Dónde se concentra la carga absoluta de casos |
| **P2** | ¿Qué departamentos tienen la **mayor tasa de contagio por cada 100.000 habitantes**? | casos + **población** (MariaDB) | Compara territorios de distinto tamaño de forma justa: un departamento pequeño puede estar peor que uno grande |
| **P3** | Por **región natural** (Andina, Caribe, Pacífica, Orinoquía, Amazonía, Insular): ¿cuántos casos y fallecidos hubo, cuál fue la **letalidad (%)** y cuántos **días promedio** pasaron entre los síntomas y el diagnóstico? | casos + **región** (MariaDB) | Priorizar regiones con alta letalidad o diagnóstico tardío |
| **P4** | ¿Cómo se **distribuyen los casos y la letalidad por grupo de edad y sexo**? | casos | Identificar la población de mayor riesgo, por ejemplo para priorizar la vacunación |
| **P5** | ¿Cuáles fueron los **10 días con más casos reportados**? | casos de **archivo EC2 + URL** | Detectar los picos de la ola. Esta pregunta solo se responde bien integrando los datos recientes de la URL |
| **P6** | ¿Cuáles son los **10 municipios con más casos** y **dónde están** (latitud/longitud)? | casos + municipios (**URL DIVIPOLA**) + departamentos | Ubicar geográficamente los focos (mapa) |

> Cada pregunta usa al menos una fuente distinta a los archivos, de modo que **las tres fuentes son
> necesarias** para responder el conjunto de preguntas.

### Dónde se responde cada pregunta

| Pregunta | SQL (Athena / Hive / SparkSQL) | PySpark DataFrames | API + Streamlit |
|---|---|---|---|
| P1 a P6 | `6_consultas_sql/consultas.sql` (bloques `-- P1 \|` … `-- P6 \|`) | `7_analitica_pyspark/analitica_pyspark.py` (funciones `p1_...` a `p6_...`) | `8_visualizacion_api/` (`GET /?pregunta=P1` … `P6`) |

### Respuestas obtenidas (resumen)

| Pregunta | Hallazgo |
|---|---|
| P1 | Bogotá D.C. lidera con 52.165 casos, seguida de Atlántico (35.248) y Valle del Cauca (15.184) |
| P2 | **Amazonas** tiene la mayor tasa: 3.051 casos por cada 100 mil habitantes, más del doble que Atlántico (1.295) |
| P3 | La región **Caribe** tiene la mayor letalidad (6,37 %), casi el doble que la Andina (3,23 %), y el diagnóstico más tardío (11,6 días) |
| P4 | La mayoría de casos está entre 20 y 39 años, pero la letalidad crece con la edad: 80+ hombres 46,2 %, 80+ mujeres 35,4 %. En todos los grupos los hombres tienen mayor letalidad |
| P5 | El pico es el 10-jul-2020 con 6.801 casos. 9 de los 10 días pico salen de los datos de la URL (julio) |
| P6 | Bogotá, Barranquilla, Cali, Cartagena y Soledad son los 5 municipios con más casos |

---

## Evidencias para el informe (Punto 1)

- Copiar la tabla de preguntas (1.2) en el documento del proyecto.
- Pantallazo de las páginas de los datasets en datos.gov.co.
