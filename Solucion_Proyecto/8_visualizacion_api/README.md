# Punto 8 – Visualización (matplotlib) + API Gateway + Streamlit

**Archivos:**
- `lambda_api.py`: backend de la API (AWS Lambda), que ejecuta las consultas en Athena.
- `app_streamlit.py`: aplicación web de visualización (Streamlit + matplotlib + mapa).

## 8.1 Arquitectura de la capa de consumo

```
 Navegador ──► Streamlit (EC2 :8501) ──HTTP GET──► API Gateway (HTTP API) ──► Lambda lambda_api.py
                 │  tabla + gráfica matplotlib         https://<id>.execute-api...   │  lee consultas.sql (en su .zip)
                 │  + mapa (P6)                                                       ▼
                 └─────────────────── JSON ◄──────────────────────────────── Athena ─► Glue Catalog ─► S3 trusted/
```

### Endpoints de la API

| Método y ruta | Respuesta |
|---|---|
| `GET /` | `{"preguntas": [{"id": "P1", "titulo": "..."}, ...]}` (catálogo de preguntas) |
| `GET /?pregunta=P1` | `{"pregunta": "P1", "titulo": "...", "columnas": [...], "filas": [{...}, ...]}` |
| `GET /?pregunta=P9` | HTTP **404** `{"error": "Pregunta P9 no existe", "disponibles": [...]}` |
| error en Athena | HTTP **500** `{"error": "<motivo>"}` |

Ejemplo real de respuesta (`?pregunta=P1`, primera fila):
`{"departamento": "Bogotá D.C.", "total_casos": 52165, "total_fallecidos": 1768}`

### ¿Por qué Streamlit consume la API y no está "detrás" de API Gateway?

Streamlit mantiene una conexión **WebSocket** (`/_stcore/stream`) entre el navegador y el servidor.
Las integraciones HTTP/proxy de API Gateway **no soportan WebSockets**, así que la página se quedaría
cargando. La arquitectura correcta es que **API Gateway exponga la API de datos** (Lambda + Athena) y
que **Streamlit sea la aplicación cliente** que la consume. Esto también separa el *frontend* del
*backend*: otra aplicación (Power BI, una app móvil, `curl`) puede usar la misma API.

---

## 8.2 Desplegar la Lambda

### 1. Empaquetar (la Lambda necesita el `.py` y el `consultas.sql` del Punto 6)

```bash
cd ~/<su-repo>/Solucion_Proyecto/8_visualizacion_api
zip -j lambda.zip lambda_api.py ../6_consultas_sql/consultas.sql      # -j: sin carpetas dentro del zip
unzip -l lambda.zip                                                   # debe listar los 2 archivos
```

### 2. Crear la función (consola)

**Lambda → Create function → Author from scratch**:

| Campo | Valor |
|---|---|
| Function name | `covid-api` |
| Runtime | **Python 3.12** |
| Execution role | *Use an existing role* → **LabRole** |

Después de crearla:
- **Code → Upload from → .zip file** → `lambda.zip`.
- **Code → Runtime settings → Edit → Handler:** `lambda_api.lambda_handler`, es decir
  *archivo*.*función*.
- **Configuration → General configuration → Timeout:** **30 s**. El valor por defecto es 3 s y
  Athena tarda unos 2-5 s.
- **Configuration → Environment variables:**
  - `DATABASE` = `covid_trusted`
  - `OUTPUT_S3` = `s3://<bucket>/athena-results/`
- **Test** con este evento, que debe responder `statusCode 200`:
  ```json
  {"queryStringParameters": {"pregunta": "P1"}}
  ```

CLI equivalente (desde la terminal del Learner Lab o CloudShell):

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
aws lambda create-function --function-name covid-api --runtime python3.12 \
  --handler lambda_api.lambda_handler --role arn:aws:iam::$ACCOUNT:role/LabRole \
  --zip-file fileb://lambda.zip --timeout 30 \
  --environment "Variables={DATABASE=covid_trusted,OUTPUT_S3=s3://$BUCKET/athena-results/}"
```

## 8.3 Crear la API Gateway

### Consola

**API Gateway → Create API → HTTP API → Build**:

| Paso | Valor |
|---|---|
| Integrations | **Lambda** → `covid-api` |
| API name | `covid-api` |
| Routes | Method **GET**, Resource path **/** → integración `covid-api` (la consola sugiere `ANY /covid-api`: cámbiela a `GET /`) |
| Stages | `$default` con *Auto-deploy* |

Al crearla, copie el **Invoke URL** (ej. `https://abc123.execute-api.us-east-1.amazonaws.com`).
La consola agrega sola el permiso para que API Gateway invoque la Lambda.

### CLI equivalente

```bash
API_ID=$(aws apigatewayv2 create-api --name covid-api --protocol-type HTTP \
  --target arn:aws:lambda:us-east-1:$ACCOUNT:function:covid-api --query ApiId --output text)
aws lambda add-permission --function-name covid-api --statement-id apigateway-invoke \
  --action lambda:InvokeFunction --principal apigateway.amazonaws.com \
  --source-arn "arn:aws:execute-api:us-east-1:$ACCOUNT:$API_ID/*"
echo "https://$API_ID.execute-api.us-east-1.amazonaws.com"
```

### Probar la API

```bash
export API_URL=https://<id>.execute-api.us-east-1.amazonaws.com
curl "$API_URL/"
curl "$API_URL/?pregunta=P2"
```

## 8.4 Ejecutar la app Streamlit (en la EC2)

```bash
cd ~/<su-repo>/Solucion_Proyecto/8_visualizacion_api
export API_URL=https://<id>.execute-api.us-east-1.amazonaws.com
nohup ~/venv/bin/streamlit run app_streamlit.py --server.port 8501 --server.address 0.0.0.0 > ~/streamlit.log 2>&1 &
```

Abrir en el navegador `http://<IP-publica-EC2>:8501`. El puerto 8501 debe estar abierto en el
Security Group (Punto 2). `nohup ... &` deja la app corriendo aunque se cierre la sesión SSH.

En la barra lateral se elige la pregunta. Cada pregunta muestra:
- una **tabla** con el JSON de la API;
- una **gráfica matplotlib** (barras horizontales, barras verticales o barras agrupadas por sexo);
- en **P6**, además, un **mapa** con los municipios: el tamaño del punto es proporcional a los casos.

---

## 8.5 Explicación del código `lambda_api.py`

### Árbol de llamadas

```
(cold start: la primera vez que AWS carga la función)
  boto3.client("athena")
  CONSULTAS = leer_consultas(".../consultas.sql")       ← dict {"P1": (titulo, sql), ...}

API Gateway (cada petición HTTP)
└── lambda_handler(event, context)
    ├── sin ?pregunta        → respuesta(200, lista de preguntas)
    ├── ?pregunta inexistente → respuesta(404, ...)
    └── ?pregunta=Pn
        ├── ejecutar_athena(sql)
        │   ├── athena.start_query_execution(...)    ← lanza la consulta (asíncrona)
        │   ├── athena.get_query_execution(...)      ← cada 0,5 s hasta SUCCEEDED/FAILED
        │   ├── athena.get_query_results(...)        ← trae las filas
        │   └── convertir(valor, tipo) por celda      ← "52165" → 52165
        └── respuesta(200 | 500, ...)
```

### Función por función

**Variables globales:** se crean **una vez por contenedor**, en el *cold start*, y se reutilizan en
las siguientes invocaciones. Leer `consultas.sql` en cada petición sería un desperdicio.
- `DATABASE` y `OUTPUT_S3` vienen de las variables de entorno: la misma función sirve para otro
  bucket o base de datos sin cambiar código.

**`leer_consultas(ruta_sql)`**: igual a la de `consultas_sparksql.py` (Punto 6), pero devuelve un
**diccionario** `{id: (titulo, sql)}` para buscar por id. Así la API ejecuta **exactamente** el mismo SQL
que se probó en Athena, Hive y SparkSQL.

**`ejecutar_athena(sql)`**: Athena es **asíncrono**.
1. `start_query_execution` envía el SQL con la BD (`QueryExecutionContext`) y la carpeta S3 donde
   Athena escribe el resultado (`OutputLocation`, obligatoria). Devuelve un `QueryExecutionId`.
2. Ciclo `while True` que consulta el estado cada 0,5 s (*polling*) hasta un estado final.
3. Si no es `SUCCEEDED`, lanza `RuntimeError` con el motivo (`StateChangeReason`, ej. "Table not
   found").
4. `get_query_results`: la **primera fila es el encabezado**, por eso se usa `["Rows"][1:]`. Cada
   celda viene como `{"VarCharValue": "texto"}`; si la celda es NULL viene vacía, y por eso
   `dato.get(...)` devuelve `None`.

**`convertir(valor, tipo)`**: Athena devuelve **todo como texto**. Con el tipo de cada columna
(`ResultSetMetadata.ColumnInfo[].Type`: `bigint`, `double`, `varchar`, `date`) se convierte a `int`
o `float`, así el JSON tiene números de verdad y Streamlit puede graficarlos.

**`respuesta(codigo, cuerpo)`**: formato que exige la integración *proxy* de API Gateway:
`statusCode`, `headers` y `body`, donde `body` es un **string** JSON.
- `ensure_ascii=False` conserva las tildes ("Bogotá").
- `default=str` serializa tipos raros (fechas).
- `Access-Control-Allow-Origin: *` habilita CORS por si un navegador llama la API directamente.

**`lambda_handler(event, context)`**: el punto de entrada que se configura como *Handler*.
- `event["queryStringParameters"]` trae los parámetros de la URL. Si no hay ninguno, la clave no
  viene o viene en `null` (según el tipo de API): por eso `event.get(...) or {}`.
- `.upper()` hace que `?pregunta=p1` y `?pregunta=P1` sean lo mismo.
- Solo se ejecuta SQL que está en `CONSULTAS`: el usuario **elige** una pregunta pero **no puede
  enviar SQL**, así que no hay inyección SQL.

---

## 8.6 Explicación del código `app_streamlit.py`

### Cómo funciona Streamlit (concepto clave)

Streamlit **re-ejecuta el script completo de arriba a abajo** cada vez que el usuario interactúa,
por ejemplo al cambiar de pregunta. Por eso:
- `@st.cache_data(ttl=300)` guarda durante 5 minutos la respuesta de la API para cada combinación
  de parámetros, y así no se vuelve a llamar Athena en cada clic;
- el estado (la pregunta elegida) lo guarda el propio widget `st.sidebar.radio`.

### Árbol de llamadas

```
streamlit run app_streamlit.py
└── main()
    ├── st.set_page_config / st.title / st.caption
    ├── api_url = st.sidebar.text_input(..., os.getenv("API_URL"))
    ├── llamar_api(api_url)                  ← GET /  → lista de preguntas  (cacheado)
    ├── st.sidebar.radio(...)                ← el usuario elige la pregunta
    ├── llamar_api(api_url, id_pregunta)     ← GET /?pregunta=Pn            (cacheado)
    ├── pd.DataFrame(filas, columns=columnas)
    ├── st.dataframe(df)                     ← tabla
    ├── st.pyplot(graficar(df, GRAFICAS[id]))← gráfica matplotlib
    └── si P6: st.map(df con lat/lon, size="total_casos")
```

### Función por función

**`GRAFICAS`** (diccionario de configuración): para cada pregunta define el tipo de gráfica y las
columnas. Ejemplo: P2 → barras horizontales de `casos_por_100k` por `departamento`. Separar la
configuración del código de dibujo evita escribir 6 funciones de gráfica.

**`llamar_api(api_url, pregunta=None)`**: `requests.get` con `params` (requests arma
`?pregunta=P1`), `timeout=60` y `raise_for_status()`. Si la API falla, Streamlit muestra el error en
pantalla. Devuelve el JSON como diccionario.

**`graficar(df, config)`**: crea la figura con `plt.subplots`.
- `"agrupada"` (P4): `df.pivot(index=grupo_edad, columns=sexo, values=total_casos)` convierte la
  tabla larga en una tabla ancha (una columna por sexo) y `tabla.plot(kind="bar")` dibuja barras
  lado a lado.
- `"barh"` (P1, P2, P6): `df.iloc[::-1]` invierte el orden para que el mayor quede **arriba**, porque
  matplotlib dibuja de abajo hacia arriba.
- `"bar"` (P3, P5): barras verticales con etiquetas rotadas 45°.
- Ejes, cuadrícula y `tight_layout()` para que no se corten las etiquetas.

**`main()`**: arma la página. Si no hay URL de la API muestra un aviso y termina (`return`). Con
`st.columns([2, 3])` divide la pantalla: 40 % tabla y 60 % gráfica. En P6 renombra `latitud/longitud`
a `lat/lon`, que son los nombres que exige `st.map`.

---

## Evidencias para el informe (Punto 8)

1. Consola Lambda: función `covid-api` (runtime, handler, timeout 30 s, variables de entorno) y un
   **Test** exitoso.
2. Consola API Gateway: la API HTTP con la ruta `GET /` y el *Invoke URL*.
3. Navegador o `curl`: `GET /` y `GET /?pregunta=P2` con el JSON de respuesta.
4. **Streamlit**: un pantallazo por pregunta (tabla + gráfica) y el **mapa** de la P6.
5. Athena → *Recent queries*: las consultas lanzadas por la Lambda (prueba de que la API usa Athena).
