# Punto 3 – Ingesta automática hacia el Datalake (S3, zona raw)

**Archivo:** `ingesta.py` · **Dónde corre:** EC2 `covid-ec2` · **Cómo es automático:** `cron`

## 3.1 Qué hace (visión general)

```
 ~/datos/*.csv (EC2) ──────────────┐
 RDS MariaDB  SELECT * ... ────────┼──► ingesta.py ──► s3://<bucket>/raw/<fuente>/<dataset>/fecha_ingesta=AAAA-MM-DD/<archivo>.csv
 API datos.gov.co (2 URLs) ────────┘
```

- Lleva **las 3 fuentes** a la zona **raw** del datalake **sin transformarlas** (copia fiel).
- Organiza raw por **fuente → dataset → fecha de ingesta**. Las "zonas correctas" que pide el enunciado:
  - `raw/ec2_archivos/casos_covid/`
  - `raw/mariadb/departamentos/`
  - `raw/url/casos_covid_api/`
  - `raw/url/divipola_municipios/`
- La carpeta `fecha_ingesta=AAAA-MM-DD` (formato *Hive partition*) conserva el **histórico** de cada
  carga. Spark la lee luego como una columna, y así el ETL sabe cuál es la carga más reciente.

---

## 3.2 Cómo ejecutarlo

### Manual (primera vez, para probar)

```bash
cd ~/<su-repo>/Solucion_Proyecto/3_ingesta
~/venv/bin/python ingesta.py --bucket $BUCKET --db-host <ENDPOINT_RDS> --db-pass <CLAVE>
aws s3 ls s3://$BUCKET/raw/ --recursive --human-readable
```

Salida real de la prueba:

```
[2026-11-17 23:11:43] === INICIO INGESTA -> s3://bucket-prueba/raw/ ===
[2026-11-17 23:11:43] FUENTE 1 - Archivos EC2 en /home/ubuntu/datos
[2026-11-17 23:11:43]    subido -> s3://.../raw/ec2_archivos/casos_covid/fecha_ingesta=2026-11-17/Casos_positivos_de_COVID-19_en_Colombia-100K.csv
[2026-11-17 23:11:43] FUENTE 2 - RDS MariaDB covid-mariadb.../covid_db
[2026-11-17 23:11:43]    tabla departamentos: 33 filas
[2026-11-17 23:11:43]    subido -> s3://.../raw/mariadb/departamentos/fecha_ingesta=2026-11-17/departamentos.csv
[2026-11-17 23:11:43] FUENTE 3 - URL https://www.datos.gov.co/resource/gt2j-8ykr.csv
[2026-11-17 23:12:06]    dataset casos_covid_api: 59067 filas descargadas
[2026-11-17 23:12:06]    subido -> s3://.../raw/url/casos_covid_api/fecha_ingesta=2026-11-17/casos_covid_api.csv
[2026-11-17 23:12:06] FUENTE 3 - URL https://www.datos.gov.co/resource/gdxc-w37w.csv
[2026-11-17 23:12:07]    dataset divipola_municipios: 1122 filas descargadas
[2026-11-17 23:12:07]    subido -> s3://.../raw/url/divipola_municipios/fecha_ingesta=2026-11-17/divipola_municipios.csv
[2026-11-17 23:12:07] === FIN INGESTA OK ===
```

### Automático con cron

```bash
crontab -e
```

Pegar al final (las variables van **arriba**, porque cron no carga el `.bashrc`):

```cron
BUCKET=<su-bucket>
DB_HOST=<ENDPOINT_RDS>
DB_PASS=<CLAVE>
DIR_ARCHIVOS=/home/ubuntu/datos
# cada 6 horas (minuto 0). Para la demo puede usar */10 * * * * (cada 10 minutos)
0 */6 * * * /home/ubuntu/venv/bin/python /home/ubuntu/<su-repo>/Solucion_Proyecto/3_ingesta/ingesta.py >> /home/ubuntu/ingesta.log 2>&1
```

Verificar que quedó programado y que está corriendo:

```bash
crontab -l
tail -f ~/ingesta.log
```

**Formato de cron:** `minuto hora día-mes mes día-semana comando`. `0 */6 * * *` significa "en el
minuto 0 de cada 6 horas". `>> archivo 2>&1` agrega la salida normal y los errores al log.

---

## 3.3 Explicación del código `ingesta.py` (de lo general a lo particular)

### Árbol de llamadas

```
cron / usted
└── main()
    ├── argparse: lee --bucket, --dir-archivos, --db-host, --db-user, --db-pass, --db-name
    │             (si no vienen, toma las variables de entorno BUCKET, DB_HOST, ...)
    ├── boto3.client("s3")                       ← cliente S3 con credenciales del rol de la EC2
    ├── ingestar_archivos_ec2(s3, bucket, dir)   ← FUENTE 1
    │   └── subir_a_s3(...)  por cada *.csv
    ├── ingestar_mariadb(s3, bucket, host, ...)  ← FUENTE 2
    │   ├── pymysql.connect(...)
    │   ├── cursor.execute("SELECT * FROM departamentos")
    │   ├── csv.writer → /tmp/ingesta/departamentos.csv
    │   └── subir_a_s3(...)
    └── ingestar_urls(s3, bucket)                ← FUENTE 3
        └── por cada dataset en FUENTES_URL:
            ├── descargar_url(url, params, ruta)  ← paginación de la API
            │   └── requests.get(...) varias veces hasta la última página
            └── subir_a_s3(...)
```

### Constantes de configuración (arriba del archivo)

| Constante | Valor | Para qué |
|---|---|---|
| `FUENTES_URL` | diccionario con 2 datasets | Cada clave es el nombre de la carpeta en S3. `url` es el endpoint CSV de Socrata y `params` el filtro SoQL. Para agregar otra URL basta con otra entrada |
| `TABLAS_MARIADB` | `["departamentos"]` | Tablas de `covid_db` que se copian |
| `TAM_PAGINA` | `50000` | Filas por petición a la API (máximo recomendado por Socrata) |
| `DIR_TEMPORAL` | `/tmp/ingesta` | Carpeta local donde se arman los CSV antes de subirlos |

### Función por función

**`log(mensaje)`**: imprime con fecha y hora; `flush=True` obliga a escribir de inmediato, para que el
log de cron se vea en tiempo real. *La llaman todas las demás funciones.*

**`subir_a_s3(s3, bucket, ruta_local, fuente, dataset)`**: construye la llave S3
`raw/<fuente>/<dataset>/fecha_ingesta=<hoy>/<nombre_archivo>` y llama a `s3.upload_file()`, que
sube el archivo (por partes si es grande). Si se ejecuta dos veces el mismo día, sobrescribe ese
día; si se ejecuta otro día, crea una carpeta nueva y así se conserva el histórico.
*La llaman las tres funciones `ingestar_*`.*

**`ingestar_archivos_ec2(s3, bucket, dir_archivos)`**: busca con `glob` todos los `*.csv` de la
carpeta. Si no hay ninguno lanza `FileNotFoundError`, que detiene la ejecución y queda en el log.
Cada archivo se sube **tal cual** a `raw/ec2_archivos/casos_covid/`. *La llama `main()`.*

**`ingestar_mariadb(s3, bucket, host, usuario, clave, base_datos)`**:
1. `pymysql.connect(..., charset="utf8mb4")` abre la conexión TCP al puerto 3306 de la RDS. Funciona
   porque la RDS se creó con *Connect to an EC2 compute resource*.
2. Por cada tabla: `SELECT *`. `cursor.description` trae los nombres de las columnas y `fetchall()`
   trae todas las filas como tuplas.
3. `csv.writer` escribe primero el encabezado y luego las filas, y el archivo se sube a S3.
4. `finally: conexion.close()` cierra la conexión aunque ocurra un error.

*La llama `main()`.*

**`descargar_url(url, params, ruta_salida)`**: el corazón de la fuente URL.
1. Abre el CSV de salida y entra a un ciclo `while True`.
2. Agrega `$limit=50000` y `$offset` a los parámetros: `dict(params, **{...})` crea una copia del
   diccionario sin modificar el original.
3. `requests.get(..., timeout=300)` y `raise_for_status()`: si la API responde un error HTTP (4xx/5xx)
   se lanza una excepción.
4. `csv.reader(io.StringIO(texto))` parsea la página respetando las comillas. Por ejemplo, las
   coordenadas `"-75,58"` traen una coma adentro.
5. El encabezado se escribe **solo en la primera página** (`offset == 0`).
6. Si la página trajo menos de 50.000 filas es la última y se sale. Si no, `offset += 50000`.

   Ejemplo: los casos de julio son 59.067 → página 1 = 50.000, página 2 = 9.067 → fin.
7. Devuelve el total de filas.

*La llama `ingestar_urls()`.*

**`ingestar_urls(s3, bucket)`**: recorre `FUENTES_URL`, descarga cada dataset con
`descargar_url()` y lo sube a `raw/url/<dataset>/`. *La llama `main()`.*

**`main()`**: lee los parámetros (la línea de comandos tiene prioridad sobre las variables de
entorno). Si falta el bucket, el host o la clave, `parser.error()` termina con un mensaje claro.
Crea la carpeta temporal y el cliente S3, y ejecuta las 3 fuentes en orden.
*Se ejecuta por el bloque `if __name__ == "__main__":`, es decir, cuando se corre el archivo
directamente, ya sea a mano o desde cron.*

### Decisiones de diseño que puede defender

| Decisión | Justificación |
|---|---|
| raw = copia fiel (no se limpia nada) | Si el ETL tiene un error, se puede reprocesar desde raw sin volver a las fuentes |
| Particiones `fecha_ingesta=` | Histórico y trazabilidad: se sabe qué llegó cada día |
| Credenciales S3 por rol (`LabInstanceProfile`) | No hay llaves de AWS escritas en el código ni en el servidor |
| Paginación con `$order` | Sin un orden fijo, la API podría repetir o saltar filas entre páginas |
| Un solo script para las 3 fuentes | Pocas piezas que mantener y un solo log que revisar |

---

## Evidencias para el informe (Punto 3)

1. Terminal EC2: ejecución manual con su salida (como el bloque de arriba).
2. `crontab -l` mostrando la programación.
3. `tail ~/ingesta.log` con al menos **2 ejecuciones automáticas** (con horas distintas).
4. Consola S3: las carpetas `raw/ec2_archivos`, `raw/mariadb`, `raw/url` y dentro `fecha_ingesta=...`.
5. Extracto de código: `descargar_url()` (paginación) y `subir_a_s3()` (zonas).
