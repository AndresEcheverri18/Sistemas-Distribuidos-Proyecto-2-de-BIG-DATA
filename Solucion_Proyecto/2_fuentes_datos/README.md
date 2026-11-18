# Punto 2 – Fuentes de datos: RDS MariaDB, archivos en EC2 y URLs en Internet

El enunciado pide **simular** datos en tres tipos de fuente. Así quedan:

| Fuente | Recurso AWS / URL | Contenido | Cómo se crea |
|---|---|---|---|
| Base de datos | **RDS MariaDB** `covid-mariadb`, BD `covid_db`, tabla `departamentos` | 33 departamentos (código, nombre, región, población) | `mariadb_departamentos.sql` |
| Archivos | **EC2** Ubuntu, carpeta `/home/ubuntu/datos/` | `Casos_positivos_de_COVID-19_en_Colombia-100K.csv` (100.000 filas, 17 MB) | se copia del repo del curso |
| URL | API Socrata de **datos.gov.co** | casos 2-15 jul 2020 (59.067) y DIVIPOLA (1.122 municipios) | ya existen en Internet, solo se consumen |

La EC2 cumple **dos papeles**: es la *fuente de archivos* y también es el *servidor* donde corren la
ingesta (Punto 3) y la app Streamlit (Punto 8). Así se usan menos recursos.

---

## 2.1 Crear la instancia EC2 (fuente de archivos)

Consola → **EC2 → Launch instance**:

| Campo | Valor |
|---|---|
| Name | `covid-ec2` |
| AMI | **Ubuntu Server 24.04 LTS** |
| Instance type | `t3.small` (o `t2.micro`) |
| Key pair | `vockey` (la del Learner Lab) |
| Security group | permitir **SSH 22** y **TCP 8501** (Streamlit) desde `0.0.0.0/0` |
| Advanced details → IAM instance profile | **`LabInstanceProfile`** |

`LabInstanceProfile` le da a la EC2 permisos sobre S3 **sin** guardar llaves en el servidor: boto3
toma las credenciales del rol automáticamente.

Conectarse e instalar lo necesario:

```bash
ssh -i labsuser.pem ubuntu@<IP-publica-EC2>

sudo apt update
sudo apt install -y python3-venv python3-pip git mariadb-client zip

# código del proyecto (su repositorio GitHub) y repo del curso (trae el dataset)
git clone https://github.com/<su-usuario>/<su-repo>.git          # contiene Solucion_Proyecto/
git clone https://github.com/si3007eafit/si3007-262.git

# entorno virtual de Python (Ubuntu 24.04 no deja instalar con pip en el sistema)
python3 -m venv ~/venv
~/venv/bin/pip install -r ~/<su-repo>/Solucion_Proyecto/requirements.txt

# FUENTE "ARCHIVOS": carpeta de datos de la EC2
mkdir -p ~/datos
cp ~/si3007-262/datasets/covid19/Casos_positivos_de_COVID-19_en_Colombia-100K.csv ~/datos/
ls -lh ~/datos
```

---

## 2.2 Crear la base de datos RDS MariaDB

Consola → **RDS → Create database**:

| Campo | Valor |
|---|---|
| Creation method | Standard create |
| Engine | **MariaDB** (versión por defecto) |
| Templates | **Free tier** (o Dev/Test) |
| DB instance identifier | `covid-mariadb` |
| Master username | `admin` |
| Master password | la que quiera (anótela) |
| Instance class | `db.t3.micro` / `db.t4g.micro` |
| Storage | 20 GB gp2 |
| Connectivity | **Connect to an EC2 compute resource** → seleccionar `covid-ec2` |
| Public access | No |

La opción *Connect to an EC2 compute resource* crea automáticamente las reglas del Security Group
para que **solo la EC2** pueda entrar al puerto 3306 de la RDS. No hace falta abrir la base a Internet.

Cuando el estado sea *Available*, copiar el **Endpoint**
(ej. `covid-mariadb.abc123xyz.us-east-1.rds.amazonaws.com`).

### Cargar la tabla (desde la EC2)

```bash
cd ~/<su-repo>/Solucion_Proyecto/2_fuentes_datos
mariadb -h <ENDPOINT_RDS> -u admin -p < mariadb_departamentos.sql
```

Salida esperada (la última consulta del script; el cliente la imprime separada por tabuladores):

```
region      departamentos   poblacion_total
Andina      11              27806370
Caribe      7               11369434
Pacífica    4               8196442
Orinoquía   4               1905813
Amazonía    6               1030673
Insular     1               63692
```

Verificación manual:

```bash
mariadb -h <ENDPOINT_RDS> -u admin -p -e "SELECT * FROM covid_db.departamentos LIMIT 5;"
```

---

## 2.3 Fuentes URL (no hay que crear nada)

Son endpoints públicos de la API **Socrata** del portal de datos abiertos. Se pueden probar en el
navegador o con `curl`:

```bash
# 3 casos de julio 2020 (formato CSV)
curl "https://www.datos.gov.co/resource/gt2j-8ykr.csv?\$limit=3&\$where=fecha_reporte_web%20between%20'2020-07-02T00:00:00'%20and%20'2020-07-15T23:59:59'"

# 3 municipios DIVIPOLA
curl "https://www.datos.gov.co/resource/gdxc-w37w.csv?\$limit=3"
```

Parámetros de la API que se usan (lenguaje **SoQL**):

| Parámetro | Para qué |
|---|---|
| `$where=fecha_reporte_web between '...' and '...'` | filtra solo los casos del 2 al 15 de julio, que continúan al archivo de la EC2 (llega hasta el 1-jul) |
| `$order=id_de_caso` | orden estable, necesario para paginar sin repetir ni saltar filas |
| `$limit` / `$offset` | paginación: 50.000 filas por petición |

---

## 2.4 Explicación del código: `mariadb_departamentos.sql`

| Bloque | Qué hace |
|---|---|
| `SET NAMES utf8mb4` | La sesión usa UTF-8 completo, para que tildes y eñes (Bogotá, Nariño) se guarden bien |
| `CREATE DATABASE IF NOT EXISTS covid_db CHARACTER SET utf8mb4` | Crea la BD solo si no existe, así el script se puede ejecutar varias veces (es *idempotente*) |
| `DROP TABLE IF EXISTS departamentos` | Borra la tabla anterior para recrearla limpia |
| `CREATE TABLE departamentos (...)` | `cod_departamento` es **PRIMARY KEY** (código DIVIPOLA: 5 = Antioquia, 11 = Bogotá). Es la llave con la que el ETL cruza los casos |
| `INSERT INTO ... VALUES (...)` | 33 filas: 32 departamentos + Bogotá D.C. La población suma 50.372.424 (proyección DANE 2020) |
| `SELECT region, COUNT(*), SUM(poblacion) ... GROUP BY region` | Consulta de verificación que sirve como evidencia |

**¿Quién usa esta tabla después?** `3_ingesta/ingesta.py`, en la función `ingestar_mariadb()`, hace
`SELECT * FROM departamentos` y la sube a `s3://<bucket>/raw/mariadb/departamentos/`.

---

## Evidencias para el informe (Punto 2)

1. Consola EC2: instancia `covid-ec2` en estado *Running* con `LabInstanceProfile`.
2. Terminal EC2: `ls -lh ~/datos` mostrando el CSV de 17 MB.
3. Consola RDS: `covid-mariadb` *Available* con su endpoint.
4. Terminal EC2: salida del script SQL (conteo por región) y un `SELECT * FROM departamentos`.
5. Navegador o `curl`: respuesta de las dos URLs de datos.gov.co.
