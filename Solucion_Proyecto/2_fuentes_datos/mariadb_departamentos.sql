-- =====================================================================
-- PUNTO 2 - Fuente de datos "Base de Datos" (AWS RDS MariaDB)
-- Crea la base de datos covid_db y la tabla maestra de departamentos.
--
-- Ejecutar desde la instancia EC2 (que tiene el cliente mariadb):
--   mariadb -h <ENDPOINT_RDS> -u admin -p < mariadb_departamentos.sql
--
-- Poblacion: proyeccion DANE 2020 (valores aproximados, uso academico).
-- Region: region natural de Colombia a la que pertenece el departamento.
-- cod_departamento: codigo DIVIPOLA oficial del departamento (llave que
-- luego se cruza con los casos de COVID en la zona trusted).
-- =====================================================================

SET NAMES utf8mb4;

CREATE DATABASE IF NOT EXISTS covid_db CHARACTER SET utf8mb4;
USE covid_db;

DROP TABLE IF EXISTS departamentos;

CREATE TABLE departamentos (
    cod_departamento INT          NOT NULL PRIMARY KEY,
    departamento     VARCHAR(60)  NOT NULL,
    region           VARCHAR(20)  NOT NULL,
    poblacion        INT          NOT NULL
) CHARACTER SET utf8mb4;

INSERT INTO departamentos (cod_departamento, departamento, region, poblacion) VALUES
( 5, 'Antioquia',          'Andina',    6677930),
( 8, 'Atlántico',          'Caribe',    2722128),
(11, 'Bogotá D.C.',        'Andina',    7743955),
(13, 'Bolívar',            'Caribe',    2180976),
(15, 'Boyacá',             'Andina',    1242731),
(17, 'Caldas',             'Andina',    1018453),
(18, 'Caquetá',            'Amazonía',   410521),
(19, 'Cauca',              'Pacífica',  1491937),
(20, 'Cesar',              'Caribe',    1295387),
(23, 'Córdoba',            'Caribe',    1828947),
(25, 'Cundinamarca',       'Andina',    3242999),
(27, 'Chocó',              'Pacífica',   544764),
(41, 'Huila',              'Andina',    1122622),
(44, 'La Guajira',         'Caribe',     965718),
(47, 'Magdalena',          'Caribe',    1427026),
(50, 'Meta',               'Orinoquía', 1063454),
(52, 'Nariño',             'Pacífica',  1627589),
(54, 'Norte de Santander', 'Andina',    1620318),
(63, 'Quindío',            'Andina',     555401),
(66, 'Risaralda',          'Andina',     961055),
(68, 'Santander',          'Andina',    2280908),
(70, 'Sucre',              'Caribe',     949252),
(73, 'Tolima',             'Andina',    1339998),
(76, 'Valle del Cauca',    'Pacífica',  4532152),
(81, 'Arauca',             'Orinoquía',  294206),
(85, 'Casanare',           'Orinoquía',  435195),
(86, 'Putumayo',           'Amazonía',   359127),
(88, 'San Andrés',         'Insular',     63692),
(91, 'Amazonas',           'Amazonía',    79020),
(94, 'Guainía',            'Amazonía',    50636),
(95, 'Guaviare',           'Amazonía',    86657),
(97, 'Vaupés',             'Amazonía',    44712),
(99, 'Vichada',            'Orinoquía',  112958);

-- Verificacion rapida (evidencia para el informe)
SELECT region, COUNT(*) AS departamentos, SUM(poblacion) AS poblacion_total
FROM departamentos
GROUP BY region
ORDER BY poblacion_total DESC;
