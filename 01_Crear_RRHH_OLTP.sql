-- 01_Crear_RRHH_OLTP.sql
-- Crear base de datos OLTP y tablas para RRHH
-- Ejecutar en psql (si es necesario crear la DB):
--   CREATE DATABASE rrhh_oltp;
--   \c rrhh_oltp

-- Tablas OLTP (esquema normalizado para RRHH)

CREATE TABLE IF NOT EXISTS oficinas (
  oficina_id SERIAL PRIMARY KEY,
  codigo_oficina VARCHAR(50) NOT NULL UNIQUE,
  nombre VARCHAR(150),
  direccion TEXT,
  ciudad VARCHAR(100),
  region VARCHAR(100),
  pais VARCHAR(100),
  codigo_postal VARCHAR(20),
  telefono VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS departamentos (
  departamento_id SERIAL PRIMARY KEY,
  codigo_departamento VARCHAR(50) NOT NULL UNIQUE,
  nombre VARCHAR(150) NOT NULL,
  descripcion TEXT,
  oficina_id INT REFERENCES oficinas(oficina_id)
);

CREATE TABLE IF NOT EXISTS puestos (
  puesto_id SERIAL PRIMARY KEY,
  titulo VARCHAR(150) NOT NULL,
  nivel VARCHAR(50),
  salario_min NUMERIC(12,2),
  salario_max NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS empleados (
  empleado_id BIGSERIAL PRIMARY KEY,
  numero_empleado VARCHAR(20) NOT NULL UNIQUE,
  nombre VARCHAR(100) NOT NULL,
  apellido VARCHAR(100) NOT NULL,
  genero VARCHAR(10),
  fecha_nacimiento DATE,
  estado_civil VARCHAR(50),
  correo VARCHAR(200),
  telefono VARCHAR(50),
  fecha_contratacion DATE,
  salario NUMERIC(12,2),
  departamento_id INT REFERENCES departamentos(departamento_id),
  puesto_id INT REFERENCES puestos(puesto_id),
  oficina_id INT REFERENCES oficinas(oficina_id),
  jefe_id BIGINT REFERENCES empleados(empleado_id),
  estado VARCHAR(50) DEFAULT 'Activo'
);

CREATE INDEX IF NOT EXISTS idx_empleados_departamento ON empleados(departamento_id);

CREATE TABLE IF NOT EXISTS ausencias (
  ausencia_id BIGSERIAL PRIMARY KEY,
  empleado_id BIGINT REFERENCES empleados(empleado_id) ON DELETE CASCADE,
  tipo_ausencia VARCHAR(50) NOT NULL,
  fecha_inicio DATE NOT NULL,
  fecha_fin DATE NOT NULL,
  dias NUMERIC(6,2) NOT NULL,
  justificada BOOLEAN DEFAULT FALSE,
  motivo TEXT,
  fecha_registro TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS evaluaciones (
  evaluacion_id BIGSERIAL PRIMARY KEY,
  empleado_id BIGINT REFERENCES empleados(empleado_id) ON DELETE CASCADE,
  evaluador_id BIGINT REFERENCES empleados(empleado_id),
  fecha_evaluacion DATE NOT NULL,
  calificacion NUMERIC(3,2) CHECK(calificacion>=1.0 AND calificacion<=5.0),
  comentarios TEXT,
  fecha_registro TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS capacitaciones (
  capacitacion_id BIGSERIAL PRIMARY KEY,
  nombre VARCHAR(200) NOT NULL,
  descripcion TEXT,
  proveedor VARCHAR(200),
  fecha_inicio DATE,
  fecha_fin DATE,
  costo NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS empleados_capacitaciones (
  empleado_capacitacion_id BIGSERIAL PRIMARY KEY,
  empleado_id BIGINT REFERENCES empleados(empleado_id) ON DELETE CASCADE,
  capacitacion_id BIGINT REFERENCES capacitaciones(capacitacion_id) ON DELETE CASCADE,
  fecha_asignacion DATE DEFAULT now(),
  fecha_completado DATE,
  calificacion INT CHECK(calificacion>=0 AND calificacion<=100),
  estado VARCHAR(50) DEFAULT 'En Curso',
  comentarios TEXT
);

-- índices para acelerar carga y join en ETL
CREATE INDEX IF NOT EXISTS idx_ausencias_empleado ON ausencias(empleado_id);
CREATE INDEX IF NOT EXISTS idx_evaluaciones_empleado ON evaluaciones(empleado_id);
CREATE INDEX IF NOT EXISTS idx_ec_empleado ON empleados_capacitaciones(empleado_id);
