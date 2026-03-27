-- CREACIÓN DE BASE DE DATOS OLTP RRHH
CREATE SCHEMA IF NOT EXISTS oltp;

-- 1. Tabla Oficinas
CREATE TABLE IF NOT EXISTS oltp.oficinas (
    codigo_oficina VARCHAR(20) PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    ciudad VARCHAR(50) NOT NULL,
    pais VARCHAR(50) NOT NULL,
    region VARCHAR(50) NOT NULL,
    codigo_postal VARCHAR(20) NOT NULL,
    telefono VARCHAR(20) NOT NULL,
    direccion VARCHAR(100) NOT NULL
);

-- 2. Tabla Departamentos Organizacionales
CREATE TABLE IF NOT EXISTS oltp.departamentos (
    departamento_id VARCHAR(20) PRIMARY KEY,
    nombre VARCHAR(50) NOT NULL,
    descripcion TEXT,
    codigo_oficina VARCHAR(20) NOT NULL REFERENCES oltp.oficinas(codigo_oficina)
);

-- 3. Tabla Puestos de Trabajo
CREATE TABLE IF NOT EXISTS oltp.puestos (
    puesto_id SERIAL PRIMARY KEY,
    nombre VARCHAR(50) NOT NULL,
    nivel_salarial VARCHAR(20) NOT NULL,
    salario_minimo NUMERIC(10, 2) NOT NULL,
    salario_maximo NUMERIC(10, 2) NOT NULL,
    CONSTRAINT chk_salarios CHECK (salario_maximo >= salario_minimo)
);

-- 4. Tabla Empleados
CREATE TABLE IF NOT EXISTS oltp.empleados (
    empleado_id VARCHAR(20) PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    apellidos VARCHAR(100) NOT NULL,
    fecha_nacimiento DATE NOT NULL,
    genero VARCHAR(20),
    estado_civil VARCHAR(50),
    email VARCHAR(100) UNIQUE NOT NULL,
    telefono VARCHAR(20),
    fecha_contratacion DATE NOT NULL,
    departamento_id VARCHAR(20) NOT NULL REFERENCES oltp.departamentos(departamento_id),
    puesto_id INT NOT NULL REFERENCES oltp.puestos(puesto_id),
    salario_actual NUMERIC(10, 2) NOT NULL,
    jefe_id VARCHAR(20) REFERENCES oltp.empleados(empleado_id),
    codigo_oficina VARCHAR(20) NOT NULL REFERENCES oltp.oficinas(codigo_oficina)
);

-- 5. Tabla Ausencias
CREATE TABLE IF NOT EXISTS oltp.ausencias (
    ausencia_id SERIAL PRIMARY KEY,
    empleado_id VARCHAR(20) NOT NULL REFERENCES oltp.empleados(empleado_id),
    tipo_ausencia VARCHAR(100) NOT NULL,
    fecha_inicio DATE NOT NULL,
    fecha_fin DATE NOT NULL,
    dias_totales INT GENERATED ALWAYS AS (fecha_fin - fecha_inicio + 1) STORED,
    justificada BOOLEAN,
    comentarios TEXT,
    fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_fechas_ausencia CHECK (fecha_fin >= fecha_inicio)
);

-- 6. Tabla Evaluaciones de Desempeño
CREATE TABLE IF NOT EXISTS oltp.evaluaciones (
    evaluacion_id SERIAL PRIMARY KEY,
    empleado_id VARCHAR(20) NOT NULL REFERENCES oltp.empleados(empleado_id),
	fecha_evaluacion DATE NOT NULL, 
	calificacion NUMERIC(3, 2) NOT NULL,
    evaluador_id VARCHAR(20) NOT NULL REFERENCES oltp.empleados(empleado_id),
    comentarios TEXT,
    fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_calificacion CHECK (calificacion >= 1.0 AND calificacion <= 5.0)
);

-- 7. Tabla Capacitaciones
CREATE TABLE IF NOT EXISTS oltp.capacitaciones (
    capacitacion_id SERIAL PRIMARY KEY,
    nombre VARCHAR(150) NOT NULL,
    descripcion TEXT,
    proveedor VARCHAR(100),
    costo NUMERIC(10, 2),
    fecha_inicio DATE NOT NULL,
    fecha_fin DATE NOT NULL,
    duracion_dias INT GENERATED ALWAYS AS (fecha_fin - fecha_inicio + 1) STORED,
    CONSTRAINT chk_fechas_cap CHECK (fecha_fin >= fecha_inicio)
);

-- 8. Tabla Asignación Empleados - Capacitaciones
CREATE TABLE IF NOT EXISTS oltp.empleados_capacitaciones (
    empleado_id VARCHAR(20) NOT NULL REFERENCES oltp.empleados(empleado_id),
    capacitacion_id INT NOT NULL REFERENCES oltp.capacitaciones(capacitacion_id),
	calificacion NUMERIC(5, 2),
	fecha_completado DATE,
    estado VARCHAR(20) NOT NULL,
    comentarios TEXT,
    CONSTRAINT chk_calificacion_cap CHECK (calificacion >= 0 AND calificacion <= 100),
	PRIMARY KEY (empleado_id, capacitacion_id)
);