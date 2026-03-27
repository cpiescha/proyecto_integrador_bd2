-- 07_ETL_Cargar_Dimensiones.sql
-- Procedimientos SCD Tipo 2 para dimensiones: Empleado, Departamento, Oficina
-- Comunicación entre esquemas: oltp (origen) y rrhh_dw (destino)

CREATE SCHEMA IF NOT EXISTS rrhh_dw;

CREATE TABLE IF NOT EXISTS rrhh_dw.dim_oficina (
  of_sk SERIAL PRIMARY KEY,
  codigo_oficina VARCHAR(50) NOT NULL,
  nombre VARCHAR(150),
  ciudad VARCHAR(100),
  pais VARCHAR(100),
  fecha_inicio_vigencia TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
  fecha_fin_vigencia TIMESTAMP WITHOUT TIME ZONE,
  vigente BOOLEAN DEFAULT TRUE
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_oficina_codigo_vigente ON rrhh_dw.dim_oficina(codigo_oficina, vigente) WHERE vigente;

CREATE TABLE IF NOT EXISTS rrhh_dw.dim_departamento (
  dept_sk SERIAL PRIMARY KEY,
  departamento_id VARCHAR(50) NOT NULL,
  nombre VARCHAR(150),
  codigo_oficina VARCHAR(50),
  fecha_inicio_vigencia TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
  fecha_fin_vigencia TIMESTAMP WITHOUT TIME ZONE,
  vigente BOOLEAN DEFAULT TRUE
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_dept_codigo_vigente ON rrhh_dw.dim_departamento(departamento_id, vigente) WHERE vigente;

CREATE TABLE IF NOT EXISTS rrhh_dw.dim_empleado (
  emp_sk BIGSERIAL PRIMARY KEY,
  empleado_id VARCHAR(20) NOT NULL,
  nombre VARCHAR(100),
  apellidos VARCHAR(100),
  genero VARCHAR(20),
  fecha_nacimiento DATE,
  fecha_contratacion DATE,
  salario_actual NUMERIC(12,2),
  departamento_id VARCHAR(50),
  codigo_oficina VARCHAR(50),
  jefe_id VARCHAR(20),
  fecha_inicio_vigencia TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
  fecha_fin_vigencia TIMESTAMP WITHOUT TIME ZONE,
  vigente BOOLEAN DEFAULT TRUE
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_emp_id_vigente ON rrhh_dw.dim_empleado(empleado_id, vigente) WHERE vigente;

-- Función SCD2: oficinas (de oltp a rrhh_dw)
CREATE OR REPLACE FUNCTION rrhh_dw.scd2_upsert_oficinas()
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  r RECORD;
  existing RECORD;
BEGIN
  FOR r IN SELECT * FROM oltp.oficinas LOOP
    SELECT * INTO existing FROM rrhh_dw.dim_oficina WHERE codigo_oficina = r.codigo_oficina AND vigente = true LIMIT 1;
    IF NOT FOUND THEN
      INSERT INTO rrhh_dw.dim_oficina(codigo_oficina, nombre, ciudad, pais) VALUES (r.codigo_oficina, r.nombre, r.ciudad, r.pais);
    ELSE
      IF existing.nombre IS DISTINCT FROM r.nombre OR existing.ciudad IS DISTINCT FROM r.ciudad OR existing.pais IS DISTINCT FROM r.pais THEN
        UPDATE rrhh_dw.dim_oficina SET fecha_fin_vigencia = now(), vigente = false WHERE of_sk = existing.of_sk;
        INSERT INTO rrhh_dw.dim_oficina(codigo_oficina, nombre, ciudad, pais) VALUES (r.codigo_oficina, r.nombre, r.ciudad, r.pais);
      END IF;
    END IF;
  END LOOP;
END;$$;

-- Función SCD2: departamentos (de oltp a rrhh_dw)
CREATE OR REPLACE FUNCTION rrhh_dw.scd2_upsert_departamentos()
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  r RECORD;
  existing RECORD;
BEGIN
  FOR r IN SELECT d.* FROM oltp.departamentos d LOOP
    SELECT * INTO existing FROM rrhh_dw.dim_departamento WHERE departamento_id = r.departamento_id AND vigente = true LIMIT 1;
    IF NOT FOUND THEN
      INSERT INTO rrhh_dw.dim_departamento(departamento_id, nombre, codigo_oficina) VALUES (r.departamento_id, r.nombre, r.codigo_oficina);
    ELSE
      IF existing.nombre IS DISTINCT FROM r.nombre OR existing.codigo_oficina IS DISTINCT FROM r.codigo_oficina THEN
        UPDATE rrhh_dw.dim_departamento SET fecha_fin_vigencia = now(), vigente = false WHERE dept_sk = existing.dept_sk;
        INSERT INTO rrhh_dw.dim_departamento(departamento_id, nombre, codigo_oficina) VALUES (r.departamento_id, r.nombre, r.codigo_oficina);
      END IF;
    END IF;
  END LOOP;
END;$$;

-- Función SCD2: empleados (de oltp a rrhh_dw)
CREATE OR REPLACE FUNCTION rrhh_dw.scd2_upsert_empleados()
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  r RECORD;
  existing RECORD;
BEGIN
  FOR r IN SELECT e.* FROM oltp.empleados e LOOP
    SELECT * INTO existing FROM rrhh_dw.dim_empleado WHERE empleado_id = r.empleado_id AND vigente = true LIMIT 1;
    IF NOT FOUND THEN
      INSERT INTO rrhh_dw.dim_empleado(empleado_id, nombre, apellidos, genero, fecha_nacimiento, fecha_contratacion, salario_actual, departamento_id, codigo_oficina, jefe_id)
      VALUES (r.empleado_id, r.nombre, r.apellidos, r.genero, r.fecha_nacimiento, r.fecha_contratacion, r.salario_actual, r.departamento_id, r.codigo_oficina, r.jefe_id);
    ELSE
      IF existing.nombre IS DISTINCT FROM r.nombre OR existing.apellidos IS DISTINCT FROM r.apellidos OR
         existing.salario_actual IS DISTINCT FROM r.salario_actual OR existing.departamento_id IS DISTINCT FROM r.departamento_id OR
         existing.codigo_oficina IS DISTINCT FROM r.codigo_oficina OR existing.jefe_id IS DISTINCT FROM r.jefe_id THEN
        UPDATE rrhh_dw.dim_empleado SET fecha_fin_vigencia = now(), vigente = false WHERE emp_sk = existing.emp_sk;
        INSERT INTO rrhh_dw.dim_empleado(empleado_id, nombre, apellidos, genero, fecha_nacimiento, fecha_contratacion, salario_actual, departamento_id, codigo_oficina, jefe_id)
        VALUES (r.empleado_id, r.nombre, r.apellidos, r.genero, r.fecha_nacimiento, r.fecha_contratacion, r.salario_actual, r.departamento_id, r.codigo_oficina, r.jefe_id);
      END IF;
    END IF;
  END LOOP;
END;$$;
