-- 07_ETL_Cargar_Dimensiones.sql
-- Procedimientos SCD Tipo 2 para dimensiones: Empleado, Departamento, Oficina
-- Suponemos que las tablas OLTP están en el esquema public y que el DW usa rrhh_dw

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
  codigo_departamento VARCHAR(50) NOT NULL,
  nombre VARCHAR(150),
  codigo_oficina VARCHAR(50),
  fecha_inicio_vigencia TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
  fecha_fin_vigencia TIMESTAMP WITHOUT TIME ZONE,
  vigente BOOLEAN DEFAULT TRUE
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_dept_codigo_vigente ON rrhh_dw.dim_departamento(codigo_departamento, vigente) WHERE vigente;

CREATE TABLE IF NOT EXISTS rrhh_dw.dim_empleado (
  emp_sk BIGSERIAL PRIMARY KEY,
  numero_empleado VARCHAR(20) NOT NULL,
  nombre VARCHAR(100),
  apellido VARCHAR(100),
  genero VARCHAR(10),
  fecha_nacimiento DATE,
  fecha_contratacion DATE,
  salario NUMERIC(12,2),
  codigo_departamento VARCHAR(50),
  codigo_oficina VARCHAR(50),
  numero_jefe VARCHAR(20),
  fecha_inicio_vigencia TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
  fecha_fin_vigencia TIMESTAMP WITHOUT TIME ZONE,
  vigente BOOLEAN DEFAULT TRUE
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_emp_empnum_vigente ON rrhh_dw.dim_empleado(numero_empleado, vigente) WHERE vigente;

-- Función SCD2: actualizar/insertar oficinas (usa tablas OLTP en español)
CREATE OR REPLACE FUNCTION rrhh_dw.scd2_upsert_oficinas()
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  r RECORD;
  existing RECORD;
BEGIN
  FOR r IN SELECT * FROM public.oficinas LOOP
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

-- Función SCD2: actualizar/insertar departamentos
CREATE OR REPLACE FUNCTION rrhh_dw.scd2_upsert_departamentos()
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  r RECORD;
  existing RECORD;
  codigo_ofi TEXT;
BEGIN
  FOR r IN SELECT d.*, o.codigo_oficina FROM public.departamentos d LEFT JOIN public.oficinas o ON d.oficina_id = o.oficina_id LOOP
    codigo_ofi := r.codigo_oficina;
    SELECT * INTO existing FROM rrhh_dw.dim_departamento WHERE codigo_departamento = r.codigo_departamento AND vigente = true LIMIT 1;
    IF NOT FOUND THEN
      INSERT INTO rrhh_dw.dim_departamento(codigo_departamento, nombre, codigo_oficina) VALUES (r.codigo_departamento, r.nombre, codigo_ofi);
    ELSE
      IF existing.nombre IS DISTINCT FROM r.nombre OR existing.codigo_oficina IS DISTINCT FROM codigo_ofi THEN
        UPDATE rrhh_dw.dim_departamento SET fecha_fin_vigencia = now(), vigente = false WHERE dept_sk = existing.dept_sk;
        INSERT INTO rrhh_dw.dim_departamento(codigo_departamento, nombre, codigo_oficina) VALUES (r.codigo_departamento, r.nombre, codigo_ofi);
      END IF;
    END IF;
  END LOOP;
END;$$;

-- Función SCD2: actualizar/insertar empleados
CREATE OR REPLACE FUNCTION rrhh_dw.scd2_upsert_empleados()
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  r RECORD;
  existing RECORD;
  codigo_ofi TEXT;
  codigo_dept TEXT;
  numero_jefe TEXT;
BEGIN
  FOR r IN SELECT e.*, d.codigo_departamento, o.codigo_oficina, m.numero_empleado AS numero_jefe
           FROM public.empleados e
           LEFT JOIN public.departamentos d ON e.departamento_id = d.departamento_id
           LEFT JOIN public.oficinas o ON e.oficina_id = o.oficina_id
           LEFT JOIN public.empleados m ON e.jefe_id = m.empleado_id
  LOOP
    codigo_ofi := r.codigo_oficina;
    codigo_dept := r.codigo_departamento;
    numero_jefe := r.numero_jefe;
    SELECT * INTO existing FROM rrhh_dw.dim_empleado WHERE numero_empleado = r.numero_empleado AND vigente = true LIMIT 1;
    IF NOT FOUND THEN
      INSERT INTO rrhh_dw.dim_empleado(numero_empleado, nombre, apellido, genero, fecha_nacimiento, fecha_contratacion, salario, codigo_departamento, codigo_oficina, numero_jefe)
      VALUES (r.numero_empleado, r.nombre, r.apellido, r.genero, r.fecha_nacimiento, r.fecha_contratacion, r.salario, codigo_dept, codigo_ofi, numero_jefe);
    ELSE
      IF existing.nombre IS DISTINCT FROM r.nombre OR existing.apellido IS DISTINCT FROM r.apellido OR
         existing.salario IS DISTINCT FROM r.salario OR existing.codigo_departamento IS DISTINCT FROM codigo_dept OR
         existing.codigo_oficina IS DISTINCT FROM codigo_ofi OR existing.numero_jefe IS DISTINCT FROM numero_jefe THEN
        UPDATE rrhh_dw.dim_empleado SET fecha_fin_vigencia = now(), vigente = false WHERE emp_sk = existing.emp_sk;
        INSERT INTO rrhh_dw.dim_empleado(numero_empleado, nombre, apellido, genero, fecha_nacimiento, fecha_contratacion, salario, codigo_departamento, codigo_oficina, numero_jefe)
        VALUES (r.numero_empleado, r.nombre, r.apellido, r.genero, r.fecha_nacimiento, r.fecha_contratacion, r.salario, codigo_dept, codigo_ofi, numero_jefe);
      END IF;
    END IF;
  END LOOP;
END;$$;

-- Uso: SELECT rrhh_dw.scd2_upsert_oficinas(); SELECT rrhh_dw.scd2_upsert_departamentos(); SELECT rrhh_dw.scd2_upsert_empleados();
