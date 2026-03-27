-- 08_ETL_Cargar_Hechos.sql
-- Carga de hechos desde esquema oltp hacia esquema rrhh_dw

CREATE SCHEMA IF NOT EXISTS rrhh_dw;

-- Tablas de hechos en el esquema rrhh_dw
CREATE TABLE IF NOT EXISTS rrhh_dw.hechos_ausencias (
  hecho_id BIGSERIAL PRIMARY KEY,
  fecha DATE,
  emp_sk BIGINT,
  departamento_id VARCHAR(50),
  codigo_oficina VARCHAR(50),
  tipo_ausencia VARCHAR(100),
  dias_totales INT,
  justificada BOOLEAN
);

CREATE TABLE IF NOT EXISTS rrhh_dw.hechos_evaluaciones (
  hecho_id BIGSERIAL PRIMARY KEY,
  fecha DATE,
  emp_sk BIGINT,
  evaluador_emp_sk BIGINT,
  calificacion NUMERIC(3,2)
);

CREATE TABLE IF NOT EXISTS rrhh_dw.hechos_capacitaciones (
  hecho_id BIGSERIAL PRIMARY KEY,
  fecha DATE,
  emp_sk BIGINT,
  capacitacion_id BIGINT,
  estado VARCHAR(50),
  calificacion NUMERIC(5,2)
);

-- Carga hechos: ausencias (de oltp a rrhh_dw)
CREATE OR REPLACE FUNCTION rrhh_dw.cargar_hechos_ausencias()
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO rrhh_dw.hechos_ausencias(fecha, emp_sk, departamento_id, codigo_oficina, tipo_ausencia, dias_totales, justificada)
  SELECT a.fecha_inicio,
         e.emp_sk,
         e.departamento_id,
         e.codigo_oficina,
         a.tipo_ausencia,
         a.dias_totales,
         a.justificada
  FROM oltp.ausencias a
  JOIN rrhh_dw.dim_empleado e ON e.empleado_id = a.empleado_id AND e.vigente = true
  WHERE NOT EXISTS (
    SELECT 1 FROM rrhh_dw.hechos_ausencias f WHERE f.fecha = a.fecha_inicio AND f.emp_sk = e.emp_sk AND f.tipo_ausencia = a.tipo_ausencia AND f.dias_totales = a.dias_totales
  );
END;$$;

-- Carga hechos: evaluaciones (de oltp a rrhh_dw)
CREATE OR REPLACE FUNCTION rrhh_dw.cargar_hechos_evaluaciones()
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO rrhh_dw.hechos_evaluaciones(fecha, emp_sk, evaluador_emp_sk, calificacion)
  SELECT ev.fecha_evaluacion,
         e.emp_sk,
         evr.emp_sk,
         ev.calificacion
  FROM oltp.evaluaciones ev
  JOIN rrhh_dw.dim_empleado e ON e.empleado_id = ev.empleado_id AND e.vigente = true
  LEFT JOIN rrhh_dw.dim_empleado evr ON evr.empleado_id = ev.evaluador_id AND evr.vigente = true
  WHERE NOT EXISTS (
    SELECT 1 FROM rrhh_dw.hechos_evaluaciones f WHERE f.fecha = ev.fecha_evaluacion AND f.emp_sk = e.emp_sk AND f.calificacion = ev.calificacion
  );
END;$$;

-- Carga hechos: capacitaciones (de oltp a rrhh_dw)
CREATE OR REPLACE FUNCTION rrhh_dw.cargar_hechos_capacitaciones()
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO rrhh_dw.hechos_capacitaciones(fecha, emp_sk, capacitacion_id, estado, calificacion)
  SELECT ec.fecha_completado::date,
         e.emp_sk,
         ec.capacitacion_id,
         ec.estado,
         ec.calificacion
  FROM oltp.empleados_capacitaciones ec
  JOIN rrhh_dw.dim_empleado e ON e.empleado_id = ec.empleado_id AND e.vigente = true
  WHERE ec.fecha_completado IS NOT NULL
    AND NOT EXISTS (
      SELECT 1 FROM rrhh_dw.hechos_capacitaciones f WHERE f.fecha = ec.fecha_completado::date AND f.emp_sk = e.emp_sk AND f.capacitacion_id = ec.capacitacion_id
    );
END;$$;
