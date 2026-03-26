-- 08_ETL_Cargar_Hechos.sql
-- Carga de hechos desde OLTP hacia rrhh_dw.fact_*

CREATE TABLE IF NOT EXISTS rrhh_dw.fact_ausencias (
  fact_id BIGSERIAL PRIMARY KEY,
  fecha DATE,
  emp_sk BIGINT,
  codigo_departamento VARCHAR(50),
  codigo_oficina VARCHAR(50),
  tipo_ausencia VARCHAR(50),
  dias NUMERIC(6,2),
  justificada BOOLEAN
);

CREATE TABLE IF NOT EXISTS rrhh_dw.fact_evaluaciones (
  fact_id BIGSERIAL PRIMARY KEY,
  fecha DATE,
  emp_sk BIGINT,
  evaluador_emp_sk BIGINT,
  calificacion NUMERIC(3,2)
);

CREATE TABLE IF NOT EXISTS rrhh_dw.fact_capacitaciones (
  fact_id BIGSERIAL PRIMARY KEY,
  fecha DATE,
  emp_sk BIGINT,
  capacitacion_id BIGINT,
  estado VARCHAR(50),
  calificacion INT
);

-- Carga hechos: ausencias (función en español)
CREATE OR REPLACE FUNCTION rrhh_dw.cargar_hechos_ausencias()
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO rrhh_dw.fact_ausencias(fecha, emp_sk, codigo_departamento, codigo_oficina, tipo_ausencia, dias, justificada)
  SELECT a.fecha_inicio,
         e.emp_sk,
         e.codigo_departamento,
         e.codigo_oficina,
         a.tipo_ausencia,
         a.dias,
         a.justificada
  FROM public.ausencias a
  JOIN rrhh_dw.dim_empleado e ON e.numero_empleado = (SELECT numero_empleado FROM public.empleados WHERE empleado_id = a.empleado_id LIMIT 1) AND e.vigente = true
  WHERE NOT EXISTS (
    SELECT 1 FROM rrhh_dw.fact_ausencias f WHERE f.fecha = a.fecha_inicio AND f.emp_sk = e.emp_sk AND f.tipo_ausencia = a.tipo_ausencia AND f.dias = a.dias
  );
END;$$;

-- Carga hechos: evaluaciones
CREATE OR REPLACE FUNCTION rrhh_dw.cargar_hechos_evaluaciones()
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO rrhh_dw.fact_evaluaciones(fecha, emp_sk, evaluador_emp_sk, calificacion)
  SELECT ev.fecha_evaluacion,
         e.emp_sk,
         evr.emp_sk,
         ev.calificacion
  FROM public.evaluaciones ev
  JOIN rrhh_dw.dim_empleado e ON e.numero_empleado = (SELECT numero_empleado FROM public.empleados WHERE empleado_id = ev.empleado_id LIMIT 1) AND e.vigente = true
  LEFT JOIN rrhh_dw.dim_empleado evr ON evr.numero_empleado = (SELECT numero_empleado FROM public.empleados WHERE empleado_id = ev.evaluador_id LIMIT 1) AND evr.vigente = true
  WHERE NOT EXISTS (
    SELECT 1 FROM rrhh_dw.fact_evaluaciones f WHERE f.fecha = ev.fecha_evaluacion AND f.emp_sk = e.emp_sk AND f.calificacion = ev.calificacion
  );
END;$$;

-- Carga hechos: capacitaciones
CREATE OR REPLACE FUNCTION rrhh_dw.cargar_hechos_capacitaciones()
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO rrhh_dw.fact_capacitaciones(fecha, emp_sk, capacitacion_id, estado, calificacion)
  SELECT ec.fecha_completado::date,
         e.emp_sk,
         ec.capacitacion_id,
         ec.estado,
         ec.calificacion
  FROM public.empleados_capacitaciones ec
  JOIN rrhh_dw.dim_empleado e ON e.numero_empleado = (SELECT numero_empleado FROM public.empleados WHERE empleado_id = ec.empleado_id LIMIT 1) AND e.vigente = true
  WHERE ec.fecha_completado IS NOT NULL
    AND NOT EXISTS (
      SELECT 1 FROM rrhh_dw.fact_capacitaciones f WHERE f.fecha = ec.fecha_completado::date AND f.emp_sk = e.emp_sk AND f.capacitacion_id = ec.capacitacion_id
    );
END;$$;

-- Uso: ejecutar en este orden después de cargar dimensiones: SELECT rrhh_dw.cargar_hechos_ausencias(); SELECT rrhh_dw.cargar_hechos_evaluaciones(); SELECT rrhh_dw.cargar_hechos_capacitaciones();
