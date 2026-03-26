-- 06_ETL_Poblar_DimTiempo.sql
-- Genera y puebla la dimensión de tiempo (Dim_Tiempo)
-- Ejecutar en la base donde existen las tablas OLTP y/o el DW

CREATE SCHEMA IF NOT EXISTS rrhh_dw;

CREATE TABLE IF NOT EXISTS rrhh_dw.dim_tiempo (
  fecha DATE PRIMARY KEY,
  dia INT,
  mes INT,
  anio INT,
  nombre_mes VARCHAR(20),
  trimestre INT,
  dia_semana INT,
  nombre_dia VARCHAR(20),
  es_fin_de_semana BOOLEAN
);

-- Procedimiento que puebla dim_tiempo entre dos fechas (nombre en español)
CREATE OR REPLACE FUNCTION rrhh_dw.poblar_dim_tiempo(p_inicio DATE, p_fin DATE)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  d DATE := p_inicio;
BEGIN
  IF p_fin < p_inicio THEN
    RAISE EXCEPTION 'p_fin debe ser mayor o igual a p_inicio';
  END IF;
  WHILE d <= p_fin LOOP
    INSERT INTO rrhh_dw.dim_tiempo(fecha, dia, mes, anio, nombre_mes, trimestre, dia_semana, nombre_dia, es_fin_de_semana)
    VALUES (
      d,
      EXTRACT(DAY FROM d)::int,
      EXTRACT(MONTH FROM d)::int,
      EXTRACT(YEAR FROM d)::int,
      to_char(d,'Month'),
      EXTRACT(QUARTER FROM d)::int,
      EXTRACT(ISODOW FROM d)::int,
      to_char(d,'Day'),
      (EXTRACT(ISODOW FROM d) IN (6,7))
    ) ON CONFLICT (fecha) DO NOTHING;
    d := d + INTERVAL '1 day';
  END LOOP;
END;$$;

-- Ejemplo: SELECT rrhh_dw.poblar_dim_tiempo('2018-01-01','2026-12-31');
