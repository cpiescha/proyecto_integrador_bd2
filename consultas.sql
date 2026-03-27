
SELECT rrhh_dw.cargar_hechos_ausencias();
SELECT rrhh_dw.cargar_hechos_evaluaciones();
SELECT rrhh_dw.cargar_hechos_capacitaciones();

SELECT * FROM rrhh_dw.dim_oficina;
SELECT * FROM rrhh_dw.dim_empleado;
SELECT * FROM rrhh_dw.dim_departamento;

SELECT * FROM oltp.oficinas;
SELECT * FROM oltp.departamentos;
SELECT * FROM oltp.ausencias;
