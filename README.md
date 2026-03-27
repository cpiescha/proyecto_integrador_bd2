# proyecto_integrador_bd2

Este repositorio contiene los scripts para crear una base de datos operacional OLTP de Recursos Humanos (RRHH), poblarla con datos realistas y ejecutar procesos ETL para cargar un Data Warehouse (RRHH_DW). A continuación se describen en detalle los scripts creados y las tablas principales.

**Scripts y descripción detallada**

- `01_Crear_RRHH_OLTP.sql`:
	- Propósito: crear la estructura OLTP (tablas y relaciones) en PostgreSQL.
	- Tablas creadas:
		- `oficinas`: almacena las oficinas de la empresa. Columnas clave: `oficina_id` (PK), `codigo_oficina` (código único), `nombre`, `direccion`, `ciudad`, `region`, `pais`, `codigo_postal`, `telefono`.
		- `departamentos`: estructura organizacional. Columnas: `departamento_id` (PK), `codigo_departamento`, `nombre`, `descripcion`, `oficina_id` (FK → `oficinas`).
		- `puestos`: catálogo de puestos. Columnas: `puesto_id` (PK), `titulo`, `nivel`, `salario_min`, `salario_max`.
		- `empleados`: registro maestro de empleados. Columnas principales: `empleado_id` (PK), `numero_empleado` (código único), `nombre`, `apellido`, `genero`, `fecha_nacimiento`, `estado_civil`, `correo`, `telefono`, `fecha_contratacion`, `salario`, `departamento_id` (FK → `departamentos`), `puesto_id` (FK → `puestos`), `oficina_id` (FK → `oficinas`), `jefe_id` (auto-referencia), `estado`.
		- `ausencias`: movimientos de ausencias. Columnas: `ausencia_id` (PK), `empleado_id` (FK → `empleados`), `tipo_ausencia`, `fecha_inicio`, `fecha_fin`, `dias`, `justificada`, `motivo`, `fecha_registro`.
		- `evaluaciones`: evaluaciones de desempeño. Columnas: `evaluacion_id` (PK), `empleado_id` (FK), `evaluador_id` (FK), `fecha_evaluacion`, `calificacion` (1.0-5.0), `comentarios`, `fecha_registro`.
		- `capacitaciones`: catálogo de capacitaciones. Columnas: `capacitacion_id` (PK), `nombre`, `descripcion`, `proveedor`, `fecha_inicio`, `fecha_fin`, `costo`.
		- `empleados_capacitaciones`: tabla intermedia Empleado↔Capacitación. Columnas: `empleado_capacitacion_id` (PK), `empleado_id` (FK), `capacitacion_id` (FK), `fecha_asignacion`, `fecha_completado`, `calificacion` (0-100), `estado`, `comentarios`.

- `Poblar_RRHH_OLTP.sql`:
	- Propósito: insertar datos de ejemplo y realistas para pruebas y para alimentar el ETL.
	- Contenido y reglas de poblado:
		- Inserta mínimo 10 registros en `oficinas` (varias ciudades/países).
		- Inserta mínimo 10 registros en `departamentos` y >=12 en `puestos`.
		- Inserta 60 empleados con distribución aleatoria entre departamentos y oficinas (cumple el mínimo solicitado de 50 empleados). Cada empleado tiene `numero_empleado` único.
		- Asigna jefes a empleados: se seleccionan 8 managers y se asignan como jefe a otros empleados.
		- Inserta 140 ausencias distribuidas entre 2023-2024 (mínimo 100 requerido). Cada ausencia tiene `tipo_ausencia`, `fecha_inicio`, `fecha_fin`, `dias` y `justificada`.
		- Inserta al menos 1 evaluación por empleado (garantizando que cada empleado tenga evaluación) y luego inserta evaluaciones adicionales (~40) para alcanzar ~100 evaluaciones totales (superior al mínimo de 80).
		- Inserta 10+ capacitaciones en el catálogo y 80 asignaciones en `empleados_capacitaciones` (cumpliendo mínimo 60, con algunos empleados repetidos para reflejar múltiples capacitaciones).

- `06_ETL_Poblar_DimTiempo.sql`:
	- Propósito: crear la dimensión de tiempo en el esquema `rrhh_dw` y proveer una función para poblarla.
	- Objeto creado: `rrhh_dw.dim_tiempo` con columnas en español: `fecha` (PK), `dia`, `mes`, `anio`, `nombre_mes`, `trimestre`, `dia_semana`, `nombre_dia`, `es_fin_de_semana`.
	- Función: `rrhh_dw.poblar_dim_tiempo(p_inicio DATE, p_fin DATE)` que inserta filas desde `p_inicio` hasta `p_fin` sin duplicados.

- `07_ETL_Cargar_Dimensiones.sql`:
	- Propósito: crear dimensiones SCD tipo 2 en `rrhh_dw` y procedimientos para sincronizarlas desde el OLTP.
	- Dimensiones creadas:
		- `rrhh_dw.dim_oficina`: historiza oficinas con `codigo_oficina`, `nombre`, `ciudad`, `pais`, `fecha_inicio_vigencia`, `fecha_fin_vigencia`, `vigente`.
		- `rrhh_dw.dim_departamento`: historiza departamentos con `codigo_departamento`, `nombre`, `codigo_oficina`, y campos de vigencia.
		- `rrhh_dw.dim_empleado`: historiza empleados con `numero_empleado`, `nombre`, `apellido`, `genero`, `fecha_nacimiento`, `fecha_contratacion`, `salario`, `codigo_departamento`, `codigo_oficina`, `numero_jefe`, y campos de vigencia.
	- Funciones SCD2:
		- `rrhh_dw.scd2_upsert_oficinas()` — sincroniza `oficinas` a `dim_oficina` aplicando SCD tipo 2.
		- `rrhh_dw.scd2_upsert_departamentos()` — sincroniza `departamentos` a `dim_departamento`.
		- `rrhh_dw.scd2_upsert_empleados()` — sincroniza `empleados` a `dim_empleado`.
	- Nota: las funciones comparan los valores actuales y crean nuevas versiones cuando detectan cambios, cerrando la vigencia anterior.

- `08_ETL_Cargar_Hechos.sql`:
	- Propósito: crear tablas de hechos en `rrhh_dw` y funciones para cargar hechos desde OLTP.
	- Tablas de hechos:
		- `rrhh_dw.fact_ausencias` (fecha, emp_sk, codigo_departamento, codigo_oficina, tipo_ausencia, dias, justificada).
		- `rrhh_dw.fact_evaluaciones` (fecha, emp_sk, evaluador_emp_sk, calificacion).
		- `rrhh_dw.fact_capacitaciones` (fecha, emp_sk, capacitacion_id, estado, calificacion).
	- Funciones de carga:
		- `rrhh_dw.cargar_hechos_ausencias()` — inserta ausencias desde `public.ausencias` uniendo con `dim_empleado` para resolver `emp_sk`.
		- `rrhh_dw.cargar_hechos_evaluaciones()` — inserta evaluaciones desde `public.evaluaciones`.
		- `rrhh_dw.cargar_hechos_capacitaciones()` — inserta registros de finalización de capacitaciones desde `public.empleados_capacitaciones`.

**Instrucciones de ejecución sugeridas**

1. Crear la base de datos y ejecutar el script de creación de tablas:
```bash
psql -U <usuario> -h <host> -p <puerto> -f 01_Crear_RRHH_OLTP.sql
```

2. Poblar la base OLTP con datos de ejemplo:
```bash
psql -U <usuario> -h <host> -p <puerto> -d rrhh_oltp -f Poblar_RRHH_OLTP.sql
```

3. Crear y poblar `dim_tiempo`:
```bash
psql -U <usuario> -h <host> -p <puerto> -d rrhh_oltp -f 06_ETL_Poblar_DimTiempo.sql
psql -U <usuario> -h <host> -p <puerto> -d rrhh_oltp -c "SELECT rrhh_dw.poblar_dim_tiempo('2018-01-01','2026-12-31');"
```

4. Crear dimensiones SCD2 y sincronizarlas:
```bash
psql -U <usuario> -h <host> -p <puerto> -d rrhh_oltp -f 07_ETL_Cargar_Dimensiones.sql
psql -U <usuario> -h <host> -p <puerto> -d rrhh_oltp -c "SELECT rrhh_dw.scd2_upsert_oficinas(); SELECT rrhh_dw.scd2_upsert_departamentos(); SELECT rrhh_dw.scd2_upsert_empleados();"
```

5. Crear tablas de hechos y cargarlas:
```bash
psql -U <usuario> -h <host> -p <puerto> -d rrhh_oltp -f 08_ETL_Cargar_Hechos.sql
psql -U <usuario> -h <host> -p <puerto> -d rrhh_oltp -c "SELECT rrhh_dw.cargar_hechos_ausencias(); SELECT rrhh_dw.cargar_hechos_evaluaciones(); SELECT rrhh_dw.cargar_hechos_capacitaciones();"
```

**Notas y recomendaciones**
- Revisa credenciales y crea la base `rrhh_oltp` si no existe antes de ejecutar los scripts.
- Ajusta rangos y volúmenes en `Poblar_RRHH_OLTP.sql` si necesitas más registros reales.
- Antes de ejecutar los ETL de hechos, ejecuta las funciones SCD2 para asegurar que las dimensiones están actualizadas.
- Los scripts usan funciones PL/pgSQL; ejecuta los archivos en un cliente `psql` con privilegios adecuados.
