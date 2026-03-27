-- Poblar_RRHH_OLTP.sql
-- Poblado con datos realistas en el esquema OLTP

-- 1. Insertar oficinas
INSERT INTO oltp.oficinas (codigo_oficina, nombre, direccion, ciudad, region, pais, codigo_postal, telefono)
VALUES
('MAD-CENTRO','TalentCorp Madrid Centro','C/ Gran Vía 10','Madrid','Comunidad de Madrid','España','28013','+34 912 345 000'),
('BOG-NORTE','TalentCorp Bogotá Norte','Av. 19 #100-10','Bogotá','Cundinamarca','Colombia','110221','+57 1 3432100'),
('LIM-SUR','TalentCorp Lima Sur','Av. Arequipa 2000','Lima','Lima','Perú','15046','+51 1 6180000'),
('MEX-CENTRO','TalentCorp CDMX Centro','Paseo de la Reforma 100','Ciudad de México','CDMX','México','06600','+52 55 1234000'),
('SCL-NORTE','TalentCorp Santiago Norte','Av. Libertador 50','Santiago','Metropolitana','Chile','8320000','+56 2 2345000'),
('SAO-SUL','TalentCorp São Paulo Sul','Rua Augusta 200','São Paulo','São Paulo','Brasil','01304000','+55 11 40000000'),
('QRO-CENTRO','TalentCorp Querétaro','Calle 5 de Mayo 12','Querétaro','Querétaro','México','76000','+52 442 0000000'),
('BUE-OLIVOS','TalentCorp Olivos','Av. del Libertador 3000','Buenos Aires','Buenos Aires','Argentina','1636','+54 11 55550000'),
('SJO-CENTRO','TalentCorp San José Centro','Calle Central 150','San José','San José','Costa Rica','10101','+506 22220000'),
('LIM-NORTE','TalentCorp Lima Norte','Av. La Marina 1500','Lima','Lima','Perú','15076','+51 1 6181111');

-- 2. Insertar departamentos
INSERT INTO oltp.departamentos (departamento_id, nombre, descripcion, codigo_oficina)
VALUES
('RRHH','Recursos Humanos','Gestión de Talento y Nómina','MAD-CENTRO'),
('TECH','Tecnología','Desarrollo y Operaciones','MAD-CENTRO'),
('VENTAS','Ventas','Fuerza de Ventas y Comercial','BOG-NORTE'),
('FIN','Finanzas','Control financiero y contabilidad','LIM-SUR'),
('MKT','Marketing','Estrategia y comunicación','BOG-NORTE'),
('LEGAL','Legal','Asuntos legales y compliance','MEX-CENTRO'),
('OPER','Operaciones','Operaciones y logística','SCL-NORTE'),
('COMPR','Compras','Compras y proveedores','SAO-SUL'),
('ATC','Atención al Cliente','Soporte y atención','QRO-CENTRO'),
('DATA','Datos','Analítica y BI','BUE-OLIVOS');

-- 3. Insertar puestos
INSERT INTO oltp.puestos (nombre, nivel_salarial, salario_minimo, salario_maximo)
VALUES
('Gerente RRHH','Senior',50000,80000),
('Analista RRHH','Mid',25000,40000),
('Desarrollador Back-end','Mid',30000,60000),
('Desarrollador Front-end','Mid',28000,55000),
('Gerente Ventas','Senior',45000,75000),
('Analista Financiero','Mid',27000,48000),
('Especialista Marketing','Mid',24000,45000),
('Soporte TI','Junior',15000,28000),
('Data Engineer','Mid',32000,60000),
('Científico de Datos','Senior',45000,80000),
('Analista Comercial','Junior',18000,32000),
('Coordinador de Capacitaciones','Mid',22000,38000);

-- 4. Generar 60 empleados
WITH params AS (
  SELECT
    ARRAY['Carlos','María','Juan','Ana','Luis','Sofía','Diego','Laura','Andrés','Paula','Miguel','Lucía','Javier','Camila','Pedro','Valentina','Ricardo','Isabel','Martín','Elena'] AS fn,
    ARRAY['García','Rodríguez','Martínez','López','Pérez','Gómez','Hernández','Sánchez','Ramírez','Torres','Flores','Ruiz','Rojas','Vargas','Castro','Ortiz','Silva','Alvarez','Morales','Mendoza'] AS ln,
    ARRAY['Masculino','Femenino'] AS genders,
    ARRAY['Soltero','Casado','Divorciado'] AS marital
), gen AS (
  SELECT generate_series(1,60) AS i
)
INSERT INTO oltp.empleados (empleado_id, nombre, apellidos, genero, fecha_nacimiento, estado_civil, email, telefono, fecha_contratacion, salario_actual, departamento_id, puesto_id, codigo_oficina)
SELECT
  format('EMP%04s', i) AS empleado_id,
  fn[((i-1) % array_length(fn,1))+1] AS nombre,
  ln[((i-1) % array_length(ln,1))+1] AS apellidos,
  genders[1 + ((i-1) % 2)] AS genero,
  (DATE '1970-01-01' + ((floor(random()*15000))::int) * INTERVAL '1 day')::date AS fecha_nacimiento,
  marital[1 + ((i-1) % 3)] AS estado_civil,
  lower(fn[((i-1) % array_length(fn,1))+1] || '.' || ln[((i-1) % array_length(ln,1))+1] || (i%100)::text || '@talentcorp.com') AS email,
  ('+34' || (600000000 + i)::text) AS telefono,
  (DATE '2016-01-01' + ((floor(random()*3000))::int) * INTERVAL '1 day')::date AS fecha_contratacion,
  (SELECT salario_minimo + random()*(salario_maximo-salario_minimo) FROM oltp.puestos ORDER BY random() LIMIT 1) AS salario_actual,
  (SELECT departamento_id FROM oltp.departamentos ORDER BY random() LIMIT 1) AS departamento_id,
  (SELECT puesto_id FROM oltp.puestos ORDER BY random() LIMIT 1) AS puesto_id,
  (SELECT codigo_oficina FROM oltp.oficinas ORDER BY random() LIMIT 1) AS codigo_oficina
FROM gen CROSS JOIN LATERAL (SELECT params.* FROM params) p(fn,ln,genders,marital);

-- 5. Asignar jefes
WITH mgrs AS (
  SELECT empleado_id FROM oltp.empleados ORDER BY empleado_id LIMIT 8
), subs AS (
  SELECT e.empleado_id as emp, (SELECT empleado_id FROM mgrs ORDER BY random() LIMIT 1) as mgr
  FROM oltp.empleados e
  WHERE e.empleado_id NOT IN (SELECT empleado_id FROM mgrs)
)
UPDATE oltp.empleados SET jefe_id = subs.mgr
FROM subs
WHERE oltp.empleados.empleado_id = subs.emp;

-- 6. Insertar ausencias
INSERT INTO oltp.ausencias (empleado_id, tipo_ausencia, fecha_inicio, fecha_fin, justificada, comentarios)
SELECT
  (SELECT empleado_id FROM oltp.empleados ORDER BY random() LIMIT 1),
  (ARRAY['Vacaciones','Enfermedad','Permiso Personal','Licencia Médica'])[(floor(random()*4)+1)::int],
  d::date,
  (d + ((floor(random()*7)+1))::int)::date,
  (random() < 0.75),
  'Registro generado automáticamente'
FROM generate_series(1,140) g(i), LATERAL (SELECT DATE '2023-01-01' + (floor(random()*730))::int) s(d);

-- 7. Insertar evaluaciones
INSERT INTO oltp.evaluaciones (empleado_id, evaluador_id, fecha_evaluacion, calificacion, comentarios)
SELECT
  e.empleado_id,
  (SELECT empleado_id FROM oltp.empleados WHERE empleado_id <> e.empleado_id ORDER BY random() LIMIT 1),
  (DATE '2023-01-01' + (floor(random()*730))::int),
  round((1 + random()*4)::numeric,2),
  'Evaluación de desempeño anual'
FROM oltp.empleados e;

-- 8. Insertar capacitaciones
INSERT INTO oltp.capacitaciones (nombre, descripcion, proveedor, fecha_inicio, fecha_fin, costo)
VALUES
('Liderazgo y Gestión','Curso de liderazgo para mandos medios','Academia Global','2023-03-01','2023-03-05',1500),
('Excel Avanzado','Análisis y modelado en Excel','DataAcademy','2023-05-10','2023-05-12',300),
('Power BI','Visualización con Power BI','DataViz Ltda','2023-06-15','2023-06-17',600),
('Python para Datos','Fundamentos de Python','CodeTrain','2023-09-01','2023-09-05',800),
('Marketing Digital','Estrategias digitales','MarketPro','2023-11-01','2023-11-03',700),
('Comunicación Efectiva','Habilidades blandas','SkillsLab','2024-01-20','2024-01-22',400),
('Gestión de Proyectos','Metodologías ágiles','PMI','2023-04-10','2023-04-12',900),
('Negociación Comercial','Técnicas de negociación','SalesPro','2023-07-05','2023-07-07',500),
('Seguridad de la Información','Buenas prácticas','InfoSec','2023-08-15','2023-08-16',350),
('Atención al Cliente','Servicio y experiencia','CustomerLab','2023-10-01','2023-10-02',250);

-- 9. Asignaciones empleados-capacitaciones
INSERT INTO oltp.empleados_capacitaciones (empleado_id, capacitacion_id, fecha_completado, calificacion, estado, comentarios)
SELECT
  (SELECT empleado_id FROM oltp.empleados ORDER BY random() LIMIT 1),
  (SELECT capacitacion_id FROM oltp.capacitaciones ORDER BY random() LIMIT 1),
  CASE WHEN random() < 0.8 THEN (DATE '2022-01-01' + (floor(random()*1200))::int) ELSE NULL END,
  CASE WHEN random() < 0.8 THEN (floor(random()*41)+60) ELSE NULL END,
  CASE WHEN random() < 0.8 THEN 'Completada' ELSE 'En Curso' END,
  'Asignación de plan de carrera'
FROM generate_series(1,100) g(i)
ON CONFLICT (empleado_id, capacitacion_id) DO NOTHING;
