-- ================================================================
-- queries_validacion.sql — Consultas para verificar la carga
-- ================================================================
--
-- Ejecuta estas consultas en VS Code después de correr el pipeline:
--   1. Instala la extensión "PostgreSQL" de Chris Kolkman
--   2. Conéctate a: localhost:5432 / data_jobs / postgres
--   3. Abre este archivo y ejecuta con F5 (o selecciona y ejecuta)
--
-- Todas las horas que aparecen en los resultados están en Colombia
-- (America/Bogota, UTC-5) — el campo ingested_at lo refleja.
-- ================================================================


-- ================================================================
-- 1. ¿CUÁNTAS FILAS HAY EN CADA TABLA?
--    Ejecuta esto primero para confirmar que la carga fue exitosa.
--    Los números deben coincidir con el resumen que imprimió el pipeline.
-- ================================================================
SELECT
    tabla,
    filas,
    CASE
        WHEN tabla = 'raw_jobs'          THEN 'Copia exacta del CSV (filas válidas)'
        WHEN tabla = 'raw_jobs_rejected' THEN 'Filas del CSV con problemas'
        WHEN tabla = 'dim_companies'     THEN 'Empresas únicas'
        WHEN tabla = 'dim_locations'     THEN 'Ubicaciones únicas'
        WHEN tabla = 'dim_skills'        THEN 'Habilidades técnicas únicas'
        WHEN tabla = 'fact_jobs'         THEN 'Avisos de trabajo (tabla principal)'
        WHEN tabla = 'bridge_job_skills' THEN 'Relaciones aviso↔habilidad'
    END AS descripcion
FROM (
    SELECT 'raw_jobs'            AS tabla, COUNT(*) AS filas FROM raw_jobs
    UNION ALL
    SELECT 'raw_jobs_rejected',  COUNT(*) FROM raw_jobs_rejected
    UNION ALL
    SELECT 'dim_companies',      COUNT(*) FROM dim_companies
    UNION ALL
    SELECT 'dim_locations',      COUNT(*) FROM dim_locations
    UNION ALL
    SELECT 'dim_skills',         COUNT(*) FROM dim_skills
    UNION ALL
    SELECT 'fact_jobs',          COUNT(*) FROM fact_jobs
    UNION ALL
    SELECT 'bridge_job_skills',  COUNT(*) FROM bridge_job_skills
) t
ORDER BY tabla;


-- ================================================================
-- 2. VERIFICACIONES DE INTEGRIDAD
--    Todos estos resultados deben ser 0.
--    Si alguno no es 0, hay un problema con los datos.
-- ================================================================

-- ¿Hay avisos que no tienen empresa asignada?
-- (Esto ocurre cuando el CSV traía company_name vacío — es normal)
SELECT COUNT(*) AS avisos_sin_empresa
FROM fact_jobs
WHERE company_id IS NULL;

-- ¿Hay avisos que no tienen ubicación asignada?
-- (Ocurre cuando job_location venía vacío en el CSV — es normal)
SELECT COUNT(*) AS avisos_sin_ubicacion
FROM fact_jobs
WHERE location_id IS NULL;

-- ¿Hay algún par (aviso, habilidad) repetido en la tabla puente?
-- Debe ser siempre 0 — no se permiten duplicados.
SELECT COUNT(*) AS pares_duplicados
FROM (
    SELECT job_id, skill_id, COUNT(*) AS n
    FROM bridge_job_skills
    GROUP BY job_id, skill_id
    HAVING COUNT(*) > 1
) t;

-- ¿Hay habilidades en la tabla puente que no existen en dim_skills?
-- Debe ser siempre 0.
SELECT COUNT(*) AS habilidades_huerfanas
FROM bridge_job_skills bs
LEFT JOIN dim_skills s ON bs.skill_id = s.skill_id
WHERE s.skill_id IS NULL;


-- ================================================================
-- 3. ¿CUÁNDO SE CARGARON LOS DATOS? (hora Colombia)
--    Confirma que el campo ingested_at está en hora colombiana.
-- ================================================================
SELECT
    'raw_jobs'   AS tabla,
    MIN(ingested_at) AS primera_carga,
    MAX(ingested_at) AS ultima_carga,
    COUNT(*)         AS total_filas
FROM raw_jobs

UNION ALL

SELECT
    'raw_jobs_rejected',
    MIN(ingested_at),
    MAX(ingested_at),
    COUNT(*)
FROM raw_jobs_rejected;


-- ================================================================
-- 4. ¿POR QUÉ bridge_job_skills TIENE TANTAS FILAS?
--    Esta consulta explica el número de filas de esa tabla.
--    Es normal que tenga millones — no hay datos repetidos.
-- ================================================================
SELECT
    MIN(habilidades_por_aviso)             AS minimo_habilidades,
    MAX(habilidades_por_aviso)             AS maximo_habilidades,
    ROUND(AVG(habilidades_por_aviso), 1)  AS promedio_habilidades,
    COUNT(*)                               AS avisos_con_habilidades,
    SUM(habilidades_por_aviso)             AS total_filas_en_bridge  -- debe coincidir con COUNT en bridge_job_skills
FROM (
    SELECT job_id, COUNT(*) AS habilidades_por_aviso
    FROM bridge_job_skills
    GROUP BY job_id
) t;


-- ================================================================
-- 5. ¿CUÁLES SON LAS HABILIDADES MÁS PEDIDAS?
-- ================================================================
SELECT
    s.skill_name                                           AS habilidad,
    s.skill_category                                       AS categoria,
    COUNT(bs.job_id)                                       AS avisos_que_la_piden,
    ROUND(COUNT(bs.job_id) * 100.0 /
          (SELECT COUNT(*) FROM fact_jobs), 1) || '%'      AS porcentaje_avisos
FROM bridge_job_skills bs
JOIN dim_skills s ON bs.skill_id = s.skill_id
GROUP BY s.skill_id, s.skill_name, s.skill_category
ORDER BY avisos_que_la_piden DESC
LIMIT 15;


-- ================================================================
-- 6. SALARIO PROMEDIO POR TIPO DE PUESTO
--    Solo incluye avisos que publicaron salario.
-- ================================================================
SELECT
    job_title_short                                   AS titulo_puesto,
    COUNT(*)                                          AS total_avisos,
    COUNT(salary_year_avg)                            AS avisos_con_salario,
    '$' || TO_CHAR(ROUND(AVG(salary_year_avg)::NUMERIC, 0), 'FM999,999') AS salario_promedio_anual,
    '$' || TO_CHAR(ROUND(MIN(salary_year_avg)::NUMERIC, 0), 'FM999,999') AS salario_minimo,
    '$' || TO_CHAR(ROUND(MAX(salary_year_avg)::NUMERIC, 0), 'FM999,999') AS salario_maximo
FROM fact_jobs
WHERE salary_year_avg IS NOT NULL
GROUP BY job_title_short
ORDER BY AVG(salary_year_avg) DESC;


-- ================================================================
-- 7. ¿CUÁNTOS AVISOS HAY POR TIPO DE PUESTO?
-- ================================================================
SELECT
    job_title_short                                        AS tipo_puesto,
    COUNT(*)                                               AS cantidad_avisos,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) || '%'  AS porcentaje
FROM fact_jobs
GROUP BY job_title_short
ORDER BY cantidad_avisos DESC;


-- ================================================================
-- 8. TRABAJO REMOTO VS PRESENCIAL POR PAÍS (top 10)
-- ================================================================
SELECT
    l.country                                               AS pais,
    COUNT(*) FILTER (WHERE fj.work_from_home = TRUE)       AS remotos,
    COUNT(*) FILTER (WHERE fj.work_from_home = FALSE)      AS presenciales,
    COUNT(*)                                                AS total
FROM fact_jobs fj
JOIN dim_locations l ON fj.location_id = l.location_id
GROUP BY l.country
ORDER BY total DESC
LIMIT 10;


-- ================================================================
-- 9. EMPRESAS CON MÁS AVISOS PUBLICADOS (top 10)
-- ================================================================
SELECT
    c.company_name                                         AS empresa,
    COUNT(fj.job_id)                                       AS avisos_publicados,
    '$' || TO_CHAR(ROUND(AVG(fj.salary_year_avg)::NUMERIC, 0), 'FM999,999') AS salario_promedio
FROM fact_jobs fj
JOIN dim_companies c ON fj.company_id = c.company_id
GROUP BY c.company_id, c.company_name
ORDER BY avisos_publicados DESC
LIMIT 10;


-- ================================================================
-- 10. HABILIDADES POR CATEGORÍA
--     ¿Qué tipo de habilidades se piden más?
-- ================================================================
SELECT
    s.skill_category                                       AS categoria,
    COUNT(DISTINCT s.skill_id)                             AS habilidades_distintas,
    COUNT(bs.job_id)                                       AS veces_mencionada,
    ROUND(COUNT(bs.job_id) * 100.0 /
          (SELECT COUNT(*) FROM bridge_job_skills), 1) || '%'  AS porcentaje
FROM dim_skills s
LEFT JOIN bridge_job_skills bs ON s.skill_id = bs.skill_id
GROUP BY s.skill_category
ORDER BY veces_mencionada DESC;


-- ================================================================
-- 11. REVISAR LAS FILAS RECHAZADAS DEL CSV
--     Si hay filas en raw_jobs_rejected, aquí puedes ver cuáles son.
-- ================================================================

-- ¿Cuántas filas tuvieron problemas?
SELECT COUNT(*) AS filas_con_problemas FROM raw_jobs_rejected;

-- Ver el detalle de las primeras 50 filas problemáticas
SELECT
    line_number                        AS numero_linea_csv,
    fields_expected                    AS columnas_esperadas,
    LEFT(raw_content, 200)             AS contenido_de_la_fila,
    ingested_at                        AS hora_de_carga_colombia
FROM raw_jobs_rejected
ORDER BY line_number
LIMIT 50;

-- ¿Qué porcentaje del CSV llegó bien?
SELECT
    (SELECT COUNT(*) FROM raw_jobs)           AS filas_validas,
    (SELECT COUNT(*) FROM raw_jobs_rejected)  AS filas_con_problemas,
    ROUND(
        (SELECT COUNT(*) FROM raw_jobs) * 100.0 /
        NULLIF(
            (SELECT COUNT(*) FROM raw_jobs) +
            (SELECT COUNT(*) FROM raw_jobs_rejected), 0
        ), 2
    ) || '%'                                  AS porcentaje_calidad;
