-- ================================================================
-- schema.sql — Estructura de la base de datos
-- ================================================================
--
-- Este archivo define todas las tablas de la base de datos.
-- Docker lo ejecuta automáticamente la primera vez que levanta
-- el contenedor de PostgreSQL.
--
-- Se puede ejecutar varias veces sin problema: usa CREATE TABLE
-- IF NOT EXISTS, así que nunca borra ni duplica datos existentes.
--
-- Zona horaria: todos los campos ingested_at (cuándo se cargó
-- cada registro) usan la hora de Colombia (America/Bogota, UTC-5).
-- ================================================================


-- ----------------------------------------------------------------
-- PARTE 1: Tablas de trabajo temporal (staging)
-- Aquí llegan los datos directamente del CSV, sin transformar.
-- Se borran y recargan completamente en cada ejecución del pipeline.
-- ----------------------------------------------------------------

-- raw_jobs: copia exacta de las filas válidas del CSV.
-- Sirve como respaldo y punto de partida para la transformación.
CREATE TABLE IF NOT EXISTS raw_jobs (
    raw_id                SERIAL        PRIMARY KEY,
    job_title_short       TEXT,          -- título estándar del puesto (ej: "Data Engineer")
    job_title             TEXT,          -- título completo tal como aparece en el aviso
    job_location          TEXT,          -- ubicación tal como viene en el CSV ("New York, NY")
    job_via               TEXT,          -- plataforma donde se publicó (LinkedIn, Indeed, etc.)
    job_schedule_type     TEXT,          -- tipo de jornada: Full-time, Part-time, Contractor...
    job_work_from_home    BOOLEAN,       -- ¿es trabajo remoto?
    search_location       TEXT,          -- ubicación que se usó para la búsqueda original
    job_posted_date       TIMESTAMP,     -- cuándo se publicó el aviso
    job_no_degree_mention BOOLEAN,       -- ¿el aviso dice que no se necesita título?
    job_health_insurance  BOOLEAN,       -- ¿el aviso menciona seguro médico?
    job_country           TEXT,          -- país extraído de la ubicación
    salary_rate           TEXT,          -- ¿el salario es anual o por hora? ("year" | "hour")
    salary_year_avg       FLOAT,         -- salario anual promedio en dólares
    salary_hour_avg       FLOAT,         -- salario por hora promedio en dólares
    company_name          TEXT,          -- nombre de la empresa
    job_skills            TEXT,          -- habilidades requeridas: "['python', 'sql']"
    job_type_skills       TEXT,          -- habilidades por categoría: "{'programming': ['python']}"
    ingested_at           TIMESTAMPTZ   DEFAULT (NOW() AT TIME ZONE 'America/Bogota')
);
COMMENT ON TABLE raw_jobs IS
  'Copia directa del CSV. Se borra y recarga en cada ejecución del pipeline.';


-- raw_jobs_rejected: filas del CSV que no pudieron procesarse.
-- Útil para auditar problemas de calidad en el archivo fuente.
CREATE TABLE IF NOT EXISTS raw_jobs_rejected (
    rejected_id      SERIAL    PRIMARY KEY,
    line_number      INTEGER   NOT NULL,   -- en qué línea del CSV estaba el problema
    fields_found     INTEGER,              -- cuántas columnas se encontraron
    fields_expected  INTEGER,              -- cuántas se esperaban (17)
    raw_content      TEXT,                 -- el contenido de la fila problemática
    ingested_at      TIMESTAMPTZ  DEFAULT (NOW() AT TIME ZONE 'America/Bogota')
);
COMMENT ON TABLE raw_jobs_rejected IS
  'Filas del CSV con columnas incorrectas. Para auditoría de calidad.';

CREATE INDEX IF NOT EXISTS idx_rejected_line
    ON raw_jobs_rejected(line_number);


-- ----------------------------------------------------------------
-- PARTE 2: Tablas de catálogo (dimensiones)
-- Cada entidad única vive aquí exactamente una vez.
-- Esto elimina la repetición: "Google" aparece una sola vez
-- y los avisos apuntan a ella por número (ID).
-- ----------------------------------------------------------------

-- dim_companies: una fila por empresa.
CREATE TABLE IF NOT EXISTS dim_companies (
    company_id    SERIAL  PRIMARY KEY,
    company_name  TEXT    NOT NULL  UNIQUE
);
COMMENT ON TABLE dim_companies IS
  'Una fila por empresa. Los avisos apuntan aquí por company_id.';


-- dim_locations: una fila por ubicación única.
-- La ciudad se extrae automáticamente del campo job_location:
-- "New York, NY" → city = "New York"
CREATE TABLE IF NOT EXISTS dim_locations (
    location_id   SERIAL  PRIMARY KEY,
    raw_location  TEXT,    -- valor original tal como viene en el CSV
    city          TEXT,    -- ciudad extraída (texto antes de la primera coma)
    country       TEXT,    -- país
    UNIQUE (raw_location, country)
);
COMMENT ON TABLE dim_locations IS
  'Una fila por ubicación única, con ciudad extraída y país.';


-- dim_skills: catálogo de habilidades técnicas.
-- La categoría viene del campo job_type_skills del CSV.
CREATE TABLE IF NOT EXISTS dim_skills (
    skill_id       SERIAL  PRIMARY KEY,
    skill_name     TEXT    NOT NULL  UNIQUE,
    skill_category TEXT     -- "programming", "cloud", "analyst_tools", "libraries"...
);
COMMENT ON TABLE dim_skills IS
  'Una fila por habilidad técnica, con su categoría funcional.';


-- ----------------------------------------------------------------
-- PARTE 3: Tabla principal de avisos
-- Un registro por aviso de trabajo publicado.
-- Apunta a las tablas de catálogo por número en lugar de repetir texto.
-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_jobs (
    job_id              SERIAL   PRIMARY KEY,
    company_id          INTEGER  REFERENCES dim_companies(company_id),  -- → quién publicó
    location_id         INTEGER  REFERENCES dim_locations(location_id), -- → dónde
    job_title_short     TEXT,    -- título estándar del puesto
    job_title           TEXT,    -- título completo del aviso
    job_via             TEXT,    -- plataforma de publicación
    schedule_type       TEXT,    -- tipo de jornada
    work_from_home      BOOLEAN, -- ¿es remoto?
    posted_date         TIMESTAMP, -- fecha de publicación
    no_degree_mention   BOOLEAN, -- ¿no requiere título?
    health_insurance    BOOLEAN, -- ¿incluye seguro médico?
    salary_rate         TEXT,    -- "year" o "hour"
    salary_year_avg     FLOAT,   -- salario anual en dólares
    salary_hour_avg     FLOAT    -- salario por hora en dólares
);
COMMENT ON TABLE fact_jobs IS
  '1 fila = 1 aviso de trabajo. Los campos company_id y location_id
   pueden ser NULL si el CSV no traía esa información para ese aviso.';


-- ----------------------------------------------------------------
-- PARTE 4: Tabla puente (habilidades por aviso)
-- Resuelve la relación de muchos-a-muchos:
--   un aviso puede pedir muchas habilidades
--   una habilidad puede aparecer en muchos avisos
--
-- Esta tabla es el resultado de "explotar" la lista de habilidades
-- del CSV: si un aviso pide ['python', 'sql', 'spark'], aquí
-- se generan 3 filas: (job_id, skill_python), (job_id, skill_sql),
-- (job_id, skill_spark).
--
-- Por eso tiene millones de filas — es correcto y esperado.
-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS bridge_job_skills (
    job_id    INTEGER  REFERENCES fact_jobs(job_id)    ON DELETE CASCADE,
    skill_id  INTEGER  REFERENCES dim_skills(skill_id) ON DELETE CASCADE,
    PRIMARY KEY (job_id, skill_id)
);
COMMENT ON TABLE bridge_job_skills IS
  'Una fila por par (aviso, habilidad). Con ~5 habilidades promedio
   y ~785.000 avisos, el total es ~3.9 millones de filas. Correcto.';


-- ----------------------------------------------------------------
-- PARTE 5: Índices para acelerar las consultas
-- Se crean aquí para la primera vez que corre Docker.
-- En ejecuciones posteriores del pipeline, los índices se crean
-- al final de la carga (no durante) para no ralentizar las inserciones.
-- ----------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_fact_jobs_company
    ON fact_jobs(company_id);

CREATE INDEX IF NOT EXISTS idx_fact_jobs_location
    ON fact_jobs(location_id);

CREATE INDEX IF NOT EXISTS idx_fact_jobs_date
    ON fact_jobs(posted_date);

CREATE INDEX IF NOT EXISTS idx_fact_jobs_title
    ON fact_jobs(job_title_short);

CREATE INDEX IF NOT EXISTS idx_bridge_job_id
    ON bridge_job_skills(job_id);

CREATE INDEX IF NOT EXISTS idx_bridge_skill_id
    ON bridge_job_skills(skill_id);

CREATE INDEX IF NOT EXISTS idx_skills_category
    ON dim_skills(skill_category);

CREATE INDEX IF NOT EXISTS idx_skills_name
    ON dim_skills(skill_name);
