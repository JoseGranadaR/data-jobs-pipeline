"""
transform.py — Fase 2: organizar los datos en el modelo relacional
==================================================================

Qué hace esta fase
------------------
Toma los datos crudos del CSV y los reorganiza en tablas bien estructuradas
que siguen las reglas de la Tercera Forma Normal (3NF). El objetivo es
eliminar repeticiones: si "Google" aparece 500 veces en el CSV, en la
base de datos final existe una sola vez y los 500 avisos simplemente
apuntan a ese registro.

Las tres etapas de transformación
-----------------------------------

Etapa 1 — Deduplicar
  Se extraen los valores únicos para construir las tablas de catálogo:
  - dim_companies  → una fila por empresa (deduplicado de company_name)
  - dim_locations  → una fila por ubicación (ciudad + país)
  - dim_skills     → una fila por habilidad técnica (con su categoría)

Etapa 2 — Asignar identificadores
  Se construye fact_jobs, la tabla principal. Cada aviso de trabajo recibe
  un ID único y apunta a su empresa y ubicación mediante números (FKs)
  en lugar de repetir el texto completo.

  Se usan mapas directos (diccionarios Python) en vez de JOINs de pandas
  porque un JOIN sobre columnas con datos repetidos puede multiplicar
  filas sin avisar. El mapa garantiza exactamente una asignación por fila.

Etapa 3 — Expandir la lista de habilidades
  El CSV guarda las habilidades como una lista en una sola columna:
    job_id=1 | job_skills = "['python', 'sql', 'spark']"

  Eso viola la Primera Forma Normal (un campo, un valor). Esta etapa
  "explota" esa lista: genera una fila por cada par (aviso, habilidad):
    (job_id=1, skill_id=45)   ← python
    (job_id=1, skill_id=89)   ← sql
    (job_id=1, skill_id=112)  ← spark

  Esa tabla puente (bridge_job_skills) es lo que permite responder
  preguntas como "¿cuántos avisos piden Python?" sin escanear texto.

Por qué bridge_job_skills tiene millones de filas
--------------------------------------------------
Con ~5 habilidades promedio por aviso y ~785.000 avisos, la tabla
acumula ~3.6 millones de filas. Eso es correcto y esperado. Cada fila
es solo dos números (job_id, skill_id) — no hay datos repetidos.

Carga rápida con COPY FROM
---------------------------
Los datos se insertan usando el mecanismo nativo de carga masiva de
PostgreSQL (COPY FROM), que es 10-20 veces más rápido que INSERT normal.
Los índices se crean al final, no durante la carga, para no ralentizar
los millones de inserciones.

Zona horaria
------------
Los registros de auditoría usan la hora de Colombia (America/Bogota).

Idempotencia
------------
Las tablas se vacían antes de cada carga. Ejecutar el pipeline
varias veces siempre produce el mismo resultado final.
"""
import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

from pipeline.config import TZ, get_connection_string
from pipeline.db_utils import bulk_copy, run_analyze

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# Definición de las tablas 3NF
# ─────────────────────────────────────────────────────────────────
# Nota: la fuente de verdad principal de este DDL es sql/schema.sql,
# que Docker ejecuta automáticamente al crear el contenedor.
# Este bloque es el respaldo para cuando no se usa Docker.

_SCHEMA_3NF_DDL = """
CREATE TABLE IF NOT EXISTS dim_companies (
    company_id    SERIAL  PRIMARY KEY,
    company_name  TEXT    NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS dim_locations (
    location_id   SERIAL  PRIMARY KEY,
    raw_location  TEXT,
    city          TEXT,
    country       TEXT,
    UNIQUE (raw_location, country)
);
CREATE TABLE IF NOT EXISTS dim_skills (
    skill_id       SERIAL  PRIMARY KEY,
    skill_name     TEXT    NOT NULL UNIQUE,
    skill_category TEXT
);
CREATE TABLE IF NOT EXISTS fact_jobs (
    job_id              SERIAL   PRIMARY KEY,
    company_id          INTEGER  REFERENCES dim_companies(company_id),
    location_id         INTEGER  REFERENCES dim_locations(location_id),
    job_title_short     TEXT,
    job_title           TEXT,
    job_via             TEXT,
    schedule_type       TEXT,
    work_from_home      BOOLEAN,
    posted_date         TIMESTAMP,
    no_degree_mention   BOOLEAN,
    health_insurance    BOOLEAN,
    salary_rate         TEXT,
    salary_year_avg     FLOAT,
    salary_hour_avg     FLOAT
);
CREATE TABLE IF NOT EXISTS bridge_job_skills (
    job_id    INTEGER  REFERENCES fact_jobs(job_id)    ON DELETE CASCADE,
    skill_id  INTEGER  REFERENCES dim_skills(skill_id) ON DELETE CASCADE,
    PRIMARY KEY (job_id, skill_id)
);
"""

_INDEXES_DDL = """
CREATE INDEX IF NOT EXISTS idx_fact_jobs_company  ON fact_jobs(company_id);
CREATE INDEX IF NOT EXISTS idx_fact_jobs_location ON fact_jobs(location_id);
CREATE INDEX IF NOT EXISTS idx_fact_jobs_date     ON fact_jobs(posted_date);
CREATE INDEX IF NOT EXISTS idx_fact_jobs_title    ON fact_jobs(job_title_short);
CREATE INDEX IF NOT EXISTS idx_bridge_job_id      ON bridge_job_skills(job_id);
CREATE INDEX IF NOT EXISTS idx_bridge_skill_id    ON bridge_job_skills(skill_id);
CREATE INDEX IF NOT EXISTS idx_skills_category    ON dim_skills(skill_category);
"""


def create_schema(engine) -> None:
    """Crea las tablas 3NF si no existen (sin índices — se crean al final)."""
    with engine.begin() as conn:
        conn.execute(text(_SCHEMA_3NF_DDL))
    logger.info("Estructura de tablas 3NF verificada.")


def create_indexes_post_load(engine) -> None:
    """
    Crea los índices después de que todos los datos estén cargados.

    Los índices aceleran las consultas pero ralentizan las inserciones
    porque PostgreSQL tiene que actualizarlos en cada fila nueva.
    Crearlos al final (en lugar de tenerlos activos durante la carga)
    puede ahorrar varios minutos en datasets grandes.
    """
    with engine.begin() as conn:
        conn.execute(text(_INDEXES_DDL))
    logger.info("Índices creados para acelerar las consultas.")


def truncate_3nf_tables(engine) -> None:
    """
    Vacía todas las tablas 3NF antes de la nueva carga.

    El orden importa: primero se vacía bridge_job_skills (que depende de
    fact_jobs y dim_skills), luego fact_jobs, y por último las dimensiones.
    RESTART IDENTITY reinicia los contadores de ID desde 1.
    """
    with engine.begin() as conn:
        conn.execute(text(
            "TRUNCATE bridge_job_skills, fact_jobs, "
            "dim_skills, dim_locations, dim_companies "
            "RESTART IDENTITY CASCADE;"
        ))
    logger.info("Tablas 3NF vaciadas. Contadores de ID reiniciados desde 1.")


# ─────────────────────────────────────────────────────────────────
# Etapa 1 — Deduplicar: construir las tablas de catálogo
# ─────────────────────────────────────────────────────────────────

def build_dim_companies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea la tabla de empresas únicas a partir del CSV.

    Si "Google" aparece 500 veces en el CSV, aquí aparece solo una vez
    con su propio ID. Los avisos de trabajo luego apuntarán a ese ID
    en lugar de repetir el texto "Google" 500 veces.

    Los registros con company_name vacío se descartan aquí.
    Los avisos correspondientes tendrán company_id = NULL en fact_jobs.
    """
    companies = (
        df["company_name"]
        .dropna()
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )
    dim = pd.DataFrame({
        "company_id":   range(1, len(companies) + 1),
        "company_name": companies.values,
    })
    logger.info(f"Empresas únicas encontradas: {len(dim):,}")
    return dim


def build_dim_locations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea la tabla de ubicaciones únicas.

    La ciudad se extrae del campo job_location tomando el texto antes de
    la primera coma. Ejemplo: "New York, NY" → city = "New York".

    Los registros con job_location vacío se descartan.
    Los avisos correspondientes tendrán location_id = NULL en fact_jobs.
    """
    locs = (
        df[["job_location", "job_country"]]
        .drop_duplicates()
        .dropna(subset=["job_location"])
        .reset_index(drop=True)
    )
    locs.columns = ["raw_location", "country"]
    locs["city"] = locs["raw_location"].str.split(",").str[0].str.strip()
    locs.insert(0, "location_id", range(1, len(locs) + 1))
    logger.info(f"Ubicaciones únicas encontradas: {len(locs):,}")
    return locs


def build_dim_skills(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea el catálogo de habilidades técnicas únicas con su categoría.

    El proceso tiene dos pasos:
      1. Lee job_type_skills para saber la categoría de cada habilidad
         (por ejemplo, 'python' → 'programming', 'tableau' → 'analyst_tools').
      2. Recopila todas las habilidades únicas de job_skills y les asigna
         su categoría. Las que no tienen categoría quedan como 'other'.
    """
    skill_category_map: dict = {}
    for type_dict in df["job_type_skills_parsed"]:
        if not isinstance(type_dict, dict):
            continue
        for category, skills in type_dict.items():
            if isinstance(skills, list):
                for s in skills:
                    if s and s.strip().lower() not in skill_category_map:
                        skill_category_map[s.strip().lower()] = category

    all_skills: set = set()
    for skill_list in df["job_skills_parsed"]:
        if isinstance(skill_list, list):
            all_skills.update(s.strip() for s in skill_list if s)

    rows = [
        {
            "skill_name":     skill,
            "skill_category": skill_category_map.get(skill.lower(), "other"),
        }
        for skill in sorted(all_skills)
    ]

    dim = (
        pd.DataFrame(rows) if rows
        else pd.DataFrame(columns=["skill_name", "skill_category"])
    )
    dim.insert(0, "skill_id", range(1, len(dim) + 1))
    logger.info(f"Habilidades únicas encontradas: {len(dim):,}")
    return dim


# ─────────────────────────────────────────────────────────────────
# Etapa 2 — Asignar identificadores: construir fact_jobs
# ─────────────────────────────────────────────────────────────────

def build_fact_jobs(
    df: pd.DataFrame,
    dim_companies: pd.DataFrame,
    dim_locations: pd.DataFrame,
) -> pd.DataFrame:
    """
    Construye la tabla principal de avisos de trabajo con sus FK asignadas.

    Reemplaza el texto de empresa y ubicación por sus IDs numéricos.
    Ejemplo: en lugar de guardar "Google" en cada aviso, guarda company_id=64.

    Por qué se usan diccionarios y no JOINs de pandas
    --------------------------------------------------
    Un JOIN de pandas sobre columnas con valores repetidos puede generar
    filas de más sin avisar (producto cartesiano). El diccionario garantiza
    exactamente una asignación por fila, sin riesgo de duplicación.

    La columna _csv_row
    --------------------
    Guarda la posición original de cada fila en el CSV. La Etapa 3 la usa
    para conectar cada aviso con sus habilidades de forma segura, sin asumir
    que el orden del DataFrame coincide con los IDs de PostgreSQL.

    Los avisos sin empresa o sin ubicación (vacíos en el CSV) tendrán
    company_id = NULL o location_id = NULL. Eso es correcto: el aviso
    existe y tiene información útil, solo le falta ese dato específico.
    """
    fact = df.copy().reset_index(drop=True)
    fact["_csv_row"] = range(len(fact))

    company_map = dict(zip(dim_companies["company_name"], dim_companies["company_id"]))
    location_map = dict(zip(dim_locations["raw_location"], dim_locations["location_id"]))

    fact["company_id"] = fact["company_name"].map(company_map)
    fact["location_id"] = fact["job_location"].map(location_map)

    # Convertir a entero nullable (Int64) para evitar que los NaN
    # conviertan toda la columna a float (18147.0 en vez de 18147)
    for fk_col in ("company_id", "location_id"):
        fact[fk_col] = fact[fk_col].astype("Int64")

    null_co = fact["company_id"].isna().sum()
    null_lo = fact["location_id"].isna().sum()
    if null_co:
        logger.warning(f"{null_co} avisos sin empresa en el CSV → company_id quedará vacío")
    if null_lo:
        logger.warning(f"{null_lo} avisos sin ubicación en el CSV → location_id quedará vacío")

    col_map = {
        "company_id":            "company_id",
        "location_id":           "location_id",
        "job_title_short":       "job_title_short",
        "job_title":             "job_title",
        "job_via":               "job_via",
        "job_schedule_type":     "schedule_type",
        "job_work_from_home":    "work_from_home",
        "job_posted_date":       "posted_date",
        "job_no_degree_mention": "no_degree_mention",
        "job_health_insurance":  "health_insurance",
        "salary_rate":           "salary_rate",
        "salary_year_avg":       "salary_year_avg",
        "salary_hour_avg":       "salary_hour_avg",
    }

    result = fact[["_csv_row"] + [c for c in col_map if c in fact.columns]].rename(
        columns=col_map
    )
    result.insert(0, "job_id", range(1, len(result) + 1))
    logger.info(f"Avisos de trabajo procesados: {len(result):,}")
    return result


# ─────────────────────────────────────────────────────────────────
# Etapa 3 — Expandir: tabla puente de habilidades (N:M)
# ─────────────────────────────────────────────────────────────────

def build_bridge_job_skills(
    df: pd.DataFrame,
    fact_jobs: pd.DataFrame,
    dim_skills: pd.DataFrame,
) -> pd.DataFrame:
    """
    Genera la tabla puente que conecta cada aviso con sus habilidades.

    El CSV guarda las habilidades como una lista en una sola columna,
    lo que impide hacer consultas relacionales eficientes. Esta función
    "explota" esa lista: por cada habilidad en la lista de un aviso,
    genera una fila (job_id, skill_id) en la tabla puente.

    Se usa pd.Series.explode() — una operación vectorizada de pandas
    que hace esto de forma eficiente sin recorrer fila por fila.

    Por qué hay tantas filas en esta tabla
    ---------------------------------------
    Con ~5 habilidades promedio y ~785.000 avisos, el total es:
    785.000 × 5 ≈ 3.9 millones de filas. Es correcto. Cada fila es
    solo dos números enteros — no se está repitiendo información.

    La columna _csv_row garantiza que cada aviso quede conectado
    exactamente con sus habilidades, incluso si el DataFrame fue
    filtrado o reordenado antes de llegar a esta función.
    """
    tmp = pd.DataFrame({
        "job_id":  fact_jobs["job_id"].values,
        "skills":  df["job_skills_parsed"].values,
    })

    tmp = tmp[tmp["skills"].apply(lambda x: isinstance(x, list) and len(x) > 0)]

    if tmp.empty:
        logger.info("Ningún aviso tiene habilidades registradas — tabla puente vacía.")
        return pd.DataFrame(columns=["job_id", "skill_id"])

    exploded = (
        tmp.explode("skills")
        .rename(columns={"skills": "skill_name"})
    )
    exploded["skill_name"] = exploded["skill_name"].str.strip()

    bridge = (
        exploded
        .merge(dim_skills[["skill_id", "skill_name"]], on="skill_name", how="inner")
        [["job_id", "skill_id"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    logger.info(f"Relaciones aviso-habilidad generadas: {len(bridge):,}")
    return bridge


# ─────────────────────────────────────────────────────────────────
# Verificación de entrada
# ─────────────────────────────────────────────────────────────────

def _validate_input_df(df: pd.DataFrame) -> None:
    """
    Verifica que haya datos para transformar antes de continuar.

    Si el DataFrame está vacío (porque todas las filas del CSV fueron
    rechazadas), el pipeline falla aquí con un mensaje claro en lugar
    de continuar y cargar tablas vacías silenciosamente.
    """
    if df is None or len(df) == 0:
        raise ValueError(
            "No hay datos para transformar — el DataFrame está vacío.\n"
            "Posible causa: todas las filas del CSV fueron rechazadas.\n"
            "Revisa la tabla raw_jobs_rejected para ver qué pasó."
        )


# ─────────────────────────────────────────────────────────────────
# Orquestador principal
# ─────────────────────────────────────────────────────────────────

def run_3nf_pipeline(df: pd.DataFrame) -> dict:
    """
    Ejecuta las tres etapas de transformación y carga los resultados en PostgreSQL.

    Secuencia completa
    ------------------
    0. Verificar que haya datos para procesar.
    1. Preparar las tablas: crear si no existen + vaciar las existentes.
    2. Deduplicar → construir dim_companies, dim_locations, dim_skills.
    3. Asignar IDs → construir fact_jobs con referencias a las dimensiones.
    4. Expandir → construir bridge_job_skills con el EXPLODE de habilidades.
    5. Cargar en PostgreSQL con COPY FROM (carga masiva, 10-20x más rápido).
    6. Crear índices al final + actualizar estadísticas del motor de consultas.

    El orden de carga respeta las dependencias entre tablas:
      primero las dimensiones (sin dependencias),
      luego fact_jobs (que apunta a las dimensiones),
      por último bridge_job_skills (que apunta a fact_jobs y dim_skills).

    Retorna un diccionario con los DataFrames generados, útil para el
    resumen final que se imprime en la consola.
    """
    _validate_input_df(df)

    now = datetime.now(tz=TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
    engine = create_engine(get_connection_string())

    logger.info(f"=== TRANSFORMACIÓN 3NF INICIADA — {now} ===")
    create_schema(engine)
    truncate_3nf_tables(engine)

    logger.info("--- Etapa 1: deduplicar ---")
    dim_companies = build_dim_companies(df)
    dim_locations = build_dim_locations(df)
    dim_skills = build_dim_skills(df)

    logger.info("--- Etapa 2: asignar identificadores ---")
    fact_jobs = build_fact_jobs(df, dim_companies, dim_locations)

    logger.info("--- Etapa 3: expandir habilidades ---")
    bridge = build_bridge_job_skills(df, fact_jobs, dim_skills)

    logger.info("--- Cargando en PostgreSQL (COPY FROM) ---")
    bulk_copy(dim_companies[["company_name"]],                    "dim_companies", engine)
    bulk_copy(dim_locations[["raw_location", "city", "country"]], "dim_locations", engine)
    bulk_copy(dim_skills[["skill_name", "skill_category"]],       "dim_skills",    engine)

    fact_cols = [c for c in fact_jobs.columns if c not in ("job_id", "_csv_row")]
    bulk_copy(fact_jobs[fact_cols], "fact_jobs", engine)
    bulk_copy(bridge, "bridge_job_skills", engine)

    logger.info("--- Creando índices y actualizando estadísticas ---")
    create_indexes_post_load(engine)
    for tbl in ["fact_jobs", "bridge_job_skills", "dim_skills"]:
        run_analyze(engine, tbl)

    now_end = datetime.now(tz=TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(f"=== TRANSFORMACIÓN 3NF COMPLETADA — {now_end} ===")

    return {
        "dim_companies":     dim_companies,
        "dim_locations":     dim_locations,
        "dim_skills":        dim_skills,
        "fact_jobs":         fact_jobs,
        "bridge_job_skills": bridge,
    }
