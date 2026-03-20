"""
ingestion.py — Fase 1: leer el CSV y guardarlo en la base de datos
==================================================================

Qué hace esta fase
------------------
Toma el archivo data_jobs.csv y lo carga en dos tablas de la base de datos:

  raw_jobs           → todas las filas que llegaron bien formadas
  raw_jobs_rejected  → las filas que tenían algún problema (para auditoría)

Antes de guardar, verifica que los datos tengan el formato correcto
usando Pandera — si una columna tiene datos del tipo equivocado
(por ejemplo, texto donde debería ir un número), el pipeline lo reporta.

El problema del CSV y cómo se resuelve
---------------------------------------
El archivo CSV fue exportado desde Excel, lo que le agrega comillas
externas a cada fila. Una fila normal del CSV se ve así:

    "Data Engineer,Google,\"\"New York, NY\"\",Full-time,..."
    ↑ comilla extra que Excel agrega ↑

Esto confunde a los lectores de CSV normales porque no saben si las
comillas son parte del dato o delimitadores del archivo.

La función _parse_csv_line resuelve esto en tres pasos:
  1. Quita las comillas externas que Excel agrega.
  2. Convierte las comillas dobles internas ("") en comillas simples (").
  3. Lee el resultado como CSV normal.

Las columnas job_skills y job_type_skills
------------------------------------------
Estas dos columnas guardan listas y diccionarios de Python como texto:

  job_skills      → "['python', 'sql', 'spark']"      (lista de habilidades)
  job_type_skills → "{'programming': ['python']}"     (habilidades por categoría)

Las funciones _safe_parse_list y _safe_parse_dict convierten ese texto
de vuelta a listas y diccionarios reales de Python para poder trabajar
con ellos en la Fase 2.

Zona horaria
------------
El campo ingested_at registra exactamente cuándo se guardaron los datos.
Usa la hora de Colombia (America/Bogota, UTC-5) definida en el .env.

Idempotencia — qué pasa si ejecuto el pipeline dos veces
---------------------------------------------------------
Las tablas se vacían completamente antes de cada carga nueva.
Ejecutar el pipeline 5 veces produce exactamente el mismo resultado
que ejecutarlo 1 vez. No se acumulan datos duplicados.
"""
import ast
import csv
import io
import logging
from datetime import datetime
from typing import Optional

import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check
from sqlalchemy import create_engine, text

from pipeline.config import CSV_PATH, TZ, get_connection_string
from pipeline.db_utils import bulk_copy

logger = logging.getLogger(__name__)

# Número de columnas que debe tener cada fila del CSV.
# Si el archivo cambia en el futuro, actualizar este número.
EXPECTED_COLS: int = 17

# Los únicos títulos de trabajo válidos en este dataset.
# Si aparece un título diferente, Pandera lo reportará.
VALID_JOB_TITLES: frozenset = frozenset({
    "Data Analyst", "Data Engineer", "Data Scientist",
    "Senior Data Analyst", "Senior Data Engineer", "Senior Data Scientist",
    "Business Analyst", "Software Engineer", "Machine Learning Engineer",
    "Cloud Engineer",
})


# ─────────────────────────────────────────────────────────────────
# Reglas de validación de datos (Pandera)
# ─────────────────────────────────────────────────────────────────
# Antes de guardar en la base de datos, verificamos que los datos
# cumplan estas reglas. Si algo falla, el pipeline se detiene y
# muestra exactamente qué filas tienen el problema.

RAW_SCHEMA = DataFrameSchema(
    columns={
        "job_title_short": Column(
            str,
            Check.isin(VALID_JOB_TITLES),
            nullable=False,
            description="Título del puesto — debe ser uno de los 10 títulos estándar",
        ),
        "job_location": Column(
            str,
            nullable=True,
            description="Ubicación del puesto tal como aparece en el CSV",
        ),
        "job_work_from_home": Column(
            bool,
            nullable=False,
            description="True si el trabajo es remoto, False si es presencial",
        ),
        "job_no_degree_mention": Column(
            bool,
            nullable=False,
            description="True si el aviso menciona que no se requiere título universitario",
        ),
        "job_health_insurance": Column(
            bool,
            nullable=False,
            description="True si el aviso menciona seguro médico",
        ),
        "salary_year_avg": Column(
            float,
            Check.ge(0),
            nullable=True,
            description="Salario anual promedio en dólares. Puede estar vacío.",
        ),
        "salary_hour_avg": Column(
            float,
            Check.ge(0),
            nullable=True,
            description="Salario por hora en dólares. Puede estar vacío.",
        ),
        "job_posted_date": Column(
            "datetime64[ns]",
            nullable=True,
            description="Fecha y hora en que se publicó el aviso",
        ),
    },
    coerce=True,    # intenta convertir los tipos automáticamente antes de validar
    strict=False,   # permite columnas adicionales que no están en este schema
    name="raw_jobs_schema",
)


def validate_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Verifica que los datos del CSV tengan el formato correcto antes de guardarlos.

    Usa Pandera para revisar tipos de datos, valores permitidos y rangos.
    Si encuentra problemas, los acumula todos y los reporta juntos
    (con lazy=True) en lugar de detenerse en el primer error — así puedes
    ver todos los problemas de una vez y corregirlos.

    Si los datos son correctos, retorna el mismo DataFrame sin cambios.
    Si hay errores, lanza SchemaErrors con detalle de qué filas fallaron.
    """
    logger.info("Verificando calidad de los datos con Pandera...")
    try:
        validated = RAW_SCHEMA.validate(df, lazy=True)
        logger.info(f"Datos verificados correctamente: {len(df):,} filas.")
        return validated
    except pa.errors.SchemaErrors as exc:
        logger.error(f"Se encontraron {len(exc.failure_cases)} problemas de calidad:")
        logger.error(exc.failure_cases.to_string())
        raise


# ─────────────────────────────────────────────────────────────────
# Parseo de valores individuales del CSV
# ─────────────────────────────────────────────────────────────────

def _safe_parse_list(value) -> list:
    """
    Convierte el texto "['python', 'sql']" en una lista real de Python.

    El CSV guarda las habilidades como texto — esta función las convierte
    de vuelta a una lista real para poder procesarlas en la Fase 2.

    Retorna una lista vacía si el valor está en blanco o tiene algún error.

    Ejemplos:
        "['python', 'sql']"  →  ['python', 'sql']
        ""                   →  []
        NaN                  →  []
    """
    if pd.isna(value) or str(value).strip() in ("", "nan", "[]"):
        return []
    try:
        result = ast.literal_eval(str(value))
        return result if isinstance(result, list) else []
    except (ValueError, SyntaxError):
        return []


def _safe_parse_dict(value) -> dict:
    """
    Convierte el texto "{'programming': ['python']}" en un diccionario real.

    El CSV guarda las categorías de habilidades como texto — esta función
    las convierte de vuelta a un diccionario real de Python.

    Retorna un diccionario vacío si el valor está en blanco o tiene algún error.

    Ejemplos:
        "{'programming': ['python', 'sql']}"  →  {'programming': ['python', 'sql']}
        ""                                    →  {}
    """
    if pd.isna(value) or str(value).strip() in ("", "nan", "{}"):
        return {}
    try:
        result = ast.literal_eval(str(value))
        return result if isinstance(result, dict) else {}
    except (ValueError, SyntaxError):
        return {}


# ─────────────────────────────────────────────────────────────────
# Lectura del CSV con formato especial de Excel
# ─────────────────────────────────────────────────────────────────

def _parse_csv_line(raw_line: str) -> Optional[list]:
    """
    Lee una línea del CSV manejando el formato especial que genera Excel.

    El problema: Excel envuelve cada fila entre comillas y duplica
    las comillas internas. Esta función deshace ese formato en 3 pasos:

      1. Quita las comillas externas que Excel agrega a toda la fila.
      2. Convierte "" (dos comillas juntas) en una sola comilla ".
      3. Lee la línea resultante como CSV normal.

    Retorna la lista de 17 campos si todo salió bien, o None si la
    línea está vacía o tiene un número incorrecto de campos.
    """
    line = raw_line.rstrip("\r\n")
    if not line.strip():
        return None

    # Quitar comillas externas si las tiene
    if line.startswith('"') and line.endswith('"'):
        inner = line[1:-1]
    else:
        inner = line

    # Convertir comillas dobles escapadas a comillas simples
    inner = inner.replace('""', '\x01').replace('\x01', '"')

    try:
        reader = csv.reader(io.StringIO(inner))
        row = next(reader)
        return row if len(row) == EXPECTED_COLS else None
    except Exception:
        return None


def _split_good_bad_lines(csv_path: str) -> tuple:
    """
    Lee el CSV completo y separa las filas buenas de las problemáticas.

    Una fila es buena si tiene exactamente 17 columnas después del parseo.
    Una fila es problemática si tiene más o menos columnas de lo esperado.

    Retorna tres cosas:
      - Los nombres de las 17 columnas (del encabezado)
      - Las filas buenas como listas de 17 valores
      - Las filas problemáticas con información de diagnóstico
    """
    good_rows:   list = []
    bad_records: list = []

    with open(csv_path, encoding="utf-8", errors="replace") as fh:
        header_line = fh.readline()
        header_cols = [c.strip() for c in header_line.split(",")]

        if len(header_cols) != EXPECTED_COLS:
            logger.warning(
                f"El encabezado tiene {len(header_cols)} columnas "
                f"pero se esperaban {EXPECTED_COLS}. Revisa el archivo."
            )

        for line_num, raw_line in enumerate(fh, start=2):
            row = _parse_csv_line(raw_line)
            if row is not None:
                good_rows.append(row)
            else:
                stripped = raw_line.rstrip("\r\n")
                if stripped.strip():
                    bad_records.append({
                        "line_number":     line_num,
                        "fields_found":    None,
                        "fields_expected": EXPECTED_COLS,
                        "raw_content":     stripped[:500],
                    })

    return header_cols, good_rows, bad_records


# ─────────────────────────────────────────────────────────────────
# Función principal: leer el CSV
# ─────────────────────────────────────────────────────────────────

def read_csv(csv_path: str = CSV_PATH) -> tuple:
    """
    Lee el archivo CSV y lo convierte en un DataFrame listo para usar.

    Pasos que realiza:
      1. Lee el archivo línea por línea separando buenas de problemáticas.
      2. Convierte cada columna al tipo de dato correcto (fechas, números, booleanos).
      3. Verifica la calidad de los datos con Pandera.
      4. Convierte las columnas de habilidades de texto a listas y diccionarios.

    Retorna:
      - Un DataFrame con las filas válidas y sus columnas bien tipadas.
      - Una lista con información de las filas que no pudieron leerse.
    """
    logger.info(f"Leyendo archivo: {csv_path}")

    header_cols, good_rows, bad_records = _split_good_bad_lines(csv_path)

    # Avisar sobre filas problemáticas
    if bad_records:
        logger.warning(f"Se encontraron {len(bad_records):,} fila(s) con problemas:")
        logger.warning("-" * 68)
        logger.warning(f"  {'Línea':<10} {'Esperado':<12} Contenido (primeros 120 caracteres)")
        logger.warning("-" * 68)
        for rec in bad_records[:20]:
            logger.warning(
                f"  {rec['line_number']:<10} "
                f"{rec['fields_expected']:<12} "
                f"{rec['raw_content'][:120]}"
            )
        if len(bad_records) > 20:
            logger.warning(
                f"  ... y {len(bad_records) - 20} más. "
                "Revisa la tabla raw_jobs_rejected en la base de datos."
            )
        logger.warning("-" * 68)
    else:
        logger.info("Todas las filas del CSV tienen el formato correcto.")

    # Construir el DataFrame
    df = pd.DataFrame(good_rows, columns=header_cols)

    # Columnas de texto
    for col in ["job_title_short", "job_title", "job_location", "job_via",
                "job_schedule_type", "search_location", "job_country",
                "salary_rate", "company_name", "job_skills", "job_type_skills"]:
        if col in df.columns:
            df[col] = df[col].replace("", None).astype("string")

    # Fecha de publicación
    df["job_posted_date"] = pd.to_datetime(
        df["job_posted_date"], format="mixed", dayfirst=False, errors="coerce"
    )

    # Salarios como números
    df["salary_year_avg"] = pd.to_numeric(df["salary_year_avg"], errors="coerce")
    df["salary_hour_avg"] = pd.to_numeric(df["salary_hour_avg"], errors="coerce")

    # Columnas verdadero/falso
    for col in ["job_work_from_home", "job_no_degree_mention", "job_health_insurance"]:
        if col in df.columns:
            df[col] = df[col].map(
                lambda x: True if str(x).strip().lower() == "true" else False
            )

    # Verificación de calidad
    df = validate_dataframe(df)

    # Convertir habilidades de texto a listas y diccionarios
    df["job_skills_parsed"]      = df["job_skills"].apply(_safe_parse_list)
    df["job_type_skills_parsed"] = df["job_type_skills"].apply(_safe_parse_dict)

    logger.info(
        f"Archivo procesado: {len(df):,} filas válidas | "
        f"{len(bad_records):,} filas con problemas"
    )
    return df, bad_records


# ─────────────────────────────────────────────────────────────────
# Preparar y cargar las tablas RAW en PostgreSQL
# ─────────────────────────────────────────────────────────────────

def _setup_raw_tables(engine) -> None:
    """
    Crea las tablas de staging si no existen y las vacía para la nueva carga.

    raw_jobs          → recibirá las filas válidas del CSV
    raw_jobs_rejected → recibirá las filas que tuvieron algún problema

    El campo ingested_at registra cuándo se cargaron los datos.
    Usa la hora de Colombia (definida en config.py) para que el registro
    refleje la hora local del equipo donde corre el pipeline.

    Al vaciar las tablas (TRUNCATE) antes de cargar, garantizamos que
    ejecutar el pipeline múltiples veces siempre produce el mismo resultado.
    """
    now = datetime.now(tz=TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(f"Preparando tablas RAW — hora Colombia: {now}")

    ddl = """
    CREATE TABLE IF NOT EXISTS raw_jobs (
        raw_id                SERIAL        PRIMARY KEY,
        job_title_short       TEXT,
        job_title             TEXT,
        job_location          TEXT,
        job_via               TEXT,
        job_schedule_type     TEXT,
        job_work_from_home    BOOLEAN,
        search_location       TEXT,
        job_posted_date       TIMESTAMP,
        job_no_degree_mention BOOLEAN,
        job_health_insurance  BOOLEAN,
        job_country           TEXT,
        salary_rate           TEXT,
        salary_year_avg       FLOAT,
        salary_hour_avg       FLOAT,
        company_name          TEXT,
        job_skills            TEXT,
        job_type_skills       TEXT,
        ingested_at           TIMESTAMPTZ DEFAULT (NOW() AT TIME ZONE 'America/Bogota')
    );
    CREATE TABLE IF NOT EXISTS raw_jobs_rejected (
        rejected_id      SERIAL    PRIMARY KEY,
        line_number      INTEGER   NOT NULL,
        fields_found     INTEGER,
        fields_expected  INTEGER,
        raw_content      TEXT,
        ingested_at      TIMESTAMPTZ DEFAULT (NOW() AT TIME ZONE 'America/Bogota')
    );
    CREATE INDEX IF NOT EXISTS idx_rejected_line
        ON raw_jobs_rejected(line_number);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE raw_jobs, raw_jobs_rejected RESTART IDENTITY;"))

    logger.info("Tablas RAW listas para recibir datos.")


def load_to_raw(df: pd.DataFrame, bad_records: list) -> None:
    """
    Guarda los datos en las tablas RAW de la base de datos.

    Qué guarda dónde:
      - Las filas válidas van a raw_jobs.
      - Las filas problemáticas van a raw_jobs_rejected.

    Antes de guardar, vacía ambas tablas para evitar duplicados.
    Usa COPY FROM (carga masiva de PostgreSQL) en lugar de INSERT
    normal porque es 10-20 veces más rápido para datasets grandes.
    """
    engine = create_engine(get_connection_string())
    _setup_raw_tables(engine)

    # Guardar filas válidas
    cols = [
        "job_title_short", "job_title", "job_location", "job_via",
        "job_schedule_type", "job_work_from_home", "search_location",
        "job_posted_date", "job_no_degree_mention", "job_health_insurance",
        "job_country", "salary_rate", "salary_year_avg", "salary_hour_avg",
        "company_name", "job_skills", "job_type_skills",
    ]
    df_raw = df[[c for c in cols if c in df.columns]].copy()
    df_raw["job_skills"]      = df_raw["job_skills"].astype(str)
    df_raw["job_type_skills"] = df_raw["job_type_skills"].astype(str)
    bulk_copy(df_raw, "raw_jobs", engine)

    # Guardar filas problemáticas (si las hay)
    if bad_records:
        df_rej = pd.DataFrame(bad_records)[
            ["line_number", "fields_found", "fields_expected", "raw_content"]
        ]
        bulk_copy(df_rej, "raw_jobs_rejected", engine)
    else:
        logger.info("  raw_jobs_rejected: ninguna fila problemática.")
