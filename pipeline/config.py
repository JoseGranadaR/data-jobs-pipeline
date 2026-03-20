"""
config.py — Configuración central del pipeline
===============================================

Este archivo es el único lugar donde viven los ajustes del pipeline.
Todo lo sensible (contraseñas, rutas) se guarda en el archivo .env
para que nunca quede expuesto en el código ni en Git.

Qué hace este archivo
---------------------
1. Lee el archivo .env y expone sus valores como variables de Python.
2. Define la zona horaria de Colombia para que todos los registros
   de auditoría (cuándo se ejecutó el pipeline) usen la hora local.
3. Ofrece una función de validación que revisa que todo esté bien
   configurado antes de intentar conectar a la base de datos.

Variables disponibles en .env
------------------------------
DB_HOST     → dirección del servidor PostgreSQL     (por defecto: localhost)
DB_PORT     → puerto de conexión                    (por defecto: 5432)
DB_NAME     → nombre de la base de datos            (por defecto: data_jobs)
DB_USER     → usuario de PostgreSQL                 (por defecto: postgres)
DB_PASSWORD → contraseña  ← OBLIGATORIA, sin valor por defecto
CSV_PATH    → ruta al archivo de datos              (por defecto: data/data_jobs.csv)
LOG_LEVEL   → detalle del registro de eventos       (por defecto: INFO)
TZ_NAME     → zona horaria local                    (por defecto: America/Bogota)

Seguridad
---------
La contraseña nunca se escribe directamente en el código.
Vive en .env, que está excluido de Git mediante .gitignore.
"""
import logging
import os

import pytz
from dotenv import load_dotenv

# Lee el archivo .env del directorio donde se ejecuta el pipeline.
# Si no existe (por ejemplo en servidores CI/CD), las variables
# se toman directamente del entorno del sistema operativo.
load_dotenv()

logger = logging.getLogger(__name__)

# ── Conexión a la base de datos ───────────────────────────────────
DB_CONFIG: dict = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME", "data_jobs"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# ── Otras configuraciones ─────────────────────────────────────────
CSV_PATH:  str = os.getenv("CSV_PATH", "data/data_jobs.csv")
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

# Zona horaria Colombia (UTC-5, sin horario de verano).
# Se usa en todos los registros de auditoría (ingested_at)
# para que la hora refleje cuándo se ejecutó el pipeline
# según el reloj colombiano, no el del servidor.
TZ_NAME:   str = os.getenv("TZ_NAME", "America/Bogota")
TZ:        pytz.BaseTzInfo = pytz.timezone(TZ_NAME)


def now_bogota():
    """
    Retorna la fecha y hora actual en la zona horaria configurada.

    Se usa para registrar en qué momento se cargó cada lote de datos,
    de forma que el campo ingested_at refleje la hora colombiana.
    """
    import datetime
    return datetime.datetime.now(tz=TZ)


def validate_config() -> None:
    """
    Verifica que la configuración esté completa antes de iniciar el pipeline.

    Si algo falta, el pipeline falla de inmediato con un mensaje claro
    en lugar de fallar más tarde con un error confuso de conexión.

    Qué verifica
    ------------
    - Que DB_PASSWORD esté definido en el .env.
    - Que el archivo CSV exista en la ruta indicada.

    Si algo falla, verás un mensaje que te dice exactamente qué falta
    y cómo solucionarlo.
    """
    if not DB_CONFIG["password"]:
        raise EnvironmentError(
            "Falta la contraseña de la base de datos.\n"
            "Solución: abre el archivo .env y define DB_PASSWORD=tu_contraseña"
        )

    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(
            f"No se encontró el archivo CSV en: '{CSV_PATH}'\n"
            "Solución: copia data_jobs.csv a la carpeta data/ "
            "o ajusta CSV_PATH en el archivo .env"
        )

    logger.info(
        f"Configuración lista — base de datos: {DB_CONFIG['host']}:{DB_CONFIG['port']}"
        f" | archivo: {CSV_PATH} | zona horaria: {TZ_NAME}"
    )


def get_connection_string() -> str:
    """
    Construye la cadena de conexión para SQLAlchemy.

    Retorna algo como:
        postgresql+psycopg2://usuario:contraseña@localhost:5432/data_jobs
    """
    c = DB_CONFIG
    return (
        f"postgresql+psycopg2://{c['user']}:{c['password']}"
        f"@{c['host']}:{c['port']}/{c['dbname']}"
    )
