"""
main.py — El punto de partida del pipeline
==========================================

Este archivo es el que ejecutas para poner todo en marcha.
Coordina las dos fases del proceso de principio a fin:

  Fase 1 — Leer el CSV
    Abre el archivo data_jobs.csv, separa las filas que llegaron bien
    de las que tienen algún problema, verifica que los datos tengan
    el formato esperado, y los guarda en la base de datos.

  Fase 2 — Organizar los datos
    Toma lo que se guardó en la Fase 1 y lo reorganiza en tablas
    bien estructuradas: una tabla por empresa, una por ubicación,
    una por habilidad técnica, y la tabla principal de avisos.

Cada vez que lo ejecutas, los datos anteriores se reemplazan
completamente. Puedes correrlo 10 veces con el mismo CSV y
siempre obtendrás el mismo resultado — sin duplicados.

Cómo usarlo
-----------
    python main.py                    → proceso completo
    python main.py --csv otro.csv     → usar un CSV diferente
    python main.py --skip-raw         → solo re-organizar (si el CSV no cambió)

Configuración
-------------
Todas las credenciales y rutas viven en el archivo .env.
El pipeline verifica que estén bien configuradas antes de empezar.

Los registros de actividad se guardan en dos lugares:
  - En pantalla para verlos en tiempo real
  - En pipeline.log para consultarlos después

Zona horaria
------------
Todos los registros de hora usan la hora de Colombia (America/Bogota).
"""
import sys

# Configurar UTF-8 en la terminal antes que cualquier otra cosa.
# En Windows la terminal usa por defecto una codificación que no
# soporta tildes ni caracteres especiales — esto lo corrige.
import io as _io
if isinstance(sys.stdout, _io.TextIOWrapper):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if isinstance(sys.stderr, _io.TextIOWrapper):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import argparse
import logging
import time

from pipeline.config import CSV_PATH, LOG_LEVEL, TZ, now_bogota, validate_config
from pipeline.ingestion import load_to_raw, read_csv
from pipeline.transform import run_3nf_pipeline

# Configurar el sistema de registro de eventos (logging).
# LOG_LEVEL se lee del archivo .env — por defecto es INFO.
# Si quieres más detalle, cambia a DEBUG en el .env.
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("main")


def parse_args() -> argparse.Namespace:
    """Lee los argumentos que se pasan al ejecutar el script."""
    parser = argparse.ArgumentParser(
        description="Pipeline de avisos de trabajo → base de datos PostgreSQL"
    )
    parser.add_argument(
        "--csv",
        default=CSV_PATH,
        help=f"Ruta al archivo CSV (por defecto: {CSV_PATH})",
    )
    parser.add_argument(
        "--skip-raw",
        action="store_true",
        help="Omite la Fase 1 y va directo a reorganizar. Útil si el CSV no cambió.",
    )
    return parser.parse_args()


def main() -> None:
    """Orquesta las dos fases del pipeline de principio a fin."""
    args  = parse_args()
    start = time.time()
    inicio = now_bogota().strftime("%Y-%m-%d %H:%M:%S %Z")

    logger.info("=" * 54)
    logger.info("  PIPELINE DE AVISOS DE TRABAJO  v1.0")
    logger.info(f"  Inicio: {inicio}")
    logger.info("  Cada ejecución reemplaza todos los datos anteriores")
    logger.info("=" * 54)

    # Verificar que la configuración esté completa antes de empezar
    validate_config()

    # ── Fase 1: Leer y guardar el CSV ─────────────────────────────
    logger.info("FASE 1 — Leyendo el CSV y guardando en base de datos")
    df, bad_records = read_csv(args.csv)

    if not args.skip_raw:
        load_to_raw(df, bad_records)
    else:
        logger.info("  (--skip-raw activo: guardado de datos crudos omitido)")

    # ── Fase 2: Organizar los datos en el modelo 3NF ──────────────
    logger.info("FASE 2 — Organizando los datos en tablas relacionales")
    results = run_3nf_pipeline(df)

    # ── Resumen final ─────────────────────────────────────────────
    elapsed = time.time() - start
    fin = now_bogota().strftime("%Y-%m-%d %H:%M:%S %Z")

    logger.info("-" * 54)
    logger.info("  RESULTADO FINAL")
    logger.info("-" * 54)
    logger.info(f"  Avisos leídos del CSV          : {len(df):>8,}")
    logger.info(f"  Filas con problemas (rechazadas): {len(bad_records):>8,}")
    logger.info(f"  Empresas únicas (dim_companies) : {len(results['dim_companies']):>8,}")
    logger.info(f"  Ubicaciones únicas (dim_locations): {len(results['dim_locations']):>7,}")
    logger.info(f"  Habilidades únicas (dim_skills) : {len(results['dim_skills']):>8,}")
    logger.info(f"  Avisos en la BD (fact_jobs)     : {len(results['fact_jobs']):>8,}")
    logger.info(f"  Relaciones aviso-habilidad      : {len(results['bridge_job_skills']):>8,}")
    logger.info("-" * 54)
    logger.info(f"  Tiempo total  : {elapsed:.0f}s ({elapsed/60:.1f} min)")
    logger.info(f"  Fin           : {fin}")
    logger.info("  Pipeline completado exitosamente.")
    logging.shutdown()


if __name__ == "__main__":
    main()
