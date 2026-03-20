"""
db_utils.py — Carga rápida de datos a PostgreSQL
=================================================

Por qué no usamos INSERT normal
---------------------------------
La forma estándar de cargar datos con Python (df.to_sql con método INSERT)
envía una sentencia SQL por cada grupo de filas. PostgreSQL tiene que
parsear, validar y ejecutar cada una. Para 5 millones de filas, eso
acumula minutos de trabajo repetitivo.

La solución: COPY FROM
------------------------
PostgreSQL tiene un mecanismo de carga masiva llamado COPY FROM que
escribe los datos directamente en el archivo de la tabla, saltándose
el paso de parsear SQL por cada fila.

Resultados típicos:
  INSERT normal:   ~50.000 filas por segundo
  COPY FROM:      ~500.000 filas por segundo  (10 veces más rápido)

Cómo funciona aquí
-------------------
1. El DataFrame se convierte a texto CSV en memoria (sin escribir en disco).
2. Ese texto se envía directamente al socket de PostgreSQL.
3. PostgreSQL lo carga en la tabla de destino en un solo stream.

Se usa el símbolo | (pipe) como separador en lugar de la coma habitual
para evitar confusión con las comas que aparecen dentro de los textos
de los avisos de trabajo.

Índices al final
-----------------
Los índices (que aceleran las consultas) se crean después de cargar
todos los datos, no durante la carga. Un índice activo mientras se
insertan millones de filas obliga a PostgreSQL a actualizarlo en cada
inserción, lo que puede duplicar el tiempo. Crear el índice al final
es mucho más rápido.
"""
import io
import logging
from typing import Optional

import pandas as pd
from sqlalchemy import text

logger = logging.getLogger(__name__)


def bulk_copy(
    df: pd.DataFrame,
    table_name: str,
    engine,
    columns: Optional[list] = None,
) -> int:
    """
    Carga un DataFrame en una tabla PostgreSQL usando COPY FROM.

    Es la alternativa rápida a df.to_sql() para datasets grandes.
    10-20 veces más rápido porque escribe directo al heap de PostgreSQL
    sin parsear SQL por cada fila.

    Parámetros
    ----------
    df         → los datos a cargar
    table_name → nombre de la tabla destino (debe existir previamente)
    engine     → conexión a PostgreSQL
    columns    → lista de columnas a cargar (si no se indica, usa todas)

    Retorna el número de filas cargadas.

    Nota sobre valores nulos
    -------------------------
    Los valores vacíos (NaN) se representan como \\N en el formato CSV
    que entiende PostgreSQL — así sabe que debe guardar NULL, no el texto "nan".

    Nota sobre tipos de datos
    --------------------------
    pandas guarda columnas con enteros que tienen valores nulos como float64,
    lo que hace que 18147 se serialice como "18147.0" — PostgreSQL rechaza
    eso en columnas INTEGER. Esta función detecta esos casos y los convierte
    al formato correcto antes de enviarlos.
    """
    if df is None or len(df) == 0:
        logger.info(f"  {table_name}: sin datos para cargar.")
        return 0

    cols = columns if columns else list(df.columns)
    df_to_load = df[cols].copy()

    # Normalizar tipos para que PostgreSQL los acepte correctamente
    for col in df_to_load.columns:
        dtype = df_to_load[col].dtype
        # Entero nullable de pandas (Int64): convertir a object para que
        # los nulos queden como None y se serialicen como \N
        if hasattr(dtype, "numpy_dtype"):
            df_to_load[col] = df_to_load[col].astype(object).where(
                df_to_load[col].notna(), other=None
            )
        # float64 que en realidad son enteros (ej: 18147.0 → 18147)
        elif dtype == "float64":
            non_null = df_to_load[col].dropna()
            if len(non_null) > 0 and (non_null == non_null.astype("int64")).all():
                df_to_load[col] = df_to_load[col].astype("Int64").astype(object).where(
                    df_to_load[col].notna(), other=None
                )

    # Serializar a CSV en memoria (sin escribir archivos en disco)
    buffer = io.StringIO()
    df_to_load.to_csv(
        buffer,
        index=False,
        header=False,
        sep="|",
        na_rep="\\N",
    )
    buffer.seek(0)

    # Enviar al socket de PostgreSQL con COPY FROM
    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            cols_sql = ", ".join(cols)
            copy_sql = (
                f"COPY {table_name} ({cols_sql}) "
                f"FROM STDIN WITH (FORMAT CSV, DELIMITER '|', NULL '\\N')"
            )
            cur.copy_expert(copy_sql, buffer)
        conn.commit()
    finally:
        conn.close()

    logger.info(f"  -> {table_name}: {len(df_to_load):,} filas cargadas (COPY FROM).")
    return len(df_to_load)


def create_indexes_deferred(engine, table_name: str) -> None:
    """
    Reconstruye los índices de una tabla después de una carga masiva.

    Usa REINDEX CONCURRENTLY (PostgreSQL 12+) que reconstruye los índices
    sin bloquear las consultas que otros usuarios puedan estar haciendo
    al mismo tiempo sobre esa tabla.
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(f"REINDEX TABLE CONCURRENTLY {table_name};"))
        logger.info(f"  Índices de {table_name} reconstruidos.")
    except Exception as exc:
        logger.warning(f"  Reconstrucción concurrente falló ({exc}), usando método estándar.")
        with engine.begin() as conn:
            conn.execute(text(f"REINDEX TABLE {table_name};"))


def run_analyze(engine, table_name: str) -> None:
    """
    Actualiza las estadísticas internas de PostgreSQL sobre una tabla.

    Después de cargar millones de filas nuevas, el motor de consultas
    de PostgreSQL todavía tiene estadísticas del estado anterior de la tabla.
    ANALYZE las actualiza para que las consultas analíticas posteriores
    usen los mejores planes de ejecución posibles.
    """
    with engine.connect() as conn:
        conn.execute(text(f"ANALYZE {table_name};"))
    logger.info(f"  Estadísticas actualizadas en {table_name}.")
