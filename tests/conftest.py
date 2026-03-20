"""
conftest.py
===========
Fixtures compartidas y configuración global de pytest.

Mocking de dependencias externas
---------------------------------
Los tests unitarios deben ejecutarse SIN necesidad de:
  - Una base de datos PostgreSQL real.
  - Un archivo .env con credenciales.
  - La librería pandera instalada (opcional para tests rápidos).

Se mockean sqlalchemy, dotenv y se parcha validate_dataframe
para retornar el DataFrame sin modificación durante los tests.

Fixtures
--------
sample_df : pd.DataFrame
    DataFrame de 3 filas que simula el resultado de read_csv().
    Incluye job_skills_parsed y job_type_skills_parsed.

sample_csv_path : str
    Ruta a un CSV temporal de 3 filas para tests de read_csv().
"""
import sys
import types

import pandas as pd
import pytest

# ── Mock de dependencias externas ─────────────────────────────────
# Permite ejecutar los tests sin BD real, sin .env y sin pandera.
for mod_name in ["sqlalchemy", "sqlalchemy.orm", "sqlalchemy.exc", "dotenv"]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = types.ModuleType(mod_name)

sys.modules["sqlalchemy"].create_engine = lambda *a, **kw: None
sys.modules["sqlalchemy"].text = lambda s: s
sys.modules["dotenv"].load_dotenv = lambda: None

# Mock de pandera: validate_dataframe devuelve el df sin modificarlo
try:
    import pandera  # noqa: F401
except ImportError:
    pandera_mock = types.ModuleType("pandera")
    pandera_mock.errors = types.ModuleType("pandera.errors")
    pandera_mock.errors.SchemaErrors = Exception
    sys.modules["pandera"] = pandera_mock
    sys.modules["pandera.errors"] = pandera_mock.errors

from pipeline.ingestion import _safe_parse_list, _safe_parse_dict  # noqa: E402
import pipeline.ingestion as _ing_module  # noqa: E402

# Patch validate_dataframe para que no ejecute el schema real en tests
_ing_module.validate_dataframe = lambda df: df


# ── Datos compartidos ──────────────────────────────────────────────

_SAMPLE_DATA = {
    "job_title_short":       ["Data Scientist", "Data Engineer", "Data Analyst"],
    "job_title":             ["Sr. Data Scientist", "Data Engineer II", "Junior Analyst"],
    "job_location":          ["New York, NY", "San Francisco, CA", "New York, NY"],
    "job_via":               ["LinkedIn", "Indeed", "LinkedIn"],
    "job_schedule_type":     ["Full-time", "Full-time", "Part-time"],
    "job_work_from_home":    [True, False, True],
    "search_location":       ["United States", "United States", "United States"],
    "job_posted_date":       pd.to_datetime(["2023-01-01", "2023-02-15", "2023-03-10"]),
    "job_no_degree_mention": [False, True, False],
    "job_health_insurance":  [True, False, True],
    "job_country":           ["United States", "United States", "United States"],
    "salary_rate":           ["year", "year", None],
    "salary_year_avg":       [120000.0, 130000.0, None],
    "salary_hour_avg":       [None, None, 45.0],
    "company_name":          ["Google", "Meta", "Google"],
    "job_skills": [
        "['Python', 'SQL', 'TensorFlow']",
        "['Python', 'Spark', 'Airflow']",
        "['SQL', 'Excel', 'Tableau']",
    ],
    "job_type_skills": [
        "{'programming': ['Python'], 'databases': ['SQL'], 'ml_frameworks': ['TensorFlow']}",
        "{'programming': ['Python', 'Spark'], 'orchestration': ['Airflow']}",
        "{'databases': ['SQL'], 'analyst_tools': ['Excel', 'Tableau']}",
    ],
}


@pytest.fixture(scope="session")
def sample_df() -> pd.DataFrame:
    """
    DataFrame de 3 filas que simula el resultado de read_csv().

    Características
    ---------------
    - Google aparece 2 veces → dim_companies debe tener 2 filas (no 3).
    - New York, NY aparece 2 veces → dim_locations debe tener 2 filas.
    - Python aparece en job 1 y job 2 → bridge debe tener 2 links para Python.
    - Total skills: 3 + 3 + 3 = 9 relaciones en bridge.
    """
    df = pd.DataFrame(_SAMPLE_DATA)
    df["job_skills_parsed"] = df["job_skills"].apply(_safe_parse_list)
    df["job_type_skills_parsed"] = df["job_type_skills"].apply(_safe_parse_dict)
    return df


@pytest.fixture(scope="session")
def sample_csv_path(tmp_path_factory) -> str:
    """
    Escribe un CSV temporal con el formato real del proyecto (comillas externas).
    Usado por TestReadCsv para probar read_csv() end-to-end.
    """
    tmp = tmp_path_factory.mktemp("data")
    path = tmp / "test_jobs.csv"

    header = "job_title_short,job_title,job_location,job_via,job_schedule_type," \
             "job_work_from_home,search_location,job_posted_date,job_no_degree_mention," \
             "job_health_insurance,job_country,salary_rate,salary_year_avg,salary_hour_avg," \
             "company_name,job_skills,job_type_skills"

    rows = [
        '"Data Scientist,Sr. Data Scientist,"New York, NY",via LinkedIn,Full-time,'
        'False,United States,2023-01-01 10:00:00,False,True,United States,'
        'year,120000,,Google,"[""python"",""sql""]","{"analyst_tools": ["tableau"]}"\"',

        '"Data Engineer,Data Engineer II,"San Francisco, CA",via Indeed,Full-time,'
        'False,United States,2023-02-15 11:00:00,True,False,United States,'
        ',,,Meta,"[""python"",""spark""]","{"programming": ["python","spark"]}"\"',

        '"Data Analyst,Junior Analyst,"New York, NY",via LinkedIn,Part-time,'
        'True,United States,2023-03-10 12:00:00,False,True,United States,'
        'hour,,45.0,Google,"[""sql"",""excel""]","{"analyst_tools": ["excel"]}"\"',
    ]

    with open(path, "w", encoding="utf-8") as f:
        f.write(header + "\n")
        for row in rows:
            f.write(row + "\n")

    return str(path)
