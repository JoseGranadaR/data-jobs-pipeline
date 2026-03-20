"""
test_pipeline.py
================
Tests unitarios del pipeline Data Jobs ETL.

Cobertura
---------
- Parsers de valores: _safe_parse_list, _safe_parse_dict
- Parseo CSV: _parse_csv_line, _split_good_bad_lines
- Proceso DAX:
    D: build_dim_companies, build_dim_locations, build_dim_skills
    A: build_fact_jobs
    X: build_bridge_job_skills
- Robustez: DataFrames vacíos, errores de entrada, error handling
- Integración: run_3nf_pipeline (mockeado sin BD)
- Validación Pandera: validate_dataframe (mockeado en conftest)

Ejecución
---------
    pytest tests/ -v
    pytest tests/ -v --cov=pipeline --cov-report=term-missing
"""
import pytest
import pandas as pd

from pipeline.ingestion import (
    _safe_parse_list,
    _safe_parse_dict,
    _parse_csv_line,
    _split_good_bad_lines,
    EXPECTED_COLS,
)
from pipeline.transform import (
    build_dim_companies,
    build_dim_locations,
    build_dim_skills,
    build_fact_jobs,
    build_bridge_job_skills,
    _validate_input_df,
    run_3nf_pipeline,
)


# ══════════════════════════════════════════════════════════════════
# Helpers internos de test
# ══════════════════════════════════════════════════════════════════

def _make_csv_line(fields: list, wrap_outer: bool = True) -> str:
    """Genera una línea CSV con formato Excel (comillas externas)."""
    parts = []
    for f in fields:
        if "," in f or '"' in f:
            parts.append('"' + f.replace('"', '""') + '"')
        else:
            parts.append(f)
    inner = ",".join(parts)
    return f'"{inner}"' if wrap_outer else inner


def _good_fields(title: str = "Data Analyst") -> list:
    """Genera una lista de 17 campos válidos para tests."""
    return [
        title, "Full Job Title", "New York, NY", "via LinkedIn",
        "Full-time", "False", "United States", "2023-01-15 13:00:00",
        "False", "False", "United States", "year", "90000", "",
        "Acme Corp", "['python','sql']",
        "{'programming':['python','sql']}",
    ]


# ══════════════════════════════════════════════════════════════════
# TestParsers — _safe_parse_list y _safe_parse_dict
# ══════════════════════════════════════════════════════════════════

class TestParsers:
    """Verifica el parseo de las columnas semi-estructuradas del CSV."""

    def test_parse_list_normal(self):
        assert _safe_parse_list("['Python', 'SQL']") == ["Python", "SQL"]

    def test_parse_list_single_item(self):
        assert _safe_parse_list("['Python']") == ["Python"]

    def test_parse_list_empty_string(self):
        assert _safe_parse_list("") == []

    def test_parse_list_empty_brackets(self):
        assert _safe_parse_list("[]") == []

    def test_parse_list_nan_float(self):
        assert _safe_parse_list(float("nan")) == []

    def test_parse_list_nan_string(self):
        assert _safe_parse_list("nan") == []

    def test_parse_list_malformed(self):
        assert _safe_parse_list("not_a_list") == []

    def test_parse_list_returns_list_type(self):
        assert isinstance(_safe_parse_list("['x']"), list)

    def test_parse_list_with_spaces(self):
        result = _safe_parse_list("['python', 'sql', 'spark']")
        assert result == ["python", "sql", "spark"]

    def test_parse_dict_normal(self):
        r = _safe_parse_dict("{'programming': ['Python', 'SQL']}")
        assert isinstance(r, dict) and "programming" in r

    def test_parse_dict_multiple_categories(self):
        r = _safe_parse_dict(
            "{'programming': ['Python'], 'cloud': ['AWS'], 'databases': ['SQL']}"
        )
        assert len(r) == 3 and "cloud" in r

    def test_parse_dict_empty_braces(self):
        assert _safe_parse_dict("{}") == {}

    def test_parse_dict_nan_string(self):
        assert _safe_parse_dict("nan") == {}

    def test_parse_dict_none(self):
        assert _safe_parse_dict(None) == {}

    def test_parse_dict_malformed(self):
        assert _safe_parse_dict("{bad json}") == {}


# ══════════════════════════════════════════════════════════════════
# TestParseCsvLine — parseo de formato Excel con comillas externas
# ══════════════════════════════════════════════════════════════════

class TestParseCsvLine:
    """
    Verifica que _parse_csv_line maneje correctamente el formato
    especial del CSV con comillas externas envolventes (artefacto Excel).
    """

    def test_17_campos_con_comillas_externas(self):
        row = _parse_csv_line(_make_csv_line(_good_fields(), wrap_outer=True))
        assert row is not None and len(row) == EXPECTED_COLS

    def test_17_campos_sin_comillas_externas(self):
        row = _parse_csv_line(_make_csv_line(_good_fields(), wrap_outer=False))
        assert row is not None and len(row) == EXPECTED_COLS

    def test_ciudad_con_coma_es_un_campo(self):
        """'New York, NY' debe parsearse como UN campo, no dos."""
        fields = _good_fields()
        fields[2] = "New York, NY"
        row = _parse_csv_line(_make_csv_line(fields))
        assert row is not None
        assert row[2] == "New York, NY"

    def test_lista_skills_con_comas_es_un_campo(self):
        """['python', 'sql', 'spark'] debe contar como 1 campo."""
        fields = _good_fields()
        fields[15] = "['python', 'sql', 'spark', 'hadoop', 'kafka']"
        row = _parse_csv_line(_make_csv_line(fields))
        assert row is not None and len(row) == EXPECTED_COLS

    def test_dict_type_skills_con_comas_es_un_campo(self):
        """{'programming': ['python', 'sql'], 'cloud': ['aws']} = 1 campo."""
        fields = _good_fields()
        fields[16] = "{'programming': ['python', 'sql'], 'cloud': ['aws', 'azure']}"
        row = _parse_csv_line(_make_csv_line(fields))
        assert row is not None and len(row) == EXPECTED_COLS

    def test_menos_de_17_campos_retorna_none(self):
        assert _parse_csv_line('"a,b,c"') is None

    def test_linea_vacia_retorna_none(self):
        assert _parse_csv_line("") is None

    def test_solo_espacios_retorna_none(self):
        assert _parse_csv_line("   ") is None

    def test_contenido_correcto_en_cada_campo(self):
        """Los 17 campos deben tener el valor correcto después del parseo."""
        fields = _good_fields("Data Engineer")
        row = _parse_csv_line(_make_csv_line(fields))
        assert row is not None
        assert row[0] == "Data Engineer"
        assert row[14] == "Acme Corp"


# ══════════════════════════════════════════════════════════════════
# TestSplitGoodBadLines — separación de filas válidas y corruptas
# ══════════════════════════════════════════════════════════════════

class TestSplitGoodBadLines:
    """Verifica la separación de filas válidas y corruptas del CSV."""

    def _write_csv(self, tmp_path, rows: list) -> str:
        header = ",".join([
            "job_title_short", "job_title", "job_location", "job_via",
            "job_schedule_type", "job_work_from_home", "search_location",
            "job_posted_date", "job_no_degree_mention", "job_health_insurance",
            "job_country", "salary_rate", "salary_year_avg", "salary_hour_avg",
            "company_name", "job_skills", "job_type_skills",
        ])
        path = tmp_path / "test.csv"
        content = header + "\n" + "\n".join(rows) + "\n"
        path.write_text(content, encoding="utf-8")
        return str(path)

    def _good_row(self) -> str:
        return _make_csv_line(_good_fields())

    def test_todas_las_filas_validas(self, tmp_path):
        path = self._write_csv(tmp_path, [self._good_row(), self._good_row()])
        _, good, bad = _split_good_bad_lines(path)
        assert len(good) == 2
        assert len(bad) == 0

    def test_fila_corrupta_detectada(self, tmp_path):
        path = self._write_csv(tmp_path, [self._good_row(), '"solo,tres,campos"'])
        _, good, bad = _split_good_bad_lines(path)
        assert len(good) == 1
        assert len(bad) == 1

    def test_numero_de_linea_en_bad_record(self, tmp_path):
        path = self._write_csv(tmp_path, ['"mala"'])
        _, _, bad = _split_good_bad_lines(path)
        assert bad[0]["line_number"] == 2  # header=1, primera data=2

    def test_raw_content_en_bad_record(self, tmp_path):
        path = self._write_csv(tmp_path, ['"contenido_corrupto"'])
        _, _, bad = _split_good_bad_lines(path)
        assert "contenido_corrupto" in bad[0]["raw_content"]

    def test_lineas_vacias_ignoradas(self, tmp_path):
        path = self._write_csv(tmp_path, [self._good_row(), "", self._good_row()])
        _, good, bad = _split_good_bad_lines(path)
        assert len(good) == 2
        assert len(bad) == 0

    def test_lista_con_comas_no_cuenta_como_corrupta(self, tmp_path):
        """Fila con skills=['python','sql','spark'] NO debe ser rechazada."""
        fields = _good_fields()
        fields[15] = "['python', 'sql', 'spark', 'hadoop']"
        path = self._write_csv(tmp_path, [_make_csv_line(fields)])
        _, good, bad = _split_good_bad_lines(path)
        assert len(good) == 1
        assert len(bad) == 0


# ══════════════════════════════════════════════════════════════════
# TestDimCompanies — D: Deduplicate
# ══════════════════════════════════════════════════════════════════

class TestDimCompanies:
    """Verifica la construcción de dim_companies."""

    def test_no_duplicates(self, sample_df):
        dim = build_dim_companies(sample_df)
        assert dim["company_name"].nunique() == len(dim)

    def test_correct_count_deduplication(self, sample_df):
        # Google aparece 2 veces -> debe quedar 1 fila
        dim = build_dim_companies(sample_df)
        assert len(dim) == 2

    def test_primary_key_unique(self, sample_df):
        dim = build_dim_companies(sample_df)
        assert dim["company_id"].is_unique

    def test_primary_key_starts_at_1(self, sample_df):
        dim = build_dim_companies(sample_df)
        assert dim["company_id"].min() == 1

    def test_no_null_names(self, sample_df):
        dim = build_dim_companies(sample_df)
        assert dim["company_name"].notna().all()

    def test_known_companies_present(self, sample_df):
        dim = build_dim_companies(sample_df)
        names = set(dim["company_name"])
        assert "Google" in names and "Meta" in names

    def test_df_vacio_retorna_dim_vacia(self):
        empty = pd.DataFrame({"company_name": []})
        dim = build_dim_companies(empty)
        assert len(dim) == 0


# ══════════════════════════════════════════════════════════════════
# TestDimLocations — D: Deduplicate
# ══════════════════════════════════════════════════════════════════

class TestDimLocations:
    """Verifica la construcción de dim_locations."""

    def test_no_duplicates(self, sample_df):
        dim = build_dim_locations(sample_df)
        assert dim["raw_location"].nunique() == len(dim)

    def test_correct_count(self, sample_df):
        # "New York, NY" aparece 2 veces -> 1 fila
        dim = build_dim_locations(sample_df)
        assert len(dim) == 2

    def test_city_extracted_before_comma(self, sample_df):
        dim = build_dim_locations(sample_df)
        ny = dim[dim["raw_location"] == "New York, NY"]
        assert not ny.empty
        assert ny.iloc[0]["city"] == "New York"

    def test_city_sf_extracted(self, sample_df):
        dim = build_dim_locations(sample_df)
        sf = dim[dim["raw_location"] == "San Francisco, CA"]
        assert sf.iloc[0]["city"] == "San Francisco"

    def test_primary_key_unique(self, sample_df):
        dim = build_dim_locations(sample_df)
        assert dim["location_id"].is_unique


# ══════════════════════════════════════════════════════════════════
# TestDimSkills — D: Deduplicate
# ══════════════════════════════════════════════════════════════════

class TestDimSkills:
    """Verifica la construcción de dim_skills con categorías."""

    def test_no_duplicates(self, sample_df):
        dim = build_dim_skills(sample_df)
        assert dim["skill_name"].nunique() == len(dim)

    def test_total_unique_skills(self, sample_df):
        # Python, SQL, TensorFlow, Spark, Airflow, Excel, Tableau = 7
        dim = build_dim_skills(sample_df)
        assert len(dim) == 7

    def test_all_skills_present(self, sample_df):
        dim = build_dim_skills(sample_df)
        skill_names = set(dim["skill_name"])
        expected = {"Python", "SQL", "TensorFlow", "Spark", "Airflow", "Excel", "Tableau"}
        assert expected.issubset(skill_names)

    def test_category_programming(self, sample_df):
        dim = build_dim_skills(sample_df)
        cat = dim[dim["skill_name"] == "Python"].iloc[0]["skill_category"]
        assert cat == "programming"

    def test_category_analyst_tools(self, sample_df):
        dim = build_dim_skills(sample_df)
        cat = dim[dim["skill_name"] == "Excel"].iloc[0]["skill_category"]
        assert cat == "analyst_tools"

    def test_primary_key_unique(self, sample_df):
        dim = build_dim_skills(sample_df)
        assert dim["skill_id"].is_unique

    def test_df_sin_skills_retorna_dim_vacia(self):
        df = pd.DataFrame({
            "job_skills_parsed":      [[], [], []],
            "job_type_skills_parsed": [{}, {}, {}],
        })
        dim = build_dim_skills(df)
        assert len(dim) == 0


# ══════════════════════════════════════════════════════════════════
# TestFactJobs — A: Assign surrogate keys
# ══════════════════════════════════════════════════════════════════

class TestFactJobs:
    """Verifica la construcción de fact_jobs con FKs y columna auxiliar."""

    def _build(self, df):
        dc = build_dim_companies(df)
        dl = build_dim_locations(df)
        fj = build_fact_jobs(df, dc, dl)
        return fj, dc, dl

    def test_row_count_preservado(self, sample_df):
        fj, *_ = self._build(sample_df)
        assert len(fj) == len(sample_df)

    def test_fk_company_not_null(self, sample_df):
        fj, *_ = self._build(sample_df)
        assert fj["company_id"].notna().all()

    def test_fk_location_not_null(self, sample_df):
        fj, *_ = self._build(sample_df)
        assert fj["location_id"].notna().all()

    def test_fk_company_referencia_dim(self, sample_df):
        fj, dc, _ = self._build(sample_df)
        assert set(fj["company_id"].dropna()).issubset(set(dc["company_id"]))

    def test_fk_location_referencia_dim(self, sample_df):
        fj, _, dl = self._build(sample_df)
        assert set(fj["location_id"].dropna()).issubset(set(dl["location_id"]))

    def test_columna_csv_row_presente(self, sample_df):
        """_csv_row es la columna auxiliar para el join robusto en bridge."""
        fj, *_ = self._build(sample_df)
        assert "_csv_row" in fj.columns

    def test_schedule_type_renombrada(self, sample_df):
        fj, *_ = self._build(sample_df)
        assert "schedule_type" in fj.columns
        assert "job_schedule_type" not in fj.columns

    def test_pk_unica(self, sample_df):
        fj, *_ = self._build(sample_df)
        assert fj["job_id"].is_unique

    def test_columnas_skills_excluidas(self, sample_df):
        """Las columnas de skills no deben estar en fact_jobs."""
        fj, *_ = self._build(sample_df)
        assert "job_skills" not in fj.columns
        assert "job_skills_parsed" not in fj.columns


# ══════════════════════════════════════════════════════════════════
# TestBridgeJobSkills — X: eXpand (EXPLODE N:M)
# ══════════════════════════════════════════════════════════════════

class TestBridgeJobSkills:
    """
    Verifica la construcción de bridge_job_skills.

    La bridge resuelve la relación N:M entre jobs y skills.
    Con el sample_df de 3 jobs con 3 skills cada uno → 9 relaciones totales.
    """

    def _build_all(self, df):
        dc = build_dim_companies(df)
        dl = build_dim_locations(df)
        ds = build_dim_skills(df)
        fj = build_fact_jobs(df, dc, dl)
        br = build_bridge_job_skills(df, fj, ds)
        return fj, ds, br

    def test_not_empty(self, sample_df):
        _, _, br = self._build_all(sample_df)
        assert len(br) > 0

    def test_no_pares_duplicados(self, sample_df):
        _, _, br = self._build_all(sample_df)
        assert br.duplicated(["job_id", "skill_id"]).sum() == 0

    def test_job_ids_validos(self, sample_df):
        fj, _, br = self._build_all(sample_df)
        assert set(br["job_id"]).issubset(set(fj["job_id"]))

    def test_skill_ids_validos(self, sample_df):
        _, ds, br = self._build_all(sample_df)
        assert set(br["skill_id"]).issubset(set(ds["skill_id"]))

    def test_python_en_dos_jobs(self, sample_df):
        """Python aparece en DS y DE -> la bridge debe tener 2 links."""
        _, ds, br = self._build_all(sample_df)
        py_id = ds[ds["skill_name"] == "Python"].iloc[0]["skill_id"]
        assert len(br[br["skill_id"] == py_id]) == 2

    def test_sql_en_dos_jobs(self, sample_df):
        """SQL aparece en DS y DA -> 2 links."""
        _, ds, br = self._build_all(sample_df)
        sql_id = ds[ds["skill_name"] == "SQL"].iloc[0]["skill_id"]
        assert len(br[br["skill_id"] == sql_id]) == 2

    def test_total_9_relaciones(self, sample_df):
        """3 skills/job × 3 jobs = 9 relaciones."""
        _, _, br = self._build_all(sample_df)
        assert len(br) == 9

    def test_columnas_correctas(self, sample_df):
        _, _, br = self._build_all(sample_df)
        assert set(br.columns) == {"job_id", "skill_id"}

    def test_alineacion_robusta_con_csv_row(self, sample_df):
        """
        Verifica que el join por _csv_row asigna el job_id correcto.

        Python aparece en filas 0 y 1 del DataFrame.
        fact_jobs.job_id es [1, 2, 3].
        La bridge debe linkar Python con job_id=1 y job_id=2.
        """
        dc = build_dim_companies(sample_df)
        dl = build_dim_locations(sample_df)
        ds = build_dim_skills(sample_df)
        fj = build_fact_jobs(sample_df, dc, dl)
        br = build_bridge_job_skills(sample_df, fj, ds)

        py_id = ds[ds["skill_name"] == "Python"].iloc[0]["skill_id"]
        linked_jobs = set(br[br["skill_id"] == py_id]["job_id"])
        assert linked_jobs == {1, 2}  # DS=job_id1, DE=job_id2


# ══════════════════════════════════════════════════════════════════
# TestRobustez — Manejo de errores y casos borde
# ══════════════════════════════════════════════════════════════════

class TestRobustez:
    """Verifica el manejo de errores y casos límite del pipeline."""

    def test_validate_input_df_vacio_lanza_error(self):
        """run_3nf_pipeline debe fallar con error descriptivo si el df está vacío."""
        with pytest.raises(ValueError, match="DataFrame de entrada está vacío"):
            _validate_input_df(pd.DataFrame())

    def test_validate_input_df_none_lanza_error(self):
        with pytest.raises(ValueError):
            _validate_input_df(None)

    def test_validate_input_df_valido_no_lanza(self, sample_df):
        _validate_input_df(sample_df)  # no debe lanzar excepción

    def test_build_bridge_sin_skills_retorna_vacio(self, sample_df):
        """Si ningún job tiene skills, la bridge debe estar vacía."""
        df_sin_skills = sample_df.copy()
        df_sin_skills["job_skills_parsed"] = [[], [], []]
        dc = build_dim_companies(df_sin_skills)
        dl = build_dim_locations(df_sin_skills)
        ds = build_dim_skills(df_sin_skills)
        fj = build_fact_jobs(df_sin_skills, dc, dl)
        br = build_bridge_job_skills(df_sin_skills, fj, ds)
        assert len(br) == 0

    def test_fact_jobs_con_company_nula(self, sample_df):
        """Rows con company_name nulo deben tener company_id NULL (no error)."""
        df_null = sample_df.copy()
        df_null.loc[0, "company_name"] = None
        dc = build_dim_companies(df_null)
        dl = build_dim_locations(df_null)
        fj = build_fact_jobs(df_null, dc, dl)
        assert fj.loc[0, "company_id"] != fj.loc[0, "company_id"]  # NaN check

    def test_run_3nf_pipeline_df_vacio_lanza_error(self):
        """run_3nf_pipeline debe lanzar ValueError antes de conectar a la BD."""
        with pytest.raises(ValueError, match="DataFrame de entrada está vacío"):
            run_3nf_pipeline(pd.DataFrame())
