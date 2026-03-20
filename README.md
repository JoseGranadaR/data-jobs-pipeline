# Pipeline de Avisos de Trabajo

Este proyecto toma el archivo `data_jobs.csv` — con cientos de miles de avisos de trabajo en tecnología — y lo convierte en una base de datos organizada en PostgreSQL. Desde ahí puedes responder preguntas como: ¿cuáles son las habilidades más pedidas?, ¿qué empresas publican más avisos?, ¿cuánto pagan los trabajos remotos?

---

## Índice

1. [Cómo funciona](#cómo-funciona-en-general)
2. [Cómo están organizados los datos](#cómo-están-organizados-los-datos)
3. [El problema del CSV y cómo se resuelve](#el-problema-del-csv-y-cómo-se-resuelve)
4. [Estructura del proyecto](#estructura-del-proyecto)
5. [Cómo instalarlo](#cómo-instalarlo)
6. [Cómo ejecutarlo](#cómo-ejecutarlo)
7. [Cómo verificar que todo salió bien](#cómo-verificar-que-todo-salió-bien)
8. [Cómo correr los tests](#cómo-correr-los-tests)
9. [Decisiones de diseño](#decisiones-de-diseño)
10. [Modelo analítico (para dashboards de BI)](#modelo-analítico-para-dashboards-de-bi)

---

## Cómo funciona

El pipeline tiene dos fases que se ejecutan en orden:

```
data_jobs.csv
      │
      ▼
┌─────────────────────────────────────────────────────┐
│  FASE 1 — Leer el CSV                               │
│                                                     │
│  1. Abre el archivo línea por línea                 │
│  2. Separa las filas buenas de las problemáticas    │
│  3. Verifica que los tipos de datos sean correctos  │
│  4. Guarda todo en la base de datos                 │
│                                                     │
│     raw_jobs          → las filas que llegaron bien │
│     raw_jobs_rejected → las que tuvieron problemas  │
└─────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────┐
│  FASE 2 — Organizar los datos                       │
│                                                     │
│  Etapa 1: Deduplicar                                │
│     Si "Google" aparece 500 veces → guardarla 1 vez │
│     dim_companies  → una fila por empresa           │
│     dim_locations  → una fila por ciudad/país       │
│     dim_skills     → una fila por habilidad técnica │
│                                                     │
│  Etapa 2: Asignar números de referencia             │
│     fact_jobs      → cada aviso apunta a su empresa │
│                       y ubicación por número        │
│                                                     │
│  Etapa 3: Expandir la lista de habilidades          │
│     "['python','sql','spark']" → 3 filas separadas  │
│     bridge_job_skills → aviso ↔ habilidad           │
└─────────────────────────────────────────────────────┘
```

Cada vez que ejecutas el pipeline, los datos anteriores se reemplazan completamente. No se acumulan duplicados aunque lo corras varias veces.

Todos los registros de hora usan la **zona horaria de Colombia** (America/Bogota, UTC-5).

---

## Cómo están organizados los datos

> **ERD completo:** https://dbdiagram.io/d/69bd700afb2db18e3bcb8e7d

Piénsalo así: en lugar de guardar el nombre "Google" en cada uno de los 500 avisos de esa empresa, lo guardamos una sola vez y los 500 avisos simplemente dicen "empresa número 64". Esto ahorra espacio y hace las consultas mucho más rápidas.

```
dim_companies                    dim_locations
─────────────                    ─────────────
company_id │ company_name        location_id │ city          │ country
───────────┼──────────           ────────────┼───────────────┼────────────
    64     │ Google                  1       │ New York      │ United States
   188     │ Meta                    2       │ San Francisco │ United States
   ...     │ ...                    ...      │ ...           │ ...
                │                                │
                └────────────┬───────────────────┘
                             │
                        fact_jobs
                        ─────────
                        job_id │ company_id │ location_id │ job_title_short │ salary_year_avg │ ...
                        ───────┼────────────┼─────────────┼─────────────────┼─────────────────┼────
                          1    │    64      │      1      │ Data Engineer   │   120,000       │ ...
                          2    │   188      │      2      │ Data Scientist  │   135,000       │ ...
                               │
                               └──── bridge_job_skills        dim_skills
                                     ────────────────          ──────────
                                     job_id │ skill_id   →    skill_id │ skill_name │ category
                                     ───────┼─────────        ────────┼────────────┼──────────
                                       1    │   45            45      │ python     │ programming
                                       1    │   89            89      │ sql        │ programming
                                       1    │  112           112      │ spark      │ libraries
```

### ¿Qué guarda cada tabla?

| Tabla | Qué contiene | Cuántas filas esperar |
|---|---|---|
| `raw_jobs` | Copia exacta del CSV (filas válidas)
| `raw_jobs_rejected` | Filas del CSV con problemas
| `dim_companies` | Una fila por empresa única
| `dim_locations` | Una fila por ubicación única 
| `dim_skills` | Una fila por habilidad técnica 
| `fact_jobs` | Un aviso de trabajo por fila 
| `bridge_job_skills` | Una fila por par aviso-habilidad 

La tabla `bridge_job_skills` Es la de mayores registros porque cada aviso puede pedir varias habilidades. Con 5 habilidades promedio y 785.000 avisos, el total es aproximadamente 3.9 millones de filas.

---

## El problema del CSV y cómo se resuelve

El archivo viene exportado desde Excel, lo que agrega comillas extra alrededor de cada fila:

```
"Data Engineer,Google,""New York, NY"",Full-time,['python','sql'],..."
↑ comilla extra que Excel agrega                                    ↑
```

Esto confunde a los lectores de CSV normales porque no saben si las comillas hacen parte del dato. El pipeline resuelve esto en tres pasos:

1. Quita las comillas externas que Excel agrega a toda la fila.
2. Convierte las comillas dobles internas (`""`) en comillas simples (`"`).
3. Lee el resultado como CSV normal.

Las columnas `job_skills` y `job_type_skills` también son especiales — guardan listas y diccionarios de Python como texto. El pipeline las convierte de vuelta a estructuras reales para poder procesarlas.

---

## Estructura del proyecto

```
data_jobs_pipeline/
│
├── main.py               El script que ejecutas para correr todo
├── requirements.txt      Lista de librerías Python necesarias
├── docker-compose.yml    Levanta PostgreSQL con un solo comando
├── .env.example          Plantilla de configuración (copia a .env y edita)
├── .gitignore            Le dice a Git qué archivos ignorar
├── README.md             Este archivo
│
├── pipeline/             El código del pipeline
│   ├── config.py         Lee la configuración del .env, maneja zona horaria Colombia
│   ├── ingestion.py      Fase 1: lee el CSV y lo guarda en la BD
│   ├── transform.py      Fase 2: organiza los datos en tablas relacionales
│   └── db_utils.py       Carga masiva con COPY FROM (10-20x más rápido)
│
├── sql/
│   ├── schema.sql             Define la estructura de todas las tablas
│   └── queries_validacion.sql Consultas para verificar que la carga salió bien
│
├── tests/
│   ├── conftest.py       Datos de prueba compartidos entre todos los tests
│   └── test_pipeline.py  48 pruebas automáticas del pipeline
│
└── .github/
    └── workflows/
        └── ci.yml        Corre los tests automáticamente en cada commit
```

---

## Cómo instalarlo

### Lo que necesitas tener instalado

| Herramienta | Para qué | Dónde conseguirla |
|---|---|---|
| Python 3.11+ | Correr el pipeline | [python.org](https://python.org) |
| Docker Desktop | Levantar PostgreSQL | [docker.com](https://docker.com) |
| Git | Clonar el proyecto | [git-scm.com](https://git-scm.com) |

### Paso a paso

```bash
# 1. Descargar el proyecto
git clone https://github.com/JoseGranadaR/data-jobs-pipeline.git
cd data-jobs-pipeline

# 2. Crear el archivo de configuración
cp .env.example .env
# Abre .env con cualquier editor y cambia DB_PASSWORD por tu contraseña

# 3. Poner el archivo CSV en la carpeta correcta
cp /ruta/donde/tienes/data_jobs.csv data/

# 4. Crear el entorno virtual e instalar librerías
python -m venv .venv
.venv\Scripts\activate       # Windows

pip install -r requirements.txt

# 5. Levantar PostgreSQL con Docker
docker-compose up -d

# 6. Verificar que PostgreSQL está listo
docker-compose ps
# Debe aparecer: data_jobs_db ... (healthy)
```

---

## Cómo ejecutarlo

```bash
# Proceso completo (lo que usarás normalmente)
python main.py

# Usar un archivo CSV diferente
python main.py --csv /ruta/a/otro.csv

# Solo re-organizar sin volver a leer el CSV
# (útil cuando cambias algo en transform.py pero el CSV no cambió)
python main.py --skip-raw
```

### Qué verás en pantalla

```
2026-03-19 22:33:05 | INFO | main | ══════════════════════════════════════════════════════
2026-03-19 22:33:05 | INFO | main |   PIPELINE DE AVISOS DE TRABAJO  v1.0
2026-03-19 22:33:05 | INFO | main |   Inicio: 2026-03-19 22:33:05 COT
2026-03-19 22:33:05 | INFO | main |   Cada ejecución reemplaza todos los datos anteriores
2026-03-19 22:33:34 | INFO | ingestion | Todas las filas del CSV tienen el formato correcto.
2026-03-19 22:33:44 | INFO | ingestion | Datos verificados correctamente: 785,723 filas.
...
2026-03-19 23:04:09 | INFO | main |   RESULTADO FINAL
2026-03-19 23:04:09 | INFO | main |   Avisos leídos del CSV           :  785,723
2026-03-19 23:04:09 | INFO | main |   Filas con problemas (rechazadas):       18
2026-03-19 23:04:09 | INFO | main |   Empresas únicas                 :  139,980
2026-03-19 23:04:09 | INFO | main |   Avisos en la BD                 :  785,723
2026-03-19 23:04:09 | INFO | main |   Relaciones aviso-habilidad       : 3,594,847
2026-03-19 23:04:09 | INFO | main |   Tiempo total  : 892s (14.9 min)
2026-03-19 23:04:09 | INFO | main |   Fin: 2026-03-19 23:04:09 COT
2026-03-19 23:04:09 | INFO | main |   Pipeline completado exitosamente.
```

El log completo también se guarda en `pipeline.log`.

### Ver los datos en VS Code

1. Instala la extensión **PostgreSQL** (autor: Chris Kolkman).
2. Crea una nueva conexión: `localhost:5432` / base de datos `data_jobs` / usuario `postgres`.

---

## Cómo verificar que todo salió bien

Abre `sql/queries_validacion.sql` en VS Code y ejecuta las consultas. Están organizadas en secciones:

1. **Conteo por tabla** — confirma que los números coinciden con el resumen del pipeline.
2. **Verificaciones de integridad** — todos deben retornar 0.
3. **Hora de carga** — confirma que `ingested_at` muestra hora colombiana.
4. **Por qué bridge tiene muchas filas** — explica el número de filas de esa tabla.
5. **Habilidades más pedidas** — un análisis listo para usar.
6. **Salarios por puesto** — comparativa salarial.
7. **Trabajo remoto vs presencial** — distribución por país.

---

## Cómo correr los tests

Los tests verifican que la lógica de transformación funciona correctamente. Corren sin necesidad de una base de datos real ni del archivo CSV.

```bash
# Correr todos los tests y ver cuáles pasan
pytest tests/ -v

# Correr tests y ver qué porcentaje del código está cubierto
pytest tests/ -v --cov=pipeline --cov-report=term-missing
```

Los 48 tests cubren:
- El parseo del formato especial del CSV (comillas de Excel).
- La separación de filas buenas y problemáticas.
- La deduplicación de empresas, ubicaciones y habilidades.
- La asignación de IDs y referencias entre tablas.
- La expansión de la lista de habilidades a la tabla puente.
- Los casos de error: CSV vacío, datos nulos, DataFrames sin filas.

---

## Decisiones de diseño

### ¿Por qué PostgreSQL y no SQLite o Excel?

PostgreSQL soporta restricciones de integridad referencial (las FK que conectan las tablas). Esto garantiza que nunca haya un aviso apuntando a una empresa que no existe. SQLite no las aplica por defecto, y Excel no tiene este concepto. Docker hace que montar PostgreSQL sea tan simple como un solo comando.

### ¿Por qué no guardar "Google" directamente en cada aviso?

Si "Google" cambia su nombre mañana, habría que actualizar 500 filas. Con el modelo actual, solo se actualiza 1 fila en `dim_companies` y automáticamente se refleja en todos los avisos. También permite agrupar y filtrar por empresa de forma eficiente.

### ¿Por qué bridge_job_skills y no una columna de texto?

El CSV guarda las habilidades como texto: `"['python', 'sql', 'spark']"`. Dejarlo así impide hacer consultas como "dame todos los avisos que piden Python". Con la tabla puente, esa consulta es directa y rápida:

```sql
SELECT COUNT(*) FROM bridge_job_skills bs
JOIN dim_skills s ON bs.skill_id = s.skill_id
WHERE s.skill_name = 'python';
```

### ¿Por qué COPY FROM en vez de INSERT?

Para cargar 5 millones de filas, INSERT normal hace ~1.000 operaciones separadas. COPY FROM las envía todas de una vez y PostgreSQL las escribe directamente al disco sin parsear SQL por cada fila. El resultado: 10-20 veces más rápido.

### ¿Por qué crear los índices al final?

Un índice activo durante la carga se actualiza en cada fila insertada. Con millones de filas, eso puede duplicar el tiempo. Crearlos al final, una sola vez, es mucho más eficiente.

### ¿Por qué los datos se reemplazan completamente en cada ejecución?

Porque el CSV puede cambiar entre ejecuciones (nuevos avisos, correcciones, etc.). Reemplazar todo garantiza que la base de datos siempre refleje exactamente el CSV actual, sin mezclar datos de versiones distintas.

---

## Modelo analítico para dashboards de BI

Si quisieras conectar esta base de datos a una herramienta de dashboards como Power BI, Tableau o Looker, lo más eficiente sería reorganizarla en un modelo estrella (Star Schema):

```
                    dim_fecha
                  (de posted_date)
                        │
dim_empresa ────────────┼──── fact_avisos ──── dim_ubicacion
                        │          │
               dim_banderas        └──── bridge_habilidades ──── dim_habilidad
            (remoto, seguro, etc.)
```

**La tabla central (fact_avisos)** tendría una fila por aviso y las métricas numéricas: salario anual, salario por hora, conteo de avisos.

**Las dimensiones** son las tablas de catálogo que ya existen, más `dim_fecha` (derivada de `posted_date`) y `dim_banderas` que agrupa los campos verdadero/falso.

**dim_banderas** merece una explicación: las columnas `work_from_home`, `no_degree_mention` y `health_insurance` solo tienen dos valores posibles cada una, lo que da 2³ = 8 combinaciones distintas. En lugar de guardar esas 3 columnas en la tabla principal, se crea una tabla con las 8 combinaciones y cada aviso apunta a la que le corresponde. Eso se llama "junk dimension" en la literatura de BI.

**Un ejemplo de consulta analítica** que sería directa con este modelo:

```sql
-- ¿Cuáles son las habilidades mejor pagadas?
SELECT s.skill_name, AVG(f.salary_year_avg) AS salario_promedio
FROM fact_jobs f
JOIN bridge_job_skills b ON f.job_id = b.job_id
JOIN dim_skills s ON b.skill_id = s.skill_id
WHERE f.salary_year_avg IS NOT NULL
GROUP BY s.skill_name
ORDER BY salario_promedio DESC;
```
