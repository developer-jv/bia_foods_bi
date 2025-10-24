# Proyecto BI — BIA Foods

Flujo analítico end-to-end para validar, transformar y cargar datos SAP en un data warehouse PostgreSQL, con soporte para orquestación en Pentaho y consumo analítico en Power BI.

> Para un arranque rápido consulta **Quickstart**.

---

## Contenido
- [Arquitectura del flujo](#arquitectura-del-flujo)
- [Estructura del repositorio](#estructura-del-repositorio)
- [Requisitos](#requisitos)
- [Configuración](#configuración)
- [Quickstart](#quickstart)
- [Validación de datos](#validación-de-datos)
- [Transformación de datos](#transformación-de-datos)
- [Carga a PostgreSQL](#carga-a-postgresql)
- [Modelado y orquestación](#modelado-y-orquestación)
- [Power BI y datasets](#power-bi-y-datasets)
- [Solución de problemas](#solución-de-problemas)
- [Siguientes pasos](#siguientes-pasos)
- [Licencia](#licencia)

---

## Arquitectura del flujo

1. **Ingesta**: los archivos CSV provenientes de SAP se colocan en `RAW_DIR` (por defecto `./data`).
2. **Validación** (`scripts/validate_data.py`): Great Expectations estandariza columnas, verifica tipos/patrones y genera reportes JSON. Los datos aprobados se copian a `data/validated/`.
3. **Transformación** (`scripts/Pandas Transform.py`): pandas + PyArrow enriquecen ventas con catálogos y calendario, conservando IDs como texto y escribiendo datasets Parquet en `data/curated/`.
4. **Carga** (`scripts/load_to_postgres.py`): los Parquet curados se cargan a PostgreSQL (esquema `staging`) mediante SQLAlchemy.
5. **Modelado opcional**: `dbt/` contiene la definición base del proyecto para crear modelos analíticos en un esquema aguas arriba (ej. `analytics`).
6. **Orquestación y BI**: Pentaho (archivos `.kjb`/`.ktr` en `ETL/`) puede automatizar el pipeline; Power BI consume las tablas resultantes para visualizaciones.

---

## Estructura del repositorio

```
.
├─ README.md
├─ docker-compose.yml            # Servicio PostgreSQL + volumen persistente
├─ requirements.txt              # Dependencias Python (pandas, pyarrow, GE, SQLAlchemy, dbt, etc.)
├─ .env                          # Variables usadas por los scripts
├─ scripts/
│  ├─ validate_data.py           # Validación con Great Expectations y exporte a data/validated
│  ├─ Pandas Transform.py        # Transformación y escritura Parquet en data/curated
│  └─ load_to_postgres.py        # Carga de Parquet a PostgreSQL (esquema staging)
├─ data/
│  ├─ sap_customers.csv          # Archivos fuente (configurables mediante RAW_DIR)
│  ├─ sap_products.csv
│  ├─ sap_calendar.csv
│  ├─ sap_sales.csv
│  ├─ validated/                 # CSV validados automáticamente
│  └─ curated/                   # Parquet curados (sales_enriched, dim_*)
├─ ge_reports/                   # Reportes JSON de Great Expectations
├─ ETL/
│  ├─ Job 1.kjb                  # Flujo Pentaho para orquestar el pipeline
│  └─ Transformation 1.ktr       # Transformación Pentaho de referencia
├─ dbt/
│  └─ dbt_project.yml            # Proyecto dbt base (sin modelos todavía)
└─ initdb/                       # Scripts SQL opcionales para inicializar PostgreSQL
```

---

## Requisitos

- **Docker** y **Docker Compose** (para levantar PostgreSQL).
- **Python 3.10+** con `pip` y posibilidad de crear entornos virtuales.
- **Power BI Desktop** (consumo analítico).
- **Pentaho Data Integration (Spoon)** si se desea orquestar con los `.kjb/.ktr`.
- **dbt-core** + **dbt-postgres** (opcional, para modelado en el DW).

---

## Configuración

1. Revisa `.env` y ajusta rutas/credenciales según tu entorno:

   ```
   PG_HOST=localhost
   PG_PORT=5432
   PG_DB=bia_dw
   PG_USER=bia_user
   PG_PASSWORD=bia_password
   PG_SCHEMA=staging
   RAW_DIR=./data               # Cambia a ./data/raw si separas la capa raw
   VALIDATED_DIR=./data/validated
   CURATED_DIR=./data/curated
   GE_REPORTS_DIR=./ge_reports
   ```

2. Crea y activa un entorno virtual de Python, luego instala dependencias:

   ```bash
   python -m venv .venv
   # Windows
   .venv\Scripts\activate
   # Linux/Mac
   source .venv/bin/activate

   pip install -r requirements.txt
   ```

3. Inicia PostgreSQL en contenedor:

   ```bash
   docker compose up -d
   ```

   > Los scripts automáticamente crean `data/validated`, `data/curated` y `ge_reports` si no existen.

---

## Quickstart

1. Coloca los CSV fuente en la carpeta definida por `RAW_DIR` (por defecto `data/`):
   - `sap_customers.csv`
   - `sap_products.csv`
   - `sap_calendar.csv`
   - `sap_sales.csv`
2. Valida la capa raw:

   ```bash
   python scripts/validate_data.py
   ```

3. Genera datasets curados en Parquet:

   ```bash
   python "scripts/Pandas Transform.py"
   ```

4. Carga los Parquet en PostgreSQL:

   ```bash
   python scripts/load_to_postgres.py
   ```

5. Comprueba tablas en PostgreSQL (`staging.fct_sales_enriched`, `staging.dim_*`) desde tu cliente SQL preferido o Power BI.

---

## Validación de datos

- Basada en Great Expectations (`ge.from_pandas`) sin necesidad de DataContext.
- Normaliza nombres en minúsculas, fuerza IDs a texto y calcula `revenue` si faltaba.
- Expectativas clave:
  - Formato de `customer_id` y `product_id` (`C00001`, `P00001`).
  - Fechas ISO (`YYYY-MM-DD`) con tolerancia al 5%.
  - Métricas no negativas (`quantity`, `price`, `revenue`).
  - Columnas obligatorias según el dataset.
- Resultados:
  - CSV validados en `data/validated/`.
  - Reportes JSON en `ge_reports/sap_*_ge_report.json` con el estado de cada archivo.

---

## Transformación de datos

- Implementada en `scripts/Pandas Transform.py`.
- Lee los CSV validados (manteniendo IDs como texto) y realiza joins left con catálogos y calendario.
- Calcula `revenue` en caso de que no exista (`quantity * price`).
- Exporta Parquet utilizando `pyarrow.dataset.write_dataset`:
  - `data/curated/sales_enriched/` (particionado por fecha si existe).
  - `data/curated/dim_customers/`
  - `data/curated/dim_products/`
  - `data/curated/dim_calendar/`

---

## Carga a PostgreSQL

- Ejecuta `scripts/load_to_postgres.py` con las credenciales de `.env`.
- Crea el esquema `PG_SCHEMA` si aún no existe (por defecto `staging`).
- Lee todos los Parquet de cada subcarpeta y realiza `to_sql(..., if_exists="replace")`.
- Tablas generadas:
  - `staging.fct_sales_enriched`
  - `staging.dim_customers`
  - `staging.dim_products`
  - `staging.dim_calendar`
- Ideal para servir como capa semilla antes de modelar con dbt o consumir desde Power BI.

---

## Modelado y orquestación

### Modelado con dbt (opcional)

- `dbt/dbt_project.yml` es el punto de partida; agrega modelos en `models/` según tu convención.
- Configura tu perfil en `~/.dbt/profiles.yml` apuntando al contenedor PostgreSQL (`localhost:5432`).
- Comandos típicos:

  ```bash
  dbt debug
  dbt run
  dbt test
  ```

### Orquestación con Pentaho Data Integration

- El directorio `ETL/` incluye `Job 1.kjb` y `Transformation 1.ktr` como base para automatizar.
- Cada paso Shell puede invocar los mismos comandos Python usados en Quickstart.
- Ajusta rutas al intérprete de tu entorno virtual y habilita logging por paso.

---

## Power BI y datasets

- Conecta Power BI Desktop a PostgreSQL → servidor `localhost`, base `bia_dw`.
- Selecciona las tablas `staging.fct_sales_enriched` y las dimensiones `staging.dim_*`.
- Relaciones sugeridas:
  - `fct_sales_enriched[customer_id]` → `dim_customers[customer_id]`
  - `fct_sales_enriched[product_id]` → `dim_products[product_id]`
  - `fct_sales_enriched[date]` → `dim_calendar[date]`
- Medidas DAX de referencia:

  ```DAX
  Total Net Sales (USD) = SUM ( 'staging fct_sales_enriched'[revenue] )
  Units Sold = SUM ( 'staging fct_sales_enriched'[quantity] )
  Average Selling Price (USD) =
  DIVIDE ( [Total Net Sales (USD)], [Units Sold] )
  ```

---

## Solución de problemas

- **`ModuleNotFoundError` para dependencias**: asegúrate de que el entorno virtual esté activo e instalaste `requirements.txt`.
- **Rutas incorrectas**: si tus CSV viven en otra carpeta, actualiza `RAW_DIR` y `VALIDATED_DIR` en `.env`.
- **Ejecutar scripts con espacios**: en Windows usa comillas (`python "scripts/Pandas Transform.py"`).
- **Error al conectar a PostgreSQL**: revisa que el contenedor esté sano (`docker compose ps`) y que credenciales/puerto coincidan.
- **Expectativas fallidas**: revisa el JSON en `ge_reports/` para ver qué campo incumplió la validación.

---

## Siguientes pasos

- Añadir modelos y pruebas dbt (`not null`, `unique`, relaciones).
- Crear índices en PostgreSQL para acelerar consultas frecuentes.
- Automatizar el pipeline (Pentaho, Airflow o GitHub Actions) y versionar los datasets.
- Publicar el dashboard en Power BI Service con actualizaciones programadas.

---

## Licencia

Indica aquí la licencia del proyecto (por ejemplo, MIT, Apache-2.0 o de uso interno).

