# Run book — cómo ejecutar

> Se completa a medida que avanzan los pasos. Esqueleto del flujo previsto.

## Requisitos
- Workspace Databricks con **Unity Catalog** y compute **serverless**.
- CLI de Databricks configurada (perfil propio).
- Repo clonado en Databricks (Repos) o subido al Workspace.

## Orden de ejecución
1. `notebooks/00_setup/` — crea catálogo, schemas, Volume y carga los CSV.
2. `notebooks/01_bronze/` — ingesta raw.
3. `notebooks/02_silver/` — limpieza, estandarización y data quality.
4. `notebooks/03_gold/` — star schema y marts.
5. `tests/` — chequeos de integridad.

## Conexión Power BI
1. Obtener Server hostname y HTTP path del **SQL Warehouse**.
2. Power BI Desktop → Obtener datos → **Azure Databricks**.
3. Autenticar; elegir catálogo `nexo_retail` → schema `gold`.
4. Importar `fact_*`, `dim_*`, `mart_*`; armar relaciones estrella y medidas DAX.
