# Run book — cómo ejecutar / reproducir

Flujo real del pipeline, de la landing de CSV hasta el consumo en Power BI.

## Prerequisites
- Workspace **Databricks** con **Unity Catalog**.
- Permisos para crear catálogo/schemas/Volume y ejecutar en **compute serverless**.
- Un **SQL Warehouse** (para que Power BI consuma Gold).
- Los 5 CSV del challenge (`data/raw/`).

## Setup / Landing (vía CLI)
El setup inicial se realizó con la **CLI de Databricks** (no hay notebook en `notebooks/00_setup/`):
1. Crear catálogo `nexo_retail` y schemas `bronze`, `silver`, `gold`.
2. Crear el **Managed Volume** `nexo_retail.bronze.landing`.
3. Subir los 5 CSV al Volume (`/Volumes/nexo_retail/bronze/landing/`).

## Bronze — `notebooks/01_bronze/01_ingest_bronze.py`
- Materializa **5 tablas Bronze** (`ventas`, `devoluciones`, `precios`, `costos`, `objetivos`).
- Datos **raw 1:1**: todas las columnas como **STRING** (`inferSchema=False`), cada CSV con su
  delimitador propio (Costos usa `;`).
- Agrega metadata de ingestión: `_source_file`, `_ingested_at`, `_ingest_id`.

## Silver — `notebooks/02_silver/01_build_silver.py`
- **Parsing / tipado** (fechas en dos formatos, decimal con coma en costos).
- **Normalización** (país canónico, regla determinística de canal).
- **Deduplicación** (duplicados exactos + detección de `ID_Venta` conflictivo).
- **Data Quality** (log en `silver.dq_resultados`) y **cuarentena** (`silver.*_rejected`).
- **Dimensiones conformadas** (`dim_producto`, `dim_tienda`) y **point-in-time** de precios.

## Gold — DDL + ETL separados
- `sql/ddl/gold_tables.sql` → **DDL** (`CREATE TABLE` de las 6 tablas Gold; contratos de esquema).
- `sql/etl/gold_load.sql` → **ETL** (`INSERT OVERWRITE` que pueblan Gold desde Silver;
  point-in-time de precios, flags de devoluciones, objetivos 2025).
- `notebooks/03_gold/01_build_gold.py` → **orquestador**: ejecuta el DDL, luego el ETL, y corre
  las validaciones del modelo.

## Validation
- **Silver:** 14 controles DQ *hard* (PASS/FAIL) + reconciliación de conteos, persistidos en
  `silver.dq_resultados`; filas inválidas a `silver.*_rejected` (hoy vacías).
- **Gold:** el notebook valida row counts, unicidad de grano, nulos en claves, orphans
  fact↔dim, reconciliación vs Silver y el point-in-time de precios.
- **Nota:** `tests/` **no contiene tests automatizados** por ahora; los controles viven en el
  notebook de Silver / `silver.dq_resultados`.

## Power BI
1. Abrir **`Nexo-Retail-Data-Engineering.pbip`** en Power BI Desktop (trae el modelo en TMDL y el
   informe en PBIR; el `.pbix` no se versiona).
2. Power BI consume **Gold** mediante el **Databricks SQL Warehouse** (catálogo `nexo_retail`,
   schema `gold`, modo Import).
3. El informe tiene **3 páginas** (`1 · Resumen Ejecutivo`, `2 · Diagnóstico`,
   `3 · Devoluciones y Objetivos`) y **15 medidas** DAX sobre un modelo estrella.
