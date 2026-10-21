# Nexo-Retail-Data-Engineering

Solución de **Data Engineering** sobre el challenge técnico de Nexo Retail, construida en
**Databricks** con arquitectura **Medallion (Bronze → Silver → Gold)**, **Delta Lake**,
**PySpark** y **Spark SQL**, con **Power BI** como capa de consumo.

> Proyecto hermano, orientado a ingeniería, de la solución de BI/Analysis
> `Nexo-Retail-BI-Challenge` (SQL Server + Power BI).

## Arquitectura

```
CSV (Volume) → BRONZE (raw Delta) → SILVER (limpio + conformado) → GOLD (star schema) → Power BI
```

| Capa | Responsabilidad | Tecnología |
|------|-----------------|------------|
| Bronze | Datos crudos 1:1, todo string + metadata de ingesta | PySpark |
| Silver | Tipado, estandarización, dedup, calidad de datos, dimensiones conformadas | PySpark + Spark SQL |
| Gold | Star schema (dims + hechos) y marts de negocio | Spark SQL |

Detalle completo en [docs/architecture.md](docs/architecture.md).

## Estructura

```
├── conf/            Configuración central (catálogo, schemas, parsing por fuente)
├── data/raw/        Copia reproducible de los 5 CSV del challenge
├── notebooks/
│   ├── 00_setup/    Catálogo, schemas, Volume, carga de CSV
│   ├── 01_bronze/   Ingesta raw
│   ├── 02_silver/   Limpieza + calidad
│   └── 03_gold/     Modelo analítico
├── sql/
│   ├── ddl/         CREATE SCHEMA / CREATE TABLE (contratos de esquema)
│   └── etl/         MERGE / INSERT que pueblan las tablas
├── docs/            architecture, data_quality, data_dictionary, run_book
└── tests/           Chequeos de integridad y reconciliación
```

## Modelo Gold (star schema)

- **Dimensiones:** `dim_fecha`, `dim_producto`, `dim_tienda`
- **Hechos:** `fact_ventas`, `fact_devoluciones` (con `ID_Cliente` como dimensión degenerada)
- **Marts:** `mart_ventas_netas` (con margen), `mart_cumplimiento_objetivo`

## Cómo ejecutar

Ver [docs/run_book.md](docs/run_book.md). Resumen: correr los notebooks en orden
`00_setup → 01_bronze → 02_silver → 03_gold` y conectar Power BI al SQL Warehouse
sobre el schema `gold`.

## Supuestos

Unity Catalog · compute serverless · notebooks en formato `.py` (source) · Power BI vía SQL Warehouse (Import).
