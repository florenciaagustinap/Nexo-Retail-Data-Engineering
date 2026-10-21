# Arquitectura — Nexo-Retail-Data-Engineering

## Vista general

```
CSV (Volume) → BRONZE → SILVER → GOLD → Power BI
```

Un catálogo `nexo_retail` con tres schemas: `bronze`, `silver`, `gold`.

## Capas y responsabilidades

### Bronze — preservar el raw
- Lee cada CSV tal cual, respetando su delimitador / encoding / decimal propios.
- Todo como **string** (no se castea ni se limpia → no se pierde ningún dato por un cast fallido).
- Agrega metadata: `_source_file`, `_ingested_at`, `_ingest_id`.
- Una tabla Bronze por archivo.

### Silver — limpieza, estandarización y calidad
- Casteo de tipos (fechas, decimales).
- Normaliza **dos formatos de fecha**: ISO (`yyyy-MM-dd`) en Ventas/Precios y `dd/MM/yyyy` en Devoluciones.
- Normaliza **decimal con coma** en Costos (`279,81` → `279.81`).
- Estandariza texto: `trim`, unifica país (`México/MEXICO/" México"`, `brazil→brasil`, `italy→italia`).
- Deduplica por clave de negocio.
- **Controles de calidad** + cuarentena de filas inválidas (ver `data_quality.md`).
- Point-in-time de precios: deriva `vigencia_hasta` desde `Vigencia_Desde`.
- Extrae entidades conformadas (producto, tienda) desde la desnormalización de Ventas.

### Gold — consumo analítico (star schema)
- Dimensiones + hechos listos para BI.
- Marts con las métricas del negocio.

## Tablas por capa

| Capa | Tablas |
|------|--------|
| bronze | `ventas`, `devoluciones`, `precios`, `costos`, `objetivos` |
| silver | `ventas`, `devoluciones`, `precios`, `costos`, `objetivos`, `dim_producto`, `dim_tienda`, `dq_resultados` |
| gold | `dim_fecha`, `dim_producto`, `dim_tienda`, `fact_ventas`, `fact_devoluciones`, `mart_ventas_netas`, `mart_cumplimiento_objetivo` |

> **DimCliente descartada** a propósito: los objetivos son por tienda, la geografía la cubre
> la tienda y `Nombre_Cliente` es PII ruidosa. Se conserva `ID_Cliente` como **dimensión
> degenerada** en los hechos (conteo de clientes únicos sin el costo de una dimensión).

## División PySpark vs Spark SQL

| Capa | PySpark | Spark SQL |
|------|---------|-----------|
| Setup | subir archivos al Volume | `CREATE CATALOG/SCHEMA/VOLUME` |
| Bronze | **todo** (lectura con opciones por archivo + metadata) | — |
| Silver | parsing de fechas/decimales, trim/normalización, dedup, cuarentena | reglas de negocio, point-in-time, entidades conformadas |
| Gold | orquestación / logging | **todo el modelado** (dims, hechos, MERGE) |

## DDL vs ETL

- `sql/ddl/` → contratos de esquema (`CREATE SCHEMA`, `CREATE TABLE ... USING DELTA`) para Gold y `dq_resultados`.
- `sql/etl/` → `MERGE INTO` / `INSERT OVERWRITE` que pueblan las tablas.

## Consumo en Power BI

Power BI → conector Databricks → **SQL Warehouse** → catálogo `nexo_retail`, schema `gold`,
modo **Import**. Relaciones estrella hechos→dimensiones y medidas DAX sobre los marts.

## Supuestos

Unity Catalog · compute serverless · notebooks `.py` (source format) · Power BI SQL Warehouse (Import).
