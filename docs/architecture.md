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
- Dimensiones + hechos listos para BI, con valores a **nivel transaccional**.
- **No hay marts materializados.** Las métricas analíticas (ventas netas, margen %, tasa de
  devolución, cumplimiento, variaciones) se calculan como **medidas DAX en Power BI**, no como
  tablas en Gold.

## Tablas por capa

| Capa | Tablas |
|------|--------|
| bronze | `ventas`, `devoluciones`, `precios`, `costos`, `objetivos` |
| silver | `ventas`, `devoluciones`, `precios`, `costos`, `objetivos`, `dim_producto`, `dim_tienda`, `dq_resultados` (+ `*_rejected` de cuarentena) |
| gold | `dim_fecha`, `dim_producto`, `dim_tienda`, `fact_ventas`, `fact_devoluciones`, `fact_objetivos` |

### Modelo Gold — grano de cada tabla
| Tabla | Tipo | Grano |
|-------|------|-------|
| `gold.dim_fecha` | dimensión | 1 día (Date Table) |
| `gold.dim_producto` | dimensión | 1 producto |
| `gold.dim_tienda` | dimensión | 1 tienda |
| `gold.fact_ventas` | hecho | 1 fila por `ID_Venta` |
| `gold.fact_devoluciones` | hecho | 1 fila por `ID_Devolucion` |
| `gold.fact_objetivos` | hecho | 1 fila por mes × tienda (solo 2025) |

> **DimCliente descartada** a propósito: los objetivos son por tienda, la geografía la cubre
> la tienda y `Nombre_Cliente` es PII ruidosa. Se conserva `ID_Cliente` como **dimensión
> degenerada** en los hechos (conteo de clientes únicos sin el costo de una dimensión).

> **`dim_producto` conformada a una categoría por producto:** en el origen la
> `Categoría_Producto` es ambigua (los 101 productos aparecen con 2–4 categorías según la
> transacción). Se conforma a la **categoría más frecuente** por producto para tener una
> dimensión consistente y evitar una categoría multivaluada. Afecta levemente (~0,5 pp) solo a
> métricas por categoría; totales y análisis por país no se ven afectados. Detalle en
> [data_quality.md](data_quality.md#ambigüedad-de-categoría-de-producto).

## División PySpark vs Spark SQL

| Capa | PySpark | Spark SQL |
|------|---------|-----------|
| Setup | subir archivos al Volume | `CREATE CATALOG/SCHEMA/VOLUME` |
| Bronze | **todo** (lectura con opciones por archivo + metadata) | — |
| Silver | parsing de fechas/decimales, trim/normalización, dedup, cuarentena | reglas de negocio, point-in-time, entidades conformadas |
| Gold | orquestación / logging | **todo el modelado** (dims, hechos, `INSERT OVERWRITE`) |

## DDL vs ETL (separación asimétrica, a propósito)

- `sql/ddl/` → contratos de esquema (`CREATE TABLE ... USING DELTA`) **solo para Gold**.
- `sql/etl/` → `INSERT OVERWRITE` que pueblan **las tablas de Gold**.

La separación DDL/ETL en archivos SQL se aplica **únicamente a Gold**, y es una decisión
deliberada: cada capa usa la tecnología que corresponde a la naturaleza de su trabajo, no se
fuerza SQL "por SQL". El criterio por capa:

- **Bronze → PySpark.** Ingesta parametrizada por un diccionario de fuentes, con **opciones
  de lectura por archivo** (delimitador `;` en Costos, encoding, `quote`/`escape`) y **metadata
  de runtime** (`_ingest_id` por corrida, `_ingested_at`). El esquema es *schema-on-read* (todo
  string, 1:1 con la fuente); un DDL explícito congelaría el raw y un `CREATE TABLE` por archivo
  solo duplicaría la lista de columnas de cada CSV. No hay archivos SQL de Bronze.

- **Silver → PySpark + Spark SQL embebido.** PySpark para lo procedural: **parsing** (fechas en
  dos formatos, decimal con coma), **normalización** (país canónico, regla determinística de
  canal), **deduplicación** (duplicados exactos + detección de `ID_Venta` conflictivo),
  **cuarentena** (ruteo de filas a `*_rejected`) y **Data Quality** (log tipado en
  `dq_resultados` + reconciliación). Spark SQL **donde aporta**: point-in-time de precios
  (`LEAD`) y dimensiones conformadas (`SELECT DISTINCT`). El esquema de Silver lo define la
  transformación tipada; un DDL externo sería una segunda fuente de verdad (riesgo de drift).
  Las tablas (incluida `dq_resultados`) se crean con `saveAsTable` / `CREATE OR REPLACE TABLE`
  dentro del notebook. No hay archivos SQL de Silver.

- **Gold → DDL y ETL separados en SQL.** Es el **modelo dimensional estable de consumo**: su
  esquema es un contrato publicado (lo lee Power BI) y su lógica es declarativa/set-based
  (joins, point-in-time, métricas transaccionales). Por eso el DDL vive en
  `sql/ddl/gold_tables.sql`, el ETL en `sql/etl/gold_load.sql`, y el notebook
  `03_gold/01_build_gold.py` actúa como **orquestador** (ejecuta ambos y corre las validaciones).

## Consumo en Power BI

Power BI → conector Databricks → **SQL Warehouse** → catálogo `nexo_retail`, schema `gold`,
modo **Import**. Relaciones estrella hechos→dimensiones y **medidas DAX** (15) calculadas sobre
los hechos de Gold. El informe es un proyecto **PBIP** versionado (modelo en TMDL, informe en PBIR).

## Supuestos

Unity Catalog · compute serverless · notebooks `.py` (source format) · Power BI SQL Warehouse (Import).
