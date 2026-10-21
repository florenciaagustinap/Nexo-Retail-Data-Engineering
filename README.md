# Nexo-Retail-Data-Engineering

Pipeline de **Data Engineering** sobre el challenge de Nexo Retail, construido en **Databricks**
con arquitectura **Medallion (Bronze → Silver → Gold)**, **Delta Lake**, **PySpark** y
**Spark SQL**, y consumido desde **Power BI** sobre un **Databricks SQL Warehouse**.

> Proyecto hermano, orientado a ingeniería, de la solución de BI/Analysis
> `Nexo-Retail-BI-Challenge` (SQL Server + Power BI). Ambas trabajan sobre el mismo dataset y
> **cuentan la misma historia de negocio**.

## Business Context

Nexo Retail opera en **México, Brasil e Italia**, por canal **Online / Offline** y segmento
**Retail / Wholesale**, con cuatro categorías de producto: **Electronics, Furniture, Clothing y
Food** (Electrónica, Muebles, Ropa, Alimentos). El período analizado es **2024–2025**. El negocio
necesita entender ventas, margen, devoluciones y cumplimiento de objetivos de forma consistente y
reproducible.

## Objective

Construir un **pipeline reproducible** que tome los CSV crudos del challenge y los lleve, de punta
a punta, a consumo analítico:

1. **Ingestión** cruda y trazable (Bronze).
2. **Limpieza y conformación** + **Data Quality** (Silver).
3. **Modelo dimensional** estrella para consumo (Gold).
4. **Consumo analítico** en Power BI sobre el SQL Warehouse.

## Architecture

```text
CSV
  ↓
BRONZE — Raw Delta            (1:1 con la fuente, todo string + metadata de ingesta)
  ↓
SILVER — Clean / Conformed / DQ   (tipado, normalización, dedup, cuarentena, dimensiones conformadas)
  ↓
GOLD — Star Schema           (dimensiones + hechos, point-in-time de precios, métricas transaccionales)
  ↓
Databricks SQL Warehouse
  ↓
Power BI
```

- **Bronze** preserva el dato crudo (no se limpia ni castea): foto reproducible de la fuente.
- **Silver** tipa, estandariza, deduplica, controla calidad (quarantine) y conforma dimensiones.
- **Gold** expone un modelo estrella estable para BI; las métricas agregadas se calculan como
  **medidas DAX en Power BI** (Gold deja valores a nivel transaccional).
- **Power BI** lee Gold vía **Databricks SQL Warehouse**.

Detalle completo en [docs/architecture.md](docs/architecture.md).

## Technology Stack

Databricks · Unity Catalog · Managed Volume · PySpark · Spark SQL · Delta Lake · SQL · Power BI · Git / GitHub.

## Repository Structure

```text
conf/                 Configuración central (catálogo, schemas, parsing por fuente)
data/raw/             Copia reproducible de los 5 CSV del challenge
notebooks/
  00_setup/           (sin notebook ejecutable — ver nota abajo)
  01_bronze/          01_ingest_bronze.py   — ingesta raw
  02_silver/          01_build_silver.py    — limpieza, conformación y DQ
  03_gold/            01_build_gold.py      — orquesta DDL + ETL + validaciones
sql/
  ddl/                gold_tables.sql       — CREATE TABLE de Gold (contratos de esquema)
  etl/                gold_load.sql         — INSERT OVERWRITE que pueblan Gold
docs/                 architecture · data_quality · data_dictionary · run_book
tests/                (sin tests automatizados aún — DQ vive en silver.dq_resultados)
Nexo-Retail-Data-Engineering.pbip             Proyecto Power BI (PBIP)
Nexo-Retail-Data-Engineering.Report/          Informe en formato PBIR (3 páginas)
Nexo-Retail-Data-Engineering.SemanticModel/   Modelo semántico en TMDL (6 tablas, 15 medidas)
```

> **Nota sobre `00_setup/`:** actualmente **no contiene un notebook ejecutable**. El setup inicial
> (catálogo `nexo_retail`, schemas `bronze/silver/gold`, Managed Volume y carga de los 5 CSV) se
> realizó mediante la **CLI de Databricks**. Ver [docs/run_book.md](docs/run_book.md).

## Data Flow

1. **CSV → Volume** (`/Volumes/nexo_retail/bronze/landing`) — setup por CLI.
2. **Bronze**: `01_ingest_bronze.py` lee cada CSV con su delimitador propio, todo como `string`,
   + metadata (`_source_file`, `_ingested_at`, `_ingest_id`).
3. **Silver**: `01_build_silver.py` castea, normaliza (fechas en 2 formatos, decimal con coma,
   países/categorías), deduplica, rutea filas inválidas a cuarentena, resuelve el point-in-time de
   precios y conforma `dim_producto` / `dim_tienda`.
4. **Gold**: `01_build_gold.py` ejecuta `sql/ddl/gold_tables.sql` y `sql/etl/gold_load.sql`
   (DDL y ETL separados) y corre las validaciones del modelo.
5. **Power BI**: lee Gold vía SQL Warehouse.

## Gold Model (star schema)

**Dimensiones**
- `gold.dim_fecha` — grano 1 día (`fecha_key`, `fecha`, `anio`, `mes`, `nombre_mes`, `numero_mes`, `trimestre`); marcada como Date Table.
- `gold.dim_producto` — grano 1 producto (`producto_key`, `id_producto`, `nombre_producto`, `categoria`).
- `gold.dim_tienda` — grano 1 tienda (`tienda_key`, `id_tienda`, `pais`, `canal_venta`).

**Hechos**
- `gold.fact_ventas` — grano 1 `id_venta`; métricas transaccionales (`facturacion`, `costo_total`,
  `margen_bruto`, `margen_porcentaje`, `descuento_porcentaje`) + `precio_lista` por point-in-time;
  `id_cliente` y `categoria_venta` como atributos degenerados.
- `gold.fact_devoluciones` — grano 1 `id_devolucion`; doble fecha (`fecha_venta_key`,
  `fecha_devolucion_key`), `valor_devuelto`, flags `es_venta_huerfana` / `es_devolucion_excesiva`.
- `gold.fact_objetivos` — grano 1 mes × tienda (`objetivo_ventas`, solo 2025).

> **No existen** `mart_ventas_netas` ni `mart_cumplimiento_objetivo`. Las métricas analíticas
> (ventas netas, margen %, tasa de devolución, cumplimiento, variaciones) se calculan como
> **medidas DAX en Power BI**, no como tablas materializadas en Gold.

## Data Quality (resultados reales)

| Métrica | Valor |
|---|---|
| Ventas raw (Bronze) | 80.536 |
| Ventas Silver/Gold | 80.295 |
| IDs duplicados / filas duplicadas exactas | 241 IDs · 482 filas (241 eliminadas) |
| Canales de venta reconstruidos (regla determinística por tienda) | 340 |
| Devoluciones huérfanas (preservadas y marcadas) | 12 |
| Devoluciones excesivas (detectadas y marcadas) | 8 |
| Normalización | países (`México/Brasil/Italia`) y categorías |
| Objetivos | disponibles únicamente para **2025** |
| Controles DQ *hard* | **14 PASS · 0 FAIL** |
| Cuarentena (`*_rejected`) | **sin registros** |

Detalle y decisiones en [docs/data_quality.md](docs/data_quality.md).

## Business Results

| KPI (2025) | Valor |
|---|---|
| Ventas | 20.030.113 USD |
| Crecimiento YoY | +20,5% |
| Margen | 33,01% |
| Variación margen | +0,73 pp |
| Devoluciones | 991.171 USD |
| Variación devoluciones | +47,3% |
| Tasa de devolución | 4,95% |
| Cumplimiento de objetivo | 117,4% |
| Brasil / México / Italia (YoY ventas) | +40,8% / +38,2% / −38,6% |
| Online / Offline (YoY ventas) | +99,2% / +0,3% |
| Electronics (variación margen) | −5,14 pp |

**Data Engineering y BI cuentan la misma historia**: estos resultados reconcilian exactamente con
la solución de BI original. Existe una **diferencia puntual en las tasas de devolución por
categoría** (Ropa 7,70% vs 7,18%; Electrónica 5,95% vs 5,48%, ~0,5 pp) por la **ambigüedad de
`Categoría_Producto` en origen** y la conformación de `dim_producto`, documentada en
[docs/data_quality.md](docs/data_quality.md#ambigüedad-de-categoría-de-producto). Totales y
análisis por país no se ven afectados.

## Key Technical Decisions

- **Sin `dim_cliente`**: los objetivos son por tienda y la geografía la cubre la tienda;
  `ID_Cliente` queda como **atributo degenerado** en los hechos.
- **Precios point-in-time**: `silver.precios` deriva `vigencia_hasta` (`LEAD`) y Gold aplica el
  precio vigente a la fecha de la venta (sin join plano por producto).
- **Devoluciones por cohorte de fecha de venta**: la relación activa en Power BI es
  `fecha_venta_key` (para reconciliar con la solución BI); `fecha_devolucion_key` queda inactiva.
- **Categoría de producto conformada** a la categoría más frecuente por producto (dimensión consistente).
- **DDL y ETL separados** en Gold (`sql/ddl` vs `sql/etl`); el notebook orquesta.
- **PySpark** para parsing, lógica procedural y Data Quality; **Spark SQL** para lógica
  declarativa y modelado dimensional.

## Power BI

- **PBIP versionado** en el repo: `Nexo-Retail-Data-Engineering.pbip` (+ `.Report` en PBIR y
  `.SemanticModel` en TMDL).
- **3 páginas**: `1 · Resumen Ejecutivo`, `2 · Diagnóstico`, `3 · Devoluciones y Objetivos`.
- **15 medidas** DAX sobre un **modelo estrella** (9 relaciones, `dim_fecha` como Date Table).
- Flujo de consumo: **Gold → Databricks SQL Warehouse → Power BI** (Import).

## Documentation

- [docs/architecture.md](docs/architecture.md)
- [docs/data_quality.md](docs/data_quality.md)
- [docs/data_dictionary.md](docs/data_dictionary.md)
- [docs/run_book.md](docs/run_book.md)
