# Data Quality — resultados reales

Los controles corren en **Silver** y persisten su resultado en `silver.dq_resultados`
(`regla`, `tabla`, `severidad`, `filas_afectadas`, `estado`, `detalle`, `ts`). Las reglas
**duras** rutean la fila a cuarentena (`silver.<tabla>_rejected`); las **blandas** se registran
como warning sin descartar datos.

## Sales (ventas)

- **80.536** registros raw (Bronze) → **80.295** tras deduplicación (Silver/Gold).
- **241 IDs duplicados** / **482 filas duplicadas exactas** → se eliminan 241 (dedup de fila
  completa, conservando una copia).
- **Revisión de conflictos:** se detecta si un mismo `ID_Venta` queda con filas **NO idénticas**
  tras el dedup exacto → **0 casos** (ninguna fila a revisión/rechazo).
- **340 canales de venta reconstruidos** por regla de negocio determinística: `ID_Tienda`
  **19/20/21 → Online**, resto → **Offline** (validada contra las filas con canal informado).

## Returns (devoluciones)

- **4.715** registros · fechas en formato **DD/MM/YYYY** (distinto de Ventas).
- **12 devoluciones** cuyo `ID_Venta` no tiene match en ventas → **preservadas** y marcadas
  `es_venta_huerfana = 1` (no se inventan relaciones; `fecha_venta_key` queda nula).
- **8 casos** donde `cantidad_devuelta` supera la cantidad vendida → marcados
  `es_devolucion_excesiva = 1` (se conservan).
- **99 devoluciones con fecha de devolución en 2026** vinculadas a **ventas de diciembre 2025**:
  tratadas como **late returns** y atribuidas por **cohorte de fecha de venta** (`fecha_venta_key`).

## Reference Data

- **101 productos** · **21 tiendas**.
- **Normalización** de países y categorías: país canónico **México / Brasil / Italia**
  (`MEXICO`, `" México"`, `brazil`, `italy`… → forma única).
- **Precios** (SCD por vigencia) y **costos** (mensual, decimal con coma `279,81 → 279.81`) tipados.
- **Point-in-time pricing**: `silver.precios` deriva `vigencia_hasta` (`LEAD`) para rangos
  contiguos sin solape; Gold aplica el precio vigente a la fecha de la venta.

## Objectives

- **246** registros · únicamente **2025**.
- **240 registros mensuales normales** (20 tiendas × 12 meses) + **6 registros de la tienda 18**
  (cobertura parcial de esa tienda).
- **Tratamiento:** cumplimiento y brecha se evalúan **solo donde existe objetivo** (2025); no se
  generan objetivos ficticios para 2024.

## Quarantine

- Reglas **duras** rutean la fila a `silver.<tabla>_rejected` (con `_reject_reason`): clave nula,
  fecha no parseable, `cantidad <= 0`, precio/costo inválido, y `ID_Venta` duplicado conflictivo.
- **Actualmente todas las tablas `*_rejected` están vacías (0 registros).**

## DQ Controls

- **14 controles *hard* → 14 PASS · 0 FAIL**, sobre las 5 tablas Silver:
  - Reglas duras por tabla (ventas, devoluciones, precios, costos, objetivos).
  - Unicidad de PK (una por tabla).
  - Reconciliación de conteos (ventas: `80.536 = 80.295 + 241`; devoluciones: `4.715 = 4.715`).
  - Claves no nulas en `silver.ventas`.
  - Rangos de vigencia de precios **sin solapamiento**.
- **Warnings** (informativos, no bloquean): 12 devoluciones huérfanas; objetivos solo 2025;
  `ID_Venta` conflictivo = 0; productos vendidos sin costo = 0.

## Ambigüedad de categoría de producto

- El origen contiene productos que aparecen asociados a **múltiples `Categoría_Producto`**
  según la transacción (la categoría no es estable por producto en los datos crudos).
- Hay **101 productos** y presentan entre **2 y 4 categorías distintas** en el origen.
- En **Silver/Gold** se conformó `dim_producto` a **una categoría por producto**, usando la
  **categoría más frecuente** del producto (desempate determinístico por `ROW_NUMBER`).
- Esta decisión garantiza una **dimensión de producto consistente** (una fila = un producto,
  una categoría), pero puede producir **pequeñas diferencias en métricas agregadas por
  categoría** frente a una solución que conserva la categoría **a nivel de transacción**.
- **Totales generales y análisis por país NO se ven afectados** por esta decisión (no dependen
  de la conformación de categoría); reconcilian exactamente con la solución BI.
- Efecto medido — **tasas de devolución 2025 por categoría**:

  | Categoría | Solución actual (DE, categoría conformada) | Solución BI (categoría a nivel transacción) | Diferencia |
  |-----------|--------------------------------------------|----------------------------------------------|------------|
  | Ropa        | **7,70%** | 7,18% | ~0,5 pp |
  | Electrónica | **5,95%** | 5,48% | ~0,5 pp |

- **Decisión:** se **mantiene la conformación de `dim_producto`** (una categoría por producto)
  para preservar una dimensión consistente y **no introducir una categoría multivaluada** en una
  dimensión. La diferencia (~0,5 pp, solo a nivel categoría) se documenta y se considera aceptable
  frente al beneficio de un modelo dimensional limpio.
