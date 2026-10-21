# Diccionario de datos — fuentes

> Columnas tal como llegan en los CSV crudos. El tipado y la estandarización ocurren en Silver.

## Ventas.csv (`,` · fecha `yyyy-MM-dd` · 80.536 filas)
`ID_Venta`, `ID_Pedido`, `Fecha`, `ID_Producto`, `ID_Cliente`, `ID_Tienda`, `Cantidad`,
`Precio_Unitario`, `Canal_Venta`, `Categoría_Venta`, `Nombre_Cliente`, `Ubicación_Cliente`,
`Nombre_Tienda`, `Ubicación_Tienda`, `Nombre_Producto`, `Categoría_Producto`, `Subcategoría_Producto`

## Devoluciones.csv (`,` · fecha `dd/MM/yyyy` · 4.715 filas)
`ID_Devolución`, `ID_Venta`, `Fecha`, `ID_Producto`, `ID_Cliente`, `ID_Tienda`,
`Cantidad_Devuelta`, `Motivo`, `Precio_Unitario`, `Nombre_Cliente`, `Ubicación_Cliente`,
`Nombre_Tienda`, `Ubicación_Tienda`, `Nombre_Producto`, `Categoría_Producto`

## Precios.csv (`,` · fecha `yyyy-MM-dd` · 641 filas) — SCD por vigencia
`ID_Producto`, `Vigencia_Desde`, `Precio_Lista`

## Costos.csv (`;` · decimal con coma · período `yyyy-MM` · 2.424 filas)
`Periodo`, `ID_Producto`, `Costo_Unitario`

## Objetivos.csv (`,` · período `yyyy-MM` · 246 filas)
`Periodo`, `ID_Tienda`, `Objetivo_Ventas`

## Notas de calidad detectadas
- Fechas en dos formatos distintos (Ventas/Precios vs Devoluciones).
- Decimal con coma solo en Costos; delimitador `;`.
- País sucio/bilingüe: `México/MEXICO/" México"`, `brazil/brasil`, `italy/italia`.
- `Nombre_Cliente` a veces con sufijo `(id)` → `Diego Costa (10)`.
- Objetivos solo 2025; ventas 2024–2025 (gap de períodos).

---

# Modelo Gold (`nexo_retail.gold`)

Modelo estrella. Las métricas agregadas se calculan como **medidas DAX en Power BI** (Gold deja
valores a nivel transaccional). Claves sustitutas = `*_key`.

## Dimensiones

### `gold.dim_fecha` — grano: 1 día · PK `fecha_key`
`fecha_key` (INT, `yyyyMMdd`) · `fecha` (DATE, Date Table) · `anio` · `mes` (`yyyy-MM`) ·
`nombre_mes` · `numero_mes` · `trimestre`. Cubre 2024-01-01 → 2026-01-31 (incluye late returns).

### `gold.dim_producto` — grano: 1 producto · PK `producto_key`
`producto_key` · `id_producto` · `nombre_producto` · `categoria`.
> `categoria` **conformada a la categoría más frecuente** por producto (ver
> [data_quality.md](data_quality.md#ambigüedad-de-categoría-de-producto)).

### `gold.dim_tienda` — grano: 1 tienda · PK `tienda_key`
`tienda_key` · `id_tienda` · `pais` · `canal_venta` (derivado por regla 19/20/21 = Online).

## Hechos

### `gold.fact_ventas` — grano: 1 fila por `ID_Venta` · PK `venta_key`
Claves: `venta_key`, `fecha_key`, `producto_key`, `tienda_key`. Degeneradas: `id_venta`,
`id_cliente`, `categoria_venta` (Retail/Wholesale). Medidas base: `cantidad`, `precio_unitario`,
`precio_lista` (point-in-time), `costo_unitario`, `facturacion`, `costo_total`, `margen_bruto`,
`margen_porcentaje`, `descuento_porcentaje`.

### `gold.fact_devoluciones` — grano: 1 fila por `ID_Devolucion` · PK `devolucion_key`
Claves de fecha: `fecha_venta_key` (cohorte venta), `fecha_devolucion_key` (fecha real).
`producto_key`, `tienda_key`. Degeneradas: `id_devolucion`, `id_venta`. Valores:
`cantidad_devuelta`, `precio_unitario`, `valor_devuelto`. Flags: `es_venta_huerfana`,
`es_devolucion_excesiva`.

### `gold.fact_objetivos` — grano: 1 fila por mes × tienda (solo 2025) · PK `objetivo_key`
`objetivo_key` · `fecha_key` (primer día del mes) · `tienda_key` · `objetivo_ventas`.

## Relationships (modelo estrella en Power BI)

- Dimensiones → hechos, `1→*`, filtro de una dirección:
  `dim_fecha` → ventas / devoluciones / objetivos · `dim_producto` → ventas / devoluciones ·
  `dim_tienda` → ventas / devoluciones / objetivos.
- **Doble fecha en devoluciones:** `fact_devoluciones[fecha_venta_key] → dim_fecha` es la
  relación **activa** (análisis por cohorte de venta); `fact_devoluciones[fecha_devolucion_key] →
  dim_fecha` queda **inactiva** (alternativa, vía `USERELATIONSHIP`).
- Sin relaciones entre hechos (no fact↔fact).
