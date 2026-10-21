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
- Objetivos arrancan en 2025; ventas en 2024 (gap de períodos).
