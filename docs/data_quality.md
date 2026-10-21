# Estrategia de Data Quality

Los controles corren en **Silver** y persisten su resultado en `silver.dq_resultados`
(`regla`, `tabla`, `filas_afectadas`, `estado` PASS/FAIL, `ts`). Filas que fallan reglas
**duras** van a cuarentena (no se borran, se aíslan); las reglas **blandas** se loguean como warning.

## Controles previstos

| Tipo | Regla concreta | Dureza |
|------|----------------|--------|
| Schema / parsing | `Fecha` parsea en ISO y `dd/MM/yyyy`; `Costo_Unitario` convierte desde `279,81` | dura |
| Not null en claves | `ID_Venta`, `ID_Producto`, `ID_Tienda` no nulos | dura |
| Unicidad | `ID_Venta` único; `ID_Devolución` único | dura |
| Integridad referencial | `ID_Producto` de ventas existe en `dim_producto` y en `precios` | dura |
| Rangos | `Cantidad > 0`, `Precio_Unitario >= 0` | dura |
| Reconciliación | filas Bronze = filas Silver + filas cuarentena | dura |
| Regla de negocio | gap de períodos: Objetivos (2025) vs Ventas (2024) | blanda |

> _Se completa con los resultados reales al implementar la capa Silver (paso 5)._
