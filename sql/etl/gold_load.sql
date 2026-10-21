-- ============================================================================
-- ETL · Carga de la capa Gold desde Silver.
-- Las tablas ya existen (sql/ddl/gold_tables.sql). Acá solo se pueblan.
-- Orden: dimensiones primero (las facts resuelven sus keys contra ellas).
-- ============================================================================

-- dim_fecha: 2024-01-01 .. 2026-01-31 (cubre ventas 2024-2025 y devoluciones hasta 2026-01-15)
INSERT OVERWRITE nexo_retail.gold.dim_fecha
SELECT
    CAST(date_format(d, 'yyyyMMdd') AS INT)                         AS fecha_key,
    d                                                              AS fecha,
    YEAR(d)                                                        AS anio,
    date_format(d, 'yyyy-MM')                                      AS mes,
    element_at(array('enero','febrero','marzo','abril','mayo','junio',
                     'julio','agosto','septiembre','octubre','noviembre','diciembre'),
               MONTH(d))                                           AS nombre_mes,
    MONTH(d)                                                       AS numero_mes,
    QUARTER(d)                                                     AS trimestre
FROM (SELECT explode(sequence(DATE'2024-01-01', DATE'2026-01-31', INTERVAL 1 DAY)) AS d);

-- dim_producto (surrogate key estable por id_producto)
INSERT OVERWRITE nexo_retail.gold.dim_producto
SELECT
    ROW_NUMBER() OVER (ORDER BY id_producto) AS producto_key,
    id_producto,
    nombre_producto,
    categoria_producto AS categoria
FROM nexo_retail.silver.dim_producto;

-- dim_tienda (pais = ubicacion, canal por regla determinística)
INSERT OVERWRITE nexo_retail.gold.dim_tienda
SELECT
    ROW_NUMBER() OVER (ORDER BY id_tienda) AS tienda_key,
    id_tienda,
    ubicacion_tienda AS pais,
    CASE WHEN id_tienda IN (19, 20, 21) THEN 'Online' ELSE 'Offline' END AS canal_venta
FROM nexo_retail.silver.dim_tienda;

-- fact_ventas: point-in-time de precios + costo mensual + métricas transaccionales
INSERT OVERWRITE nexo_retail.gold.fact_ventas
WITH base AS (
    SELECT
        v.id_venta, v.fecha, v.id_producto, v.id_tienda, v.id_cliente,
        v.categoria_venta, v.cantidad, v.precio_unitario,
        p.precio_lista,
        c.costo_unitario
    FROM nexo_retail.silver.ventas v
    -- point-in-time: precio vigente a la fecha de la venta (rangos sin solape -> 1 match)
    LEFT JOIN nexo_retail.silver.precios p
           ON v.id_producto = p.id_producto
          AND v.fecha >= p.vigencia_desde
          AND (p.vigencia_hasta IS NULL OR v.fecha <= p.vigencia_hasta)
    -- costo del mes de la venta
    LEFT JOIN nexo_retail.silver.costos c
           ON v.id_producto = c.id_producto
          AND date_format(v.fecha, 'yyyy-MM') = c.periodo
)
SELECT
    ROW_NUMBER() OVER (ORDER BY b.id_venta)                 AS venta_key,
    b.id_venta,
    CAST(date_format(b.fecha, 'yyyyMMdd') AS INT)           AS fecha_key,
    dp.producto_key,
    dt.tienda_key,
    b.id_cliente,
    b.categoria_venta,
    b.cantidad,
    b.precio_unitario,
    b.precio_lista,
    b.costo_unitario,
    CAST(b.cantidad * b.precio_unitario AS DECIMAL(18,2))   AS facturacion,
    CAST(b.cantidad * b.costo_unitario  AS DECIMAL(18,2))   AS costo_total,
    CAST(b.cantidad * (b.precio_unitario - b.costo_unitario) AS DECIMAL(18,2)) AS margen_bruto,
    CASE WHEN b.precio_unitario = 0 OR b.costo_unitario IS NULL THEN NULL
         ELSE CAST((b.precio_unitario - b.costo_unitario) / b.precio_unitario AS DECIMAL(9,6))
    END                                                     AS margen_porcentaje,
    CASE WHEN b.precio_lista IS NULL OR b.precio_lista = 0 THEN NULL
         ELSE CAST((b.precio_lista - b.precio_unitario) / b.precio_lista AS DECIMAL(9,6))
    END                                                     AS descuento_porcentaje
FROM base b
JOIN nexo_retail.gold.dim_producto dp ON b.id_producto = dp.id_producto
JOIN nexo_retail.gold.dim_tienda   dt ON b.id_tienda   = dt.id_tienda;

-- fact_devoluciones: conserva huérfanas (sin venta) y marca excesivas
INSERT OVERWRITE nexo_retail.gold.fact_devoluciones
SELECT
    ROW_NUMBER() OVER (ORDER BY d.id_devolucion)            AS devolucion_key,
    d.id_devolucion,
    d.id_venta,
    CAST(date_format(v.fecha, 'yyyyMMdd') AS INT)           AS fecha_venta_key,       -- NULL si huérfana
    CAST(date_format(d.fecha, 'yyyyMMdd') AS INT)           AS fecha_devolucion_key,
    dp.producto_key,
    dt.tienda_key,
    d.cantidad_devuelta,
    d.precio_unitario,
    CAST(d.cantidad_devuelta * d.precio_unitario AS DECIMAL(18,2)) AS valor_devuelto,
    CASE WHEN v.id_venta IS NULL THEN 1 ELSE 0 END          AS es_venta_huerfana,
    CASE WHEN v.id_venta IS NOT NULL AND d.cantidad_devuelta > v.cantidad THEN 1 ELSE 0 END
                                                            AS es_devolucion_excesiva
FROM nexo_retail.silver.devoluciones d
LEFT JOIN nexo_retail.silver.ventas v ON d.id_venta = v.id_venta
JOIN nexo_retail.gold.dim_producto dp ON d.id_producto = dp.id_producto
JOIN nexo_retail.gold.dim_tienda   dt ON d.id_tienda   = dt.id_tienda;

-- fact_objetivos: solo 2025 (no se inventan objetivos para 2024)
INSERT OVERWRITE nexo_retail.gold.fact_objetivos
SELECT
    ROW_NUMBER() OVER (ORDER BY o.periodo, o.id_tienda)     AS objetivo_key,
    CAST(date_format(o.periodo_date, 'yyyyMMdd') AS INT)   AS fecha_key,
    dt.tienda_key,
    o.objetivo_ventas
FROM nexo_retail.silver.objetivos o
JOIN nexo_retail.gold.dim_tienda dt ON o.id_tienda = dt.id_tienda;
