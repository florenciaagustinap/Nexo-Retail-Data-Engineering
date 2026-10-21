-- ============================================================================
-- DDL · Capa Gold (modelo estrella)  ·  nexo_retail.gold
-- Contratos de esquema explícitos. El poblado vive en sql/etl/gold_load.sql.
-- Nota de naming: se usa `anio` (sin ñ) para portabilidad del identificador.
-- ============================================================================

-- Dimensión Fecha (grano: 1 día) -------------------------------------------
CREATE OR REPLACE TABLE nexo_retail.gold.dim_fecha (
    fecha_key   INT,
    fecha       DATE,
    anio        INT,
    mes         STRING,     -- 'yyyy-MM'
    nombre_mes  STRING,
    numero_mes  INT,
    trimestre   INT
) USING DELTA;

-- Dimensión Producto (grano: 1 producto) -----------------------------------
-- Sin precio ni costo: ambos tienen comportamiento temporal (viven en los hechos).
CREATE OR REPLACE TABLE nexo_retail.gold.dim_producto (
    producto_key     INT,
    id_producto      INT,
    nombre_producto  STRING,
    categoria        STRING
) USING DELTA;

-- Dimensión Tienda (grano: 1 tienda) ----------------------------------------
-- canal_venta es atributo de la tienda (regla determinística 19/20/21=Online).
CREATE OR REPLACE TABLE nexo_retail.gold.dim_tienda (
    tienda_key   INT,
    id_tienda    INT,
    pais         STRING,
    canal_venta  STRING
) USING DELTA;

-- Hecho Ventas (grano: 1 ID_Venta) ------------------------------------------
-- id_cliente y categoria_venta quedan como dimensiones degeneradas.
CREATE OR REPLACE TABLE nexo_retail.gold.fact_ventas (
    venta_key             BIGINT,
    id_venta              INT,
    fecha_key             INT,
    producto_key          INT,
    tienda_key            INT,
    id_cliente            INT,
    categoria_venta       STRING,
    cantidad              INT,
    precio_unitario       DECIMAL(12,2),
    precio_lista          DECIMAL(12,2),
    costo_unitario        DECIMAL(12,2),
    facturacion           DECIMAL(18,2),
    costo_total           DECIMAL(18,2),
    margen_bruto          DECIMAL(18,2),
    margen_porcentaje     DECIMAL(9,6),
    descuento_porcentaje  DECIMAL(9,6)
) USING DELTA;

-- Hecho Devoluciones (grano: 1 ID_Devolucion) -------------------------------
CREATE OR REPLACE TABLE nexo_retail.gold.fact_devoluciones (
    devolucion_key          BIGINT,
    id_devolucion           INT,
    id_venta                INT,
    fecha_venta_key         INT,      -- NULL cuando la venta no existe (huérfana)
    fecha_devolucion_key    INT,
    producto_key            INT,
    tienda_key              INT,
    cantidad_devuelta       INT,
    precio_unitario         DECIMAL(12,2),
    valor_devuelto          DECIMAL(18,2),
    es_venta_huerfana       INT,
    es_devolucion_excesiva  INT
) USING DELTA;

-- Hecho Objetivos (grano: 1 mes x 1 tienda) ---------------------------------
CREATE OR REPLACE TABLE nexo_retail.gold.fact_objetivos (
    objetivo_key     INT,
    fecha_key        INT,     -- primer día del mes (yyyyMM01)
    tienda_key       INT,
    objetivo_ventas  DECIMAL(14,2)
) USING DELTA;
