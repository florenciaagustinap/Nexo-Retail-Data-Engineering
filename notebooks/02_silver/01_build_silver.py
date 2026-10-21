# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — Limpieza, estandarización y calidad
# MAGIC
# MAGIC Lee las tablas `bronze.*`, aplica casteos / normalizaciones / dedup / controles de
# MAGIC calidad y materializa las tablas `silver.*`. Filas que fallan reglas **duras** van a
# MAGIC `silver.<x>_rejected`; todo queda contabilizado en `silver.dq_resultados`.
# MAGIC
# MAGIC Combina **PySpark** (parsing, normalización, dedup, cuarentena, log DQ) con
# MAGIC **Spark SQL** (point-in-time de precios con `LEAD()` y dimensiones conformadas).

# COMMAND ----------

from pyspark.sql import functions as F, Window
import json, datetime

CATALOG = "nexo_retail"
B = f"{CATALOG}.bronze"
S = f"{CATALOG}.silver"

# Acumulador de resultados de Data Quality
DQ = []
def log(regla, tabla, severidad, filas, estado, detalle=""):
    DQ.append({
        "regla": regla, "tabla": tabla, "severidad": severidad,
        "filas_afectadas": int(filas), "estado": estado, "detalle": detalle,
    })

def norm_pais(c):
    """Normaliza país al conjunto canónico {México, Brasil, Italia}."""
    base = F.upper(F.trim(F.translate(c, "áéíóúÁÉÍÓÚ", "aeiouAEIOU")))
    return (F.when(base == "MEXICO", "México")
             .when(base.isin("BRASIL", "BRAZIL"), "Brasil")
             .when(base.isin("ITALIA", "ITALY"), "Italia")
             .when((c.isNull()) | (F.trim(c) == ""), None)
             .otherwise(F.initcap(F.trim(c))))

# COMMAND ----------
# MAGIC %md ## Ventas

# COMMAND ----------

v0 = spark.table(f"{B}.ventas")
n_bronze_v = v0.count()

v = v0.select(
    F.col("ID_Venta").cast("int").alias("id_venta"),
    F.col("ID_Pedido").cast("int").alias("id_pedido"),
    F.to_date("Fecha", "yyyy-MM-dd").alias("fecha"),
    F.col("ID_Producto").cast("int").alias("id_producto"),
    F.col("ID_Cliente").cast("int").alias("id_cliente"),
    F.col("ID_Tienda").cast("int").alias("id_tienda"),
    F.col("Cantidad").cast("int").alias("cantidad"),
    F.col("Precio_Unitario").cast("decimal(12,2)").alias("precio_unitario"),
    F.col("Canal_Venta").alias("_canal_raw"),
    F.trim(F.col("Categoría_Venta")).alias("categoria_venta"),
    F.trim(F.col("Nombre_Cliente")).alias("nombre_cliente"),
    norm_pais(F.col("Ubicación_Cliente")).alias("ubicacion_cliente"),
    F.trim(F.col("Nombre_Tienda")).alias("nombre_tienda"),
    norm_pais(F.col("Ubicación_Tienda")).alias("ubicacion_tienda"),
    F.trim(F.col("Nombre_Producto")).alias("nombre_producto"),
    F.trim(F.col("Categoría_Producto")).alias("categoria_producto"),
    F.trim(F.col("Subcategoría_Producto")).alias("subcategoria_producto"),
)

# --- Regla de negocio determinística: reconstrucción de Canal_Venta -------------
# Canal vacío -> se deriva por tienda: 19/20/21 = Online, resto = Offline.
# Validado contra las filas con canal informado (mapeo tienda->canal sin excepciones).
v = v.withColumn("_canal_reconstruido",
                 F.col("_canal_raw").isNull() | (F.trim("_canal_raw") == ""))
v = v.withColumn(
    "canal_venta",
    F.when(F.col("_canal_reconstruido"),
           F.when(F.col("id_tienda").isin(19, 20, 21), "Online").otherwise("Offline"))
     .otherwise(F.trim("_canal_raw")),
)
n_canal_reconstruido = v.filter("_canal_reconstruido").count()
log("Canal_Venta reconstruido por regla determinística (tienda->canal)",
    "ventas", "INFO", n_canal_reconstruido, "APPLIED",
    "19/20/21=Online, resto=Offline")

# --- Reglas duras ---------------------------------------------------------------
hard_v = (F.col("id_venta").isNull() | F.col("id_producto").isNull() |
          F.col("id_tienda").isNull() | F.col("fecha").isNull() |
          F.col("cantidad").isNull() | (F.col("cantidad") <= 0) |
          (F.col("precio_unitario") < 0))

v_hard_rej = v.filter(hard_v).withColumn("_reject_reason", F.lit("hard_rule_fail"))
v_ok = v.filter(~hard_v)
n_hard_rej_v = v_hard_rej.count()
log("Filas que violan reglas duras (clave nula / fecha inválida / cantidad<=0 / precio<0)",
    "ventas", "HARD", n_hard_rej_v, "FAIL" if n_hard_rej_v else "PASS")

# --- Dedup: 1) filas exactas  2) ID_Venta conflictivo (NO se descarta en silencio)
v_exact = v_ok.dropDuplicates()
n_exact_removed = v_ok.count() - v_exact.count()
log("Duplicados exactos (fila idéntica) eliminados", "ventas", "INFO",
    n_exact_removed, "APPLIED")

conflict_ids = (v_exact.groupBy("id_venta").count()
                .filter("count > 1").select("id_venta"))
v_conflict = (v_exact.join(conflict_ids, "id_venta", "inner")
              .withColumn("_reject_reason", F.lit("conflicting_duplicate_id")))
n_conflict = v_conflict.count()
# warning: mismo ID_Venta con contenido distinto -> a revisión/rechazo, no se descarta solo
log("ID_Venta duplicado con registros NO idénticos (enviado a revisión)",
    "ventas", "WARN", n_conflict, "WARN" if n_conflict else "PASS")

v_clean_full = v_exact.join(conflict_ids, "id_venta", "left_anti")

# Tabla de rechazos de ventas (hard + conflictos), proyectada a columnas de negocio
reject_cols = ["id_venta", "id_pedido", "fecha", "id_producto", "id_cliente",
               "id_tienda", "cantidad", "precio_unitario", "canal_venta",
               "categoria_venta", "_reject_reason"]
ventas_rejected = v_hard_rej.select(*reject_cols).unionByName(v_conflict.select(*reject_cols))
ventas_rejected.write.format("delta").mode("overwrite").option("overwriteSchema", True) \
    .saveAsTable(f"{S}.ventas_rejected")

# silver.ventas (grano: línea de venta; ID_Cliente queda como dim. degenerada)
silver_ventas = v_clean_full.select(
    "id_venta", "id_pedido", "fecha", "id_producto", "id_cliente", "id_tienda",
    "cantidad", "precio_unitario", "canal_venta", "categoria_venta",
    F.col("_canal_reconstruido").alias("canal_reconstruido"),
)
silver_ventas.write.format("delta").mode("overwrite").option("overwriteSchema", True) \
    .saveAsTable(f"{S}.ventas")
n_clean_v = silver_ventas.count()

# Vista enriquecida (con atributos) para construir dimensiones conformadas
v_clean_full.createOrReplaceTempView("ventas_enriquecida")

# Reconciliación ventas
recon_v_ok = (n_clean_v + n_hard_rej_v + n_exact_removed + n_conflict == n_bronze_v)
log("Reconciliación: bronze = limpias + hard_rej + dups_exactos + conflictos",
    "ventas", "HARD", n_bronze_v, "PASS" if recon_v_ok else "FAIL",
    f"{n_bronze_v} = {n_clean_v}+{n_hard_rej_v}+{n_exact_removed}+{n_conflict}")

# COMMAND ----------
# MAGIC %md ## Devoluciones

# COMMAND ----------

d0 = spark.table(f"{B}.devoluciones")
n_bronze_d = d0.count()

d = d0.select(
    F.col("ID_Devolución").cast("int").alias("id_devolucion"),
    F.col("ID_Venta").cast("int").alias("id_venta"),
    F.to_date("Fecha", "dd/MM/yyyy").alias("fecha"),
    F.col("ID_Producto").cast("int").alias("id_producto"),
    F.col("ID_Cliente").cast("int").alias("id_cliente"),
    F.col("ID_Tienda").cast("int").alias("id_tienda"),
    F.col("Cantidad_Devuelta").cast("int").alias("cantidad_devuelta"),
    F.col("Precio_Unitario").cast("decimal(12,2)").alias("precio_unitario"),
    F.trim(F.col("Motivo")).alias("motivo"),
)

hard_d = (F.col("id_devolucion").isNull() | F.col("id_venta").isNull() |
          F.col("id_producto").isNull() | F.col("fecha").isNull() |
          F.col("cantidad_devuelta").isNull() | (F.col("cantidad_devuelta") <= 0))

d_hard_rej = d.filter(hard_d).withColumn("_reject_reason", F.lit("hard_rule_fail"))
d_ok = d.filter(~hard_d).dropDuplicates(["id_devolucion"])
n_hard_rej_d = d_hard_rej.count()
log("Filas que violan reglas duras", "devoluciones", "HARD",
    n_hard_rej_d, "FAIL" if n_hard_rej_d else "PASS")

d_hard_rej.write.format("delta").mode("overwrite").option("overwriteSchema", True) \
    .saveAsTable(f"{S}.devoluciones_rejected")
d_ok.write.format("delta").mode("overwrite").option("overwriteSchema", True) \
    .saveAsTable(f"{S}.devoluciones")
n_clean_d = d_ok.count()

recon_d_ok = (n_clean_d + n_hard_rej_d == n_bronze_d)
log("Reconciliación: bronze = limpias + hard_rej", "devoluciones", "HARD",
    n_bronze_d, "PASS" if recon_d_ok else "FAIL",
    f"{n_bronze_d} = {n_clean_d}+{n_hard_rej_d}")

# COMMAND ----------
# MAGIC %md ## Precios  (point-in-time con LEAD en Spark SQL)

# COMMAND ----------

p0 = spark.table(f"{B}.precios")
n_bronze_p = p0.count()
hard_p = (F.col("id_producto").isNull() | F.col("vigencia_desde").isNull() |
          F.col("precio_lista").isNull() | (F.col("precio_lista") <= 0))
p_typed = p0.select(
    F.col("ID_Producto").cast("int").alias("id_producto"),
    F.to_date("Vigencia_Desde", "yyyy-MM-dd").alias("vigencia_desde"),
    F.col("Precio_Lista").cast("decimal(12,2)").alias("precio_lista"),
)
p_hard_rej = p_typed.filter(hard_p).withColumn("_reject_reason", F.lit("hard_rule_fail"))
p_ok = p_typed.filter(~hard_p).dropDuplicates(["id_producto", "vigencia_desde"])
n_hard_rej_p = p_hard_rej.count()
log("Filas que violan reglas duras", "precios", "HARD",
    n_hard_rej_p, "FAIL" if n_hard_rej_p else "PASS")

p_hard_rej.write.format("delta").mode("overwrite").option("overwriteSchema", True) \
    .saveAsTable(f"{S}.precios_rejected")
p_ok.createOrReplaceTempView("precios_clean")

# Spark SQL: deriva vigencia_hasta (rango contiguo, sin solape; última = NULL = vigente)
spark.sql(f"""
CREATE OR REPLACE TABLE {S}.precios AS
SELECT
    id_producto,
    precio_lista,
    vigencia_desde,
    DATE_SUB(
        LEAD(vigencia_desde) OVER (PARTITION BY id_producto ORDER BY vigencia_desde),
        1
    ) AS vigencia_hasta
FROM precios_clean
""")
n_clean_p = spark.table(f"{S}.precios").count()

# COMMAND ----------
# MAGIC %md ## Costos  (decimal con coma)

# COMMAND ----------

c0 = spark.table(f"{B}.costos")
n_bronze_c = c0.count()
c_typed = c0.select(
    F.trim(F.col("Periodo")).alias("periodo"),
    F.to_date(F.concat(F.trim(F.col("Periodo")), F.lit("-01")), "yyyy-MM-dd").alias("periodo_date"),
    F.col("ID_Producto").cast("int").alias("id_producto"),
    F.regexp_replace(F.col("Costo_Unitario"), ",", ".").cast("decimal(12,2)").alias("costo_unitario"),
)
hard_c = (F.col("periodo").isNull() | (F.col("periodo") == "") |
          F.col("id_producto").isNull() | F.col("costo_unitario").isNull() |
          (F.col("costo_unitario") <= 0))
c_hard_rej = c_typed.filter(hard_c).withColumn("_reject_reason", F.lit("hard_rule_fail"))
c_ok = c_typed.filter(~hard_c).dropDuplicates(["periodo", "id_producto"])
n_hard_rej_c = c_hard_rej.count()
log("Filas que violan reglas duras", "costos", "HARD",
    n_hard_rej_c, "FAIL" if n_hard_rej_c else "PASS")

c_hard_rej.write.format("delta").mode("overwrite").option("overwriteSchema", True) \
    .saveAsTable(f"{S}.costos_rejected")
c_ok.write.format("delta").mode("overwrite").option("overwriteSchema", True) \
    .saveAsTable(f"{S}.costos")
n_clean_c = c_ok.count()

# COMMAND ----------
# MAGIC %md ## Objetivos

# COMMAND ----------

o0 = spark.table(f"{B}.objetivos")
n_bronze_o = o0.count()
o_typed = o0.select(
    F.trim(F.col("Periodo")).alias("periodo"),
    F.to_date(F.concat(F.trim(F.col("Periodo")), F.lit("-01")), "yyyy-MM-dd").alias("periodo_date"),
    F.col("ID_Tienda").cast("int").alias("id_tienda"),
    F.col("Objetivo_Ventas").cast("decimal(14,2)").alias("objetivo_ventas"),
)
hard_o = (F.col("periodo").isNull() | (F.col("periodo") == "") |
          F.col("id_tienda").isNull() | F.col("objetivo_ventas").isNull() |
          (F.col("objetivo_ventas") < 0))
o_hard_rej = o_typed.filter(hard_o).withColumn("_reject_reason", F.lit("hard_rule_fail"))
o_ok = o_typed.filter(~hard_o).dropDuplicates(["periodo", "id_tienda"])
n_hard_rej_o = o_hard_rej.count()
log("Filas que violan reglas duras", "objetivos", "HARD",
    n_hard_rej_o, "FAIL" if n_hard_rej_o else "PASS")

o_hard_rej.write.format("delta").mode("overwrite").option("overwriteSchema", True) \
    .saveAsTable(f"{S}.objetivos_rejected")
o_ok.write.format("delta").mode("overwrite").option("overwriteSchema", True) \
    .saveAsTable(f"{S}.objetivos")
n_clean_o = o_ok.count()

# COMMAND ----------
# MAGIC %md ## Dimensiones conformadas (Spark SQL)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {S}.dim_producto AS
WITH freq AS (
  SELECT id_producto, nombre_producto, categoria_producto, subcategoria_producto, COUNT(*) AS n
  FROM ventas_enriquecida
  GROUP BY id_producto, nombre_producto, categoria_producto, subcategoria_producto
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY id_producto ORDER BY n DESC, nombre_producto) AS rn
  FROM freq
)
SELECT id_producto, nombre_producto, categoria_producto, subcategoria_producto
FROM ranked WHERE rn = 1
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {S}.dim_tienda AS
WITH freq AS (
  SELECT id_tienda, nombre_tienda, ubicacion_tienda, COUNT(*) AS n
  FROM ventas_enriquecida
  GROUP BY id_tienda, nombre_tienda, ubicacion_tienda
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY id_tienda ORDER BY n DESC, nombre_tienda) AS rn
  FROM freq
)
SELECT id_tienda, nombre_tienda, ubicacion_tienda
FROM ranked WHERE rn = 1
""")

n_dim_prod = spark.table(f"{S}.dim_producto").count()
n_dim_tienda = spark.table(f"{S}.dim_tienda").count()

# COMMAND ----------
# MAGIC %md ## Validaciones finales

# COMMAND ----------

# Unicidad de PK
def dup_pk(tabla, keys):
    df = spark.table(f"{S}.{tabla}")
    n = df.groupBy(*keys).count().filter("count > 1").count()
    log(f"Unicidad PK {keys}", tabla, "HARD", n, "PASS" if n == 0 else "FAIL")
    return n

dup_pk("ventas", ["id_venta"])
dup_pk("devoluciones", ["id_devolucion"])
dup_pk("precios", ["id_producto", "vigencia_desde"])
dup_pk("costos", ["periodo", "id_producto"])
dup_pk("objetivos", ["periodo", "id_tienda"])

# Nulos en claves de silver.ventas
n_null_keys = spark.table(f"{S}.ventas").filter(
    "id_venta IS NULL OR id_producto IS NULL OR id_tienda IS NULL OR fecha IS NULL").count()
log("Claves nulas en tabla limpia", "ventas", "HARD",
    n_null_keys, "PASS" if n_null_keys == 0 else "FAIL")

# Precios: solapamiento de rangos de vigencia por producto
n_overlap = spark.sql(f"""
SELECT COUNT(*) AS c FROM (
  SELECT id_producto, vigencia_desde,
         vigencia_hasta,
         LEAD(vigencia_desde) OVER (PARTITION BY id_producto ORDER BY vigencia_desde) AS next_desde
  FROM {S}.precios
) WHERE vigencia_hasta IS NOT NULL AND next_desde IS NOT NULL AND vigencia_hasta >= next_desde
""").collect()[0]["c"]
log("Precios: rangos de vigencia sin solapamiento", "precios", "HARD",
    n_overlap, "PASS" if n_overlap == 0 else "FAIL")

# Integridad referencial (warnings)
ri_dev = spark.sql(f"""
SELECT COUNT(*) AS c FROM {S}.devoluciones d
LEFT ANTI JOIN {S}.ventas v ON d.id_venta = v.id_venta
""").collect()[0]["c"]
log("Devoluciones sin venta asociada", "devoluciones", "WARN",
    ri_dev, "WARN" if ri_dev else "PASS")

ri_costo = spark.sql(f"""
SELECT COUNT(DISTINCT v.id_producto) AS c
FROM {S}.ventas v
LEFT ANTI JOIN {S}.costos c
  ON v.id_producto = c.id_producto AND date_format(v.fecha,'yyyy-MM') = c.periodo
""").collect()[0]["c"]
log("Productos-período vendidos sin costo (afecta margen)", "ventas", "WARN",
    ri_costo, "WARN" if ri_costo else "PASS")

# Objetivos: cobertura temporal vs ventas
obj_rango = spark.sql(f"SELECT MIN(periodo) mn, MAX(periodo) mx FROM {S}.objetivos").collect()[0]
log("Objetivos cubren solo parte del rango de ventas (2025)", "objetivos", "WARN",
    0, "WARN", f"objetivos {obj_rango['mn']}..{obj_rango['mx']}")

# COMMAND ----------
# MAGIC %md ## Persistir log de Data Quality y salir

# COMMAND ----------

dq_df = (spark.createDataFrame(DQ)
         .withColumn("ts", F.current_timestamp())
         .select("regla", "tabla", "severidad", "filas_afectadas", "estado", "detalle", "ts"))
dq_df.write.format("delta").mode("overwrite").option("overwriteSchema", True) \
    .saveAsTable(f"{S}.dq_resultados")

resumen = {
    "conteos": {
        "ventas":       {"bronze": n_bronze_v, "silver": n_clean_v, "rejected": n_hard_rej_v + n_conflict,
                         "dups_exactos": n_exact_removed, "conflictos": n_conflict,
                         "canal_reconstruido": n_canal_reconstruido},
        "devoluciones": {"bronze": n_bronze_d, "silver": n_clean_d, "rejected": n_hard_rej_d},
        "precios":      {"bronze": n_bronze_p, "silver": n_clean_p, "rejected": n_hard_rej_p},
        "costos":       {"bronze": n_bronze_c, "silver": n_clean_c, "rejected": n_hard_rej_c},
        "objetivos":    {"bronze": n_bronze_o, "silver": n_clean_o, "rejected": n_hard_rej_o},
        "dim_producto": n_dim_prod, "dim_tienda": n_dim_tienda,
    },
    "dq_fails": [d for d in DQ if d["estado"] == "FAIL"],
    "dq_warnings": [d for d in DQ if d["estado"] == "WARN"],
}
dbutils.notebook.exit(json.dumps(resumen))
