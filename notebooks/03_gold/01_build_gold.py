# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — Modelo estrella (consumo analítico)
# MAGIC
# MAGIC Orquesta la construcción de Gold **separando DDL de ETL**:
# MAGIC 1. Ejecuta `sql/ddl/gold_tables.sql` (contratos de esquema).
# MAGIC 2. Ejecuta `sql/etl/gold_load.sql` (poblado desde Silver).
# MAGIC 3. Corre las validaciones obligatorias.
# MAGIC
# MAGIC Los `.sql` viven en el repo y se despliegan al Volume; el notebook solo orquesta
# MAGIC (PySpark) y los ejecuta como Spark SQL. Métricas = nivel transaccional
# MAGIC (los agregados son responsabilidad de la capa semántica de Power BI).

# COMMAND ----------

from pyspark.sql import functions as F
import json, re

CATALOG = "nexo_retail"
SQL_DIR = f"/Volumes/{CATALOG}/bronze/landing/scripts"   # DDL/ETL desplegados


def load_statements(path):
    """Lee un .sql, elimina comentarios '--' y separa en sentencias por ';'.

    Se quitan los comentarios ANTES de separar para que un ';' dentro de un
    comentario no rompa el parseo.
    """
    raw = spark.read.text(path, wholetext=True).collect()[0]["value"]
    no_comments = re.sub(r"--[^\n]*", "", raw)       # quita comentarios de línea
    return [s.strip() for s in no_comments.split(";") if s.strip()]


def run_file(path):
    stmts = load_statements(path)
    for i, s in enumerate(stmts, 1):
        print(f"  [{i}/{len(stmts)}] {s.splitlines()[0][:70]}")
        spark.sql(s)
    return len(stmts)

# COMMAND ----------
# MAGIC %md ## 1) DDL

# COMMAND ----------

n_ddl = run_file(f"{SQL_DIR}/gold_tables.sql")
print(f"DDL: {n_ddl} sentencias ejecutadas")

# COMMAND ----------
# MAGIC %md ## 2) ETL

# COMMAND ----------

n_etl = run_file(f"{SQL_DIR}/gold_load.sql")
print(f"ETL: {n_etl} sentencias ejecutadas")

# COMMAND ----------
# MAGIC %md ## 3) Validaciones obligatorias

# COMMAND ----------

G = f"{CATALOG}.gold"
S = f"{CATALOG}.silver"
val = {}
def one(sql):
    return spark.sql(sql).collect()[0][0]

# 1) Row counts
val["row_counts"] = {
    t: spark.table(f"{G}.{t}").count()
    for t in ["dim_fecha", "dim_producto", "dim_tienda",
              "fact_ventas", "fact_devoluciones", "fact_objetivos"]
}

# 2) Grain / uniqueness de cada fact
val["grain_duplicados"] = {
    "fact_ventas_id_venta":   one(f"SELECT COUNT(*) FROM (SELECT id_venta FROM {G}.fact_ventas GROUP BY id_venta HAVING COUNT(*)>1)"),
    "fact_ventas_venta_key":  one(f"SELECT COUNT(*) FROM (SELECT venta_key FROM {G}.fact_ventas GROUP BY venta_key HAVING COUNT(*)>1)"),
    "fact_dev_id_devolucion": one(f"SELECT COUNT(*) FROM (SELECT id_devolucion FROM {G}.fact_devoluciones GROUP BY id_devolucion HAVING COUNT(*)>1)"),
    "fact_obj_mes_tienda":    one(f"SELECT COUNT(*) FROM (SELECT fecha_key,tienda_key FROM {G}.fact_objetivos GROUP BY fecha_key,tienda_key HAVING COUNT(*)>1)"),
}

# 3) Nulls en claves (fecha_venta_key puede ser NULL solo en huérfanas)
val["nulls_en_claves"] = {
    "fv_fecha_key":    one(f"SELECT COUNT(*) FROM {G}.fact_ventas WHERE fecha_key IS NULL"),
    "fv_producto_key": one(f"SELECT COUNT(*) FROM {G}.fact_ventas WHERE producto_key IS NULL"),
    "fv_tienda_key":   one(f"SELECT COUNT(*) FROM {G}.fact_ventas WHERE tienda_key IS NULL"),
    "fd_producto_key": one(f"SELECT COUNT(*) FROM {G}.fact_devoluciones WHERE producto_key IS NULL"),
    "fd_tienda_key":   one(f"SELECT COUNT(*) FROM {G}.fact_devoluciones WHERE tienda_key IS NULL"),
    "fd_fdev_key":     one(f"SELECT COUNT(*) FROM {G}.fact_devoluciones WHERE fecha_devolucion_key IS NULL"),
    "fo_fecha_key":    one(f"SELECT COUNT(*) FROM {G}.fact_objetivos WHERE fecha_key IS NULL"),
    "fo_tienda_key":   one(f"SELECT COUNT(*) FROM {G}.fact_objetivos WHERE tienda_key IS NULL"),
}

# 4) + 5) Orphans / integridad referencial fact -> dim
val["orphans_fact_dim"] = {
    "fv_fecha":    one(f"SELECT COUNT(*) FROM {G}.fact_ventas f LEFT ANTI JOIN {G}.dim_fecha d ON f.fecha_key=d.fecha_key"),
    "fv_producto": one(f"SELECT COUNT(*) FROM {G}.fact_ventas f LEFT ANTI JOIN {G}.dim_producto d ON f.producto_key=d.producto_key"),
    "fv_tienda":   one(f"SELECT COUNT(*) FROM {G}.fact_ventas f LEFT ANTI JOIN {G}.dim_tienda d ON f.tienda_key=d.tienda_key"),
    "fd_fdev":     one(f"SELECT COUNT(*) FROM {G}.fact_devoluciones f LEFT ANTI JOIN {G}.dim_fecha d ON f.fecha_devolucion_key=d.fecha_key"),
    "fd_fventa":   one(f"SELECT COUNT(*) FROM {G}.fact_devoluciones f WHERE f.fecha_venta_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {G}.dim_fecha d WHERE d.fecha_key=f.fecha_venta_key)"),
    "fd_producto": one(f"SELECT COUNT(*) FROM {G}.fact_devoluciones f LEFT ANTI JOIN {G}.dim_producto d ON f.producto_key=d.producto_key"),
    "fd_tienda":   one(f"SELECT COUNT(*) FROM {G}.fact_devoluciones f LEFT ANTI JOIN {G}.dim_tienda d ON f.tienda_key=d.tienda_key"),
    "fo_fecha":    one(f"SELECT COUNT(*) FROM {G}.fact_objetivos f LEFT ANTI JOIN {G}.dim_fecha d ON f.fecha_key=d.fecha_key"),
    "fo_tienda":   one(f"SELECT COUNT(*) FROM {G}.fact_objetivos f LEFT ANTI JOIN {G}.dim_tienda d ON f.tienda_key=d.tienda_key"),
}

# 6/7/8) Reconciliación con Silver
val["reconciliacion"] = {
    "ventas":       {"gold": val["row_counts"]["fact_ventas"],       "silver": spark.table(f"{S}.ventas").count()},
    "devoluciones": {"gold": val["row_counts"]["fact_devoluciones"], "silver": spark.table(f"{S}.devoluciones").count()},
    "objetivos":    {"gold": val["row_counts"]["fact_objetivos"],    "silver": spark.table(f"{S}.objetivos").count()},
}

# 9) 12 huérfanas presentes · 10) 8 excesivas marcadas
val["huerfanas"] = one(f"SELECT SUM(es_venta_huerfana) FROM {G}.fact_devoluciones")
val["excesivas"] = one(f"SELECT SUM(es_devolucion_excesiva) FROM {G}.fact_devoluciones")

# 11) Sin objetivos para 2024
val["objetivos_2024"] = one(f"SELECT COUNT(*) FROM {G}.fact_objetivos WHERE fecha_key < 20250101")
val["objetivos_rango_anios"] = [r[0] for r in spark.sql(
    f"SELECT DISTINCT substr(CAST(fecha_key AS STRING),1,4) a FROM {G}.fact_objetivos ORDER BY a").collect()]

# 12) Point-in-time: ejemplos concretos (producto 1, que cambia de precio 3 veces)
val["pit_ejemplos"] = [r.asDict() for r in spark.sql(f"""
    SELECT v.id_venta, dp.id_producto, dfe.fecha, v.precio_unitario, v.precio_lista, v.descuento_porcentaje
    FROM {G}.fact_ventas v
    JOIN {G}.dim_producto dp ON v.producto_key=dp.producto_key
    JOIN {G}.dim_fecha   dfe ON v.fecha_key=dfe.fecha_key
    WHERE dp.id_producto = 1
    ORDER BY dfe.fecha
    LIMIT 5
""").collect()]

# 13) Margen: productos vendidos sin costo (costo_unitario NULL)
val["ventas_sin_costo"] = one(f"SELECT COUNT(*) FROM {G}.fact_ventas WHERE costo_unitario IS NULL")
val["ventas_sin_precio_lista"] = one(f"SELECT COUNT(*) FROM {G}.fact_ventas WHERE precio_lista IS NULL")

# COMMAND ----------

print(json.dumps(val, indent=2, ensure_ascii=False, default=str))
dbutils.notebook.exit(json.dumps(val, default=str))
