# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — Ingesta raw
# MAGIC
# MAGIC Lee los 5 CSV desde el Volume `nexo_retail.bronze.landing` y los materializa como
# MAGIC tablas Delta en el schema `bronze`, **1:1 con la fuente**:
# MAGIC
# MAGIC - Todas las columnas como **string** (`inferSchema=False`) → no se pierde ningún dato por un cast.
# MAGIC - **No** se limpia, castea ni estandariza nada (eso es responsabilidad de Silver).
# MAGIC - Cada fuente se lee con su delimitador propio (Costos usa `;`).
# MAGIC - Se agrega metadata de ingesta: `_source_file`, `_ingested_at`, `_ingest_id`.
# MAGIC
# MAGIC > Las specs replican `conf/config.py` (acá inline para que el notebook corra standalone).

# COMMAND ----------

from pyspark.sql import functions as F
import uuid, json

CATALOG = "nexo_retail"
BRONZE = "bronze"
VOLUME_PATH = f"/Volumes/{CATALOG}/{BRONZE}/landing"

# nombre_tabla -> (archivo, delimitador)
SOURCES = {
    "ventas":       {"file": "Ventas.csv",       "sep": ","},
    "devoluciones": {"file": "Devoluciones.csv", "sep": ","},
    "precios":      {"file": "Precios.csv",       "sep": ","},
    "costos":       {"file": "Costos.csv",        "sep": ";"},
    "objetivos":    {"file": "Objetivos.csv",     "sep": ","},
}

# COMMAND ----------

def ingest_bronze(name: str, spec: dict) -> dict:
    """Lee un CSV como todo-string + metadata y lo escribe como tabla Delta bronze."""
    path = f"{VOLUME_PATH}/{spec['file']}"

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)   # raw: todas las columnas como string
        .option("sep", spec["sep"])
        .option("encoding", "UTF-8")
        .option("multiLine", False)
        .option("quote", '"')
        .option("escape", '"')
        .csv(path)
    )

    ingest_id = str(uuid.uuid4())
    df = (
        df.withColumn("_source_file", F.lit(spec["file"]))
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_ingest_id", F.lit(ingest_id))
    )

    target = f"{CATALOG}.{BRONZE}.{name}"
    (
        df.write.format("delta")
          .mode("overwrite")               # recarga completa idempotente (fuente estática)
          .option("overwriteSchema", True)
          .saveAsTable(target)
    )
    return {"tabla": target, "filas": df.count(), "columnas": len(df.columns)}

# COMMAND ----------

resultados = [ingest_bronze(name, spec) for name, spec in SOURCES.items()]
for r in resultados:
    print(f"{r['tabla']:<28} filas={r['filas']:>8}  columnas={r['columnas']}")

# COMMAND ----------

dbutils.notebook.exit(json.dumps(resultados))
