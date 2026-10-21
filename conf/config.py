"""Configuración central del proyecto Nexo-Retail-Data-Engineering.

Un único punto para catálogo, schemas y rutas. Los notebooks importan de acá
en vez de hardcodear nombres. Pensado para Unity Catalog + Volume.

Uso en un notebook de Databricks:
    import sys; sys.path.append("/Workspace/.../Nexo-Retail-Data-Engineering")
    from conf.config import CATALOG, SCHEMAS, VOLUME_PATH, t
    spark.read... .saveAsTable(t("bronze", "ventas"))
"""

# --- Unity Catalog ---------------------------------------------------------
CATALOG = "nexo_retail"

SCHEMAS = {
    "bronze": "bronze",
    "silver": "silver",
    "gold": "gold",
}

# --- Almacenamiento de archivos crudos (Volume de UC) ----------------------
VOLUME = "landing"                       # catalog.bronze.landing
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMAS['bronze']}/{VOLUME}"

# --- Archivos fuente -------------------------------------------------------
# Cada fuente declara cómo se lee (los CSV NO son homogéneos).
SOURCES = {
    "ventas": {
        "file": "Ventas.csv",
        "sep": ",",
        "encoding": "utf-8",
        "decimal": ".",
        "date_format": "yyyy-MM-dd",      # 2024-01-01
    },
    "devoluciones": {
        "file": "Devoluciones.csv",
        "sep": ",",
        "encoding": "utf-8",
        "decimal": ".",
        "date_format": "dd/MM/yyyy",      # 04/01/2024
    },
    "precios": {
        "file": "Precios.csv",
        "sep": ",",
        "encoding": "utf-8",
        "decimal": ".",
        "date_format": "yyyy-MM-dd",
    },
    "costos": {
        "file": "Costos.csv",
        "sep": ";",                       # delimitador distinto
        "encoding": "utf-8",
        "decimal": ",",                   # 279,81  -> decimal con coma
        "date_format": "yyyy-MM",         # periodo mensual
    },
    "objetivos": {
        "file": "Objetivos.csv",
        "sep": ",",
        "encoding": "utf-8",
        "decimal": ".",
        "date_format": "yyyy-MM",         # periodo mensual
    },
}


def t(layer: str, name: str) -> str:
    """Nombre totalmente calificado de una tabla: catalog.schema.name."""
    return f"{CATALOG}.{SCHEMAS[layer]}.{name}"
