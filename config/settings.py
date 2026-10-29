"""
Configuración global del proyecto CPD-Site-Optimizer.

Centraliza rutas del Data Lake, rangos temporales y parámetros de las fuentes
para que todos los scripts usen los mismos valores.
"""
from pathlib import Path
from datetime import date

# --------------------------------------------------------------------------
# Rutas (Data Lake por capas: raw -> processed -> warehouse)
# --------------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"              # Capa bronze: datos tal cual llegan de la fuente
PROCESSED_DIR = DATA_DIR / "processed"  # Capa silver: datos limpios y normalizados
WAREHOUSE_DIR = DATA_DIR / "warehouse"  # Capa gold: Data Warehouse (DuckDB, esquema estrella)
MANUAL_DIR = RAW_DIR / "manual"         # Ficheros que hay que bajar a mano (ver docs/01_origen_datos.md)

# --------------------------------------------------------------------------
# Rangos temporales
# --------------------------------------------------------------------------
CURRENT_YEAR = date.today().year
CLIMATE_START = "2015-01-01"            # 10 años de clima para climatología estable
CLIMATE_END = f"{CURRENT_YEAR - 1}-12-31"
REE_START_YEAR = 2014                   # Serie de demanda eléctrica para forecasting
POPULATION_YEARS = [2015, 2020, CURRENT_YEAR - 1]  # Para calcular evolución demográfica

# --------------------------------------------------------------------------
# Geografía
# --------------------------------------------------------------------------
# Cajas (sur, oeste, norte, este) para España peninsular+Baleares y Canarias
BBOX_PENINSULA = (35.9, -9.5, 43.9, 4.4)
BBOX_CANARIAS = (27.5, -18.3, 29.5, -13.3)
CLIMATE_GRID_STEP = 0.5                 # Rejilla climática en grados (~50 km)

# Identificadores de comunidades autónomas en la API de Red Eléctrica (REData)
REE_CCAA_IDS = {
    "Andalucía": 4, "Aragón": 5, "Cantabria": 6, "Castilla-La Mancha": 7,
    "Castilla y León": 8, "Cataluña": 9, "País Vasco": 10,
    "Principado de Asturias": 11, "Comunidad de Madrid": 13,
    "Comunidad Foral de Navarra": 14, "Comunitat Valenciana": 15,
    "Extremadura": 16, "Galicia": 17, "La Rioja": 20, "Región de Murcia": 21,
    "Illes Balears": 8743, "Canarias": 8742, "Ceuta": 8744, "Melilla": 8745,
}

# --------------------------------------------------------------------------
# Red
# --------------------------------------------------------------------------
HTTP_TIMEOUT = 180
HTTP_RETRIES = 4
USER_AGENT = "CPD-Site-Optimizer/0.1 (proyecto academico Stucom)"
OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]
