"""
Ingesta AEMET OpenData (requiere API key gratuita).

Pedir la clave: https://opendata.aemet.es/centrodedescargas/altaUsuario
y guardarla en un fichero .env en la raíz del repo:
    AEMET_API_KEY=eyJhbGciOi...

Se descarga:
- Inventario de estaciones (con coordenadas).
- Valores climatológicos mensuales/anuales de cada estación (3 últimos años):
  temperatura media (tm_mes), máxima absoluta (ta_max), humedad (hr),
  viento medio (w_med), precipitación (p_mes), días con Tmax >= 30 ºC (nt_30)...

La API de AEMET funciona en dos pasos: la primera llamada devuelve una URL
temporal en el campo "datos" y la segunda descarga el contenido real.
El script es reanudable: si se corta, al relanzarlo continúa donde lo dejó.
"""
import os
import time
from pathlib import Path

from config.settings import ROOT_DIR, CURRENT_YEAR
from src.ingest.common import http_get, save_raw, raw_path, log

SOURCE = "aemet"
API = "https://opendata.aemet.es/opendata/api"


def _api_key() -> str:
    key = os.getenv("AEMET_API_KEY")
    env = Path(ROOT_DIR) / ".env"
    if not key and env.exists():
        for line in env.read_text(encoding="utf-8").splitlines():
            if line.startswith("AEMET_API_KEY="):
                key = line.split("=", 1)[1].strip()
    if not key:
        raise RuntimeError("Falta AEMET_API_KEY (ver cabecera de src/ingest/aemet.py)")
    return key


def _fetch(endpoint: str, key: str):
    meta = http_get(f"{API}{endpoint}", params={"api_key": key}).json()
    if meta.get("estado") != 200:
        raise RuntimeError(meta.get("descripcion", "error AEMET"))
    time.sleep(1.2)  # límite de peticiones de AEMET
    r = http_get(meta["datos"])
    r.encoding = "latin-1"  # AEMET sirve los datos en ISO-8859-15
    return r.json()


def run() -> None:
    try:
        key = _api_key()
    except RuntimeError:
        log.warning("Sin AEMET_API_KEY: se omite AEMET (el clima sale de Open-Meteo)")
        return
    inv_path = raw_path(SOURCE, "estaciones.json")
    if not inv_path.exists():
        est = _fetch("/valores/climatologicos/inventarioestaciones/todasestaciones", key)
        save_raw(SOURCE, "estaciones.json", est, "inventarioestaciones")
    import json
    estaciones = json.loads(inv_path.read_text(encoding="utf-8"))
    log.info("AEMET: %d estaciones", len(estaciones))

    y0, y1 = CURRENT_YEAR - 3, CURRENT_YEAR - 1  # máx. 36 meses por petición
    for i, e in enumerate(estaciones, 1):
        idema = e["indicativo"]
        if raw_path(SOURCE, f"mensual/{idema}.json").exists():
            continue
        ep = f"/valores/climatologicos/mensualesanuales/datos/anioini/{y0}/aniofin/{y1}/estacion/{idema}"
        try:
            data = _fetch(ep, key)
            save_raw(SOURCE, f"mensual/{idema}.json", data, ep)
        except Exception as ex:
            # Muchas estaciones no tienen climatología mensual: se marca para no reintentar
            save_raw(SOURCE, f"mensual/{idema}.json", [], f"ERROR: {ex}")
        if i % 50 == 0:
            log.info("AEMET: %d/%d estaciones", i, len(estaciones))
        time.sleep(1.2)


if __name__ == "__main__":
    run()
