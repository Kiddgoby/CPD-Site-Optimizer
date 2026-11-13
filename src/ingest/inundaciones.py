"""
Ingesta de zonas inundables (SNCZI, MITECO) vía el servicio WMS INSPIRE de
Riesgos Naturales del IGN/IDEE (sin clave):
https://servicios.idee.es/wms-inspire/riesgos-naturales/inundaciones

Capas: zonas inundables fluviales y marinas para periodos de retorno de
100 años (probabilidad media) y 500 años (probabilidad baja).

Como el servicio solo da imágenes (WMS), para cada municipio se pide un mapa
pequeño (~4 x 4 km) centrado en su centroide y se calcula el % de píxeles
pintados = % de superficie inundable alrededor del núcleo urbano. Es un
criterio casi eliminatorio para un CPD.

Limitación: el SNCZI solo cartografía las ARPSIs (tramos con riesgo
significativo). Un 0 % significa "sin zona inundable cartografiada", no
"riesgo nulo".

Requiere haber descargado antes los municipios de OSM. Es reanudable por
lotes (~8.100 municipios x 2 mapas, unos 30-60 min).
"""
from __future__ import annotations

import io
import json
import math
from concurrent.futures import ThreadPoolExecutor

from PIL import Image

from config.settings import RAW_DIR
from src.ingest.common import http_get, save_raw, raw_path, log

SOURCE = "inundaciones"
WMS = "https://servicios.idee.es/wms-inspire/riesgos-naturales/inundaciones"
CAPAS = {
    "t100": "NZ.Flood.FluvialT100,NZ.Flood.MarinaT100",
    "t500": "NZ.Flood.FluvialT500,NZ.Flood.MarinaT500",
}
RADIO_KM = 2.0
PX = 100
BATCH = 250
WORKERS = 2


def _municipios() -> list[tuple[str, float, float]]:
    path = RAW_DIR / "osm" / "municipios.json"
    if not path.exists():
        raise RuntimeError("Primero hay que ejecutar la ingesta de OSM (municipios)")
    out = []
    for el in json.loads(path.read_text(encoding="utf-8"))["elements"]:
        c = el.get("center")
        code = el.get("tags", {}).get("ine:municipio")
        if c and code:
            out.append((code, c["lat"], c["lon"]))
    return out


def pct_inundable(lat: float, lon: float, capas: str) -> float | None:
    """% de píxeles con zona inundable en un cuadrado de 2*RADIO_KM de lado."""
    dlat = RADIO_KM / 111.0
    dlon = RADIO_KM / (111.0 * math.cos(math.radians(lat)))
    params = {
        "service": "WMS", "version": "1.3.0", "request": "GetMap", "layers": capas,
        "styles": "," * (capas.count(",")), "crs": "EPSG:4326",
        # en WMS 1.3.0 con EPSG:4326 el orden de ejes es lat,lon
        "bbox": f"{lat - dlat},{lon - dlon},{lat + dlat},{lon + dlon}",
        "width": PX, "height": PX, "format": "image/png", "transparent": "true",
    }
    try:
        r = http_get(WMS, params=params, retries=4)  # el servidor da 503 a ratos
        im = Image.open(io.BytesIO(r.content)).convert("RGBA")
        alpha = im.getchannel("A").histogram()
        return round(100 * (1 - alpha[0] / (PX * PX)), 2)
    except Exception as e:
        log.warning("WMS inundaciones (%.4f, %.4f): %s", lat, lon, e)
        return None


def _fila(m: tuple[str, float, float]) -> dict:
    code, lat, lon = m
    fila = {"ine": code, "lat": lat, "lon": lon}
    for k, capas in CAPAS.items():
        fila[f"pct_inundable_{k}"] = pct_inundable(lat, lon, capas)
    return fila


def _reparar() -> None:
    """Segunda pasada: el WMS da errores 503 a ratos, así que se vuelven a pedir
    los valores que quedaron vacíos (None) en los lotes ya guardados."""
    for f in sorted((RAW_DIR / SOURCE / "lotes").glob("*.json")):
        filas = json.loads(f.read_text(encoding="utf-8"))
        huecos = [x for x in filas for k in CAPAS if x.get(f"pct_inundable_{k}") is None]
        if not huecos:
            continue
        for x in filas:
            for k, capas in CAPAS.items():
                if x.get(f"pct_inundable_{k}") is None:
                    x[f"pct_inundable_{k}"] = pct_inundable(x["lat"], x["lon"], capas)
        save_raw(SOURCE, f"lotes/{f.name}", filas, WMS)


def run() -> None:
    muni = _municipios()
    log.info("Inundaciones: %d municipios (%d lotes)", len(muni), -(-len(muni) // BATCH))
    for i in range(0, len(muni), BATCH):
        name = f"lotes/{i // BATCH:03d}.json"
        if raw_path(SOURCE, name).exists():
            continue
        with ThreadPoolExecutor(WORKERS) as ex:
            filas = list(ex.map(_fila, muni[i:i + BATCH]))
        save_raw(SOURCE, name, filas, WMS)
    _reparar()


if __name__ == "__main__":
    run()
