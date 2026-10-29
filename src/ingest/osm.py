"""
Ingesta OpenStreetMap vía Overpass API (sin clave).

Se usa OSM para todo lo que necesita coordenadas:
- Municipios: centroide + código INE (etiqueta ine:municipio) -> clave para
  unir todas las fuentes.
- CPDs existentes (telecom/building=data_center) -> etiqueta para el modelo.
- Subestaciones eléctricas (con tensión) -> capacidad de la red.
- Aeropuertos, puertos, salidas de autopista -> transporte y logística.
- Embalses -> disponibilidad hídrica.
- Polígonos industriales -> proxy de suelo industrial disponible.
- Espacios naturales protegidos -> riesgo regulatorio / ambiental.
"""
import time

from config.settings import OVERPASS_URLS
from src.ingest.common import http_get, save_raw, raw_path, log

SOURCE = "osm"
AREA = 'area["ISO3166-1"="ES"][admin_level=2]->.es;'

QUERIES = {
    "municipios": 'relation["boundary"="administrative"]["admin_level"="8"]["ine:municipio"](area.es);out center tags;',
    "datacenters": '(nwr["telecom"="data_center"](area.es);nwr["building"="data_center"](area.es););out center tags;',
    "subestaciones": 'nwr["power"="substation"](area.es);out center tags;',
    "centrales": 'nwr["power"="plant"](area.es);out center tags;',
    "aeropuertos": 'nwr["aeroway"="aerodrome"]["iata"](area.es);out center tags;',
    "puertos": '(nwr["landuse"="port"](area.es);nwr["industrial"="port"](area.es););out center tags;',
    "autopistas": 'node["highway"="motorway_junction"](area.es);out;',
    "embalses": 'nwr["water"="reservoir"]["name"](area.es);out center tags;',
    "industrial": 'way["landuse"="industrial"](area.es);out center;',
    # Espacios protegidos (Red Natura 2000, parques naturales...) -> riesgo regulatorio
    "protegidos": '(nwr["boundary"="protected_area"](area.es);nwr["leisure"="nature_reserve"](area.es);nwr["boundary"="national_park"](area.es););out center tags;',
}


def overpass(query_body: str) -> dict:
    q = f"[out:json][timeout:900];{AREA}{query_body}"
    last = None
    for url in OVERPASS_URLS:  # si un servidor falla, se prueba el espejo
        try:
            return http_get(url, method="POST", data={"data": q}, retries=2).json()
        except Exception as e:
            last = e
            log.warning("Overpass %s falló: %s", url, e)
    raise RuntimeError(last)


def run(only: list[str] | None = None) -> None:
    for name, body in QUERIES.items():
        if only and name not in only:
            continue
        if raw_path(SOURCE, f"{name}.json").exists():
            log.info("OSM %s ya descargado, se omite", name)
            continue
        log.info("OSM: descargando %s ...", name)
        try:
            data = overpass(body)
            save_raw(SOURCE, f"{name}.json", data, "overpass")
            log.info("OSM %s: %d elementos", name, len(data.get("elements", [])))
        except Exception as e:
            log.error("OSM %s fallido: %s", name, e)
        time.sleep(10)  # respetar el servidor público


if __name__ == "__main__":
    run()
