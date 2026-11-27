"""
Ingesta de hidrografía: ríos principales de España.

Fuente: Natural Earth (dominio público), a escala 1:10 millones:
    https://www.naturalearthdata.com/downloads/10m-physical-vectors/
Se bajan ne_10m_rivers_lake_centerlines y ne_10m_rivers_europe, y se recortan
a España. Los lagos de Natural Earth se descartaron: solo trae 6 en España y la
distancia mediana a ellos es de 200 km, no aporta. El agua "almacenada" que
importa en España son los embalses, que salen de OSM.

Limitación: a esta escala solo salen los ríos principales, no los arroyos. Sirve para estimar la cercanía al agua, no para un estudio
hidrológico. Se puede mejorar con la red hidrográfica del MITECO.

Genera data/bronze/hidrografia/rios.json (GeoJSON recortado).
"""
from __future__ import annotations

import json

from src.ingest.common import (cache_is_fresh, file_sha256, http_get, log, raw_path,
                               read_watermark, save_raw, write_watermark)

SOURCE = "hidrografia"
BASE = "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/"
RIOS = ["ne_10m_rivers_lake_centerlines.geojson", "ne_10m_rivers_europe.geojson"]
# Península + Baleares + Canarias + Ceuta y Melilla
LON_MIN, LON_MAX, LAT_MIN, LAT_MAX = -19.0, 5.5, 27.0, 44.5


def _valid_geojson(path) -> bool:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, UnicodeError):
        return False
    return (isinstance(payload, dict) and isinstance(payload.get("features"), list)
            and bool(payload["features"]))


def _coords(geom: dict):
    t = geom["type"]
    if t == "LineString":
        yield geom["coordinates"]
    elif t == "MultiLineString":
        yield from geom["coordinates"]
    elif t == "Polygon":
        yield geom["coordinates"][0]
    elif t == "MultiPolygon":
        for poly in geom["coordinates"]:
            yield poly[0]


def _en_espana(geom: dict) -> bool:
    return any(LON_MIN <= x <= LON_MAX and LAT_MIN <= y <= LAT_MAX
               for linea in _coords(geom) for x, y, *_ in linea)


def _bajar(ficheros: list[str], destino: str) -> None:
    path = raw_path(SOURCE, destino)
    key = f"{SOURCE}:{destino}"
    if cache_is_fresh(path, validator=_valid_geojson):
        if read_watermark(key) is None:
            write_watermark(key, f"snapshot-sha256:{file_sha256(path)}", file=destino)
        log.info("Hidrografía %s ya descargada y válida, se omite", destino)
        return
    feats = []
    for f in ficheros:
        gj = http_get(BASE + f).json()
        for ft in gj["features"]:
            if ft.get("geometry") and _en_espana(ft["geometry"]):
                p = ft.get("properties", {})
                feats.append({"geometry": ft["geometry"], "name": p.get("name") or p.get("name_es")})
        log.info("%s: %d elementos acumulados en España", f, len(feats))
    if not feats:
        raise ValueError(f"Natural Earth no devolvió geometrías para {destino}")
    path = save_raw(SOURCE, destino, {"fuente": "Natural Earth 1:10m", "features": feats},
                    BASE, validator=_valid_geojson)
    write_watermark(key, f"snapshot-sha256:{file_sha256(path)}", file=destino)


def run() -> None:
    _bajar(RIOS, "rios.json")
