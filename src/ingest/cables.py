"""
Ingesta de cables submarinos (TeleGeography, Submarine Cable Map, sin clave).
https://www.submarinecablemap.com

Para cada punto de amarre (landing point) en España se guardan sus
coordenadas y la lista de cables que llegan (nombre, año, si es planificado,
países que conecta). En la ETL se calcula para cada municipio la distancia al
amarre más cercano y cuántos cables internacionales llegan a él: es lo que
hace atractivos para CPD a Bilbao, Barcelona, Valencia, Málaga o Canarias.
"""
from __future__ import annotations

import time

from src.ingest.common import http_get, save_raw, log

SOURCE = "cables"
API = "https://www.submarinecablemap.com/api/v3"


def run() -> None:
    geo = http_get(f"{API}/landing-point/landing-point-geo.json").json()
    es = [f for f in geo["features"] if f["properties"].get("name", "").endswith(", Spain")]
    log.info("Cables: %d puntos de amarre en España", len(es))
    puntos = []
    for f in es:
        pid = f["properties"]["id"]
        lon, lat = f["geometry"]["coordinates"]
        try:
            det = http_get(f"{API}/landing-point/{pid}.json").json()
        except Exception as e:
            log.warning("Amarre %s sin detalle: %s", pid, e)
            det = {}
        cables = det.get("cables", [])
        puntos.append({
            "id": pid,
            "nombre": f["properties"]["name"].replace(", Spain", ""),
            "lat": lat, "lon": lon,
            "n_cables": len(cables),
            "n_cables_operativos": sum(1 for c in cables if not c.get("is_planned")),
            # cable internacional = toca algún amarre fuera de España
            "n_cables_internacionales": sum(
                1 for c in cables
                if any(not lp.endswith("-spain") for lp in c.get("landing_points", []))),
            "cables": [{"id": c.get("id"), "nombre": c.get("name"), "rfs": c.get("rfs_year"),
                        "planificado": c.get("is_planned")} for c in cables],
        })
        time.sleep(0.3)
    save_raw(SOURCE, "amarres_es.json", puntos, f"{API}/landing-point/")


if __name__ == "__main__":
    run()
