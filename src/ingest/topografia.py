"""
Ingesta de topografía: altitud y pendiente de cada municipio.

Usa la API pública de OpenTopoData con el modelo digital de elevaciones
europeo EU-DEM (25 m): https://www.opentopodata.org/datasets/eudem/
(Canarias no está cubierta por EU-DEM: para esos puntos se usa SRTM 30 m).

Para cada municipio se consulta la altitud en su centroide y en 4 puntos a
~1 km (N, S, E, O). La pendiente media se calcula después en la ETL.

Límites del servicio: 100 puntos por petición, 1 petición/s, 1.000/día.
Requiere haber descargado antes los municipios de OSM (src/ingest/osm.py).
El script es reanudable por lotes.
"""
from __future__ import annotations

import json
import time

from config.settings import RAW_DIR
from src.ingest.common import http_get, save_raw, raw_path, log

SOURCE = "topografia"
API = "https://api.opentopodata.org/v1/{dataset}"
OFFSET = 0.009  # ~1 km en latitud
BATCH = 100


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


def run() -> None:
    puntos = []  # (codigo_ine, posicion, lat, lon)
    for code, lat, lon in _municipios():
        for pos, (dlat, dlon) in {"c": (0, 0), "n": (OFFSET, 0), "s": (-OFFSET, 0),
                                  "e": (0, OFFSET), "o": (0, -OFFSET)}.items():
            puntos.append((code, pos, lat + dlat, lon + dlon))
    log.info("Topografía: %d puntos (%d lotes)", len(puntos), -(-len(puntos) // BATCH))

    for i in range(0, len(puntos), BATCH):
        lote = puntos[i:i + BATCH]
        name = f"lotes/{i // BATCH:04d}.json"
        if raw_path(SOURCE, name).exists():
            continue
        dataset = "srtm30m" if all(p[2] < 30 for p in lote) else "eudem25m,srtm30m"
        locs = "|".join(f"{p[2]:.5f},{p[3]:.5f}" for p in lote)
        try:
            res = http_get(API.format(dataset=dataset), params={"locations": locs}).json()
            filas = [{"ine": p[0], "pos": p[1], "lat": p[2], "lon": p[3],
                      "elevacion": r.get("elevation")}
                     for p, r in zip(lote, res.get("results", []))]
            save_raw(SOURCE, name, filas, API.format(dataset=dataset))
        except Exception as e:
            log.error("Lote %d: %s", i // BATCH, e)
        time.sleep(1.1)


if __name__ == "__main__":
    run()
