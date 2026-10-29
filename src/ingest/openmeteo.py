"""
Ingesta de clima con Open-Meteo (API histórica, gratuita y SIN clave).
https://open-meteo.com/en/docs/historical-weather-api

Alternativa a AEMET cuando no hay API key. Se descarga un año completo de
datos diarios en una rejilla de puntos sobre España (~80 km). Después, en la
ETL, cada municipio toma el clima del punto de rejilla más cercano.

Variables diarias: temperatura media y máxima, humedad relativa, viento,
precipitación y evapotranspiración (ET0) -> clima y estrés hídrico.

Límites del plan gratuito: 5.000 llamadas/hora y 10.000/día (1 año de datos
diarios de un punto cuenta como ~26 llamadas). El script es reanudable.
"""
import json
import time

import numpy as np

from config.settings import RAW_DIR, CURRENT_YEAR, BBOX_PENINSULA, BBOX_CANARIAS
from src.ingest.common import http_get, save_raw, raw_path, log

SOURCE = "openmeteo"
API = "https://archive-api.open-meteo.com/v1/archive"
STEP = 0.75  # grados
DAILY = ["temperature_2m_mean", "temperature_2m_max", "relative_humidity_2m_mean",
         "wind_speed_10m_mean", "precipitation_sum", "et0_fao_evapotranspiration"]


def _grid() -> list[tuple[float, float]]:
    pts = []
    for s, w, n, e in (BBOX_PENINSULA, BBOX_CANARIAS):
        for lat in np.arange(s, n + 1e-9, STEP):
            for lon in np.arange(w, e + 1e-9, STEP):
                pts.append((round(float(lat), 3), round(float(lon), 3)))
    # Si ya tenemos los municipios de OSM, quitamos los puntos en el mar
    muni = RAW_DIR / "osm" / "municipios.json"
    if muni.exists():
        cs = np.array([(el["center"]["lat"], el["center"]["lon"])
                       for el in json.loads(muni.read_text(encoding="utf-8"))["elements"]
                       if "center" in el])
        pts = [p for p in pts if np.min(np.hypot(cs[:, 0] - p[0], cs[:, 1] - p[1])) < STEP * 0.75]
    return pts


def run() -> None:
    year = CURRENT_YEAR - 1
    pts = _grid()
    log.info("Open-Meteo: %d puntos de rejilla, año %d", len(pts), year)
    for i, (lat, lon) in enumerate(pts, 1):
        name = f"{year}/{lat:.3f}_{lon:.3f}.json"
        if raw_path(SOURCE, name).exists():
            continue
        params = {"latitude": lat, "longitude": lon, "start_date": f"{year}-01-01",
                  "end_date": f"{year}-12-31", "daily": ",".join(DAILY), "timezone": "Europe/Madrid"}
        try:
            save_raw(SOURCE, name, http_get(API, params=params).json(), API)
        except Exception as e:
            log.error("Punto %s,%s: %s", lat, lon, e)
        if i % 25 == 0:
            log.info("Open-Meteo: %d/%d", i, len(pts))
        time.sleep(1.5)


if __name__ == "__main__":
    run()
