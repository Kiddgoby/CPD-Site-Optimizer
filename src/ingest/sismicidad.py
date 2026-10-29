"""
Ingesta de sismicidad: catálogo de terremotos (servicio FDSN del EMSC, que
integra los datos del IGN). Se usa para calcular el riesgo sísmico de cada
municipio (nº y magnitud máxima de terremotos en un radio).
"""
from __future__ import annotations

from config.settings import BBOX_PENINSULA, BBOX_CANARIAS
from src.ingest.common import http_get, save_raw, log

SOURCE = "sismicidad"
EMSC = "https://www.seismicportal.eu/fdsnws/event/1/query"
USGS = "https://earthquake.usgs.gov/fdsnws/event/1/query"


def run() -> None:
    for name, (s, w, n, e) in {"peninsula": BBOX_PENINSULA, "canarias": BBOX_CANARIAS}.items():
        params = {"starttime": "1990-01-01", "minmagnitude": 3.0, "minlatitude": s,
                  "maxlatitude": n, "minlongitude": w, "maxlongitude": e,
                  "format": "text", "limit": 20000}
        try:
            r = http_get(EMSC, params=params)
            save_raw(SOURCE, f"emsc_{name}.txt", r.text, EMSC)
        except Exception as ex:
            log.warning("EMSC falló (%s), uso USGS", ex)
            params["format"] = "csv"
            r = http_get(USGS, params=params)
            save_raw(SOURCE, f"usgs_{name}.csv", r.text, USGS)


if __name__ == "__main__":
    run()
