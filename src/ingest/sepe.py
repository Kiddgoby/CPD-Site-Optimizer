"""
Ingesta SEPE: paro registrado mensual por municipio (datos abiertos).
Catálogo: https://datos.gob.es/es/catalogo/ea0041513-paro-registrado-por-municipio
"""
from __future__ import annotations

from config.settings import CURRENT_YEAR
from src.ingest.common import http_get, save_raw, log

SOURCE = "sepe"
URL = "https://sede.sepe.gob.es/es/portaltrabaja/resources/sede/datos_abiertos/datos/Paro_por_municipios_{y}_csv.csv"


def run() -> None:
    # Último año completo publicado + el anterior (para ver tendencia)
    got = 0
    for y in range(CURRENT_YEAR, CURRENT_YEAR - 4, -1):
        url = URL.format(y=y)
        try:
            r = http_get(url, retries=1)
            save_raw(SOURCE, f"paro_municipios_{y}.csv", r.content, url)
            got += 1
        except Exception:
            log.info("Paro %d no disponible", y)
        if got == 2:
            break


if __name__ == "__main__":
    run()
