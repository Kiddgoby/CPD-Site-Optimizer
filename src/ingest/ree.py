"""
Ingesta Red Eléctrica de España (REData API, sin clave).
Docs: https://www.ree.es/es/apidatos

- Demanda eléctrica nacional mensual desde REE_START_YEAR -> serie temporal
  para predecir la demanda a 10 años.
- Demanda anual y estructura de generación (renovable / no renovable) por
  comunidad autónoma -> disponibilidad energética y % renovable territorial.
"""
import time

from config.settings import REE_START_YEAR, CURRENT_YEAR, REE_CCAA_IDS
from src.ingest.common import http_get, save_raw, log

SOURCE = "ree"
BASE = "https://apidatos.ree.es/es/datos"
HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}


def _get(category: str, widget: str, start: str, end: str, trunc: str, geo: dict | None = None):
    params = {"start_date": start, "end_date": end, "time_trunc": trunc}
    if geo:
        params.update(geo)
    return http_get(f"{BASE}/{category}/{widget}", params=params, headers=HEADERS).json()


def download_demanda_nacional() -> None:
    """La API limita el rango por petición, así que se pide año a año."""
    for y in range(REE_START_YEAR, CURRENT_YEAR + 1):
        end = f"{y}-12-31T23:59"
        try:
            data = _get("demanda", "evolucion", f"{y}-01-01T00:00", end, "month")
            save_raw(SOURCE, f"demanda_nacional/{y}.json", data, f"{BASE}/demanda/evolucion")
        except Exception as e:
            log.error("Demanda nacional %d: %s", y, e)
        time.sleep(1)


def download_ccaa() -> None:
    """Últimos 5 años, agregados anuales, por comunidad autónoma."""
    start = f"{CURRENT_YEAR - 5}-01-01T00:00"
    end = f"{CURRENT_YEAR - 1}-12-31T23:59"
    for name, gid in REE_CCAA_IDS.items():
        geo = {"geo_trunc": "electric_system", "geo_limit": "ccaa", "geo_ids": gid}
        for cat, widget in [("demanda", "evolucion"),
                            ("generacion", "evolucion-renovable-no-renovable"),
                            ("generacion", "estructura-generacion")]:
            try:
                data = _get(cat, widget, start, end, "year", geo)
                save_raw(SOURCE, f"ccaa/{gid}_{widget}.json",
                         {"ccaa": name, "geo_id": gid, "data": data}, f"{BASE}/{cat}/{widget}")
            except Exception as e:
                log.error("REE %s %s: %s", name, widget, e)
            time.sleep(1)


def run() -> None:
    download_demanda_nacional()
    download_ccaa()


if __name__ == "__main__":
    run()
