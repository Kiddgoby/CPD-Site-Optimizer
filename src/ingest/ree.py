"""
Ingesta Red Eléctrica de España (REData API, sin clave).
Docs: https://www.ree.es/es/apidatos

- Demanda eléctrica nacional mensual desde REE_START_YEAR -> serie temporal
  para predecir la demanda a 10 años.
- Demanda anual y estructura de generación (renovable / no renovable) por
  comunidad autónoma -> disponibilidad energética y % renovable territorial.
- Precio horario del mercado eléctrico -> coste operativo del CPD.
"""
from __future__ import annotations

import json
import time
import calendar
from datetime import date

from config.settings import REE_START_YEAR, CURRENT_YEAR, REE_CCAA_IDS
from src.ingest.common import (cache_is_fresh, file_sha256, http_get, log, raw_path,
                               save_raw, write_watermark, read_watermark)

SOURCE = "ree"
BASE = "https://apidatos.ree.es/es/datos"
HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}


def _download_json(dest: str, url: str, params: dict, watermark_key: str,
                   position: str, max_age_seconds: float | None,
                   wrapper=None) -> None:
    path = raw_path(SOURCE, dest)
    watermark_source = f"{SOURCE}:{watermark_key}"
    watermark = read_watermark(watermark_source)
    if cache_is_fresh(path, max_age_seconds,
                      validator=lambda p: isinstance(json.loads(p.read_text(encoding="utf-8")), dict)):
        if not path.stat().st_size:
            raise ValueError(f"REData cache is empty: {path}")
        if watermark is None:
            write_watermark(watermark_source,
                            f"snapshot-sha256:{file_sha256(path)}", file=dest)
        return
    data = http_get(url, params=params, headers=HEADERS).json()
    if not isinstance(data, dict):
        raise ValueError(f"REData response must be a JSON object: {url}")
    if wrapper is not None:
        data = wrapper(data)
    path = save_raw(SOURCE, dest, data, url)
    write_watermark(watermark_source, position,
                    file=dest, snapshot_sha256=file_sha256(path))


def download_demanda_nacional() -> None:
    """La API limita el rango por petición, así que se pide año a año."""
    for y in range(REE_START_YEAR, CURRENT_YEAR + 1):
        end = f"{y}-12-31T23:59"
        try:
            url = f"{BASE}/demanda/evolucion"
            params = {"start_date": f"{y}-01-01T00:00", "end_date": end, "time_trunc": "month"}
            age = 24 * 60 * 60 if y == CURRENT_YEAR else None
            _download_json(f"demanda_nacional/{y}.json", url, params,
                           "demanda_nacional", str(y), age)
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
                url = f"{BASE}/{cat}/{widget}"
                params = {"start_date": start, "end_date": end, "time_trunc": "year", **geo}
                dest = f"ccaa/{gid}_{widget}.json"
                _download_json(dest, url, params, f"ccaa:{gid}:{widget}",
                               end[:4], 7 * 24 * 60 * 60,
                               wrapper=lambda data: {"ccaa": name, "geo_id": gid, "data": data})
            except Exception as e:
                log.error("REE %s %s: %s", name, widget, e)
            time.sleep(1)


def download_precios() -> None:
    """
    Precio horario del mercado eléctrico (€/MWh) de los últimos 2 años,
    mes a mes (la API limita los datos horarios a ~1 mes por petición).
    Sirve para estimar el coste operativo energético del CPD.
    """
    for y in range(CURRENT_YEAR - 2, CURRENT_YEAR + 1):
        for m in range(1, 13):
            if y == CURRENT_YEAR and m > date.today().month:
                break
            dest = f"precios_hora/{y}_{m:02d}.json"
            last = calendar.monthrange(y, m)[1]
            try:
                url = f"{BASE}/mercados/precios-mercados-tiempo-real"
                start = f"{y}-{m:02d}-01T00:00"
                end = f"{y}-{m:02d}-{last}T23:59"
                params = {"start_date": start, "end_date": end, "time_trunc": "hour"}
                age = 24 * 60 * 60 if y == CURRENT_YEAR and m == date.today().month else None
                _download_json(dest, url, params, "precios_hora",
                               f"{y}-{m:02d}", age)
            except Exception as e:
                log.error("Precios %d-%02d: %s", y, m, e)
            time.sleep(1)


def run() -> None:
    download_demanda_nacional()
    download_ccaa()
    download_precios()


if __name__ == "__main__":
    run()
