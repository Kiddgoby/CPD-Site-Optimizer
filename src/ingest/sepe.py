"""
Ingesta SEPE: paro registrado mensual por municipio (datos abiertos).
Catálogo: https://datos.gob.es/es/catalogo/ea0041513-paro-registrado-por-municipio
"""
from __future__ import annotations

import pandas as pd

from config.settings import CURRENT_YEAR
from src.ingest.common import (APIRequestError, cache_is_fresh, file_sha256, http_get,
                               log, raw_path, read_watermark, save_raw, write_watermark)

SOURCE = "sepe"
URL = "https://sede.sepe.gob.es/es/portaltrabaja/resources/sede/datos_abiertos/datos/Paro_por_municipios_{y}_csv.csv"


def _valid_csv(path) -> bool:
    try:
        columns = pd.read_csv(path, sep=";", skiprows=1, encoding="latin-1", nrows=0).columns
    except (OSError, pd.errors.ParserError, UnicodeError):
        return False
    return {"Código mes", "Codigo Municipio", "total Paro Registrado"}.issubset(
        {str(column).strip() for column in columns}
    )


def run() -> None:
    # Último año completo publicado + el anterior (para ver tendencia)
    got = 0
    for y in range(CURRENT_YEAR, CURRENT_YEAR - 4, -1):
        filename = f"paro_municipios_{y}.csv"
        cached = raw_path(SOURCE, filename)
        age = 7 * 24 * 60 * 60 if y == CURRENT_YEAR else None
        if cache_is_fresh(cached, age, validator=_valid_csv):
            log.info("SEPE %d: se reutiliza el CSV válido", y)
            watermark_key = f"{SOURCE}:{y}"
            if read_watermark(watermark_key) is None:
                write_watermark(watermark_key,
                                f"snapshot-sha256:{file_sha256(cached)}",
                                file=filename, year=y)
            got += 1
            if got == 2:
                break
            continue
        url = URL.format(y=y)
        try:
            r = http_get(url)
            path = save_raw(SOURCE, filename, r.content, url, validator=_valid_csv)
            write_watermark(f"{SOURCE}:{y}", f"snapshot-sha256:{file_sha256(path)}",
                            file=path.name, year=y)
            got += 1
        except APIRequestError as exc:
            if exc.status_code != 404:
                raise
            log.info("Paro %d no disponible: %s", y, exc)
        if got == 2:
            break


if __name__ == "__main__":
    run()
