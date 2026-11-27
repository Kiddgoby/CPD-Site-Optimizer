"""
Ingesta del valor tasado medio de la vivienda libre (Ministerio de Vivienda y
Agenda Urbana, MIVAU), en euros/m², por municipio (> 25.000 habitantes) y
trimestre desde 2005. Es la mejor aproximación pública al COSTE DEL SUELO.

(El índice de alquiler del INE no sirve para esto: es un índice base 100
de cada municipio, mide subidas, no niveles.)

Para los municipios pequeños que no aparecen, la ETL usa la media provincial.
"""
from __future__ import annotations

import pandas as pd

from src.ingest.common import (cache_is_fresh, file_sha256, http_get, log, raw_path,
                               read_watermark, save_raw, write_watermark)

SOURCE = "vivienda"
URL = "https://apps.fomento.gob.es/BoletinOnline2/sedal/35103500.XLS"
BROWSER = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


def _valid_workbook(path) -> bool:
    try:
        if not path.read_bytes().startswith(b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"):
            return False
        with pd.ExcelFile(path) as workbook:
            return bool(workbook.sheet_names)
    except (OSError, ValueError, ImportError) as exc:
        log.warning("No se pudo validar el Excel local %s: %s", path, exc)
        return False


def run() -> None:
    filename = "valor_tasado_municipios.xls"
    path = raw_path(SOURCE, filename)
    key = f"{SOURCE}:snapshot"
    if cache_is_fresh(path, 90 * 24 * 60 * 60, validator=_valid_workbook):
        if read_watermark(key) is None:
            write_watermark(key, f"snapshot-sha256:{file_sha256(path)}", file=filename)
        log.info("Valor tasado descargado y válido")
        return
    r = http_get(URL, headers=BROWSER)
    path = save_raw(SOURCE, filename, r.content, URL, validator=_valid_workbook)
    write_watermark(key, f"snapshot-sha256:{file_sha256(path)}", file=filename)


if __name__ == "__main__":
    run()
