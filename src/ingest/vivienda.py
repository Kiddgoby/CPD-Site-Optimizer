"""
Ingesta del valor tasado medio de la vivienda libre (Ministerio de Vivienda y
Agenda Urbana, MIVAU), en euros/m², por municipio (> 25.000 habitantes) y
trimestre desde 2005. Es la mejor aproximación pública al COSTE DEL SUELO.

(El índice de alquiler del INE no sirve para esto: es un índice base 100
de cada municipio, mide subidas, no niveles.)

Para los municipios pequeños que no aparecen, la ETL usa la media provincial.
"""
from __future__ import annotations

from src.ingest.common import http_get, save_raw, raw_path, log

SOURCE = "vivienda"
URL = "https://apps.fomento.gob.es/BoletinOnline2/sedal/35103500.XLS"
BROWSER = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


def run() -> None:
    if raw_path(SOURCE, "valor_tasado_municipios.xls").exists():
        log.info("Valor tasado ya descargado")
        return
    r = http_get(URL, headers=BROWSER)
    save_raw(SOURCE, "valor_tasado_municipios.xls", r.content, URL)


if __name__ == "__main__":
    run()
