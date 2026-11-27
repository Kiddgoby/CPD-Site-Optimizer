"""
Ingesta de la capacidad de acceso a la red de transporte para CONSUMO (REE).

Es el dato que más limita hoy la instalación de un CPD en España: aunque haya
una subestación cerca, si el nudo no tiene MW libres para demanda, el CPD no
se puede conectar. REE publica cada mes un Excel con todos los nudos de la red
de transporte (~940) y la capacidad disponible por tipo de instalación:
https://www.ree.es/es/clientes/consumidor/acceso-conexion/conoce-la-capacidad-de-acceso

El nombre del fichero cambia cada mes (p. ej. 2026_11_03_GRT_demanda.xlsx), así
que se lee la página y se descarga el enlace más reciente.

La web de REE tiene protección anti-bots: se usa una cabecera de navegador.
Si aun así falla, descarga el Excel a mano desde la página anterior y guárdalo
como data/bronze/ree_capacidad/GRT_demanda.xlsx (la ETL lo encontrará igual).

Después se procesa con: python -m src.etl.ree_nudos
"""
from __future__ import annotations

import re
import zipfile

from src.ingest.common import (DataValidationError, file_sha256, http_get, log,
                               raw_path, read_watermark, save_raw, write_watermark)

SOURCE = "ree_capacidad"
BASE = "https://www.ree.es"
PAGE = BASE + "/es/clientes/consumidor/acceso-conexion/conoce-la-capacidad-de-acceso"
BROWSER = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/130.0 Safari/537.36"}


def _valid_xlsx(path) -> bool:
    try:
        with zipfile.ZipFile(path) as archive:
            return archive.testzip() is None and "xl/workbook.xml" in archive.namelist()
    except (OSError, zipfile.BadZipFile):
        return False


def run() -> None:
    html = http_get(PAGE, headers=BROWSER).text
    links = sorted(set(re.findall(r'/sites/default/files/[^"\']+GRT_demanda\.xlsx', html)))
    if not links:
        raise RuntimeError("No encuentro el Excel GRT_demanda en la página de REE "
                           "(descárgalo a mano, ver cabecera del script)")
    url = BASE + links[-1]                       # el nombre empieza por la fecha -> el último es el más reciente
    fecha = re.search(r"(\d{4}_\d{2}_\d{2})", url)
    nombre = f"GRT_demanda_{fecha.group(1) if fecha else 'ultimo'}.xlsx"
    cached = raw_path(SOURCE, nombre)
    if _valid_xlsx(cached):
        log.info("Capacidad de acceso %s ya descargada y válida", nombre)
        if read_watermark(SOURCE) is None:
            write_watermark(SOURCE, f"snapshot-sha256:{file_sha256(cached)}", file=nombre)
        return
    r = http_get(url, headers=BROWSER)
    if not r.content.startswith(b"PK"):
        raise DataValidationError("REE devolvió una página en vez del Excel (anti-bot). Descárgalo a mano.")
    path = save_raw(SOURCE, nombre, r.content, url, validator=_valid_xlsx)
    write_watermark(SOURCE, f"snapshot-sha256:{file_sha256(path)}",
                    file=nombre, version=fecha.group(1) if fecha else "ultimo")


if __name__ == "__main__":
    run()
