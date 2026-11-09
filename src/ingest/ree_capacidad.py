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
como data/raw/ree_capacidad/GRT_demanda.xlsx (la ETL lo encontrará igual).

Después se procesa con: python -m src.etl.ree_nudos
"""
from __future__ import annotations

import re

from src.ingest.common import http_get, save_raw, raw_path, log

SOURCE = "ree_capacidad"
BASE = "https://www.ree.es"
PAGE = BASE + "/es/clientes/consumidor/acceso-conexion/conoce-la-capacidad-de-acceso"
BROWSER = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/130.0 Safari/537.36"}


def run() -> None:
    html = http_get(PAGE, headers=BROWSER).text
    links = sorted(set(re.findall(r'/sites/default/files/[^"\']+GRT_demanda\.xlsx', html)))
    if not links:
        raise RuntimeError("No encuentro el Excel GRT_demanda en la página de REE "
                           "(descárgalo a mano, ver cabecera del script)")
    url = BASE + links[-1]                       # el nombre empieza por la fecha -> el último es el más reciente
    fecha = re.search(r"(\d{4}_\d{2}_\d{2})", url)
    nombre = f"GRT_demanda_{fecha.group(1) if fecha else 'ultimo'}.xlsx"
    if raw_path(SOURCE, nombre).exists():
        log.info("Capacidad de acceso %s ya descargada", nombre)
        return
    r = http_get(url, headers=BROWSER)
    if not r.content.startswith(b"PK"):          # un .xlsx es un zip; si no, nos ha devuelto la página anti-bot
        raise RuntimeError("REE devolvió una página en vez del Excel (anti-bot). Descárgalo a mano.")
    save_raw(SOURCE, nombre, r.content, url)


if __name__ == "__main__":
    run()
