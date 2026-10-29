"""
Ingesta INE (Instituto Nacional de Estadística).

1. Relación de municipios con su código INE (diccionario de códigos).
2. Cifras oficiales de población por municipio (Padrón) para varios años
   -> población actual y evolución demográfica.
3. Atlas de Distribución de Renta de los Hogares (ADRH) vía API JSON
   -> renta neta media por persona por municipio.
4. Índice de Precios de Vivienda en Alquiler por municipio -> coste del suelo.
"""
from __future__ import annotations

from config.settings import POPULATION_YEARS
from src.ingest.common import http_get, save_raw, raw_path, log

SOURCE = "ine"
API = "https://servicios.ine.es/wstempus/js/ES"


def download_municipios() -> None:
    """Diccionario de municipios (código INE de 5 dígitos = CPRO + CMUN)."""
    for yy in range(26, 20, -1):  # prueba el más reciente disponible
        url = f"https://www.ine.es/daco/daco42/codmun/diccionario{yy}.xlsx"
        try:
            r = http_get(url, retries=1)
            save_raw(SOURCE, "diccionario_municipios.xlsx", r.content, url)
            return
        except Exception:
            log.info("diccionario%s.xlsx no disponible, pruebo año anterior", yy)
    log.error("No se encontró el diccionario de municipios del INE")


def download_poblacion() -> None:
    """Fichero pobmunYY.xlsx con la población de todos los municipios."""
    for year in POPULATION_YEARS:
        # Si el año pedido aún no está publicado, se prueba el anterior
        for y in (year, year - 1):
            yy = str(y)[2:]
            url = f"https://www.ine.es/pob_xls/pobmun{yy}.xlsx"
            if raw_path(SOURCE, f"pobmun_{y}.xlsx").exists():
                break
            try:
                r = http_get(url, retries=1)
                save_raw(SOURCE, f"pobmun_{y}.xlsx", r.content, url)
                break
            except Exception:
                log.info("pobmun%s.xlsx no disponible", yy)


def download_renta() -> None:
    """
    ADRH (operación 353). Hay una tabla de 'Indicadores de renta media y mediana'
    por provincia. Se listan las tablas de la operación y se descargan esas,
    último periodo disponible (nult=1).
    """
    tablas = http_get(f"{API}/TABLAS_OPERACION/353").json()
    save_raw(SOURCE, "adrh_tablas.json", tablas, f"{API}/TABLAS_OPERACION/353")
    objetivo = [t for t in tablas
                if "renta media y mediana" in t.get("Nombre", "").lower()]
    log.info("ADRH: %d tablas provinciales de renta", len(objetivo))
    for t in objetivo:
        tid = t["Id"]
        dest = raw_path(SOURCE, f"adrh/renta_{tid}.json")
        if dest.exists():
            continue
        url = f"{API}/DATOS_TABLA/{tid}"
        try:
            data = http_get(url, params={"nult": 1}).json()
            # Solo nos interesan series municipales (no distritos ni secciones)
            data = [s for s in data if "distrito" not in s["Nombre"].lower()
                    and "sección" not in s["Nombre"].lower()]
            save_raw(SOURCE, f"adrh/renta_{tid}.json", data, url)
        except Exception as e:
            log.error("Tabla ADRH %s fallida: %s", tid, e)


def download_alquiler() -> None:
    """
    Índice de Precios de Vivienda en Alquiler (IPVA, estadística experimental).
    Se busca la operación por nombre para no depender de un id fijo y se
    descargan sus tablas con desglose municipal -> proxy del coste del suelo.
    """
    ops = http_get(f"{API}/OPERACIONES_DISPONIBLES").json()
    ipva = [o for o in ops if "alquiler" in o.get("Nombre", "").lower()]
    save_raw(SOURCE, "alquiler/operaciones.json", ipva, f"{API}/OPERACIONES_DISPONIBLES")
    if not ipva:
        log.error("No se encontró la operación de alquiler en el INE")
        return
    for op in ipva:
        tablas = http_get(f"{API}/TABLAS_OPERACION/{op['Id']}").json()
        for t in tablas:
            if "municipio" not in t.get("Nombre", "").lower():
                continue
            tid = t["Id"]
            if raw_path(SOURCE, f"alquiler/tabla_{tid}.json").exists():
                continue
            url = f"{API}/DATOS_TABLA/{tid}"
            try:
                save_raw(SOURCE, f"alquiler/tabla_{tid}.json",
                         {"tabla": t, "datos": http_get(url, params={"nult": 1}).json()}, url)
            except Exception as e:
                log.error("Tabla alquiler %s: %s", tid, e)


def run() -> None:
    download_municipios()
    download_poblacion()
    download_renta()
    download_alquiler()


if __name__ == "__main__":
    run()
