"""
Ingesta INE (Instituto Nacional de Estadística).

1. Relación de municipios con su código INE (diccionario de códigos).
2. Cifras oficiales de población por municipio (Padrón) para varios años
   -> población actual y evolución demográfica.
3. Atlas de Distribución de Renta de los Hogares (ADRH) vía API JSON
   -> renta neta media por persona por municipio.
4. Índice de Precios de Vivienda en Alquiler por municipio -> tendencia del alquiler.
"""
from __future__ import annotations

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
    """
    Cifras oficiales del Padrón por municipio desde la API del INE (tabla 29005,
    operación 22): todos los municipios, con su código INE, de 2015 al último año.

    Antes se usaban los ficheros pobmunYY.xlsx, pero el INE dejó de publicarlos
    a partir de 2023 (dan 404) y el script se quedaba con los datos de 2022.
    Se guarda un JSON compacto {codigo_ine: {año: habitantes}}.
    """
    if raw_path(SOURCE, "padron_municipios.json").exists():
        log.info("Padrón ya descargado")
        return
    url = f"{API}/DATOS_TABLA/29005"
    # tip=M añade los metadatos -> así viene el código INE (los nombres se repiten
    # entre provincias); nult=11 -> últimos 11 años (2015-2025)
    series = http_get(url, params={"nult": 11, "tip": "M"}).json()
    out = {}
    for s in series:
        meta = {m.get("FK_Variable"): m for m in s.get("MetaData", [])}
        muni, sexo = meta.get(19), meta.get(18)          # 19 = municipio, 18 = sexo
        if not muni or not sexo or sexo.get("Nombre") != "Total":
            continue
        out[muni["Codigo"]] = {"nombre": muni["Nombre"],
                               **{str(d["Anyo"]): d["Valor"] for d in s["Data"]}}
    log.info("Padrón: %d municipios", len(out))
    save_raw(SOURCE, "padron_municipios.json", out, url)


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
        dest = raw_path(SOURCE, f"adrh_cod/renta_{tid}.json")
        if dest.exists():
            continue
        url = f"{API}/DATOS_TABLA/{tid}"
        try:
            # tip=M -> metadatos con el código INE del municipio (los nombres se repiten)
            data = http_get(url, params={"nult": 1, "tip": "M"}).json()
            if not isinstance(data, list):
                # Provincias grandes: "restricciones de volumen" -> se pide solo el
                # indicador que usamos (variable 482 = "Renta neta media por persona")
                data = http_get(url, params={"nult": 1, "tip": "M", "tv": "482:284048"}).json()
            if not isinstance(data, list):  # el INE devuelve a veces un mensaje de error
                # Pasa con la tabla nacional (toda España junta, id 30824): no importa,
                # las 53 tablas provinciales ya cubren todos los municipios
                log.info("Tabla ADRH %s no disponible (%s); se cubre con las provinciales",
                         tid, str(data)[:80])
                continue
            # Solo nos interesan series municipales (no distritos ni secciones)
            data = [s for s in data if "distrito" not in s["Nombre"].lower()
                    and "sección" not in s["Nombre"].lower()]
            save_raw(SOURCE, f"adrh_cod/renta_{tid}.json", data, url)
        except Exception as e:
            log.error("Tabla ADRH %s fallida: %s", tid, e)


def download_alquiler() -> None:
    """
    Índice de Precios de Vivienda en Alquiler (IPVA, estadística experimental).
    OJO: es un ÍNDICE (base 2015 = 100 en cada municipio), así que mide cuánto
    ha subido el alquiler, NO cuánto cuesta: sirve como tendencia, no para
    comparar el coste del suelo entre municipios. Para eso está
    src/ingest/vivienda.py (valor tasado en €/m²).
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
                         {"tabla": t, "datos": http_get(url, params={"nult": 1, "tip": "M"}).json()}, url)
            except Exception as e:
                log.error("Tabla alquiler %s: %s", tid, e)


def run() -> None:
    download_municipios()
    download_poblacion()
    download_renta()
    download_alquiler()


if __name__ == "__main__":
    run()
