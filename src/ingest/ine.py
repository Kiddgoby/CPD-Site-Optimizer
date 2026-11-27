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

import json
import zipfile

from src.ingest.common import (cache_is_fresh, DataValidationError, file_sha256, http_get,
                               log, raw_path, read_watermark, save_raw, write_watermark)

SOURCE = "ine"
API = "https://servicios.ine.es/wstempus/js/ES"


def _valid_xlsx(path) -> bool:
    try:
        with zipfile.ZipFile(path) as workbook:
            return workbook.testzip() is None and "xl/workbook.xml" in workbook.namelist()
    except (OSError, zipfile.BadZipFile):
        return False


def _valid_json(path, expected_type) -> bool:
    try:
        return isinstance(json.loads(path.read_text(encoding="utf-8")), expected_type)
    except (OSError, ValueError, UnicodeError):
        return False


def _seed_snapshot(key: str, path) -> None:
    if read_watermark(key) is None:
        write_watermark(key, f"snapshot-sha256:{file_sha256(path)}", file=path.name)


def download_municipios() -> None:
    """Diccionario de municipios (código INE de 5 dígitos = CPRO + CMUN)."""
    path = raw_path(SOURCE, "diccionario_municipios.xlsx")
    key = f"{SOURCE}:diccionario_municipios"
    if cache_is_fresh(path, 90 * 24 * 60 * 60, validator=_valid_xlsx):
        _seed_snapshot(key, path)
        return
    for yy in range(26, 20, -1):  # prueba el más reciente disponible
        url = f"https://www.ine.es/daco/daco42/codmun/diccionario{yy}.xlsx"
        try:
            r = http_get(url, retries=1)
            path = save_raw(SOURCE, "diccionario_municipios.xlsx", r.content, url,
                            validator=_valid_xlsx)
            write_watermark(key, f"snapshot-sha256:{file_sha256(path)}",
                            file=path.name, version=yy)
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
    path = raw_path(SOURCE, "padron_municipios.json")
    key = f"{SOURCE}:padron_municipios"
    if cache_is_fresh(path, 30 * 24 * 60 * 60,
                      validator=lambda p: _valid_json(p, dict)):
        _seed_snapshot(key, path)
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
    if not out:
        raise DataValidationError("La API del INE no devolvió municipios para el padrón")
    path = save_raw(SOURCE, "padron_municipios.json", out, url,
                    validator=lambda p: _valid_json(p, dict))
    write_watermark(key, f"snapshot-sha256:{file_sha256(path)}",
                    file=path.name, records=len(out))


def download_renta() -> None:
    """
    ADRH (operación 353). Hay una tabla de 'Indicadores de renta media y mediana'
    por provincia. Se listan las tablas de la operación y se descargan esas,
    último periodo disponible (nult=1).
    """
    tables_path = raw_path(SOURCE, "adrh_tablas.json")
    tables_key = f"{SOURCE}:adrh_tablas"
    if cache_is_fresh(tables_path, 7 * 24 * 60 * 60,
                      validator=lambda p: _valid_json(p, list)):
        _seed_snapshot(tables_key, tables_path)
        tablas = json.loads(tables_path.read_text(encoding="utf-8"))
    else:
        tables_url = f"{API}/TABLAS_OPERACION/353"
        tablas = http_get(tables_url).json()
        if not isinstance(tablas, list):
            raise DataValidationError("La API del INE devolvió un catálogo ADRH inválido")
        tables_path = save_raw(SOURCE, "adrh_tablas.json", tablas, tables_url,
                               validator=lambda p: _valid_json(p, list))
        write_watermark(tables_key, f"snapshot-sha256:{file_sha256(tables_path)}",
                        file=tables_path.name, records=len(tablas))
    objetivo = [t for t in tablas
                if "renta media y mediana" in t.get("Nombre", "").lower()]
    log.info("ADRH: %d tablas provinciales de renta", len(objetivo))
    for t in objetivo:
        tid = t["Id"]
        dest = raw_path(SOURCE, f"adrh_cod/renta_{tid}.json")
        key = f"{SOURCE}:renta:{tid}"
        if cache_is_fresh(dest, 30 * 24 * 60 * 60,
                          validator=lambda p: _valid_json(p, list)):
            _seed_snapshot(key, dest)
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
            path = save_raw(SOURCE, f"adrh_cod/renta_{tid}.json", data, url,
                            validator=lambda p: _valid_json(p, list))
            write_watermark(key, f"snapshot-sha256:{file_sha256(path)}",
                            file=path.name, table=tid, records=len(data))
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
    ops_path = raw_path(SOURCE, "alquiler/operaciones.json")
    ops_key = f"{SOURCE}:alquiler:operaciones"
    if cache_is_fresh(ops_path, 7 * 24 * 60 * 60,
                      validator=lambda p: _valid_json(p, list)):
        _seed_snapshot(ops_key, ops_path)
        ops = json.loads(ops_path.read_text(encoding="utf-8"))
    else:
        ops_url = f"{API}/OPERACIONES_DISPONIBLES"
        ops = http_get(ops_url).json()
        if not isinstance(ops, list):
            raise DataValidationError("La API del INE devolvió un catálogo de operaciones inválido")
    ipva = [o for o in ops if "alquiler" in o.get("Nombre", "").lower()]
    if not cache_is_fresh(ops_path, 7 * 24 * 60 * 60,
                          validator=lambda p: _valid_json(p, list)):
        ops_path = save_raw(SOURCE, "alquiler/operaciones.json", ipva, ops_url,
                            validator=lambda p: _valid_json(p, list))
        write_watermark(ops_key, f"snapshot-sha256:{file_sha256(ops_path)}",
                        file=ops_path.name, records=len(ipva))
    if not ipva:
        raise DataValidationError("No se encontró la operación de alquiler en el INE")
    for op in ipva:
        tablas_path = raw_path(SOURCE, f"alquiler/tablas_{op['Id']}.json")
        tablas_key = f"{SOURCE}:alquiler:tablas:{op['Id']}"
        if cache_is_fresh(tablas_path, 7 * 24 * 60 * 60,
                          validator=lambda p: _valid_json(p, list)):
            _seed_snapshot(tablas_key, tablas_path)
            tablas = json.loads(tablas_path.read_text(encoding="utf-8"))
        else:
            tablas_url = f"{API}/TABLAS_OPERACION/{op['Id']}"
            tablas = http_get(tablas_url).json()
            if not isinstance(tablas, list):
                raise DataValidationError(f"Catálogo de tablas INE inválido: {op['Id']}")
            tablas_path = save_raw(SOURCE, f"alquiler/tablas_{op['Id']}.json",
                                   tablas, tablas_url,
                                   validator=lambda p: _valid_json(p, list))
            write_watermark(tablas_key, f"snapshot-sha256:{file_sha256(tablas_path)}",
                            file=tablas_path.name, records=len(tablas))
        for t in tablas:
            if "municipio" not in t.get("Nombre", "").lower():
                continue
            tid = t["Id"]
            path = raw_path(SOURCE, f"alquiler/tabla_{tid}.json")
            key = f"{SOURCE}:alquiler:tabla:{tid}"
            validator = lambda p: (
                isinstance(json.loads(p.read_text(encoding="utf-8")), dict)
                and isinstance(json.loads(p.read_text(encoding="utf-8")).get("datos"), list)
            )
            if cache_is_fresh(path, 30 * 24 * 60 * 60, validator=validator):
                _seed_snapshot(key, path)
                continue
            url = f"{API}/DATOS_TABLA/{tid}"
            try:
                data = {"tabla": t, "datos": http_get(
                    url, params={"nult": 1, "tip": "M"}
                ).json()}
                path = save_raw(SOURCE, f"alquiler/tabla_{tid}.json", data, url,
                                validator=validator)
                write_watermark(key, f"snapshot-sha256:{file_sha256(path)}",
                                file=path.name, table=tid)
            except Exception as e:
                log.error("Tabla alquiler %s: %s", tid, e)


def run() -> None:
    download_municipios()
    download_poblacion()
    download_renta()
    download_alquiler()


if __name__ == "__main__":
    run()
