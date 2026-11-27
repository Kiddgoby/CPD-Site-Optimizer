"""
Variables socioeconómicas por municipio.

- Población (INE, padrón por API, data/bronze/ine/padron_municipios.json):
  población del último año publicado, de 2015 y crecimiento % entre ambos.
- Renta (INE, Atlas de Distribución de Renta de los Hogares, tablas provinciales
  en data/bronze/ine/adrh_cod/): renta neta media por persona y por hogar y
  mediana de la renta por unidad de consumo, del último año publicado.
- Paro registrado (SEPE, data/bronze/sepe/): media de los últimos 12 meses
  publicados y tasa de paro aproximada = parados medios / población * 100.
  Es una aproximación: el SEPE no da población activa por municipio.
  El SEPE oculta los valores pequeños como "<5"; se sustituyen por 2,5
  (punto medio de 0-4) para no perder los municipios pequeños.

El valor tasado de la vivienda (coste del suelo) ya lo calcula variables_extra.py.

Salida: data/silver/municipio_socioeconomico.parquet
Uso: python -m src.etl.socioeconomico
"""
from __future__ import annotations

import json

import numpy as np
import pandas as pd

from config.settings import RAW_DIR, PROCESSED_DIR
from src.etl.geo import municipios
from src.etl.common import logged_etl, write_parquet_atomic
from src.ingest.common import log

OUTPUT = PROCESSED_DIR / "municipio_socioeconomico.parquet"
ANIO_BASE_POBLACION = "2015"
VALOR_SECRETO_PARO = 2.5          # sustituto de "<5" en el fichero del SEPE
RENTA = {                          # indicador del ADRH -> columna de salida
    "Renta neta media por persona": "renta_neta_persona",
    "Renta neta media por hogar": "renta_neta_hogar",
    "Mediana de la renta por unidad de consumo": "renta_mediana_uc",
}


def leer_paro() -> pd.DataFrame:
    """Une los CSV mensuales del SEPE: una fila por municipio y mes."""
    ficheros = sorted((RAW_DIR / "sepe").glob("paro_municipios_*.csv"))
    if not ficheros:
        raise FileNotFoundError("Sin datos del SEPE (python -m src.ingest.run_all --only sepe)")
    partes = []
    for f in ficheros:
        # La primera línea es un título; el fichero viene en latin-1 con ';'
        d = pd.read_csv(f, sep=";", skiprows=1, encoding="latin-1", dtype=str)
        d.columns = [c.strip() for c in d.columns]
        partes.append(pd.DataFrame({
            "mes": d["Código mes"].str.strip(),
            "cod_ccaa": d["Código de CA"].str.strip().str.zfill(2),
            "cod_provincia": d["Codigo Provincia"].str.strip().str.zfill(2),
            "ine": d["Codigo Municipio"].str.strip().str.zfill(5),
            "parados": d["total Paro Registrado"].str.strip(),
        }))
    paro = pd.concat(partes, ignore_index=True).dropna(subset=["ine", "mes"])
    paro["parados"] = pd.to_numeric(paro.parados.replace("<5", VALOR_SECRETO_PARO), errors="coerce")
    return paro.drop_duplicates(["ine", "mes"], keep="last")


def _poblacion() -> pd.DataFrame:
    p = json.loads((RAW_DIR / "ine" / "padron_municipios.json").read_text(encoding="utf-8"))
    df = pd.DataFrame.from_dict(p, orient="index")
    df.index.name = "ine"
    anios = sorted(c for c in df.columns if c.isdigit())
    ultimo = anios[-1]
    out = pd.DataFrame({
        "anio_poblacion": int(ultimo),
        "poblacion": df[ultimo],
        "poblacion_2015": df.get(ANIO_BASE_POBLACION),
    }).reset_index()
    out["crecimiento_poblacion_pct"] = ((out.poblacion / out.poblacion_2015 - 1) * 100).round(2)
    log.info("Población: %d municipios, años %s-%s", len(out), anios[0], ultimo)
    return out


def _renta() -> pd.DataFrame:
    filas = []
    for f in sorted((RAW_DIR / "ine" / "adrh_cod").glob("*.json")):
        for serie in json.loads(f.read_text(encoding="utf-8")):
            meta = {m["FK_Variable"]: m for m in serie.get("MetaData", [])}
            cod = meta.get(19, {}).get("Codigo", "")        # 19 = municipio
            indicador = meta.get(482, {}).get("Nombre")      # 482 = indicador de renta
            datos = [d for d in serie.get("Data", []) if d.get("Valor") is not None]
            if len(cod) != 5 or indicador not in RENTA or not datos:
                continue                                     # distritos, secciones o vacíos
            d = max(datos, key=lambda x: x["Anyo"])
            filas.append({"ine": cod, "col": RENTA[indicador], "valor": d["Valor"], "anio": d["Anyo"]})
    if not filas:
        raise FileNotFoundError("Sin datos de renta (python -m src.ingest.run_all --only ine)")
    df = pd.DataFrame(filas)
    out = df.pivot_table(index="ine", columns="col", values="valor", aggfunc="last").reset_index()
    out["anio_renta"] = int(df.anio.max())
    log.info("Renta: %d municipios, año %d", len(out), df.anio.max())
    return out


def _paro(poblacion: pd.DataFrame) -> pd.DataFrame:
    paro = leer_paro()
    meses = sorted(paro.mes.unique())[-12:]
    ult = paro[paro.mes.isin(meses)]
    out = ult.groupby("ine").parados.mean().round(1).rename("parados_medios_12m").reset_index()
    out["periodo_paro"] = f"{meses[0]}-{meses[-1]}"
    out = out.merge(poblacion[["ine", "poblacion"]], on="ine", how="left")
    out["tasa_paro_aprox_pct"] = (out.parados_medios_12m / out.poblacion * 100).round(2)
    log.info("Paro: %d municipios, meses %s a %s", len(out), meses[0], meses[-1])
    return out.drop(columns="poblacion")


@logged_etl("socioeconomico", "ine+sepe", [RAW_DIR / "ine", RAW_DIR / "sepe", RAW_DIR / "osm"], [OUTPUT])
def run() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out = municipios()[["ine", "municipio"]]
    pob = _poblacion()
    out = (out.merge(pob, on="ine", how="left")
              .merge(_renta(), on="ine", how="left")
              .merge(_paro(pob), on="ine", how="left"))
    out = out.replace([np.inf, -np.inf], np.nan)
    write_parquet_atomic(out, OUTPUT)
    vacios = out.drop(columns=["ine", "municipio"]).isna().sum()
    log.info("Socioeconómico: %d municipios. Vacíos por columna: %s",
             len(out), vacios[vacios > 0].to_dict() or "ninguno")


if __name__ == "__main__":
    run()
