"""
Jerarquía geográfica de cada municipio: municipio -> provincia -> comunidad autónoma.

- Municipios y centroides: OSM (src/etl/geo.py, los mismos 8.135 que el resto de la ETL).
- Provincia y nombre de la CCAA: código INE de provincia (2 primeros dígitos)
  con la tabla PROVINCIAS de geo.py. Los nombres de CCAA coinciden con los de
  data/reference/regulacion_ccaa.csv, así que se pueden unir por nombre.
- Código INE de la CCAA (01-19): se toma del fichero de paro del SEPE, que
  trae "Código de CA" para cada provincia. Así no hay que escribirlo a mano.

Salida: data/processed/municipio_geografia.csv
Uso: python -m src.etl.geografia
"""
from __future__ import annotations

import pandas as pd

from config.settings import PROCESSED_DIR
from src.etl.geo import municipios, distancias_km, PROVINCIAS
from src.etl.socioeconomico import leer_paro
from src.ingest.common import log


def run() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out = municipios()
    out["cod_provincia"] = out.ine.str[:2]
    # OSM incluye algunos territorios compartidos entre pueblos ("Comunidad de...",
    # "Coto mancomunado de..."). El INE les da códigos especiales (p. ej. provincia 53)
    # y no tienen población. Se mantienen, con la provincia del municipio más cercano.
    raros = ~out.cod_provincia.isin(list(PROVINCIAS))
    if raros.any():
        normales = out[~raros]
        cerca = distancias_km(out[raros].lat, out[raros].lon, normales.lat, normales.lon).argmin(axis=1)
        out.loc[raros, "cod_provincia"] = normales.cod_provincia.to_numpy()[cerca]
        log.info("Provincia asignada por cercanía a %d territorios sin provincia INE: %s",
                 raros.sum(), list(out[raros].ine))
    out["provincia"] = out.cod_provincia.map(lambda c: PROVINCIAS[c][0])
    out["ccaa"] = out.cod_provincia.map(lambda c: PROVINCIAS[c][1])

    paro = leer_paro()
    prov_ccaa = (paro[["cod_provincia", "cod_ccaa"]].drop_duplicates()
                 .set_index("cod_provincia").cod_ccaa)
    out["cod_ccaa"] = out.cod_provincia.map(prov_ccaa)
    if out.cod_ccaa.isna().any():
        raise ValueError("Hay provincias sin código de CCAA en el fichero del SEPE: "
                         f"{sorted(out[out.cod_ccaa.isna()].cod_provincia.unique())}")
    # Cada CCAA debe tener un único código (si no, PROVINCIAS y SEPE no cuadran)
    dobles = out.groupby("ccaa").cod_ccaa.nunique()
    if (dobles > 1).any():
        raise ValueError(f"CCAA con varios códigos: {list(dobles[dobles > 1].index)}")

    cols = ["ine", "municipio", "cod_provincia", "provincia", "cod_ccaa", "ccaa", "lat", "lon"]
    out[cols].to_csv(PROCESSED_DIR / "municipio_geografia.csv", index=False, encoding="utf-8")
    log.info("Geografía: %d municipios, %d provincias, %d CCAA",
             len(out), out.cod_provincia.nunique(), out.cod_ccaa.nunique())


if __name__ == "__main__":
    run()
