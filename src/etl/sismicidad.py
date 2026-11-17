"""
Riesgo sísmico por municipio a partir del catálogo EMSC (src/ingest/sismicidad.py):
terremotos de magnitud >= 3 desde 1998 en la península, Baleares y Canarias.

Para cada municipio (centroide):
- n_sismos_50km: nº de terremotos M >= 3 a menos de 50 km.
- n_sismos_m4_50km: nº de terremotos M >= 4 a menos de 50 km.
- mag_max_50km: magnitud máxima a menos de 50 km (vacío si no hubo ninguno).
- dist_sismo_m4_km: distancia al terremoto M >= 4 más cercano.

Es una medida de actividad sísmica observada, no la peligrosidad oficial
(mapa de la Norma Sismorresistente NCSE-02), que no tiene API.

Salida: data/processed/municipio_sismicidad.csv
Uso: python -m src.etl.sismicidad
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from config.settings import RAW_DIR, PROCESSED_DIR
from src.etl.geo import municipios, distancias_km
from src.ingest.common import log

RADIO_KM = 50
BLOQUE = 500   # municipios por bloque al calcular distancias (limita la memoria)


def _eventos() -> pd.DataFrame:
    ficheros = sorted((RAW_DIR / "sismicidad").glob("*.txt"))
    if not ficheros:
        raise FileNotFoundError("Sin datos sísmicos (python -m src.ingest.run_all --only sismicidad)")
    ev = pd.concat([pd.read_csv(f, sep="|") for f in ficheros], ignore_index=True)
    ev = ev.rename(columns={"#EventID": "id", "Latitude": "lat", "Longitude": "lon", "Magnitude": "mag"})
    ev = ev.drop_duplicates("id").dropna(subset=["lat", "lon", "mag"])
    log.info("Sismicidad: %d terremotos (M %.1f-%.1f)", len(ev), ev.mag.min(), ev.mag.max())
    return ev[["lat", "lon", "mag"]]


def run() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    muni = municipios()
    ev = _eventos()
    mag = ev.mag.to_numpy()
    m4 = mag >= 4
    n, n4, mmax, d4 = [], [], [], []
    for i in range(0, len(muni), BLOQUE):
        b = muni.iloc[i:i + BLOQUE]
        d = distancias_km(b.lat, b.lon, ev.lat, ev.lon)
        cerca = d < RADIO_KM
        n.append(cerca.sum(axis=1))
        n4.append((cerca & m4).sum(axis=1))
        mmax.append(np.where(cerca, mag, -np.inf).max(axis=1))
        d4.append(d[:, m4].min(axis=1))
    out = muni[["ine", "municipio"]].copy()
    out["n_sismos_50km"] = np.concatenate(n)
    out["n_sismos_m4_50km"] = np.concatenate(n4)
    out["mag_max_50km"] = np.concatenate(mmax)
    out.loc[np.isinf(out.mag_max_50km), "mag_max_50km"] = np.nan
    out["dist_sismo_m4_km"] = np.concatenate(d4).round(1)
    out.to_csv(PROCESSED_DIR / "municipio_sismicidad.csv", index=False, encoding="utf-8")
    log.info("Sismicidad: %d municipios, %d con algún terremoto en %d km",
             len(out), (out.n_sismos_50km > 0).sum(), RADIO_KM)


if __name__ == "__main__":
    run()
