"""
Variables de entorno físico y acceso por municipio:

- Topografía (src/ingest/topografia.py): altitud del centroide y pendiente media,
  calculada con las 4 alturas a ~1 km (N, S, E, O) respecto al centro.
- Agua: distancia al embalse más cercano y nº de embalses en 30 km (OSM);
  distancia al río principal más cercano (Natural Earth 1:10m,
  src/ingest/hidrografia.py: solo ríos grandes; en Canarias no hay).
- Suelo y accesos (OSM): distancia al suelo industrial más cercano y nº de
  polígonos en 10 km; distancia al enlace de autopista, puerto y aeropuerto
  más cercanos.

Las distancias son en línea recta entre el centroide del municipio y el
centro del elemento (en los ríos, el punto más cercano de la línea).

Salida: data/silver/municipio_entorno.parquet
Uso: python -m src.etl.entorno
"""
from __future__ import annotations

import json

import numpy as np
import pandas as pd
from scipy.spatial import cKDTree

from config.settings import RAW_DIR, PROCESSED_DIR
from src.etl.geo import municipios
from src.etl.variables_extra import _puntos_osm
from src.etl.common import logged_etl, write_parquet_atomic
from src.ingest.common import log

OUTPUT = PROCESSED_DIR / "municipio_entorno.parquet"
R_TIERRA_KM = 6371.0088
OFFSET_M = 0.009 * 111_320   # el ingest desplaza 0,009º (~1 km) en cada dirección


def _xyz(lat, lon) -> np.ndarray:
    la, lo = np.radians(np.asarray(lat, float)), np.radians(np.asarray(lon, float))
    return np.column_stack([np.cos(la) * np.cos(lo), np.cos(la) * np.sin(lo), np.sin(la)])


def _cuerda(km: float) -> float:
    """Radio en el espacio 3D (esfera unidad) equivalente a una distancia en km sobre la superficie."""
    return 2 * np.sin(km / R_TIERRA_KM / 2)


def _a_km(cuerda: np.ndarray) -> np.ndarray:
    return 2 * R_TIERRA_KM * np.arcsin(np.clip(cuerda / 2, 0, 1))


def _distancia_mas_cercana(muni: pd.DataFrame, pts: pd.DataFrame) -> np.ndarray:
    d, _ = cKDTree(_xyz(pts.lat, pts.lon)).query(_xyz(muni.lat, muni.lon))
    return _a_km(d).round(2)


def _contar(muni: pd.DataFrame, pts: pd.DataFrame, radio_km: float) -> np.ndarray:
    arbol = cKDTree(_xyz(pts.lat, pts.lon))
    return arbol.query_ball_point(_xyz(muni.lat, muni.lon), _cuerda(radio_km), return_length=True)


def topografia(out: pd.DataFrame) -> None:
    lotes = sorted((RAW_DIR / "topografia" / "lotes").glob("*.json"))
    if not lotes:
        log.warning("Sin topografía (python -m src.ingest.run_all --only topografia)")
        return
    df = pd.concat([pd.DataFrame(json.loads(f.read_text(encoding="utf-8"))) for f in lotes])
    df = df.drop_duplicates(["ine", "pos"])
    z = df.pivot(index="ine", columns="pos", values="elevacion")
    lat = df[df.pos == "c"].set_index("ine").lat
    dist = {"n": OFFSET_M, "s": OFFSET_M}
    dist_ew = OFFSET_M * np.cos(np.radians(lat))
    pend = pd.concat([(z[p] - z["c"]).abs() / dist[p] for p in ("n", "s")]
                     + [(z[p] - z["c"]).abs() / dist_ew for p in ("e", "o")], axis=1).mean(axis=1) * 100
    out["altitud_m"] = out["ine"].map(z["c"]).round(1)
    out["pendiente_media_pct"] = out["ine"].map(pend).round(2)


def agua(muni: pd.DataFrame, out: pd.DataFrame) -> None:
    emb = _puntos_osm("embalses")
    if emb is not None and not emb.empty:
        out["dist_embalse_km"] = _distancia_mas_cercana(muni, emb)
        out["n_embalses_30km"] = _contar(muni, emb, 30)
    else:
        log.warning("Sin embalses de OSM")
    for nombre, col in (("rios", "dist_rio_principal_km"),):
        p = RAW_DIR / "hidrografia" / f"{nombre}.json"
        if not p.exists():
            log.warning("Sin %s (python -m src.ingest.run_all --only hidrografia)", nombre)
            continue
        pts = []
        for ft in json.loads(p.read_text(encoding="utf-8"))["features"]:
            g = ft["geometry"]
            lineas = g["coordinates"] if g["type"] == "MultiLineString" else [g["coordinates"]]
            if g["type"] == "Polygon":
                lineas = g["coordinates"]
            elif g["type"] == "MultiPolygon":
                lineas = [anillo for poly in g["coordinates"] for anillo in poly]
            for linea in lineas:
                a = np.array([c[:2] for c in linea], float)
                # se densifica cada ~1 km para que el punto más cercano sea fiable
                for (x0, y0), (x1, y1) in zip(a[:-1], a[1:]):
                    n = max(1, int(np.hypot((x1 - x0) * 85, (y1 - y0) * 111) // 1))
                    t = np.linspace(0, 1, n, endpoint=False)
                    pts += list(zip(y0 + (y1 - y0) * t, x0 + (x1 - x0) * t))
                pts.append((a[-1, 1], a[-1, 0]))
        df = pd.DataFrame(pts, columns=["lat", "lon"])
        out[col] = _distancia_mas_cercana(muni, df)
        log.info("%s: %d puntos de referencia", nombre, len(df))


def acceso_y_suelo(muni: pd.DataFrame, out: pd.DataFrame) -> None:
    ind = _puntos_osm("industrial")
    if ind is not None and not ind.empty:
        out["dist_industrial_km"] = _distancia_mas_cercana(muni, ind)
        out["n_industrial_10km"] = _contar(muni, ind, 10)
    for nombre, col in (("autopistas", "dist_autopista_km"), ("puertos", "dist_puerto_km"),
                        ("aeropuertos", "dist_aeropuerto_km")):
        pts = _puntos_osm(nombre)
        if pts is None or pts.empty:
            log.warning("Sin datos OSM de %s", nombre)
            continue
        out[col] = _distancia_mas_cercana(muni, pts)


@logged_etl("entorno", "topografia+osm+hidrografia",
            [RAW_DIR / "topografia", RAW_DIR / "osm", RAW_DIR / "hidrografia"], [OUTPUT])
def run() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    muni = municipios()
    out = muni[["ine", "municipio"]].copy()
    topografia(out)
    agua(muni, out)
    acceso_y_suelo(muni, out)
    write_parquet_atomic(out, OUTPUT)
    vacios = {c: int(out[c].isna().sum()) for c in out.columns[2:] if out[c].isna().any()}
    log.info("Entorno: %d municipios, columnas %s. Vacíos: %s", len(out), list(out.columns[2:]), vacios)
    log.info("Resumen:\n%s", out.iloc[:, 2:].describe().round(1).T[["min", "50%", "max"]])


if __name__ == "__main__":
    run()
