"""
Variables por municipio a partir de las fuentes añadidas en noviembre:

- Cables submarinos (src/ingest/cables.py): distancia al amarre más cercano,
  cables internacionales que llegan a él y amarres en 100 km.
- Zonas inundables (src/ingest/inundaciones.py): % inundable T100 y T500.
- Universidades y bomberos (OSM): distancia a la más cercana y nº en 30 km.
- Coste del suelo (src/ingest/vivienda.py): valor tasado de la vivienda en
  €/m² del último trimestre; si el municipio no sale (< 25.000 hab.), la
  media de su provincia (columna valor_tasado_origen indica cuál).
- Clima (Open-Meteo): índice de aridez de la UNESCO/UNEP = precipitación /
  evapotranspiración (P/ET0; < 0,5 = semiárido o más seco -> estrés hídrico)
  y días al año con temperatura media < 18 ºC (aproximación a free cooling).

Cada bloque se calcula solo si sus datos raw existen.

Salida: data/processed/municipio_variables_extra.csv
Uso: python -m src.etl.variables_extra
"""
from __future__ import annotations

import json

import numpy as np
import pandas as pd

from config.settings import RAW_DIR, PROCESSED_DIR
from src.etl.geo import municipios, distancias_km, norm_nombre, PROVINCIAS
from src.ingest.common import log


def _puntos_osm(nombre: str) -> pd.DataFrame | None:
    p = RAW_DIR / "osm" / f"{nombre}.json"
    if not p.exists():
        return None
    filas = []
    for e in json.loads(p.read_text(encoding="utf-8"))["elements"]:
        c = e.get("center") or {"lat": e.get("lat"), "lon": e.get("lon")}
        if c.get("lat") is not None:
            filas.append((c["lat"], c["lon"]))
    return pd.DataFrame(filas, columns=["lat", "lon"])


def cables(muni: pd.DataFrame, out: pd.DataFrame) -> None:
    p = RAW_DIR / "cables" / "amarres_es.json"
    if not p.exists():
        log.warning("Sin datos de cables submarinos")
        return
    am = pd.DataFrame(json.loads(p.read_text(encoding="utf-8")))
    d = distancias_km(muni.lat, muni.lon, am.lat, am.lon)
    i = d.argmin(axis=1)
    out["amarre_cable_cercano"] = am["nombre"].to_numpy()[i]
    out["dist_amarre_cable_km"] = d.min(axis=1).round(2)
    out["cables_intl_amarre_cercano"] = am["n_cables_internacionales"].to_numpy()[i]
    out["amarres_cable_100km"] = (d <= 100).sum(axis=1)


def inundaciones(out: pd.DataFrame) -> None:
    lotes = sorted((RAW_DIR / "inundaciones" / "lotes").glob("*.json"))
    if not lotes:
        log.warning("Sin datos de inundaciones (python -m src.ingest.run_all --only inundaciones)")
        return
    df = pd.concat([pd.DataFrame(json.loads(f.read_text(encoding="utf-8"))) for f in lotes])
    df = df.drop_duplicates("ine")[["ine", "pct_inundable_t100", "pct_inundable_t500"]]
    for c in ("pct_inundable_t100", "pct_inundable_t500"):
        out[c] = out["ine"].map(df.set_index("ine")[c])


def osm_extra(muni: pd.DataFrame, out: pd.DataFrame) -> None:
    for nombre in ("universidades", "bomberos"):
        pts = _puntos_osm(nombre)
        if pts is None or pts.empty:
            log.warning("Sin datos OSM de %s (python -m src.ingest.osm)", nombre)
            continue
        d = distancias_km(muni.lat, muni.lon, pts.lat, pts.lon)
        out[f"dist_{nombre}_km"] = d.min(axis=1).round(2)
        out[f"n_{nombre}_30km"] = (d <= 30).sum(axis=1)


def valor_tasado(out: pd.DataFrame) -> None:
    p = RAW_DIR / "vivienda" / "valor_tasado_municipios.xls"
    if not p.exists():
        log.warning("Sin valor tasado (python -m src.ingest.vivienda)")
        return
    xls = pd.ExcelFile(p)
    hoja = xls.sheet_names[-1]                      # la última pestaña = último trimestre
    df = pd.read_excel(xls, hoja, header=None).iloc[:, [1, 2, 5]]
    df.columns = ["provincia", "municipio", "eur_m2"]
    df["provincia"] = df["provincia"].ffill()
    df["eur_m2"] = pd.to_numeric(df["eur_m2"], errors="coerce")
    df = df.dropna(subset=["municipio", "eur_m2"])
    prov = {norm_nombre(n): c for c, (n, _) in PROVINCIAS.items()}
    alias = {"alava": "01", "araba": "01", "baleares": "07", "illes balears": "07",
             "castellon": "12", "coruna": "15", "rioja": "26", "palmas": "35",
             "gipuzkoa": "20", "guipuzcoa": "20", "bizkaia": "48", "vizcaya": "48",
             "valencia": "46", "alicante": "03", "girona": "17", "gerona": "17",
             "lleida": "25", "lerida": "25", "ourense": "32", "orense": "32",
             # erratas y celdas partidas del propio Excel del ministerio
             "valladodid": "47", "santa cruz": "38", "tenerife": "38"}
    df["cpro"] = df.provincia.map(lambda x: prov.get(norm_nombre(x)) or alias.get(norm_nombre(x)))
    df["clave"] = df.cpro + "|" + df.municipio.map(norm_nombre)
    claves = out.ine.str[:2] + "|" + out.municipio.map(norm_nombre)
    por_muni = df.drop_duplicates("clave").set_index("clave").eur_m2
    por_prov = df.groupby("cpro").eur_m2.mean()
    v = claves.map(por_muni)
    out["valor_tasado_eur_m2"] = v.fillna(out.ine.str[:2].map(por_prov)).round(1)
    out["valor_tasado_origen"] = np.where(v.notna(), "municipio",
                                          np.where(out.valor_tasado_eur_m2.notna(), "media_provincia", None))
    log.info("Valor tasado (%s): %d municipios propios, resto con media provincial",
             hoja.strip(), v.notna().sum())


def clima(muni: pd.DataFrame, out: pd.DataFrame) -> None:
    carpetas = sorted(p for p in (RAW_DIR / "openmeteo").glob("*") if p.is_dir())
    if not carpetas:
        log.warning("Sin datos de Open-Meteo")
        return
    filas = []
    for f in carpetas[-1].glob("*.json"):
        j = json.loads(f.read_text(encoding="utf-8"))
        dd = pd.DataFrame(j["daily"])
        et0 = dd["et0_fao_evapotranspiration"].sum()
        filas.append({
            "lat": j["latitude"], "lon": j["longitude"],
            "indice_aridez": dd["precipitation_sum"].sum() / et0 if et0 else np.nan,
            "dias_tmedia_menor_18": int((dd["temperature_2m_mean"] < 18).sum()),
        })
    g = pd.DataFrame(filas)
    i = distancias_km(muni.lat, muni.lon, g.lat, g.lon).argmin(axis=1)
    out["indice_aridez"] = g["indice_aridez"].to_numpy()[i].round(3)
    out["dias_tmedia_menor_18"] = g["dias_tmedia_menor_18"].to_numpy()[i]


def run() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    muni = municipios()
    out = muni[["ine", "municipio"]].copy()
    cables(muni, out)
    inundaciones(out)
    osm_extra(muni, out)
    valor_tasado(out)
    clima(muni, out)
    out.to_csv(PROCESSED_DIR / "municipio_variables_extra.csv", index=False, encoding="utf-8")
    log.info("Variables extra: %d municipios, columnas %s", len(out), list(out.columns[2:]))


if __name__ == "__main__":
    run()
