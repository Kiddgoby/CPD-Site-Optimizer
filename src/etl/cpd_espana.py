"""
Base de datos unificada de CPD en España + variables de competencia por municipio.

Fuentes (de más a menos completa):
1. Observatorio Español de Data Centers (src/ingest/cpd_observatorio.py):
   operativos, en construcción y planificados, con tipo y potencia IT.
2. PeeringDB (src/ingest/peeringdb.py): colocation con nº de redes y de IXP
   conectados (mide la conectividad real de cada CPD).
3. OpenStreetMap (src/ingest/osm.py, "datacenters"): CPD mapeados por la
   comunidad que no salen en las otras dos.

Deduplicado: un registro de PeeringDB u OSM a menos de 1,5 km de uno ya
incluido se considera el mismo CPD (PeeringDB aporta sus redes/IXP al
registro existente).

Salidas:
    data/processed/cpd_espana.csv              un CPD por fila
    data/processed/municipio_competencia_cpd.csv
        n y MW de CPD operativos / en cartera (construcción + planificación)
        en el municipio y en 50 km, distancia al CPD operativo más cercano y
        la etiqueta tiene_cpd_operativo para el modelo.

Uso: python -m src.etl.cpd_espana
"""
from __future__ import annotations

import json
import re

import numpy as np
import pandas as pd

from config.settings import RAW_DIR, PROCESSED_DIR
from src.etl.geo import municipios, distancias_km, norm_nombre, PROVINCIAS
from src.ingest.common import log

DEDUP_KM = 1.5
RADIO_KM = 50
ESTADOS = {"operativo": "operativo", "en-construccion": "en_construccion",
           "planificacion": "planificado"}
TIPOS = {"colocation": "colocation", "hiperscaler": "hiperescala", "publico": "publico",
         "on premise": "on_premise", "edge": "edge"}


def _mw(v) -> float:
    m = re.search(r"([\d.,]+)\s*MW", str(v or ""))
    return float(m.group(1).replace(",", ".")) if m else np.nan


def observatorio() -> pd.DataFrame:
    filas = []
    for f in sorted((RAW_DIR / "cpd_observatorio" / "fichas").glob("*.json")):
        x = json.loads(f.read_text(encoding="utf-8"))
        try:
            lat, lon = float(x.get("lat")), float(x.get("lng"))
        except (TypeError, ValueError):
            lat = lon = np.nan
        filas.append({
            "fuente": "observatorio", "id_fuente": x.get("id"),
            "operador": x.get("operador"), "nombre": x.get("instalacion") or x.get("operador"),
            "tipo": TIPOS.get(str(x.get("tipo") or "").lower(), str(x.get("tipo") or "").lower() or None),
            "estado": ESTADOS.get(x.get("estado"), x.get("estado")),
            "potencia_it_mw": _mw(x.get("Potencia IT")),
            "direccion": x.get("direccion"), "ubicacion": x.get("Ubicación"),
            "lat": lat, "lon": lon, "precision_geo": x.get("geo-accuracy") or None,
            "url": x.get("url"),
        })
    return pd.DataFrame(filas)


def peeringdb() -> pd.DataFrame:
    p = json.loads((RAW_DIR / "peeringdb" / "fac_es.json").read_text(encoding="utf-8"))
    d = p["data"] if isinstance(p, dict) else p
    return pd.DataFrame([{
        "fuente": "peeringdb", "id_fuente": str(x["id"]), "operador": x.get("org_name"),
        "nombre": x.get("name"), "tipo": "colocation", "estado": "operativo",
        "potencia_it_mw": np.nan,
        "direccion": ", ".join(filter(None, [x.get("address1"), x.get("zipcode"), x.get("city")])),
        "ubicacion": x.get("city"), "lat": x.get("latitude"), "lon": x.get("longitude"),
        "precision_geo": "peeringdb", "redes_peeringdb": x.get("net_count"),
        "ixp_peeringdb": x.get("ix_count"), "url": x.get("website"),
    } for x in d if x.get("latitude") is not None])


def osm() -> pd.DataFrame:
    els = json.loads((RAW_DIR / "osm" / "datacenters.json").read_text(encoding="utf-8"))["elements"]
    filas = []
    for e in els:
        c = e.get("center") or {"lat": e.get("lat"), "lon": e.get("lon")}
        t = e.get("tags", {})
        if c.get("lat") is None:
            continue
        filas.append({"fuente": "osm", "id_fuente": f"{e['type']}/{e['id']}",
                      "operador": t.get("operator"), "nombre": t.get("name") or t.get("operator"),
                      "tipo": None, "estado": "operativo_sin_verificar", "potencia_it_mw": np.nan,
                      "direccion": t.get("addr:street"), "ubicacion": t.get("addr:city"),
                      "lat": c["lat"], "lon": c["lon"], "precision_geo": "osm"})
    return pd.DataFrame(filas)


def unir(base: pd.DataFrame, extra: pd.DataFrame, columnas_aporta: list[str]) -> pd.DataFrame:
    """Añade a 'base' los registros de 'extra' que no estén ya (a > DEDUP_KM)."""
    if extra.empty:
        return base
    b = base.dropna(subset=["lat"])
    d = distancias_km(extra.lat, extra.lon, b.lat, b.lon)
    cerca = d.min(axis=1) <= DEDUP_KM
    base = base.copy()
    for col in columnas_aporta:                     # p. ej. redes/IXP de PeeringDB
        if col not in base:
            base[col] = np.nan
        for i in np.where(cerca)[0]:
            j = b.index[d[i].argmin()]
            if pd.isna(base.at[j, col]):
                base.at[j, col] = extra.iloc[i][col]
    log.info("%s: %d ya estaban, %d nuevos", extra.fuente.iloc[0], cerca.sum(), (~cerca).sum())
    return pd.concat([base, extra[~cerca]], ignore_index=True)


def asignar_municipio(cpd: pd.DataFrame, muni: pd.DataFrame) -> pd.DataFrame:
    """Código INE del CPD: 1) nombre de municipio que aparece en la dirección,
    2) ciudad indicada en 'ubicacion', 3) municipio con el centroide más cercano."""
    muni = muni.assign(clave=muni.municipio.map(norm_nombre),
                       ccaa=muni.ine.str[:2].map(lambda p: PROVINCIAS.get(p, ("", ""))[1]))
    nombres = muni.drop_duplicates("clave").set_index("clave")["ine"]
    d = distancias_km(cpd.lat.fillna(0), cpd.lon.fillna(0), muni.lat, muni.lon)
    cercano = muni.ine.to_numpy()[d.argmin(axis=1)]
    ines, metodo = [], []
    for k, (_, r) in enumerate(cpd.iterrows()):
        elegido, m = None, None
        texto = norm_nombre(r.get("direccion") or "")
        if texto:
            # municipios cercanos (< 40 km) cuyo nombre aparece en la dirección; gana el más largo
            cand = muni[(d[k] < 40) & muni.clave.map(lambda c: len(c) > 2 and f" {c} " in f" {texto} ")]
            if not cand.empty:
                elegido, m = cand.loc[cand.clave.str.len().idxmax(), "ine"], "direccion"
        if not elegido and r.get("ubicacion"):
            c = norm_nombre(str(r["ubicacion"]).split(",")[0])
            if c in nombres.index and d[k][muni.ine.eq(nombres[c]).to_numpy()].min() < 40:
                elegido, m = nombres[c], "ubicacion"
        if not elegido and pd.notna(r.lat):
            elegido, m = cercano[k], "centroide_cercano"
        ines.append(elegido); metodo.append(m)
    cpd = cpd.copy()
    cpd["ine"], cpd["metodo_municipio"] = ines, metodo
    info = muni.set_index("ine")
    cpd["municipio"] = cpd.ine.map(info.municipio)
    cpd["ccaa"] = cpd.ine.map(info.ccaa)
    return cpd


def competencia(cpd: pd.DataFrame, muni: pd.DataFrame) -> pd.DataFrame:
    out = muni[["ine", "municipio"]].copy()
    op = cpd[cpd.estado.isin(["operativo", "operativo_sin_verificar"])]
    cartera = cpd[cpd.estado.isin(["en_construccion", "planificado"])]
    for nombre, g in (("operativos", op), ("cartera", cartera)):
        agg = g.groupby("ine").agg(n=("nombre", "size"), mw=("potencia_it_mw", "sum"))
        out[f"n_cpd_{nombre}"] = out.ine.map(agg.n).fillna(0).astype(int)
        out[f"mw_cpd_{nombre}"] = out.ine.map(agg.mw).fillna(0).round(1)
        gg = g.dropna(subset=["lat"])
        d = distancias_km(muni.lat, muni.lon, gg.lat, gg.lon)
        dentro = d <= RADIO_KM
        out[f"n_cpd_{nombre}_{RADIO_KM}km"] = dentro.sum(axis=1)
        out[f"mw_cpd_{nombre}_{RADIO_KM}km"] = (dentro * gg.potencia_it_mw.fillna(0).to_numpy()[None, :]).sum(axis=1).round(1)
        if nombre == "operativos":
            out["dist_cpd_operativo_km"] = d.min(axis=1).round(2)
    out["n_hiperescala_50km"] = 0
    h = cpd[(cpd.tipo == "hiperescala")].dropna(subset=["lat"])
    if not h.empty:
        out["n_hiperescala_50km"] = (distancias_km(muni.lat, muni.lon, h.lat, h.lon) <= RADIO_KM).sum(axis=1)
    out["tiene_cpd_operativo"] = (out.n_cpd_operativos > 0).astype(int)   # etiqueta del modelo
    return out


def run() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    cpd = observatorio()
    cpd["redes_peeringdb"] = np.nan
    cpd["ixp_peeringdb"] = np.nan
    cpd = unir(cpd, peeringdb(), ["redes_peeringdb", "ixp_peeringdb"])
    cpd = unir(cpd, osm(), [])
    muni = municipios()
    cpd = asignar_municipio(cpd, muni)
    cpd.to_csv(PROCESSED_DIR / "cpd_espana.csv", index=False, encoding="utf-8")
    comp = competencia(cpd, muni)
    comp.to_csv(PROCESSED_DIR / "municipio_competencia_cpd.csv", index=False, encoding="utf-8")
    log.info("CPD España: %d registros %s", len(cpd), cpd.estado.value_counts().to_dict())
    log.info("Municipios con CPD operativo: %d", comp.tiene_cpd_operativo.sum())


if __name__ == "__main__":
    run()
