"""
ETL de la capacidad de acceso para consumo de REE (ver src/ingest/ree_capacidad.py).

1. Lee el Excel más reciente de data/raw/ree_capacidad/ y se queda con las
   columnas útiles para un CPD.
2. Geolocaliza cada nudo (el Excel no trae coordenadas):
   a) nombre igual a una subestación de OSM (prioriza las de Red Eléctrica y
      las de mayor tensión),
   b) subestación de OSM cuyo nombre contiene el del nudo,
   c) nombre parecido (difflib >= 0.88),
   d) municipio con el mismo nombre que el nudo (aproximación),
   e) si nada encaja, queda sin coordenadas (se usa solo a nivel de CCAA).
3. Calcula variables por municipio:
   - MW disponibles para demanda en el nudo más cercano y máximos en 25/50 km
   - distancia al nudo más cercano con >= 50 MW y >= 100 MW libres
   - si el nudo más cercano está en concurso de capacidad

Un CPD tiene interfaz de electrónica de potencia, así que para REE es una
instalación "CEP". Se usa como variable principal la capacidad CEP CH (con
cumplimiento de hueco de tensión, que es lo habitual en un CPD con SAI).

Salidas:
    data/processed/ree_nudos_capacidad.csv
    data/processed/municipio_capacidad_red.csv

Uso: python -m src.etl.ree_nudos
"""
from __future__ import annotations

import difflib
import json
import re
import unicodedata
from collections import defaultdict

import numpy as np
import openpyxl
import pandas as pd

from config.settings import RAW_DIR, PROCESSED_DIR
from src.etl.geo import municipios, distancias_km
from src.ingest.common import log

# Columnas del Excel "Ficha Capacidades" (los datos empiezan en la fila 5)
COLS = {
    0: "nudo", 1: "cod_subestacion", 2: "ccaa",
    15: "margen_estatico_demanda_mw",
    85: "cap_disp_cep_ch_mw",    # CPD con SAI que soporta huecos de tensión
    86: "cap_disp_cep_sh_mw",    # CPD sin soportar huecos (más restrictivo)
    87: "cap_disp_no_cep_mw",    # demanda convencional
    84: "motivo_no_otorgable",   # p. ej. "Valor de referencia no acordado", "Nudo de concurso"
    100: "nudo_concurso",
}
CAP = "cap_disp_cep_ch_mw"
_STOP = re.compile(r"\b(SUBESTACION(ES)?|SUBESTACIO|ELECTRICA|SET|ST|SE|STR|DE|DEL|DA|DO|LA|EL|"
                   r"LOS|LAS|L|D|KV|REE|RED|ESPANA|CN|ET|E|T|B)\b")


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().upper()
    s = re.sub(r"\(.*?\)", " ", s)
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    s = _STOP.sub(" ", s)
    s = re.sub(r"\b\d+\b", " ", s)
    return " ".join(s.split())


def _num(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return np.nan


def leer_excel() -> pd.DataFrame:
    ficheros = sorted((RAW_DIR / "ree_capacidad").glob("*.xlsx"))
    if not ficheros:
        raise RuntimeError("No hay Excel de capacidad en data/raw/ree_capacidad/ "
                           "(ejecuta python -m src.ingest.run_all --only ree_capacidad)")
    path = ficheros[-1]
    log.info("Leyendo %s", path.name)
    ws = openpyxl.load_workbook(path, read_only=True, data_only=True)["Ficha Capacidades"]
    filas = []
    for r in ws.iter_rows(min_row=5, values_only=True):
        if not r[0]:
            continue
        f = {nombre: r[i] for i, nombre in COLS.items()}
        filas.append(f)
    df = pd.DataFrame(filas)
    df["tension_kv"] = df["nudo"].str.extract(r"(\d+)\s*$")[0].astype(float)
    for c in ("margen_estatico_demanda_mw", "cap_disp_cep_ch_mw", "cap_disp_cep_sh_mw", "cap_disp_no_cep_mw"):
        df[c] = df[c].map(_num)
    # Celda vacía = hoy no se puede otorgar capacidad (ver motivo_no_otorgable).
    # Se deja NaN aquí; en las variables por municipio cuenta como 0 MW.
    df["motivo_no_otorgable"] = df["motivo_no_otorgable"].fillna("").astype(str).str.strip()
    df["nudo_concurso"] = df["nudo_concurso"].astype(str).str.strip().str.upper().eq("SI")
    df["fichero_ree"] = path.name
    return df


def _indices_osm():
    els = json.loads((RAW_DIR / "osm" / "subestaciones.json").read_text(encoding="utf-8"))["elements"]
    subs = []
    for e in els:
        t = e.get("tags", {})
        c = e.get("center") or {"lat": e.get("lat"), "lon": e.get("lon")}
        if not t.get("name") or c.get("lat") is None:
            continue
        v = [int(x) for x in re.findall(r"\d+", t.get("voltage", ""))]
        prioridad = (2 if "red el" in t.get("operator", "").lower() else 0) + (max(v) / 1e5 if v else 0.5)
        subs.append((_norm(t["name"]), c["lat"], c["lon"], prioridad))
    exacto = defaultdict(list)
    for n, la, lo, p in subs:
        exacto[n].append((p, la, lo))
    return subs, exacto


def geolocalizar(df: pd.DataFrame) -> pd.DataFrame:
    subs, exacto = _indices_osm()
    muni = municipios()
    muni_idx = {_norm(n): (la, lo) for n, la, lo in zip(muni.municipio, muni.lat, muni.lon) if n}
    nombres_osm = list(exacto)
    lat, lon, metodo = [], [], []
    for nudo in df["nudo"]:
        k = _norm(re.sub(r"\s[\d.,]+$", "", str(nudo)))
        cand, m = None, None
        if k in exacto:
            cand, m = exacto[k], "osm_exacto"
        elif k:
            pat = re.compile(r"\b" + re.escape(k) + r"\b")
            c = [(p, la, lo) for n, la, lo, p in subs if pat.search(n)]
            if c:
                cand, m = c, "osm_contiene"
            else:
                parecido = difflib.get_close_matches(k, nombres_osm, n=1, cutoff=0.88)
                if parecido:
                    cand, m = exacto[parecido[0]], "osm_parecido"
                elif k in muni_idx:
                    cand, m = [(0,) + muni_idx[k]], "municipio_homonimo"
        if cand:
            _, la, lo = max(cand)
            lat.append(la); lon.append(lo); metodo.append(m)
        else:
            lat.append(np.nan); lon.append(np.nan); metodo.append("sin_coordenadas")
    df = df.copy()
    df["lat"], df["lon"], df["metodo_geo"] = lat, lon, metodo
    log.info("Geolocalización de nudos: %s", df["metodo_geo"].value_counts().to_dict())
    return df


def _sistema_ccaa(ccaa: str) -> str:
    c = str(ccaa).lower()
    return "baleares" if "balear" in c else "canarias" if "canaria" in c else \
        "ceuta" if "ceuta" in c else "melilla" if "melilla" in c else "peninsula"


def _sistema_ine(ine: str) -> str:
    return {"07": "baleares", "35": "canarias", "38": "canarias",
            "51": "ceuta", "52": "melilla"}.get(str(ine)[:2], "peninsula")


def por_municipio(nudos: pd.DataFrame) -> pd.DataFrame:
    muni = municipios()
    # Una subestación puede tener nudos a 400 y 220 kV: se suma su capacidad libre
    sub = (nudos.dropna(subset=["lat"])
           .groupby(["lat", "lon"], as_index=False)
           .agg(cap=(CAP, lambda s: s.fillna(0).sum()),
                concurso=("nudo_concurso", "any"),
                nudo=("nudo", " / ".join),
                ccaa=("ccaa", "first")))
    d = distancias_km(muni.lat, muni.lon, sub.lat, sub.lon)
    # Las islas, Ceuta y Melilla son sistemas eléctricos aislados: un municipio
    # solo puede conectarse a nudos de su propio sistema
    mismo = (muni["ine"].map(_sistema_ine).to_numpy()[:, None]
             == sub["ccaa"].map(_sistema_ccaa).to_numpy()[None, :])
    d = np.where(mismo, d, np.inf)
    cap = sub["cap"].to_numpy()
    cerca = d.argmin(axis=1)
    out = muni[["ine", "municipio"]].copy()
    out["nudo_ree_cercano"] = sub["nudo"].to_numpy()[cerca]
    dmin = d.min(axis=1)
    out["dist_nudo_ree_km"] = np.where(np.isinf(dmin), np.nan, dmin.round(2))
    out["mw_libres_nudo_cercano"] = cap[cerca]
    out["nudo_cercano_en_concurso"] = sub["concurso"].to_numpy()[cerca]
    for r in (25, 50):
        out[f"mw_libres_max_{r}km"] = np.where(d <= r, cap[None, :], 0).max(axis=1)
    for umbral in (50, 100):
        dd = np.where(cap[None, :] >= umbral, d, np.inf).min(axis=1)
        out[f"dist_nudo_{umbral}mw_km"] = np.where(np.isinf(dd), np.nan, dd.round(2))
    return out


def run() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    nudos = geolocalizar(leer_excel())
    nudos.to_csv(PROCESSED_DIR / "ree_nudos_capacidad.csv", index=False, encoding="utf-8")
    muni = por_municipio(nudos)
    muni.to_csv(PROCESSED_DIR / "municipio_capacidad_red.csv", index=False, encoding="utf-8")
    log.info("Nudos con >0 MW libres para CPD: %d de %d",
             (nudos[CAP] > 0).sum(), len(nudos))


if __name__ == "__main__":
    run()
