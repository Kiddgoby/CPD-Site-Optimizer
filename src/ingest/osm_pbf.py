"""
Ingesta OpenStreetMap desde los extractos de Geofabrik (alternativa a Overpass).

Los servidores públicos de Overpass se saturan a menudo (HTTP 429/502/504) y
las consultas de toda España no terminan. Aquí se descarga el mapa completo
de España (~1,5 GB) y de Canarias (~60 MB) en formato .osm.pbf y se extraen
los mismos elementos que pide src/ingest/osm.py, en local y sin depender de
ningún servidor:
    https://download.geofabrik.de/europe/spain.html   (incluye Ceuta y Melilla)
    https://download.geofabrik.de/africa/canary-islands.html

Genera los mismos ficheros data/raw/osm/<consulta>.json con el formato de
Overpass ({"elements": [...]}, con "lat"/"lon" en nodos y "center" en vías y
relaciones), así que la ETL no cambia. Los ficheros que ya existan se omiten.

Cómo se calcula el "center" (igual que "out center" de Overpass): centro del
rectángulo que envuelve el elemento. En vías y relaciones largas se usa una
muestra de sus nodos para no cargar millones de coordenadas en memoria; el
error es de metros, irrelevante para calcular distancias en km.

Limitaciones:
- El extracto de Geofabrik incluye una franja estrecha al otro lado de la
  frontera (Portugal, Francia, Andorra, Gibraltar). Overpass la excluía con
  el filtro de área; aquí puede colarse algún elemento fronterizo.
- Las relaciones que solo contienen otras relaciones (sin vías ni nodos)
  se omiten porque no tienen coordenadas propias.

Necesita la librería osmium (pyosmium), que lee el .pbf en C++.
"""
from __future__ import annotations

import time
from pathlib import Path

import osmium
import requests

from config.settings import RAW_DIR, USER_AGENT
from src.ingest.common import save_raw, raw_path, log
from src.ingest.osm import QUERIES

SOURCE = "osm"
PBF_DIR = RAW_DIR / "osm_pbf"
EXTRACTS = {
    "spain-latest.osm.pbf": "https://download.geofabrik.de/europe/spain-latest.osm.pbf",
    "canary-islands-latest.osm.pbf": "https://download.geofabrik.de/africa/canary-islands-latest.osm.pbf",
}
MAX_NODES_PER_WAY = 20   # muestra de nodos por vía para calcular el centro

# Pares clave=valor que pre-filtra osmium en C++ (solo esto llega a Python)
TAG_PAIRS = [
    ("boundary", "administrative"), ("boundary", "protected_area"), ("boundary", "national_park"),
    ("telecom", "data_center"), ("building", "data_center"),
    ("power", "substation"), ("power", "plant"),
    ("aeroway", "aerodrome"), ("landuse", "port"), ("industrial", "port"),
    ("highway", "motorway_junction"), ("water", "reservoir"), ("landuse", "industrial"),
    ("leisure", "nature_reserve"), ("amenity", "university"), ("amenity", "fire_station"),
]


def _matches(kind: str, t: dict) -> list[str]:
    """Traduce a Python las consultas de osm.QUERIES. kind: 'node' | 'way' | 'relation'."""
    names = []
    if (kind == "relation" and t.get("boundary") == "administrative"
            and t.get("admin_level") == "8" and "ine:municipio" in t):
        names.append("municipios")
    if t.get("telecom") == "data_center" or t.get("building") == "data_center":
        names.append("datacenters")
    if t.get("power") == "substation":
        names.append("subestaciones")
    if t.get("power") == "plant":
        names.append("centrales")
    if t.get("aeroway") == "aerodrome" and "iata" in t:
        names.append("aeropuertos")
    if t.get("landuse") == "port" or t.get("industrial") == "port":
        names.append("puertos")
    if kind == "node" and t.get("highway") == "motorway_junction":
        names.append("autopistas")
    if t.get("water") == "reservoir" and "name" in t:
        names.append("embalses")
    if kind == "way" and t.get("landuse") == "industrial":
        names.append("industrial")
    if (t.get("boundary") in ("protected_area", "national_park")
            or t.get("leisure") == "nature_reserve"):
        names.append("protegidos")
    if t.get("amenity") == "university":
        names.append("universidades")
    if t.get("amenity") == "fire_station":
        names.append("bomberos")
    return names


def _sample(refs: list[int]) -> list[int]:
    if len(refs) <= MAX_NODES_PER_WAY:
        return refs
    step = len(refs) // MAX_NODES_PER_WAY
    return refs[::step] + [refs[-1]]


def _download_extracts() -> None:
    """Descarga los .pbf en streaming (no caben en memoria) con reanudación simple."""
    PBF_DIR.mkdir(parents=True, exist_ok=True)
    for fname, url in EXTRACTS.items():
        dest = PBF_DIR / fname
        if dest.exists():
            log.info("OSM pbf: %s ya descargado (%.0f MB)", fname, dest.stat().st_size / 2**20)
            continue
        part = dest.with_suffix(".part")
        log.info("OSM pbf: descargando %s ...", url)
        for attempt in range(1, 4):
            try:
                done = part.stat().st_size if part.exists() else 0
                headers = {"User-Agent": USER_AGENT}
                if done:
                    headers["Range"] = f"bytes={done}-"
                with requests.get(url, headers=headers, stream=True, timeout=120) as r:
                    r.raise_for_status()
                    mode = "ab" if done and r.status_code == 206 else "wb"
                    with part.open(mode) as f:
                        for chunk in r.iter_content(chunk_size=1 << 20):
                            f.write(chunk)
                part.rename(dest)
                break
            except requests.RequestException as e:
                log.warning("Descarga %s fallida (intento %d/3): %s", fname, attempt, e)
                time.sleep(10 * attempt)
        else:
            raise RuntimeError(f"No se pudo descargar {url}")


def _extract(pbf: Path, wanted: set[str]) -> dict[tuple, dict]:
    """Tres pasadas por el fichero: objetos buscados -> vías de las relaciones -> coordenadas."""
    elements: dict[tuple, dict] = {}
    way_refs: dict[tuple, list[int]] = {}     # elemento (vía) -> nodos muestreados
    rel_members: dict[tuple, tuple] = {}      # elemento (relación) -> (vías, nodos)

    # 1) Objetos que cumplen alguna consulta
    for o in osmium.FileProcessor(pbf).with_filter(osmium.filter.TagFilter(*TAG_PAIRS)):
        kind = "node" if o.is_node() else "way" if o.is_way() else "relation"
        tags = {t.k: t.v for t in o.tags}
        names = [n for n in _matches(kind, tags) if n in wanted]
        if not names:
            continue
        key = (kind, o.id)
        el = {"type": kind, "id": o.id, "tags": tags, "_q": names}
        if kind == "node":
            if not o.location.valid():
                continue
            el["lat"], el["lon"] = o.location.lat, o.location.lon
        elif kind == "way":
            way_refs[key] = _sample([n.ref for n in o.nodes])
        else:
            rel_members[key] = ([m.ref for m in o.members if m.type == "w"],
                                [m.ref for m in o.members if m.type == "n"])
        elements[key] = el
    log.info("OSM pbf %s: %d objetos encontrados", pbf.name, len(elements))

    # 2) Nodos de las vías que forman las relaciones (contornos)
    member_ways = {w for ws, _ in rel_members.values() for w in ws}
    member_way_refs: dict[int, list[int]] = {}
    if member_ways:
        for w in osmium.FileProcessor(pbf, osmium.osm.WAY).with_filter(osmium.filter.IdFilter(member_ways)):
            member_way_refs[w.id] = _sample([n.ref for n in w.nodes])

    # 3) Coordenadas de todos los nodos necesarios
    need = {n for refs in way_refs.values() for n in refs}
    need.update(n for refs in member_way_refs.values() for n in refs)
    need.update(n for _, ns in rel_members.values() for n in ns)
    loc: dict[int, tuple[float, float]] = {}
    if need:
        for n in osmium.FileProcessor(pbf, osmium.osm.NODE).with_filter(osmium.filter.IdFilter(need)):
            if n.location.valid():
                loc[n.id] = (n.location.lat, n.location.lon)

    # Centro del rectángulo envolvente (como "out center" de Overpass)
    def center(node_ids) -> dict | None:
        pts = [loc[i] for i in node_ids if i in loc]
        if not pts:
            return None
        lats, lons = [p[0] for p in pts], [p[1] for p in pts]
        return {"lat": round((min(lats) + max(lats)) / 2, 7), "lon": round((min(lons) + max(lons)) / 2, 7)}

    for key, refs in way_refs.items():
        elements[key]["center"] = center(refs)
    for key, (ws, ns) in rel_members.items():
        ids = [n for w in ws for n in member_way_refs.get(w, [])] + ns
        elements[key]["center"] = center(ids)
    # Fuera los que no tienen coordenadas
    return {k: e for k, e in elements.items() if e.get("lat") is not None or e.get("center")}


def run(only: list[str] | None = None) -> None:
    wanted = {n for n in QUERIES if (not only or n in only)
              and not raw_path(SOURCE, f"{n}.json").exists()}
    if not wanted:
        log.info("OSM pbf: todas las consultas ya están descargadas, se omite")
        return
    log.info("OSM pbf: consultas pendientes: %s", ", ".join(sorted(wanted)))
    _download_extracts()

    elements: dict[tuple, dict] = {}
    for fname in EXTRACTS:
        t0 = time.time()
        elements.update(_extract(PBF_DIR / fname, wanted))
        log.info("OSM pbf %s procesado en %.0f s", fname, time.time() - t0)

    for name in sorted(wanted):
        els = [{k: v for k, v in e.items() if k != "_q"} for e in elements.values() if name in e["_q"]]
        save_raw(SOURCE, f"{name}.json", {"elements": els, "fuente": "geofabrik"}, EXTRACTS["spain-latest.osm.pbf"])
        log.info("OSM %s: %d elementos", name, len(els))


if __name__ == "__main__":
    run()
