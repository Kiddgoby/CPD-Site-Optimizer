"""
Orquestador de la ingesta: descarga todas las fuentes a data/raw/.

Uso (desde la raíz del repo):
    python -m src.ingest.run_all                 # todas las fuentes
    python -m src.ingest.run_all --only ine osm  # solo algunas

Cada fuente es independiente: si una falla, las demás siguen.
Los scripts son reanudables (no vuelven a bajar lo que ya existe).
"""
from __future__ import annotations

import argparse
import traceback

from src.ingest import ine, sepe, ree, osm, peeringdb, aemet, sismicidad, topografia, openmeteo
from src.ingest.common import log

SOURCES = {
    "ine": ine.run,
    "sepe": sepe.run,
    "ree": ree.run,
    "peeringdb": peeringdb.run,
    "sismicidad": sismicidad.run,
    "osm": osm.run,        # lenta (~10-20 min)
    "topografia": topografia.run,  # necesita OSM antes (~10 min)
    "openmeteo": openmeteo.run,    # clima sin clave (~5 min)
    "aemet": aemet.run,    # opcional: solo si hay AEMET_API_KEY en .env
}


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingesta de fuentes al Data Lake (capa raw)")
    ap.add_argument("--only", nargs="*", choices=SOURCES.keys())
    args = ap.parse_args()

    resumen = {}
    for name, fn in SOURCES.items():
        if args.only and name not in args.only:
            continue
        log.info("=" * 60 + f"\nFUENTE: {name}")
        try:
            fn()
            resumen[name] = "OK"
        except Exception as e:
            log.error("Fuente %s fallida: %s\n%s", name, e, traceback.format_exc())
            resumen[name] = f"ERROR: {e}"

    log.info("RESUMEN DE LA INGESTA")
    for k, v in resumen.items():
        log.info("  %-11s %s", k, v)


if __name__ == "__main__":
    main()
