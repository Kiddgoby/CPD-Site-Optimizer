"""
Orquestador de la ingesta: descarga todas las fuentes a data/raw/.

Uso (desde la raíz del repo):
    python -m src.ingest.run_all                 # todas las fuentes
    python -m src.ingest.run_all --only ine osm  # solo algunas

Después, la ETL de las fuentes nuevas:
    python -m src.etl.ree_nudos          # capacidad de red por municipio
    python -m src.etl.cpd_espana         # BBDD de CPD + competencia por municipio
    python -m src.etl.variables_extra    # cables, inundaciones, suelo, aridez...

Cada fuente es independiente: si una falla, las demás siguen.
Los scripts son reanudables (no vuelven a bajar lo que ya existe).
"""
from __future__ import annotations

import argparse
import traceback

from src.ingest import (ine, sepe, ree, osm, peeringdb, aemet, sismicidad, topografia, openmeteo,
                        ree_capacidad, cables, cpd_observatorio, vivienda, inundaciones)
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
    # --- Añadidas en noviembre ---
    "ree_capacidad": ree_capacidad.run,        # MW libres para consumo por nudo (REE)
    "cables": cables.run,                      # cables submarinos (TeleGeography)
    "cpd_observatorio": cpd_observatorio.run,  # directorio de CPD de España (~4 min)
    "vivienda": vivienda.run,                  # valor tasado €/m² (coste del suelo)
    "inundaciones": inundaciones.run,          # zonas inundables SNCZI (necesita OSM, ~30-60 min)
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
