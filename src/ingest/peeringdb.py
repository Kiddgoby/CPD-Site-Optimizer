"""
Ingesta PeeringDB (API pública, sin clave): https://www.peeringdb.com/apidocs/

- Instalaciones (data centers de colocation) en España con coordenadas,
  nº de redes, IXPs y operadores -> etiqueta "ya hay CPD" y conectividad.
- Puntos neutros de Internet (IXP) en España -> distancia a nodos principales.
"""
from __future__ import annotations

from src.ingest.common import http_get, save_raw

SOURCE = "peeringdb"
API = "https://www.peeringdb.com/api"


def run() -> None:
    for obj in ("fac", "ix"):
        url = f"{API}/{obj}"
        data = http_get(url, params={"country": "ES"}).json()
        save_raw(SOURCE, f"{obj}_es.json", data, url)


if __name__ == "__main__":
    run()
