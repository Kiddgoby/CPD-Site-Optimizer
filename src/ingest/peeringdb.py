"""
Ingesta PeeringDB (API pública, sin clave): https://www.peeringdb.com/apidocs/

- Instalaciones (data centers de colocation) en España con coordenadas,
  nº de redes, IXPs y operadores -> etiqueta "ya hay CPD" y conectividad.
- Puntos neutros de Internet (IXP) en España -> distancia a nodos principales.
"""
from __future__ import annotations

import json

from config.settings import RAW_DIR
from src.ingest.common import (cache_is_fresh, file_sha256, http_get, log, save_raw,
                               read_watermark, write_watermark)

SOURCE = "peeringdb"
API = "https://www.peeringdb.com/api"


def run() -> None:
    for obj in ("fac", "ix"):
        filename = f"{obj}_es.json"
        cached = RAW_DIR / SOURCE / filename
        validator = lambda p: (
            isinstance(json.loads(p.read_text(encoding="utf-8")).get("data"), list)
            and bool(json.loads(p.read_text(encoding="utf-8"))["data"])
        )
        if cache_is_fresh(cached, 7 * 24 * 60 * 60, validator=validator):
            watermark_key = f"{SOURCE}:{obj}"
            if read_watermark(watermark_key) is None:
                data = json.loads(cached.read_text(encoding="utf-8"))
                write_watermark(watermark_key, f"snapshot-sha256:{file_sha256(cached)}",
                                file=filename, records=len(data["data"]))
            log.info("PeeringDB: se reutilizan datos válidos (%s)", filename)
            continue
        url = f"{API}/{obj}"
        data = http_get(url, params={"country": "ES"}).json()
        if not isinstance(data, dict) or not isinstance(data.get("data"), list):
            raise ValueError(f"Respuesta inválida de PeeringDB para {obj}")
        path = save_raw(SOURCE, filename, data, url)
        write_watermark(f"{SOURCE}:{obj}", f"snapshot-sha256:{file_sha256(path)}",
                        file=path.name, records=len(data["data"]))


if __name__ == "__main__":
    run()
