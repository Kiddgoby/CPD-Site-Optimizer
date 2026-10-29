"""
Utilidades compartidas por todos los scripts de ingesta.

- http_get(): descarga con reintentos y backoff exponencial.
- save_raw(): guarda el fichero en la capa raw del Data Lake y registra
  la descarga en un manifiesto (trazabilidad: fuente, URL, fecha, tamaño).
"""
from __future__ import annotations

import json
import time
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests

from config.settings import RAW_DIR, HTTP_TIMEOUT, HTTP_RETRIES, USER_AGENT

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("ingest")

MANIFEST = RAW_DIR / "_manifest.jsonl"


def http_get(url: str, params: dict | None = None, method: str = "GET",
             data: dict | None = None, headers: dict | None = None,
             retries: int = HTTP_RETRIES) -> requests.Response:
    """Petición HTTP con reintentos. Lanza excepción si todos los intentos fallan."""
    h = {"User-Agent": USER_AGENT}
    if headers:
        h.update(headers)
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.request(method, url, params=params, data=data,
                                 headers=h, timeout=HTTP_TIMEOUT)
            # 429 = demasiadas peticiones; 5xx = error del servidor -> reintentar
            if r.status_code == 429 or r.status_code >= 500:
                raise requests.HTTPError(f"HTTP {r.status_code}", response=r)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            last_err = e
            wait = 5 * 2 ** (attempt - 1)
            log.warning("Intento %d/%d fallido (%s). Reintento en %ds", attempt, retries, e, wait)
            time.sleep(wait)
    raise RuntimeError(f"No se pudo descargar {url}: {last_err}")


def raw_path(source: str, filename: str) -> Path:
    """Ruta dentro de la capa raw: data/raw/<fuente>/<fichero>."""
    p = RAW_DIR / source / filename
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def save_raw(source: str, filename: str, content: bytes | str | dict | list, url: str = "") -> Path:
    """Guarda contenido en la capa raw y añade una línea al manifiesto."""
    path = raw_path(source, filename)
    if isinstance(content, (dict, list)):
        path.write_text(json.dumps(content, ensure_ascii=False), encoding="utf-8")
    elif isinstance(content, str):
        path.write_text(content, encoding="utf-8")
    else:
        path.write_bytes(content)

    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    with MANIFEST.open("a", encoding="utf-8") as f:
        f.write(json.dumps({
            "source": source,
            "file": str(path.relative_to(RAW_DIR)),
            "url": url,
            "bytes": path.stat().st_size,
            "downloaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }, ensure_ascii=False) + "\n")
    log.info("Guardado %s (%.1f KB)", path.relative_to(RAW_DIR), path.stat().st_size / 1024)
    return path
