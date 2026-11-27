"""
Utilidades compartidas para ingesta, trazabilidad y escritura de datos.

- http_get(): reintentos limitados solo para fallos transitorios.
- save_raw(): guarda los ficheros Bronze atómicamente y registra la descarga.
- etl_operation(): registra ejecuciones ETL en el manifiesto JSON Lines.
- Watermarks: posición incremental o checksum de snapshot por fuente.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Generator

import requests

from config.settings import RAW_DIR, HTTP_TIMEOUT, HTTP_RETRIES, USER_AGENT

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("ingest")

MANIFEST = RAW_DIR / "_manifest.jsonl"
WATERMARKS = RAW_DIR / "_watermarks.json"


class DataIngestionError(RuntimeError):
    """Base exception for ingestion and source data failures."""


class APIRequestError(DataIngestionError):
    """HTTP request failure that exposes its response status when available."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class DataValidationError(DataIngestionError):
    """Downloaded or transformed data failed validation."""


def _append_manifest(record: dict[str, Any]) -> None:
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    with MANIFEST.open("a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")


def _retry_after(response: requests.Response) -> float | None:
    value = response.headers.get("Retry-After")
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            when = parsedate_to_datetime(value)
            if when.tzinfo is None:
                when = when.replace(tzinfo=timezone.utc)
            return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None


def http_get(url: str, params: dict | None = None, method: str = "GET",
             data: dict | None = None, headers: dict | None = None,
             retries: int = HTTP_RETRIES) -> requests.Response:
    """Make a bounded HTTP request, retrying only transient failures."""
    if retries < 1:
        raise ValueError("retries debe ser al menos 1")
    h = {"User-Agent": USER_AGENT}
    if headers:
        h.update(headers)

    for attempt in range(1, retries + 1):
        try:
            response = requests.request(method, url, params=params, data=data,
                                        headers=h, timeout=HTTP_TIMEOUT)
        except (requests.ConnectionError, requests.Timeout) as exc:
            if attempt == retries:
                raise APIRequestError(
                    f"No se pudo conectar a {url} tras {retries} intentos: {exc}"
                ) from exc
            wait = min(5 * 2 ** (attempt - 1), 60)
            log.warning("Intento %d/%d fallido (%s). Reintento en %ds",
                        attempt, retries, exc, wait)
            time.sleep(wait)
            continue
        except requests.RequestException as exc:
            raise APIRequestError(f"Petición inválida a {url}: {exc}") from exc

        status = response.status_code
        if 400 <= status < 500 and status != 429:
            raise APIRequestError(
                f"Error HTTP permanente {status} al solicitar {url}: {response.text[:500]}",
                status_code=status,
            )
        if status == 429 or status >= 500:
            if attempt == retries:
                raise APIRequestError(
                    f"Error HTTP transitorio {status} al solicitar {url} "
                    f"tras {retries} intentos: {response.text[:500]}",
                    status_code=status,
                )
            wait = _retry_after(response) if status == 429 else None
            if wait is None:
                wait = min(5 * 2 ** (attempt - 1), 60)
            log.warning("Intento %d/%d: HTTP %d. Reintento en %.1fs",
                        attempt, retries, status, wait)
            time.sleep(wait)
            continue
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise APIRequestError(f"Error HTTP al solicitar {url}: {exc}", status) from exc
        return response

    raise APIRequestError(f"No se pudo completar la petición a {url}")


def raw_path(source: str, filename: str) -> Path:
    """Return a path inside the Bronze layer and create its parent directory."""
    path = RAW_DIR / source / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def valid_raw_file(path: Path, validator=None) -> bool:
    """Check that a cached raw file is non-empty and optionally source-valid."""
    if not path.is_file() or path.stat().st_size == 0:
        return False
    if validator is not None:
        try:
            return bool(validator(path))
        except (OSError, ValueError, TypeError, AttributeError, json.JSONDecodeError):
            return False
    suffix = path.suffix.lower()
    if suffix == ".json":
        try:
            json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            return False
    return True


def cache_is_fresh(path: Path, max_age_seconds: float | None = None,
                   validator=None) -> bool:
    """Return whether a cache exists, validates, and is within its optional TTL."""
    if not valid_raw_file(path, validator=validator):
        return False
    if max_age_seconds is None:
        return True
    age = datetime.now(timezone.utc).timestamp() - path.stat().st_mtime
    return age <= max_age_seconds


def write_watermark(source: str, position: str, **metadata: Any) -> None:
    """Atomically update one source watermark without replacing other sources."""
    WATERMARKS.parent.mkdir(parents=True, exist_ok=True)
    existing: dict[str, Any] = {}
    if WATERMARKS.exists():
        try:
            existing = json.loads(WATERMARKS.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise DataValidationError(f"Watermark file is invalid: {WATERMARKS}") from exc
        if not isinstance(existing, dict):
            raise DataValidationError(f"Watermark file must contain an object: {WATERMARKS}")
    existing[source] = {
        "position": position,
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        **metadata,
    }
    tmp = WATERMARKS.with_name(WATERMARKS.name + ".tmp")
    tmp.write_text(json.dumps(existing, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, WATERMARKS)


def read_watermark(source: str) -> dict[str, Any] | None:
    """Read the most recent successfully committed watermark for a source."""
    if not WATERMARKS.exists():
        return None
    try:
        values = json.loads(WATERMARKS.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise DataValidationError(f"Watermark file is invalid: {WATERMARKS}") from exc
    if not isinstance(values, dict):
        raise DataValidationError(f"Watermark file must contain an object: {WATERMARKS}")
    return values.get(source)


def save_raw(source: str, filename: str, content: bytes | str | dict | list,
             url: str = "", watermark: str | None = None, validator=None,
             status: str = "success") -> Path:
    """Atomically save raw content and append a download record."""
    if status not in {"success", "partial"}:
        raise ValueError(f"Unsupported download status: {status}")
    path = raw_path(source, filename)
    if isinstance(content, (dict, list)):
        payload = json.dumps(content, ensure_ascii=False).encode("utf-8")
    elif isinstance(content, str):
        payload = content.encode("utf-8")
    else:
        payload = content
    if not payload:
        raise DataValidationError(f"Refusing to save an empty response to {path}")

    tmp = path.with_name(path.name + ".tmp")
    try:
        tmp.write_bytes(payload)
        if not tmp.is_file() or tmp.stat().st_size == 0:
            raise DataValidationError(f"Downloaded file is empty: {path}")
        if validator is not None and not validator(tmp):
            raise DataValidationError(f"Downloaded file failed validation: {path}")
        os.replace(tmp, path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
    record = {
        "record_type": "download",
        "source": source,
        "file": str(path.relative_to(RAW_DIR)),
        "url": url,
        "bytes": path.stat().st_size,
        "downloaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "status": status,
    }
    _append_manifest(record)
    if watermark is not None and status == "success":
        write_watermark(source, watermark, file=record["file"])
    log.info("Guardado %s (%.1f KB)", path.relative_to(RAW_DIR), path.stat().st_size / 1024)
    return path


@contextmanager
def etl_operation(operation: str, source: str, inputs=(), outputs=(),
                  input_records: Any = None,
                  output_records: Any = None) -> Generator[dict[str, Any], None, None]:
    """Record an ETL operation and re-raise any error after logging its failure."""
    started = datetime.now(timezone.utc)
    details: dict[str, Any] = {
        "input_records": input_records,
        "output_records": output_records,
    }
    try:
        yield details
    except BaseException as exc:
        ended = datetime.now(timezone.utc)
        _append_manifest({
            "record_type": "etl",
            "operation": operation,
            "source": source,
            "started_at": started.isoformat(timespec="seconds"),
            "ended_at": ended.isoformat(timespec="seconds"),
            "status": "failed",
            "input_files": [str(p) for p in inputs],
            "output_files": [str(p) for p in outputs],
            **details,
            "error": {"type": type(exc).__name__, "message": str(exc)},
        })
        raise
    else:
        ended = datetime.now(timezone.utc)
        _append_manifest({
            "record_type": "etl",
            "operation": operation,
            "source": source,
            "started_at": started.isoformat(timespec="seconds"),
            "ended_at": ended.isoformat(timespec="seconds"),
            "status": "success",
            "input_files": [str(p) for p in inputs],
            "output_files": [str(p) for p in outputs],
            **details,
        })


def file_sha256(path: Path) -> str:
    """Return a stable content checksum for source snapshots."""
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
