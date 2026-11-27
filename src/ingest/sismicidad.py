"""
Ingesta paginada de sismicidad (EMSC primario, USGS como respaldo).

Ambos FDSN endpoints usan offset basado en 1; cada catálogo se guarda solo
después de completar todas sus páginas correctamente.
"""
from __future__ import annotations

import argparse
import csv
import io
from datetime import datetime, timezone

from config.settings import BBOX_PENINSULA, BBOX_CANARIAS, RAW_DIR
from src.ingest.common import (APIRequestError, DataValidationError, cache_is_fresh,
                               file_sha256, http_get, log, save_raw,
                               read_watermark, write_watermark)

SOURCE = "sismicidad"
EMSC = "https://www.seismicportal.eu/fdsnws/event/1/query"
USGS = "https://earthquake.usgs.gov/fdsnws/event/1/query"
PAGE_SIZE = 20000


def _read_page(text: str, delimiter: str) -> tuple[list[str] | None, list[list[str]]]:
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return None, []
    rows = list(csv.reader(io.StringIO("\n".join(lines)), delimiter=delimiter))
    if not rows:
        return None, []
    header = rows[0]
    normalized = header[0].lstrip("#").strip().lower() if header else ""
    expected = "eventid" if delimiter == "|" else "time"
    if normalized != expected:
        raise DataValidationError(f"Unexpected seismic response header: {header[:3]}")
    return header, [row for row in rows[1:] if row and any(field.strip() for field in row)]


def _event_identity(header: list[str], row: list[str], delimiter: str) -> str:
    if delimiter == "|":
        index = 0
    else:
        normalized = [column.strip().lower().lstrip("#") for column in header]
        index = normalized.index("id") if "id" in normalized else 0
    if index < len(row) and row[index].strip():
        return row[index].strip()
    return delimiter.join(row)


def _fetch_pages(url: str, params: dict, delimiter: str) -> str:
    """Fetch all pages, deduplicate page overlaps, and return one source file."""
    offset = 1
    header: list[str] | None = None
    result_rows: list[list[str]] = []
    seen: set[str] = set()
    while True:
        page_params = {**params, "limit": PAGE_SIZE, "offset": offset}
        response = http_get(url, params=page_params)
        page_header, rows = _read_page(response.text, delimiter)
        if not rows:
            if page_header is not None and header is None:
                header = page_header
            break
        if header is None:
            header = page_header
        elif page_header != header:
            raise DataValidationError("Seismic pagination returned inconsistent headers")
        for row in rows:
            identity = _event_identity(header, row, delimiter)
            if identity not in seen:
                seen.add(identity)
                result_rows.append(row)
        offset += len(rows)
    if header is None:
        raise DataValidationError("Seismic API returned no recognizable header")
    output = io.StringIO(newline="")
    writer = csv.writer(output, delimiter=delimiter, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(result_rows)
    return output.getvalue()


def _valid_catalog(path, delimiter: str) -> bool:
    try:
        header, rows = _read_page(path.read_text(encoding="utf-8-sig"), delimiter)
    except (OSError, UnicodeError, csv.Error, DataValidationError):
        return False
    return header is not None and (bool(rows) or len(header) > 1)


def _reuse_or_seed_watermark(key: str, path) -> bool:
    delimiter = "|" if path.suffix == ".txt" else ","
    if not cache_is_fresh(path, validator=lambda p: _valid_catalog(p, delimiter)):
        return False
    if read_watermark(key) is None:
        write_watermark(key, f"snapshot-sha256:{file_sha256(path)}", file=path.name)
    log.info("Sismicidad: se reutiliza el catálogo válido %s", path.name)
    return True


def _merge_catalog(existing: str, update: str, delimiter: str) -> tuple[str, int]:
    header, old_rows = _read_page(existing, delimiter)
    new_header, new_rows = _read_page(update, delimiter)
    if header is None or new_header != header:
        raise DataValidationError("Cannot merge seismic catalogs with different headers")
    merged = []
    seen = set()
    for row in old_rows + new_rows:
        identity = _event_identity(header, row, delimiter)
        if identity not in seen:
            seen.add(identity)
            merged.append(row)
    output = io.StringIO(newline="")
    writer = csv.writer(output, delimiter=delimiter, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(merged)
    return output.getvalue(), len(merged)


def _fetch_source(url: str, params: dict, delimiter: str, source: str, name: str,
                  previous: dict | None = None) -> None:
    key = f"{SOURCE}:{source}:{name}"
    request_params = dict(params)
    started = datetime.now(timezone.utc)
    previous_position = previous.get("position") if previous else None
    if previous_position and not previous_position.startswith("snapshot-sha256:"):
        request_params["updatedafter"] = previous_position
    update = _fetch_pages(url, request_params, delimiter)
    filename = f"{source}_{name}.{'txt' if source == 'emsc' else 'csv'}"
    path = RAW_DIR / SOURCE / filename
    count = 0
    if previous_position and path.is_file() and _valid_catalog(
            path, delimiter):
        content, count = _merge_catalog(
            path.read_text(encoding="utf-8-sig"), update, delimiter
        )
    else:
        content = update
        _, rows = _read_page(content, delimiter)
        count = len(rows)
    path = save_raw(SOURCE, filename, content, url,
                    validator=lambda p: _valid_catalog(p, delimiter))
    write_watermark(key, started.isoformat(timespec="seconds"),
                    file=filename, snapshot_sha256=file_sha256(path), records=count)


def run(refresh: bool = False) -> None:
    for name, (south, west, north, east) in {
        "peninsula": BBOX_PENINSULA,
        "canarias": BBOX_CANARIAS,
    }.items():
        params = {
            "starttime": "1990-01-01",
            "minmagnitude": 3.0,
            "minlatitude": south,
            "maxlatitude": north,
            "minlongitude": west,
            "maxlongitude": east,
            "format": "text",
        }
        emsc_path = RAW_DIR / SOURCE / f"emsc_{name}.txt"
        usgs_path = RAW_DIR / SOURCE / f"usgs_{name}.csv"
        if not refresh and (_reuse_or_seed_watermark(f"{SOURCE}:emsc:{name}", emsc_path)
                            or _reuse_or_seed_watermark(f"{SOURCE}:usgs:{name}", usgs_path)):
            continue
        emsc_watermark = read_watermark(f"{SOURCE}:emsc:{name}")
        try:
            _fetch_source(EMSC, params, "|", "emsc", name, emsc_watermark)
        except (APIRequestError, DataValidationError) as exc:
            log.warning("EMSC falló (%s), uso USGS", exc)
            fallback_params = {**params, "format": "csv"}
            usgs_watermark = read_watermark(f"{SOURCE}:usgs:{name}")
            try:
                _fetch_source(USGS, fallback_params, ",", "usgs", name, usgs_watermark)
            except (APIRequestError, DataValidationError):
                raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Descarga catálogos sísmicos")
    parser.add_argument("--refresh", action="store_true", help="Ignorar catálogos locales válidos")
    run(refresh=parser.parse_args().refresh)
