"""
Riesgo sísmico por municipio a partir de catálogos EMSC y USGS.

Salida: data/silver/municipio_sismicidad.parquet
Uso: python -m src.etl.sismicidad
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from config.settings import RAW_DIR, PROCESSED_DIR
from src.etl.common import logged_etl, write_parquet_atomic
from src.etl.geo import municipios, distancias_km
from src.ingest.common import DataValidationError, log

RADIO_KM = 50
BLOQUE = 500
OUTPUT = PROCESSED_DIR / "municipio_sismicidad.parquet"
EVENT_COLUMNS = ["event_id", "lat", "lon", "mag", "date", "source"]


def _column(frame: pd.DataFrame, *names: str) -> pd.Series:
    columns = {str(c).strip().lower().lstrip("#"): c for c in frame.columns}
    for name in names:
        if name.lower() in columns:
            return frame[columns[name.lower()]]
    return pd.Series(pd.NA, index=frame.index)


def _read_events(path: Path) -> pd.DataFrame:
    """Parse one EMSC pipe-delimited text or USGS CSV response."""
    try:
        if path.suffix.lower() == ".txt":
            frame = pd.read_csv(path, sep="|", dtype=str)
            source = "emsc"
        else:
            frame = pd.read_csv(path, dtype=str)
            source = "usgs"
    except (OSError, pd.errors.ParserError, UnicodeError) as exc:
        raise DataValidationError(f"Cannot parse seismic file {path}: {exc}") from exc

    events = pd.DataFrame({
        "event_id": _column(frame, "EventID", "id"),
        "lat": pd.to_numeric(_column(frame, "Latitude", "latitude"), errors="coerce"),
        "lon": pd.to_numeric(_column(frame, "Longitude", "longitude"), errors="coerce"),
        "mag": pd.to_numeric(_column(frame, "Magnitude", "mag"), errors="coerce"),
        "date": pd.to_datetime(_column(frame, "Time", "time"), errors="coerce", utc=True),
        "source": source,
    })
    events["event_id"] = events.event_id.astype("string").str.strip()
    valid_id = events.event_id.notna() & events.event_id.ne("")
    generated = (
        events.date.astype("string") + "|" + events.lat.round(4).astype("string") + "|"
        + events.lon.round(4).astype("string") + "|" + events.mag.round(1).astype("string")
    )
    events.loc[~valid_id, "event_id"] = source + ":" + generated[~valid_id]
    events = events.dropna(subset=["lat", "lon", "mag", "date"])
    events = events[events.lat.between(-90, 90) & events.lon.between(-180, 180)]
    return events[EVENT_COLUMNS].drop_duplicates("event_id", keep="first")


def _deduplicate(events: pd.DataFrame) -> pd.DataFrame:
    """Prefer EMSC IDs and match otherwise-unrelated source IDs by time/location."""
    if events.empty:
        return events
    unique = events.drop_duplicates(["source", "event_id"], keep="first")
    kept = []
    for row in unique.itertuples(index=False):
        duplicate = False
        if row.source == "usgs" and kept:
            prior = pd.DataFrame(kept)
            same_time = (prior["date"] - row.date).abs().dt.total_seconds() <= 60
            candidates = prior[same_time]
            if not candidates.empty:
                distances = distancias_km(
                    [row.lat], [row.lon], candidates.lat.to_numpy(), candidates.lon.to_numpy()
                )[0]
                duplicate = bool(
                    ((distances <= 10) &
                     (np.abs(candidates.mag.to_numpy() - row.mag) <= 0.3)).any()
                )
        if not duplicate:
            kept.append(row._asdict())
    return pd.DataFrame(kept, columns=EVENT_COLUMNS)


def _eventos() -> pd.DataFrame:
    folder = RAW_DIR / "sismicidad"
    emsc_files = sorted(folder.glob("emsc_*.txt"))
    usgs_files = sorted(folder.glob("usgs_*.csv"))
    if not emsc_files and not usgs_files:
        raise FileNotFoundError("Sin datos sísmicos (python -m src.ingest.run_all --only sismicidad)")
    frames = [_read_events(path) for path in emsc_files + usgs_files]
    events = _deduplicate(pd.concat(frames, ignore_index=True))
    if events.empty:
        raise DataValidationError("Los ficheros sísmicos no contienen eventos válidos")
    log.info("Sismicidad: %d terremotos normalizados (%s)",
             len(events), events.source.value_counts().to_dict())
    return events


@logged_etl("sismicidad", "emsc+usgs",
            [RAW_DIR / "sismicidad", RAW_DIR / "osm"], [OUTPUT])
def run() -> None:
    muni = municipios()
    events = _eventos()
    mag = events.mag.to_numpy()
    m4 = mag >= 4
    n, n4, mmax, d4 = [], [], [], []
    for i in range(0, len(muni), BLOQUE):
        block = muni.iloc[i:i + BLOQUE]
        distances = distancias_km(block.lat, block.lon, events.lat, events.lon)
        near = distances < RADIO_KM
        n.append(near.sum(axis=1))
        n4.append((near & m4).sum(axis=1))
        mmax.append(np.where(near, mag, -np.inf).max(axis=1))
        d4.append(distances[:, m4].min(axis=1) if m4.any()
                  else np.full(len(block), np.inf))
    out = muni[["ine", "municipio"]].copy()
    out["n_sismos_50km"] = np.concatenate(n)
    out["n_sismos_m4_50km"] = np.concatenate(n4)
    out["mag_max_50km"] = np.concatenate(mmax)
    out.loc[np.isinf(out.mag_max_50km), "mag_max_50km"] = np.nan
    out["dist_sismo_m4_km"] = np.concatenate(d4).round(1)
    out.loc[np.isinf(out.dist_sismo_m4_km), "dist_sismo_m4_km"] = np.nan
    write_parquet_atomic(out, OUTPUT)
    log.info("Sismicidad: %d municipios, %d con algún terremoto en %d km",
             len(out), (out.n_sismos_50km > 0).sum(), RADIO_KM)


if __name__ == "__main__":
    run()
