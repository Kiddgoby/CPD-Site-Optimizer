"""Shared Silver-layer output helpers."""
from __future__ import annotations

import csv
import json
import os
from functools import wraps
from pathlib import Path
from typing import Callable, Iterable

import pandas as pd
import pyarrow.parquet as pq
from pyarrow.lib import ArrowInvalid

from src.ingest.common import DataValidationError, etl_operation

CODE_COLUMNS = {"ine", "cod_provincia", "cod_ccaa", "cod_subestacion", "id_fuente"}


def write_parquet_atomic(frame: pd.DataFrame, path: Path) -> None:
    """Write a Silver Parquet file, validate it, then atomically replace its target."""
    path.parent.mkdir(parents=True, exist_ok=True)
    output = frame.copy()
    for column in CODE_COLUMNS.intersection(output.columns):
        output[column] = output[column].astype("string")

    tmp = path.with_name(path.name + ".tmp")
    try:
        output.to_parquet(tmp, engine="pyarrow", index=False)
        check = pd.read_parquet(tmp, engine="pyarrow")
        if list(check.columns) != list(output.columns) or len(check) != len(output):
            raise DataValidationError(f"Parquet validation failed for {path}")
        if "ine" in output and not check["ine"].equals(output["ine"]):
            raise DataValidationError(f"INE codes changed during Parquet write: {path}")
        os.replace(tmp, path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def _existing_files(paths: Iterable[Path]) -> list[Path]:
    files = []
    for path in paths:
        if path.is_file():
            files.append(path)
        elif path.is_dir():
            files.extend(sorted(p for p in path.rglob("*") if p.is_file()))
    return files


def _record_count(path: Path) -> int | None:
    try:
        if path.suffix.lower() == ".parquet":
            return pq.ParquetFile(path).metadata.num_rows
        if path.suffix.lower() == ".json":
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, list):
                return len(payload)
            if isinstance(payload, dict):
                for key in ("elements", "data", "features"):
                    if isinstance(payload.get(key), list):
                        return len(payload[key])
                return len(payload)
        if path.suffix.lower() in {".csv", ".txt"}:
            with path.open(encoding="utf-8-sig", newline="") as stream:
                return max(0, sum(1 for _ in csv.reader(stream)) - 1)
    except (OSError, UnicodeError, ValueError, csv.Error, ArrowInvalid):
        return None
    return None


def logged_etl(operation: str, source: str, input_paths: Iterable[Path],
               output_paths: Iterable[Path]) -> Callable:
    """Wrap an ETL entry point with success/failure manifest logging."""
    inputs = tuple(input_paths)
    outputs = tuple(output_paths)

    def decorate(function: Callable) -> Callable:
        @wraps(function)
        def wrapped(*args, **kwargs):
            input_files = _existing_files(inputs)
            with etl_operation(operation, source, input_files, outputs) as details:
                input_counts = {
                    str(path): count for path in input_files
                    if (count := _record_count(path)) is not None
                }
                if input_counts:
                    details["input_records"] = input_counts
                result = function(*args, **kwargs)
                counts = {
                    str(path): count for path in outputs if path.is_file()
                    if (count := _record_count(path)) is not None
                }
                if counts:
                    details["output_records"] = counts
                return result
        return wrapped
    return decorate
