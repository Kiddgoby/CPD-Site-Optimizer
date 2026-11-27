from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pandas as pd
import requests

from config import settings
from src import data_migration
from src.etl import common as etl_common
from src.etl import sismicidad as etl_sismicidad
from src.ingest import common, peeringdb, sismicidad
from src.models import split
from src.warehouse import build


class PathAndMigrationTests(unittest.TestCase):
    def test_layer_paths_and_split_use_central_configuration(self):
        self.assertEqual(settings.RAW_DIR, settings.DATA_DIR / "bronze")
        self.assertEqual(settings.PROCESSED_DIR, settings.DATA_DIR / "silver")
        self.assertEqual(settings.WAREHOUSE_DIR, settings.DATA_DIR / "gold")
        self.assertEqual(split.DB, settings.WAREHOUSE_DIR / "cpd_warehouse.duckdb")

    def test_migration_moves_legacy_dirs_and_refuses_conflicts(self):
        with tempfile.TemporaryDirectory() as temp:
            data_dir = Path(temp)
            (data_dir / "raw").mkdir()
            (data_dir / "raw" / "_manifest.jsonl").write_text("history\n", encoding="utf-8")
            with patch.object(data_migration, "DATA_DIR", data_dir):
                moves = data_migration.migrate(dry_run=True)
                self.assertEqual(len(moves), 1)
                self.assertTrue((data_dir / "raw").exists())
                data_migration.migrate()
                self.assertEqual((data_dir / "bronze" / "_manifest.jsonl").read_text(
                    encoding="utf-8"
                ), "history\n")
                (data_dir / "processed").mkdir()
                (data_dir / "silver").mkdir()
                with self.assertRaises(FileExistsError):
                    data_migration.migrate()
                self.assertTrue((data_dir / "processed").exists())


class ParquetTests(unittest.TestCase):
    def test_parquet_atomic_write_preserves_ine_as_text_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp) / "municipalities.parquet"
            frame = pd.DataFrame({"ine": ["00123", "00007"], "value": [1, 2]})
            etl_common.write_parquet_atomic(frame, target)
            etl_common.write_parquet_atomic(frame, target)
            result = pd.read_parquet(target, engine="pyarrow")
            self.assertEqual(result.ine.tolist(), ["00123", "00007"])
            self.assertEqual(len(result), 2)

    def test_failed_parquet_replace_keeps_previous_file(self):
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp) / "existing.parquet"
            target.write_bytes(b"old-version")
            with patch.object(etl_common.os, "replace", side_effect=OSError("locked")):
                with self.assertRaisesRegex(OSError, "locked"):
                    etl_common.write_parquet_atomic(
                        pd.DataFrame({"ine": ["00001"], "value": [1]}), target
                    )
            self.assertEqual(target.read_bytes(), b"old-version")
            self.assertFalse(target.with_name(target.name + ".tmp").exists())

    def test_warehouse_import_reads_parquet_and_reference_csv(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            parquet = root / "municipalities.parquet"
            reference = root / "regulacion.csv"
            pd.DataFrame({"ine": pd.Series(["00123"], dtype="string")}).to_parquet(
                parquet, engine="pyarrow", index=False
            )
            reference.write_text("region,value\nA,1\n", encoding="utf-8")
            with patch.dict(build.FUENTES, {
                "municipios": (parquet, None),
                "referencia": (reference, None),
            }, clear=True):
                connection = duckdb.connect(":memory:")
                try:
                    counts = build._registrar_fuentes(connection)
                    self.assertEqual(counts, {"municipios": 1, "referencia": 1})
                    self.assertEqual(connection.execute(
                        "SELECT ine FROM municipios"
                    ).fetchone()[0], "00123")
                finally:
                    connection.close()

    def test_features_export_is_validated_and_keeps_municipality_identifier(self):
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp) / "features.parquet"
            connection = duckdb.connect(":memory:")
            connection.execute(
                "CREATE VIEW vw_features_municipio AS "
                "SELECT CAST('00123' AS VARCHAR) AS ine, 2 AS feature"
            )
            with patch.object(build, "FEATURES_PATH", target):
                output = build._exportar_features(connection)
                self.assertTrue(output.exists())
                exported = connection.execute(
                    f"SELECT * FROM read_parquet('{output.as_posix()}')"
                ).fetchone()
                self.assertEqual(exported, ("00123", 2))
            connection.close()


class ManifestAndAtomicWriteTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)
        self.raw_patch = patch.object(common, "RAW_DIR", self.root / "bronze")
        self.manifest_patch = patch.object(common, "MANIFEST", self.root / "bronze" / "_manifest.jsonl")
        self.watermark_patch = patch.object(common, "WATERMARKS", self.root / "bronze" / "_watermarks.json")
        self.raw_patch.start()
        self.manifest_patch.start()
        self.watermark_patch.start()
        self.addCleanup(self.raw_patch.stop)
        self.addCleanup(self.manifest_patch.stop)
        self.addCleanup(self.watermark_patch.stop)

    def test_download_and_etl_success_failure_have_distinct_manifest_records(self):
        common.save_raw("sample", "one.json", {"ok": True}, "https://example.test")
        with common.etl_operation("transform", "sample", outputs=["out.parquet"]) as details:
            details["output_records"] = 1
        with self.assertRaisesRegex(ValueError, "broken"):
            with common.etl_operation("failed", "sample"):
                raise ValueError("broken")
        records = [json.loads(line) for line in common.MANIFEST.read_text(
            encoding="utf-8"
        ).splitlines()]
        self.assertEqual([row["record_type"] for row in records], ["download", "etl", "etl"])
        self.assertEqual([row["status"] for row in records],
                         ["success", "success", "failed"])
        self.assertEqual(records[-1]["error"]["message"], "broken")

    def test_raw_write_is_atomic_and_watermark_advances_only_after_success(self):
        path = common.save_raw("sample", "rows.json", {"rows": [1]}, watermark="v1")
        self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["rows"], [1])
        self.assertEqual(common.read_watermark("sample")["position"], "v1")
        with self.assertRaises(common.DataValidationError):
            common.save_raw("sample", "empty.json", b"", watermark="v2")
        self.assertEqual(common.read_watermark("sample")["position"], "v1")
        self.assertFalse((path.parent / "empty.json.tmp").exists())

    def test_invalid_raw_output_is_not_recorded_as_success(self):
        manifest = common.MANIFEST
        with self.assertRaises(common.DataValidationError):
            common.save_raw("sample", "bad.json", "not-json",
                            validator=lambda path: path.suffix == ".valid")
        self.assertFalse((self.root / "bronze" / "sample" / "bad.json").exists())
        self.assertFalse(manifest.exists())

    def test_failed_etl_does_not_advance_watermark(self):
        common.write_watermark("source", "position-1")
        with self.assertRaises(RuntimeError):
            with common.etl_operation("failed-transform", "source"):
                raise RuntimeError("transform failed")
        self.assertEqual(common.read_watermark("source")["position"], "position-1")

    def test_cache_validation_rejects_empty_and_reuses_valid_json(self):
        path = self.root / "cache.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}", encoding="utf-8")
        self.assertTrue(common.cache_is_fresh(path))
        path.write_text("", encoding="utf-8")
        self.assertFalse(common.cache_is_fresh(path))


class SeismicTests(unittest.TestCase):
    def test_emsc_text_and_usgs_csv_normalize_to_common_schema(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            emsc = root / "emsc.txt"
            usgs = root / "usgs.csv"
            emsc.write_text(
                "#EventID|Time|Latitude|Longitude|Depth|Magnitude\n"
                "e1|2025-01-01T00:00:00|40.0|-3.0|10|4.2\n",
                encoding="utf-8",
            )
            usgs.write_text(
                "time,latitude,longitude,depth,mag,id\n"
                "2025-01-01T00:00:20Z,40.01,-3.01,8,4.1,u1\n",
                encoding="utf-8",
            )
            first = etl_sismicidad._read_events(emsc)
            second = etl_sismicidad._read_events(usgs)
            self.assertEqual(list(first.columns), etl_sismicidad.EVENT_COLUMNS)
            self.assertEqual(first.iloc[0].event_id, "e1")
            self.assertEqual(second.iloc[0].event_id, "u1")
            self.assertEqual(len(etl_sismicidad._deduplicate(
                pd.concat([first, second], ignore_index=True)
            )), 1)

    def test_page_fetch_uses_one_based_offsets_and_removes_overlap(self):
        responses = [
            "#EventID|Time|Latitude|Longitude\n"
            "e1|2025-01-01|40|-3\ne2|2025-01-02|41|-4\n",
            "#EventID|Time|Latitude|Longitude\n"
            "e2|2025-01-02|41|-4\ne3|2025-01-03|42|-5\n",
            "#EventID|Time|Latitude|Longitude\n",
        ]
        calls = []

        def fake_get(url, params):
            self.assertEqual(url, sismicidad.EMSC)
            calls.append(params["offset"])
            response = MagicMock()
            response.text = responses[len(calls) - 1]
            return response

        with patch.object(sismicidad, "PAGE_SIZE", 2), patch.object(
            sismicidad, "http_get", side_effect=fake_get
        ):
            content = sismicidad._fetch_pages(sismicidad.EMSC, {}, "|")
        self.assertEqual(calls, [1, 3, 5])
        self.assertEqual(content.count("e2|"), 1)
        self.assertIn("e3|", content)

    def test_failed_page_does_not_save_partial_catalog_or_advance_watermark(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            body = MagicMock()
            body.text = "#EventID|Time|Latitude|Longitude\ne1|2025-01-01|40|-3\n"
            with patch.object(sismicidad, "RAW_DIR", root), \
                    patch.object(common, "RAW_DIR", root), \
                    patch.object(common, "MANIFEST", root / "_manifest.jsonl"), \
                    patch.object(common, "WATERMARKS", root / "_watermarks.json"), \
                    patch.object(sismicidad, "http_get",
                                 side_effect=[body, common.APIRequestError("page failed", 500)]):
                with self.assertRaises(common.APIRequestError):
                    sismicidad._fetch_source(
                        sismicidad.EMSC, {"starttime": "1990-01-01"}, "|",
                        "emsc", "peninsula"
                    )
                self.assertFalse((root / "sismicidad" / "emsc_peninsula.txt").exists())
                self.assertIsNone(common.read_watermark("sismicidad:emsc:peninsula"))
                self.assertFalse((root / "_manifest.jsonl").exists())

    def test_seismic_refresh_uses_previous_watermark_and_merges_idempotently(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source_dir = root / "sismicidad"
            source_dir.mkdir()
            catalog = source_dir / "emsc_peninsula.txt"
            catalog.write_text(
                "#EventID|Time|Latitude|Longitude\n"
                "e1|2025-01-01|40|-3\n",
                encoding="utf-8",
            )
            previous = {"position": "2025-02-01T00:00:00+00:00"}
            response = MagicMock()
            response.text = "#EventID|Time|Latitude|Longitude\n"

            def fake_get(url, params):
                self.assertEqual(url, sismicidad.EMSC)
                self.assertEqual(params["updatedafter"], previous["position"])
                return response

            with patch.object(sismicidad, "RAW_DIR", root), \
                    patch.object(common, "RAW_DIR", root), \
                    patch.object(common, "MANIFEST", root / "_manifest.jsonl"), \
                    patch.object(common, "WATERMARKS", root / "_watermarks.json"), \
                    patch.object(sismicidad, "http_get", side_effect=fake_get):
                sismicidad._fetch_source(
                    sismicidad.EMSC, {"starttime": "1990-01-01"}, "|",
                    "emsc", "peninsula", previous
                )
                self.assertEqual(catalog.read_text(encoding="utf-8").count("e1|"), 1)
                self.assertNotEqual(
                    common.read_watermark("sismicidad:emsc:peninsula")["position"],
                    previous["position"],
                )

    def test_peeringdb_reuses_recent_valid_snapshots(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "peeringdb"
            source.mkdir()
            for name in ("fac_es.json", "ix_es.json"):
                (source / name).write_text('{"data":[{"id":1}]}', encoding="utf-8")
            with patch.object(peeringdb, "RAW_DIR", root), \
                    patch.object(common, "RAW_DIR", root), \
                    patch.object(common, "MANIFEST", root / "_manifest.jsonl"), \
                    patch.object(common, "WATERMARKS", root / "_watermarks.json"), \
                    patch.object(peeringdb, "http_get") as request:
                peeringdb.run()
            request.assert_not_called()


class HttpRetryTests(unittest.TestCase):
    def _response(self, status, headers=None):
        response = MagicMock()
        response.status_code = status
        response.headers = headers or {}
        response.text = "response body"
        response.raise_for_status.return_value = None
        return response

    def test_400_and_404_are_not_retried(self):
        for status in (400, 404):
            with self.subTest(status=status), patch.object(
                common.requests, "request", return_value=self._response(status)
            ) as request:
                with self.assertRaises(common.APIRequestError) as raised:
                    common.http_get("https://example.test", retries=3)
                self.assertEqual(raised.exception.status_code, status)
                self.assertEqual(request.call_count, 1)

    def test_429_respects_retry_after_and_500_retries(self):
        for first in (429, 500):
            with self.subTest(status=first), patch.object(
                common.requests, "request",
                side_effect=[self._response(first, {"Retry-After": "2"}), self._response(200)],
            ) as request, patch.object(common.time, "sleep") as sleep:
                response = common.http_get("https://example.test", retries=2)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(request.call_count, 2)
                self.assertEqual(sleep.call_count, 1)
                if first == 429:
                    sleep.assert_called_once_with(2.0)

    def test_timeout_retries_and_exhaustion_raises_custom_error(self):
        with patch.object(common.requests, "request",
                          side_effect=[requests.Timeout("slow"), self._response(200)]) as request, \
                patch.object(common.time, "sleep"):
            self.assertEqual(common.http_get("https://example.test", retries=2).status_code, 200)
            self.assertEqual(request.call_count, 2)
        with patch.object(common.requests, "request",
                          side_effect=requests.ConnectionError("offline")), \
                patch.object(common.time, "sleep"):
            with self.assertRaises(common.APIRequestError):
                common.http_get("https://example.test", retries=2)


if __name__ == "__main__":
    unittest.main()
