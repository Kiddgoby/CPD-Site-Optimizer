"""
Construye el Data Warehouse en DuckDB a partir del Parquet de data/silver.

Esquema en estrella (ver schema.sql y docs/02_data_warehouse.md):
- fact_municipio: una fila por municipio (código INE de 5 dígitos), con la
  etiqueta del modelo (tiene_cpd_operativo).
- Dimensiones: geografía (municipio -> provincia -> CCAA, con la regulación
  de cada CCAA), red eléctrica, competencia, clima, socioeconómica, riesgos
  y conectividad.
- Tablas con otro grano: fact_cpd (cada CPD) y nudo_ree (cada nudo de REE).
- Vista vw_features_municipio: todo unido en una tabla plana para el modelo.

Comprobaciones antes de dar por buena la carga (si alguna falla, el
.duckdb anterior no se toca):
- Todas las tablas por municipio tienen exactamente los mismos municipios
  que la geografía, y la vista final también (no se pierde ninguno al unir).
- Las claves foráneas las valida DuckDB al insertar.

Requisitos: haber ejecutado la ETL (python -m src.etl.<script>) para
geografia, socioeconomico, sismicidad, ree_nudos, cpd_espana, variables_extra y entorno.

Salida: data/gold/cpd_warehouse.duckdb y data/gold/features.parquet
Uso: python -m src.warehouse.build
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import duckdb

from config.settings import PROCESSED_DIR, WAREHOUSE_DIR, DATA_DIR
from src.etl.common import logged_etl
from src.ingest.common import log

DB_PATH = WAREHOUSE_DIR / "cpd_warehouse.duckdb"
FEATURES_PATH = WAREHOUSE_DIR / "features.parquet"
SCHEMA = Path(__file__).with_name("schema.sql")
MUNICIPIOS_ESPERADOS = 8135   # referencia de la ETL actual (municipios con código INE en OSM)

# Vista temporal -> (Parquet, script de la ETL que lo genera)
FUENTES = {
    "src_geo": (PROCESSED_DIR / "municipio_geografia.parquet", "geografia"),
    "src_socio": (PROCESSED_DIR / "municipio_socioeconomico.parquet", "socioeconomico"),
    "src_sismo": (PROCESSED_DIR / "municipio_sismicidad.parquet", "sismicidad"),
    "src_red": (PROCESSED_DIR / "municipio_capacidad_red.parquet", "ree_nudos"),
    "src_nudos": (PROCESSED_DIR / "ree_nudos_capacidad.parquet", "ree_nudos"),
    "src_comp": (PROCESSED_DIR / "municipio_competencia_cpd.parquet", "cpd_espana"),
    "src_cpd": (PROCESSED_DIR / "cpd_espana.parquet", "cpd_espana"),
    "src_extra": (PROCESSED_DIR / "municipio_variables_extra.parquet", "variables_extra"),
    "src_entorno": (PROCESSED_DIR / "municipio_entorno.parquet", "entorno"),
    "src_reg": (DATA_DIR / "reference" / "regulacion_ccaa.csv", None),
}
# Tabla -> SELECT que la alimenta (se inserta por nombre de columna)
CARGAS = {
    "dim_ccaa": """
        SELECT g.cod_ccaa, g.ccaa,
               r.puntuacion_regulatoria_1a5 AS puntuacion_regulatoria,
               coalesce(r.notas ILIKE '%provisional%', TRUE) AS regulacion_provisional,
               r.ley_agilizacion_proyectos,
               CAST(r.incentivos_fiscales_cpd AS VARCHAR) AS incentivos_fiscales_cpd,
               r.plazo_tramitacion_meses,
               CAST(r.restricciones_agua AS VARCHAR) AS restricciones_agua,
               r.fuente_url, r.notas
        FROM (SELECT DISTINCT cod_ccaa, ccaa FROM src_geo) g
        LEFT JOIN src_reg r ON r.ccaa = g.ccaa""",
    "dim_provincia": "SELECT DISTINCT cod_provincia, provincia, cod_ccaa FROM src_geo",
    "dim_geografia": "SELECT ine, municipio, cod_provincia, lat, lon FROM src_geo",
    "dim_red_electrica": "SELECT * EXCLUDE (municipio) FROM src_red",
    "dim_competencia": """
        SELECT * EXCLUDE (municipio, n_cpd_operativos, mw_cpd_operativos, tiene_cpd_operativo)
        FROM src_comp""",
    "dim_clima": "SELECT ine, indice_aridez, dias_tmedia_menor_18 FROM src_extra",
    "dim_socioeconomica": """
        SELECT s.* EXCLUDE (municipio), e.valor_tasado_eur_m2, e.valor_tasado_origen
        FROM src_socio s JOIN src_extra e USING (ine)""",
    "dim_riesgos": """
        SELECT e.ine, e.pct_inundable_t100, e.pct_inundable_t500, s.* EXCLUDE (ine, municipio)
        FROM src_extra e JOIN src_sismo s USING (ine)""",
    "dim_conectividad": """
        SELECT ine, amarre_cable_cercano, dist_amarre_cable_km, cables_intl_amarre_cercano,
               amarres_cable_100km, dist_universidades_km, n_universidades_30km,
               dist_bomberos_km, n_bomberos_30km
        FROM src_extra""",
    "dim_entorno": "SELECT * EXCLUDE (municipio) FROM src_entorno",
    "fact_municipio": """
        SELECT g.ine, g.cod_provincia, g.cod_ccaa,
               c.tiene_cpd_operativo::BOOLEAN AS tiene_cpd_operativo,
               c.n_cpd_operativos, c.mw_cpd_operativos
        FROM src_geo g JOIN src_comp c USING (ine)""",
    # Los CPD que no se pudieron asignar a un municipio conocido quedan con ine vacío
    "fact_cpd": """
        SELECT row_number() OVER () AS id_cpd,
               c.* EXCLUDE (ine, municipio, ccaa),
               CASE WHEN g.ine IS NOT NULL THEN c.ine END AS ine
        FROM src_cpd c LEFT JOIN src_geo g ON g.ine = c.ine""",
    "nudo_ree": "SELECT row_number() OVER () AS id_nudo, * FROM src_nudos",
}
TABLAS_MUNICIPIO = ["dim_geografia", "dim_red_electrica", "dim_competencia", "dim_clima",
                    "dim_socioeconomica", "dim_riesgos", "dim_conectividad", "dim_entorno", "fact_municipio"]


def _sql_texto(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _registrar_fuentes(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Load each Silver Parquet file and the reference CSV into temp tables."""
    filas = {}
    for vista, (path, etl) in FUENTES.items():
        if not path.exists():
            como = f"python -m src.etl.{etl}" if etl else "el fichero de referencia del repo"
            raise FileNotFoundError(f"Falta {path}. Genéralo con: {como}")
        if path.suffix.lower() == ".csv":
            reader = f"read_csv({_sql_texto(path.as_posix())}, header = true)"
        else:
            reader = f"read_parquet({_sql_texto(path.as_posix())})"
        con.execute(f"CREATE TEMP TABLE {vista} AS SELECT * FROM {reader}")
        filas[vista] = con.execute(f"SELECT count(*) FROM {vista}").fetchone()[0]
    return filas


def _comprobar(con: duckdb.DuckDBPyConnection, filas_fuente: dict[str, int]) -> None:
    n_geo = con.execute("SELECT count(*) FROM dim_geografia").fetchone()[0]
    errores = []
    if n_geo != filas_fuente["src_geo"]:
        errores.append(f"dim_geografia tiene {n_geo} filas y el Parquet {filas_fuente['src_geo']}")
    for t in TABLAS_MUNICIPIO + ["vw_features_municipio"]:
        n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        faltan = con.execute(f"SELECT count(*) FROM dim_geografia g ANTI JOIN {t} t USING (ine)").fetchone()[0]
        if n != n_geo or faltan:
            errores.append(f"{t}: {n} filas, {faltan} municipios de la geografía sin fila")
    sin_reg = con.execute("SELECT list(ccaa) FROM dim_ccaa WHERE puntuacion_regulatoria IS NULL").fetchone()[0]
    if sin_reg:
        errores.append(f"CCAA sin fila en regulacion_ccaa.csv (¿nombre distinto?): {sin_reg}")
    if errores:
        raise RuntimeError("Comprobaciones fallidas:\n  - " + "\n  - ".join(errores))
    if n_geo != MUNICIPIOS_ESPERADOS:
        log.warning("Hay %d municipios y se esperaban %d: revisa si OSM ha cambiado", n_geo, MUNICIPIOS_ESPERADOS)
    log.info("OK: las %d tablas por municipio y la vista tienen los %d municipios",
             len(TABLAS_MUNICIPIO), n_geo)


def _informe(con: duckdb.DuckDBPyConnection) -> None:
    for t, n in con.execute("SELECT tabla, filas FROM meta_carga ORDER BY tabla").fetchall():
        log.info("  %-20s %6d filas", t, n)
    df = con.execute("SELECT * FROM vw_features_municipio").df()
    vacios = df.isna().sum()
    vacios = vacios[vacios > 0].sort_values(ascending=False)
    log.info("Vista vw_features_municipio: %d filas x %d columnas", *df.shape)
    for col, n in vacios.items():
        log.info("  vacíos %-28s %5d (%.1f %%)", col, n, 100 * n / len(df))
    sin_ine = con.execute("SELECT count(*) FROM fact_cpd WHERE ine IS NULL").fetchone()[0]
    if sin_ine:
        log.info("fact_cpd: %d CPD sin municipio asignado", sin_ine)
    prov = con.execute("SELECT count(*) FROM dim_ccaa WHERE regulacion_provisional").fetchone()[0]
    if prov:
        log.warning("Regulación: %d de 19 CCAA con puntuación provisional (sin validar)", prov)


def _exportar_features(con: duckdb.DuckDBPyConnection) -> Path:
    """Export and validate the final feature view to an adjacent temporary file."""
    tmp = FEATURES_PATH.with_name(FEATURES_PATH.name + ".tmp")
    tmp.unlink(missing_ok=True)
    target = _sql_texto(tmp.as_posix())
    con.execute(f"COPY (SELECT * FROM vw_features_municipio) TO {target} (FORMAT PARQUET)")
    expected = con.execute("SELECT count(*) FROM vw_features_municipio").fetchone()[0]
    actual, municipios, invalid_ine = con.execute(
        f"SELECT count(*), count(DISTINCT ine), "
        f"count(*) FILTER (WHERE ine IS NULL OR length(CAST(ine AS VARCHAR)) <> 5) "
        f"FROM read_parquet({target})"
    ).fetchone()
    expected_columns = [row[0] for row in con.execute(
        "DESCRIBE SELECT * FROM vw_features_municipio"
    ).fetchall()]
    exported_columns = [row[0] for row in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet({target})"
    ).fetchall()]
    if (actual != expected or municipios != expected or invalid_ine
            or exported_columns != expected_columns):
        tmp.unlink(missing_ok=True)
        raise RuntimeError(
            "features.parquet validation failed: "
            f"rows={actual}/{expected}, unique_ine={municipios}, "
            f"invalid_ine={invalid_ine}, columns_match={exported_columns == expected_columns}"
        )
    return tmp


@logged_etl("build_warehouse", "silver",
            [PROCESSED_DIR, DATA_DIR / "reference" / "regulacion_ccaa.csv"],
            [DB_PATH, FEATURES_PATH])
def run() -> None:
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = DB_PATH.with_suffix(".duckdb.tmp")
    features_tmp = FEATURES_PATH.with_name(FEATURES_PATH.name + ".tmp")
    tmp.unlink(missing_ok=True)
    con = duckdb.connect(str(tmp))
    try:
        filas_fuente = _registrar_fuentes(con)
        con.execute(SCHEMA.read_text(encoding="utf-8"))
        ahora = datetime.now()
        for tabla, select in CARGAS.items():
            con.execute(f"INSERT INTO {tabla} BY NAME {select}")
            n = con.execute(f"SELECT count(*) FROM {tabla}").fetchone()[0]
            origen = ", ".join(p.name for v, (p, _) in FUENTES.items() if v in select)
            con.execute("INSERT INTO meta_carga VALUES (?, ?, ?, ?)", [tabla, origen, n, ahora])
        _comprobar(con, filas_fuente)
        _informe(con)
        features_tmp = _exportar_features(con)
    except Exception:
        con.close()
        tmp.unlink(missing_ok=True)
        features_tmp.unlink(missing_ok=True)
        raise
    con.close()
    try:
        os.replace(tmp, DB_PATH)
    except PermissionError as e:
        features_tmp.unlink(missing_ok=True)
        raise PermissionError(f"No se puede sustituir {DB_PATH}: ciérralo si está abierto "
                              f"en otro programa (DBeaver, otro Python...). La versión nueva "
                              f"está en {tmp}") from e
    try:
        os.replace(features_tmp, FEATURES_PATH)
    except OSError as e:
        features_tmp.unlink(missing_ok=True)
        raise OSError(f"El almacén se actualizó, pero no se pudo sustituir {FEATURES_PATH}; "
                      f"el Parquet anterior se conserva") from e
    log.info("Data Warehouse creado: %s (%.1f MB)", DB_PATH, DB_PATH.stat().st_size / 2**20)


if __name__ == "__main__":
    run()
