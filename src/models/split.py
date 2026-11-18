"""Partición de los datos en entrenamiento / validación / prueba (60 / 20 / 20).

- Estratificada: los municipios con CPD operativo se reparten por igual entre los tres bloques.
- Agrupada por provincia: todos los municipios de una provincia caen en el mismo bloque, para que
  vecinos casi idénticos (misma red eléctrica, mismo clima) no estén a la vez en train y en test.
- Semilla fija: todos obtenéis exactamente la misma partición.

Uso:
    from src.models.split import cargar_particion
    X_train, y_train, X_val, y_val, X_test, y_test = cargar_particion()
"""
from pathlib import Path

import duckdb
import pandas as pd
from sklearn.model_selection import StratifiedGroupKFold

DB = Path("data/warehouse/cpd_warehouse.duckdb")
ETIQUETA = "tiene_cpd_operativo"
GRUPO = "provincia"
SEMILLA = 178  # elegida entre 300 probadas: es la que más se acerca a 60/20/20 (municipios y positivos)

# Columnas que NO pueden entrar al modelo.
# Fuga de la etiqueta: ya contienen (o incluyen al propio municipio en) los CPD operativos.
FUGA = ["n_cpd_operativos", "mw_cpd_operativos", "n_cpd_operativos_50km",
        "mw_cpd_operativos_50km", "dist_cpd_operativo_km", "n_hiperescala_50km"]
# Identificadores y metadatos sin valor predictivo (o que dejarían al modelo memorizar el sitio).
NO_PREDICTORAS = ["ine", "municipio", "provincia", "ccaa", "lat", "lon", "nudo_ree_cercano",
                  "regulacion_provisional", "anio_poblacion", "anio_renta", "periodo_paro",
                  "valor_tasado_origen"]


def cargar_vista() -> pd.DataFrame:
    con = duckdb.connect(str(DB), read_only=True)
    try:
        return con.execute("SELECT * FROM vw_features_municipio").df()
    finally:
        con.close()


def particionar(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Devuelve {'train','val','test'} con 60/20/20 aproximado, estratificado y por provincia."""
    skf = StratifiedGroupKFold(n_splits=5, shuffle=True, random_state=SEMILLA)
    fold = pd.Series(-1, index=df.index)
    for i, (_, idx) in enumerate(skf.split(df, df[ETIQUETA], groups=df[GRUPO])):
        fold.iloc[idx] = i
    bloque = fold.map({0: "test", 1: "val"}).fillna("train")
    return {b: df[bloque == b] for b in ("train", "val", "test")}


def cargar_particion():
    """Devuelve X_train, y_train, X_val, y_val, X_test, y_test (sin columnas con fuga)."""
    partes = particionar(cargar_vista())
    quitar = FUGA + NO_PREDICTORAS + [ETIQUETA]
    salida = []
    for b in ("train", "val", "test"):
        d = partes[b]
        salida += [d.drop(columns=quitar), d[ETIQUETA].astype(int)]
    return tuple(salida)


if __name__ == "__main__":
    partes = particionar(cargar_vista())
    total = sum(len(d) for d in partes.values())
    for b, d in partes.items():
        print(f"{b:5s} {len(d):5d} municipios ({len(d) / total:.0%})  "
              f"con CPD: {int(d[ETIQUETA].sum()):3d}  provincias: {d[GRUPO].nunique()}")
    sol = set(partes['train'][GRUPO]) & set(partes['test'][GRUPO])
    print("Provincias compartidas entre train y test:", len(sol))
    X = cargar_particion()[0]
    print("Variables predictoras:", X.shape[1])
