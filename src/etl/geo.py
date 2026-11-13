"""Utilidades geográficas compartidas por la ETL."""
from __future__ import annotations

import json

import numpy as np
import pandas as pd

from config.settings import RAW_DIR

R_TIERRA_KM = 6371.0


def municipios() -> pd.DataFrame:
    """Municipios con código INE y centroide (de OSM)."""
    els = json.loads((RAW_DIR / "osm" / "municipios.json").read_text(encoding="utf-8"))["elements"]
    filas = [{"ine": e["tags"]["ine:municipio"], "municipio": e["tags"].get("name"),
              "lat": e["center"]["lat"], "lon": e["center"]["lon"]}
             for e in els if e.get("center") and e.get("tags", {}).get("ine:municipio")]
    return pd.DataFrame(filas).drop_duplicates("ine").reset_index(drop=True)


def distancias_km(lat1, lon1, lat2, lon2) -> np.ndarray:
    """Matriz de distancias haversine (km) entre los puntos 1 (filas) y 2 (columnas)."""
    lat1, lon1 = np.radians(np.asarray(lat1, float))[:, None], np.radians(np.asarray(lon1, float))[:, None]
    lat2, lon2 = np.radians(np.asarray(lat2, float))[None, :], np.radians(np.asarray(lon2, float))[None, :]
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * R_TIERRA_KM * np.arcsin(np.sqrt(a))


# Código de provincia INE -> (nombre, comunidad autónoma)
PROVINCIAS = {
    "01": ("Araba/Álava", "País Vasco"), "02": ("Albacete", "Castilla-La Mancha"),
    "03": ("Alicante/Alacant", "Comunitat Valenciana"), "04": ("Almería", "Andalucía"),
    "05": ("Ávila", "Castilla y León"), "06": ("Badajoz", "Extremadura"),
    "07": ("Balears, Illes", "Illes Balears"), "08": ("Barcelona", "Cataluña"),
    "09": ("Burgos", "Castilla y León"), "10": ("Cáceres", "Extremadura"),
    "11": ("Cádiz", "Andalucía"), "12": ("Castellón/Castelló", "Comunitat Valenciana"),
    "13": ("Ciudad Real", "Castilla-La Mancha"), "14": ("Córdoba", "Andalucía"),
    "15": ("Coruña, A", "Galicia"), "16": ("Cuenca", "Castilla-La Mancha"),
    "17": ("Girona", "Cataluña"), "18": ("Granada", "Andalucía"),
    "19": ("Guadalajara", "Castilla-La Mancha"), "20": ("Gipuzkoa", "País Vasco"),
    "21": ("Huelva", "Andalucía"), "22": ("Huesca", "Aragón"), "23": ("Jaén", "Andalucía"),
    "24": ("León", "Castilla y León"), "25": ("Lleida", "Cataluña"),
    "26": ("Rioja, La", "La Rioja"), "27": ("Lugo", "Galicia"),
    "28": ("Madrid", "Comunidad de Madrid"), "29": ("Málaga", "Andalucía"),
    "30": ("Murcia", "Región de Murcia"), "31": ("Navarra", "Comunidad Foral de Navarra"),
    "32": ("Ourense", "Galicia"), "33": ("Asturias", "Principado de Asturias"),
    "34": ("Palencia", "Castilla y León"), "35": ("Palmas, Las", "Canarias"),
    "36": ("Pontevedra", "Galicia"), "37": ("Salamanca", "Castilla y León"),
    "38": ("Santa Cruz de Tenerife", "Canarias"), "39": ("Cantabria", "Cantabria"),
    "40": ("Segovia", "Castilla y León"), "41": ("Sevilla", "Andalucía"),
    "42": ("Soria", "Castilla y León"), "43": ("Tarragona", "Cataluña"),
    "44": ("Teruel", "Aragón"), "45": ("Toledo", "Castilla-La Mancha"),
    "46": ("Valencia/València", "Comunitat Valenciana"), "47": ("Valladolid", "Castilla y León"),
    "48": ("Bizkaia", "País Vasco"), "49": ("Zamora", "Castilla y León"),
    "50": ("Zaragoza", "Aragón"), "51": ("Ceuta", "Ceuta"), "52": ("Melilla", "Melilla"),
}


def norm_nombre(s) -> str:
    """Normaliza nombres de lugar para cruzarlos entre fuentes:
    sin tildes, minúsculas y con el artículo pospuesto quitado
    ("Coruña, A" == "A Coruña", "Rozas de Madrid, Las" == "Las Rozas de Madrid")."""
    import re
    import unicodedata
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    s = s.split("/")[0]                                 # "Alicante/Alacant" -> "alicante"
    m = re.match(r"^(.*),\s*(el|la|los|las|l'|els|les|a|o|os|as|lo)\s*$", s)
    if m:
        s = f"{m.group(2)} {m.group(1)}"
    s = re.sub(r"\b(el|la|los|las|l|els|les|a|o|os|as|lo|de|del|d)\b", " ", s)
    return " ".join(re.sub(r"[^a-z0-9 ]", " ", s).split())
