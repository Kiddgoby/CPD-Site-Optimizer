"""
Ingesta del directorio de CPD del Observatorio Español de Data Centers.
https://observatorioedc.com/directorio/

Es el inventario público más completo de CPD en España (~240 instalaciones):
operativos, en construcción y en planificación, con operador, tipo
(hiperescala, colocation, on-premise, edge...), potencia IT en MW cuando se
conoce, dirección y coordenadas.

Para el proyecto sirve para:
- La etiqueta del modelo (municipios donde ya hay CPD).
- Medir competencia / saturación de cada zona (MW operativos y en cartera).
- Comparar nuestro emplazamiento con los proyectos reales.

No hay API: se lee la página del directorio y luego la ficha de cada CPD,
con 1 s de pausa entre peticiones (uso académico, sin cargar el servidor).
Es reanudable: cada ficha se guarda por separado.
"""
from __future__ import annotations

import html as htmllib
import re
import time

from src.ingest.common import http_get, save_raw, raw_path, log

SOURCE = "cpd_observatorio"
DIRECTORIO = "https://observatorioedc.com/directorio/"
BROWSER = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/130.0 Safari/537.36"}
ETIQUETAS = {"Operador", "Tipo", "Ubicación", "Estado", "Potencia IT", "Superficie",
             "PUE", "Tier", "Inversión", "Año", "Apertura", "Empleo", "Agua", "Refrigeración"}


def _texto(fragmento: str) -> list[str]:
    """Convierte HTML en una lista de trozos de texto (uno por nodo)."""
    t = re.sub(r"<(script|style|svg)[\s\S]*?</\1>", " ", fragmento)
    trozos = [htmllib.unescape(x).strip() for x in re.split(r"<[^>]+>", t)]
    return [x for x in trozos if x]


def _tarjetas(html: str) -> list[dict]:
    tarjetas = []
    for m in re.finditer(r'<a class="obs-dc-card"([\s\S]*?)</a>', html):
        bloque = m.group(1)
        attr = dict(re.findall(r'(href|data-id|data-region|data-estado|data-search)="([^"]*)"', bloque))
        def clase(c):
            x = re.search(rf'class="{c}">([\s\S]*?)</', bloque)
            return htmllib.unescape(x.group(1)).strip() if x else None
        tags = [htmllib.unescape(x).strip()
                for x in re.findall(r'class="obs-dc-card__tag"[^>]*>([^<]*)<', bloque)]
        tarjetas.append({
            "url": attr.get("href"), "id": attr.get("data-id"),
            "region": attr.get("data-region"), "estado": attr.get("data-estado"),
            "operador": clase("obs-dc-card__operador"),
            "instalacion": clase("obs-dc-card__instalacion"),
            "tipo": tags[0] if tags else None,
        })
    return tarjetas


def _ficha(url: str) -> dict:
    h = http_get(url, headers=BROWSER).text
    datos = {}
    mapa = re.search(r'<div class="obs-dcp__map"([^>]*)>', h)
    if mapa:
        for k, v in re.findall(r'data-([a-z-]+)="([^"]*)"', mapa.group(1)):
            datos[k] = htmllib.unescape(v)
    a, b = h.find("Información general"), h.find("obs-dcp__map-wrap")
    if a > 0:
        trozos = _texto(h[a:b])[1:]
        for i, t in enumerate(trozos[:-1]):
            if t in ETIQUETAS and trozos[i + 1] not in ETIQUETAS:
                datos.setdefault(t, trozos[i + 1])
    return datos


def run() -> None:
    if not raw_path(SOURCE, "directorio.json").exists():
        html = http_get(DIRECTORIO, headers=BROWSER).text
        tarjetas = _tarjetas(html)
        log.info("Observatorio: %d CPD en el directorio", len(tarjetas))
        save_raw(SOURCE, "directorio.json", tarjetas, DIRECTORIO)
    import json
    tarjetas = json.loads(raw_path(SOURCE, "directorio.json").read_text(encoding="utf-8"))
    pendientes = [t for t in tarjetas if t.get("url") and t.get("id")
                  and not raw_path(SOURCE, f"fichas/{t['id']}.json").exists()]
    log.info("Observatorio: %d fichas pendientes", len(pendientes))
    for t in pendientes:
        try:
            save_raw(SOURCE, f"fichas/{t['id']}.json", {**t, **_ficha(t["url"])}, t["url"])
        except Exception as e:
            log.warning("Ficha %s: %s", t["url"], e)
        time.sleep(1.0)


if __name__ == "__main__":
    run()
