# CPD-Site-Optimizer

Plataforma Big Data que predice la **viabilidad de instalar un Centro de Procesamiento de Datos (CPD)** en cualquier municipio español, a partir de variables energéticas, climáticas, hídricas, demográficas, económicas y de infraestructuras.

Proyecto 3, Máster FP en Inteligencia Artificial y Big Data (Stucom). Entrega: 15/12/2026.

## Arquitectura

```
Fuentes públicas ──► INGESTA ──► Data Lake ─────────────────────────► Data Warehouse ──► Modelos ML ──► Simulador + Dashboard
(INE, SEPE, REE,     src/ingest   data/raw  (bronze, tal cual)          data/warehouse      src/models      src/dashboard
 AEMET, OSM,                      data/processed (silver, limpio)       (DuckDB, estrella)                  (Streamlit)
 PeeringDB, EMSC)                 ▲ ETL: src/etl
```

## Puesta en marcha

```bash
git clone https://github.com/Kiddgoby/CPD-Site-Optimizer.git
cd CPD-Site-Optimizer
git checkout marc
python -m venv .venv
.venv\Scripts\activate        # Windows  (Linux/Mac: source .venv/bin/activate)
pip install -r requirements.txt
```

**Clave de AEMET** (gratuita, llega por email): https://opendata.aemet.es/centrodedescargas/altaUsuario
Crea un fichero `.env` en la raíz con:
```
AEMET_API_KEY=tu_clave
```

## 1. Ingesta (capa raw)

```bash
python -m src.ingest.run_all                    # todo (~1 h por AEMET y OSM)
python -m src.ingest.run_all --only ine sepe    # solo algunas fuentes
```
Los scripts son reanudables: si se cortan, al relanzarlos siguen donde lo dejaron.

## Estructura

| Carpeta | Contenido |
|---|---|
| `config/` | Rutas, años y parámetros comunes |
| `src/ingest/` | Un script por fuente de datos |
| `src/etl/` | Limpieza, transformación y carga al Data Warehouse |
| `src/models/` | Modelos de ML y series temporales |
| `src/dashboard/` | Simulador y cuadro de mando (Streamlit) |
| `docs/` | Documentación del proyecto (informes del entregable) |
| `notebooks/` | Análisis exploratorio |

## Documentación
- [Informe de origen de datos](docs/01_origen_datos.md)

## Hoja de ruta
- [x] Fase 1: fuentes de datos e ingesta
- [ ] Fase 2: ETL, Data Lake y Data Warehouse (esquema estrella)
- [ ] Fase 3: EDA y correlaciones
- [ ] Fase 4: modelos de ML (clasificador de viabilidad + índice)
- [ ] Fase 5: forecasting de la demanda eléctrica a 10 años
- [ ] Fase 6: simulador y dashboard
- [ ] Fase 7: manuales, vídeo y documento de especificación
