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

## 2. ETL (capa processed)

```bash
python -m src.etl.ree_nudos          # capacidad de red por municipio
python -m src.etl.cpd_espana         # BBDD de CPD + competencia por municipio
python -m src.etl.variables_extra    # cables, inundaciones, suelo, aridez...
python -m src.etl.socioeconomico     # población, renta y paro
python -m src.etl.sismicidad         # terremotos cercanos
python -m src.etl.geografia          # municipio -> provincia -> CCAA
python -m src.etl.entorno            # altitud, pendiente, agua, suelo industrial y accesos
```

## 3. Data Warehouse (DuckDB)

```bash
python -m src.warehouse.build        # crea data/warehouse/cpd_warehouse.duckdb
```
Comprueba que no se pierde ningún municipio (8.135) y deja la vista `vw_features_municipio` lista para el modelo. Detalle en [docs/02_data_warehouse.md](docs/02_data_warehouse.md).

## Estructura

| Carpeta | Contenido |
|---|---|
| `config/` | Rutas, años y parámetros comunes |
| `src/ingest/` | Un script por fuente de datos |
| `src/etl/` | Limpieza y transformación (raw -> processed) |
| `src/warehouse/` | Esquema y carga del Data Warehouse (processed -> DuckDB) |
| `src/models/` | Modelos de ML y series temporales |
| `src/dashboard/` | Simulador y cuadro de mando (Streamlit) |
| `docs/` | Documentación del proyecto (informes del entregable) |
| `notebooks/` | Análisis exploratorio |

## Documentación
- [Informe de origen de datos](docs/01_origen_datos.md)
- [Data Warehouse](docs/02_data_warehouse.md)

## Hoja de ruta
- [x] Fase 1: fuentes de datos e ingesta
- [ ] Fase 2: ETL, Data Lake y Data Warehouse (esquema estrella)
- [ ] Fase 3: EDA y correlaciones
- [ ] Fase 4: modelos de ML (clasificador de viabilidad + índice)
- [ ] Fase 5: forecasting de la demanda eléctrica a 10 años
- [ ] Fase 6: simulador y dashboard
- [ ] Fase 7: manuales, vídeo y documento de especificación
