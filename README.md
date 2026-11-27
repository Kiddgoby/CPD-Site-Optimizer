# CPD-Site-Optimizer

Plataforma Big Data que predice la **viabilidad de instalar un Centro de Procesamiento de Datos (CPD)** en cualquier municipio español, a partir de variables energéticas, climáticas, hídricas, demográficas, económicas y de infraestructuras.

Proyecto 3, Máster FP en Inteligencia Artificial y Big Data (Stucom). Entrega: 15/12/2026.

## Arquitectura

```
Fuentes públicas ──► INGESTA ──► Data Lake ─────────────────────────► Gold ───────────► Modelos ML ──► Simulador + Dashboard
(INE, SEPE, REE,     src/ingest   data/bronze (original)             data/gold         src/models      src/dashboard
 AEMET, OSM,                      data/silver (Parquet)               DuckDB + Parquet                 (Streamlit)
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

## Migración de datos existentes

Los ficheros locales se conservan. Antes de ejecutar la nueva ingesta/ETL, migra
las carpetas antiguas una sola vez (el comando falla sin mover nada si ya existe
alguna carpeta destino):

```bash
python -m src.data_migration --dry-run
python -m src.data_migration
```

`data/raw` pasa a `data/bronze`, `data/processed` a `data/silver` y
`data/warehouse` a `data/gold`. La migración no descarga ni elimina los datos;
`data/reference/regulacion_ccaa.csv` permanece en su ubicación.

## 1. Ingesta (capa Bronze)

```bash
python -m src.ingest.run_all                    # todo (~1 h por AEMET y OSM)
python -m src.ingest.run_all --only ine sepe    # solo algunas fuentes
```
Los scripts reusan ficheros válidos y verifican formatos donde aplica. Las
descargas se escriben primero a temporales y luego se reemplazan atómicamente.
`data/bronze/_manifest.jsonl` conserva el historial de descargas y registra
también el inicio, fin, resultado y entradas/salidas de cada ETL. Los watermarks
fuente-específicos se guardan en `data/bronze/_watermarks.json`; los snapshots
estáticos usan checksum y las consultas incrementales guardan su posición.
Los errores 4xx no se reintentan (salvo 429); 429 respeta `Retry-After`, y 5xx,
errores de conexión y timeout usan reintentos limitados con backoff.

La ingesta sísmica pagina EMSC y usa USGS como respaldo. Ambos servicios
documentan offsets desde 1; `sismicidad` consulta hasta una página vacía y
solo finaliza el fichero y watermark si todas las páginas llegan correctamente.
`python -m src.ingest.sismicidad --refresh` fuerza una actualización; usa
`updatedafter` cuando hay un watermark temporal previo.

## 2. ETL (capa Silver)

```bash
python -m src.etl.ree_nudos          # capacidad de red por municipio
python -m src.etl.cpd_espana         # BBDD de CPD + competencia por municipio
python -m src.etl.variables_extra    # cables, inundaciones, suelo, aridez...
python -m src.etl.socioeconomico     # población, renta y paro
python -m src.etl.sismicidad         # terremotos cercanos
python -m src.etl.geografia          # municipio -> provincia -> CCAA
python -m src.etl.entorno            # altitud, pendiente, agua, suelo industrial y accesos
```

Las salidas ETL son Parquet (`data/silver/*.parquet`) y conservan los códigos
INE como texto. Los CSV originales de las fuentes permanecen sin cambios en
Bronze.

## 3. Gold (DuckDB + Parquet)

```bash
python -m src.warehouse.build        # crea data/gold/cpd_warehouse.duckdb y features.parquet
```
Comprueba que no se pierde ningún municipio (8.135), crea la vista
`vw_features_municipio` y exporta sus columnas a `data/gold/features.parquet`.
Detalle en [docs/02_data_warehouse.md](docs/02_data_warehouse.md).

## Estructura

| Carpeta | Contenido |
|---|---|
| `config/` | Rutas, años y parámetros comunes |
| `src/ingest/` | Un script por fuente de datos |
| `src/etl/` | Limpieza y transformación (Bronze -> Silver Parquet) |
| `src/warehouse/` | Esquema y carga del Data Warehouse (Silver -> Gold) |
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
