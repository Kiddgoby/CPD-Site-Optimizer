# Informe de origen de datos

**Proyecto:** CPD-Site-Optimizer — Sistema de predicción de viabilidad para la instalación de un CPD
**Ámbito:** todos los municipios de España (~8.100)
**Unidad de análisis:** municipio, identificado por su **código INE de 5 dígitos** (2 de provincia + 3 de municipio). Es la clave común con la que se unen todas las fuentes.

---

## 1. Fuentes automatizadas

Todas se descargan con `python -m src.ingest.run_all` y se guardan sin modificar en `data/raw/<fuente>/` (capa *raw* del Data Lake). Cada descarga queda registrada en `data/raw/_manifest.jsonl` con la URL, la fecha y el tamaño.

| # | Fuente | Organismo | Acceso | Formato | Granularidad | Variables que aporta | Bloque del enunciado |
|---|---|---|---|---|---|---|---|
| 1 | Diccionario de municipios | INE | Descarga directa | XLSX | Municipio | Código INE, nombre, provincia | Clave común |
| 2 | Cifras oficiales de población (Padrón) | INE | Descarga directa (`pobmunYY.xlsx`) | XLSX | Municipio | Población 2015, 2020 y último año → evolución demográfica | Demografía |
| 3 | Atlas de Distribución de Renta de los Hogares | INE | API JSON (`servicios.ine.es/wstempus`) | JSON | Municipio | Renta neta media por persona y por hogar | Socioeconómico |
| 4 | Paro registrado por municipio | SEPE (datos.gob.es) | Descarga directa | CSV | Municipio × mes | Parados → tasa de desempleo | Demografía |
| 5 | REData | Red Eléctrica de España | API REST sin clave | JSON | Nacional (mensual) y CCAA (anual) | Demanda eléctrica histórica (**serie temporal**), % renovable, mix de generación | Energía / Forecasting |
| 6 | OpenStreetMap (Overpass API) | Comunidad OSM | API sin clave | JSON | Puntos (lat/lon) | Centroide de cada municipio, subestaciones, centrales, aeropuertos, puertos, salidas de autopista, embalses, polígonos industriales, CPD existentes | Energía, Transporte, Agua, Suelo |
| 7 | PeeringDB | PeeringDB | API sin clave | JSON | Puntos | Data centers de colocation en España y puntos neutros de Internet (IXP) | Telecomunicaciones / **Etiqueta** |
| 8 | AEMET OpenData | AEMET | API REST con clave gratuita | JSON | Estación × mes | Temperatura media y máxima, humedad, viento, precipitación, días con Tmax ≥ 30 ºC | Climatología / Agua |
| 9 | Catálogo sísmico (FDSN) | EMSC (integra datos del IGN); USGS como respaldo | API sin clave | TXT/CSV | Evento (lat/lon) | Terremotos con M ≥ 3 desde 1990 | Riesgo sísmico |

## 2. Fuentes de descarga manual (opcionales)

No tienen API estable, así que se descargan a mano y se dejan en `data/raw/manual/`. Si no están, la ETL las omite y usa las variables aproximadas (*proxies*) indicadas.

| Fuente | Dónde | Variable | Proxy si falta |
|---|---|---|---|
| Informe de Cobertura de Banda Ancha (Excel por municipio) | [conectemos.digital.gob.es](https://conectemos.digital.gob.es) — SETID | % cobertura FTTH ≥ 100 Mbps, % 5G | Distancia al IXP / CPD más cercano |
| Líneas límite municipales | Centro de Descargas del CNIG (IGN) | Superficie → densidad de población | Sin densidad |
| Precio del suelo urbano | Ministerio de Vivienda y Agenda Urbana (estadística de precio de suelo) | €/m² | Renta + población (presión urbana) |

## 3. Cómo se calcula cada variable territorial

| Variable del enunciado | Cálculo | Fuente(s) |
|---|---|---|
| Población, evolución demográfica | Población actual; crecimiento % entre 2015 y el último año | 2 |
| Tasa de desempleo (aprox.) | Parados medios del año / población 16-64 (o / población total) | 2, 4 |
| Nivel económico | Renta neta media por persona | 3 |
| Subestaciones cercanas / capacidad de la red | Nº de subestaciones ≥ 220 kV en 25 km; distancia a la más cercana | 6 |
| % renovables, consumo eléctrico | Asignados desde la CCAA del municipio | 5 |
| Temperatura media / máxima, humedad, viento, días de calor | Interpolación de la estación AEMET más cercana (o media ponderada por distancia inversa de las 3 más cercanas) | 8 |
| Estrés hídrico | Índice: precipitación anual baja + muchos días de calor + lejanía a embalses | 6, 8 |
| Embalses cercanos | Nº de embalses en 30 km | 6 |
| Distancia a autopistas, puertos, aeropuertos | Distancia haversine al elemento más cercano | 6 |
| Puntos neutros, distancia a nodos | Distancia al IXP más cercano | 7 |
| Suelo industrial disponible | Nº de polígonos industriales en el municipio / en 10 km | 6 |
| Riesgo sísmico | Nº de terremotos M ≥ 3 en 50 km y magnitud máxima | 9 |

## 4. Variable objetivo (viabilidad)

"Viabilidad" no existe como dato publicado, así que se construye de dos formas complementarias:

1. **Etiqueta observada:** municipio con al menos un CPD existente (PeeringDB + OSM) = 1; sin CPD = 0. Se entrena un clasificador, y la probabilidad que predice para cada municipio se interpreta como su *viabilidad aprendida*: cuánto se parece a los sitios donde la industria ya ha decidido instalarse.
2. **Índice experto (0-100):** suma ponderada de las variables normalizadas (energía, clima, agua, conectividad, riesgo, coste). Es la base del **simulador**: cambiando los parámetros del CPD (MW, PUE, tipo de refrigeración, WUE) cambian los pesos y las restricciones. Por ejemplo, una refrigeración evaporativa penaliza mucho el estrés hídrico.

**Puntuación final:** combinación de las dos, y el informe de análisis las compara.

## 5. Limitaciones conocidas

- La energía (REE) solo está a nivel de CCAA: dentro de una misma comunidad, la capacidad de la red se diferencia con las subestaciones de OSM.
- OSM es colaborativo y su cobertura de subestaciones o CPD puede estar incompleta.
- Las estaciones de AEMET no cubren todos los municipios: se interpola por cercanía.
- La etiqueta "tiene CPD" está sesgada hacia grandes ciudades (efecto red). Por eso se combina con el índice experto.
