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
| 2 | Cifras oficiales de población (Padrón) | INE | API JSON, tabla 29005 (los `pobmunYY.xlsx` dejaron de publicarse en 2023) | JSON | Municipio × año | Población 2015-2025 → evolución demográfica | Demografía |
| 3 | Atlas de Distribución de Renta de los Hogares | INE | API JSON (`servicios.ine.es/wstempus`, con `tip=M` para tener el código INE) | JSON | Municipio | Renta neta media por persona y por hogar | Socioeconómico |
| 4 | Paro registrado por municipio | SEPE (datos.gob.es) | Descarga directa | CSV | Municipio × mes | Parados → tasa de desempleo | Demografía |
| 5 | REData | Red Eléctrica de España | API REST sin clave | JSON | Nacional (mensual) y CCAA (anual) | Demanda eléctrica histórica (**serie temporal**), % renovable, mix de generación | Energía / Forecasting |
| 6 | OpenStreetMap (Overpass API) | Comunidad OSM | API sin clave | JSON | Puntos (lat/lon) | Centroide de cada municipio, subestaciones, centrales, aeropuertos, puertos, salidas de autopista, embalses, polígonos industriales, CPD existentes | Energía, Transporte, Agua, Suelo |
| 7 | PeeringDB | PeeringDB | API sin clave | JSON | Puntos | Data centers de colocation en España y puntos neutros de Internet (IXP) | Telecomunicaciones / **Etiqueta** |
| 8 | AEMET OpenData | AEMET | API REST con clave gratuita | JSON | Estación × mes | Temperatura media y máxima, humedad, viento, precipitación, días con Tmax ≥ 30 ºC | Climatología / Agua |
| 8b | Open-Meteo (API histórica, reanálisis ERA5) | Open-Meteo | API sin clave | JSON | Rejilla ~80 km × día | Temperatura media y máxima, humedad, viento, precipitación, evapotranspiración. **Fuente de clima por defecto**; AEMET se usa si hay clave | Climatología / Agua |
| 9 | Catálogo sísmico (FDSN) | EMSC (integra datos del IGN); USGS como respaldo | API sin clave | TXT/CSV | Evento (lat/lon) | Terremotos con M ≥ 3 desde 1990 | Riesgo sísmico |
| 10 | Índice de Precios de Vivienda en Alquiler | INE | API JSON | JSON | Municipio (> 10.000 hab.) | Evolución del alquiler (es un índice base 100: mide subidas, **no** sirve para comparar niveles) | Suelo / Económico |
| 11 | Precio del mercado eléctrico | Red Eléctrica de España (REData) | API REST sin clave | JSON | Nacional × hora | €/MWh por hora (últimos 2 años) → **coste operativo** del CPD | Energía / Económico |
| 12 | Espacios naturales protegidos | OpenStreetMap (Red Natura 2000, parques…) | Overpass API | JSON | Puntos | Proximidad a zonas protegidas → **riesgo regulatorio/ambiental** | Legislación |
| 13 | Modelo digital de elevaciones EU-DEM 25 m (SRTM en Canarias) | Copernicus vía OpenTopoData | API sin clave | JSON | Punto | Altitud y pendiente de cada municipio | Terreno / Topografía |
| 14 | **Capacidad de acceso para consumo** en la red de transporte | Red Eléctrica de España | Excel mensual en ree.es (`src/ingest/ree_capacidad.py`) | XLSX | Nudo (~940) | MW disponibles para demanda por nudo (CEP = instalaciones con electrónica de potencia, como un CPD), nudos en concurso | Energía (**factor limitante real**) |
| 15 | Zonas inundables SNCZI (T100 y T500, fluviales y marinas) | MITECO vía WMS INSPIRE del IGN | WMS sin clave | PNG → % | Municipio (4 × 4 km alrededor del centroide) | % de superficie inundable | Riesgos |
| 16 | Cables submarinos y puntos de amarre | TeleGeography (Submarine Cable Map) | API sin clave | JSON | Punto de amarre | Amarres en España y nº de cables internacionales | Telecomunicaciones |
| 17 | **Directorio de CPD de España** | Observatorio Español de Data Centers | Web (lectura de fichas, 1 s entre peticiones) | HTML → JSON | CPD (~240) | Operador, tipo (hiperescala, colocation…), estado (operativo / construcción / planificado), potencia IT (MW), coordenadas | Competencia / **Etiqueta** |
| 18 | Valor tasado medio de la vivienda libre | Ministerio de Vivienda (MIVAU) | Descarga directa | XLS | Municipio > 25.000 hab. × trimestre | €/m² → coste del suelo (media provincial para el resto) | Suelo / Económico |
| 19 | Universidades y parques de bomberos | OpenStreetMap | Overpass API | JSON | Puntos | Talento técnico disponible; seguridad física | Socioeconómico / Riesgos |

## 2. Fuentes de descarga manual (opcionales)

No tienen API estable, así que se descargan a mano y se dejan en `data/raw/manual/`. Si no están, la ETL las omite y usa las variables aproximadas (*proxies*) indicadas.

| Fuente | Dónde | Variable | Proxy si falta |
|---|---|---|---|
| Informe de Cobertura de Banda Ancha (Excel por municipio) | [conectemos.digital.gob.es](https://conectemos.digital.gob.es) — SETID | % cobertura FTTH ≥ 100 Mbps, % 5G | Distancia al IXP / CPD más cercano |
| Líneas límite municipales | Centro de Descargas del CNIG (IGN) | Superficie → densidad de población | Sin densidad |
| Red Natura 2000 (polígonos oficiales) | MITECO, Banco de Datos de la Naturaleza | % del término municipal protegido | Proximidad a espacios protegidos de OSM |
| Consumo eléctrico por municipio | Solo lo publican algunas CCAA (p. ej. ICAEN en Cataluña) | MWh/año | Demanda de la CCAA (REE) repartida por población |
| Regulación por comunidad autónoma | `data/reference/regulacion_ccaa.csv` (plantilla a rellenar por el equipo con leyes de agilización de proyectos, incentivos y tramitación) | Puntuación regulatoria 1-5 | Neutra (3) |

## 3. Cómo se calcula cada variable territorial

| Variable del enunciado | Cálculo | Fuente(s) |
|---|---|---|
| Población, evolución demográfica | Población actual; crecimiento % entre 2015 y el último año | 2 |
| Tasa de desempleo (aprox.) | Parados medios del año / población 16-64 (o / población total) | 2, 4 |
| Nivel económico | Renta neta media por persona | 3 |
| Subestaciones cercanas / capacidad de la red | Nº de subestaciones ≥ 220 kV en 25 km; distancia a la más cercana | 6 |
| **MW libres en la red** | Nudos REE geolocalizados con las subestaciones de OSM (nombre exacto / contenido / parecido / municipio homónimo). Por municipio: MW libres en el nudo más cercano, máximo en 25 y 50 km, distancia al nudo con ≥ 50 y ≥ 100 MW libres, si está en concurso (`src/etl/ree_nudos.py`) | 14, 6 |
| Riesgo de inundación | % de píxeles inundables T100 y T500 en 4 × 4 km (`src/etl/variables_extra.py`) | 15 |
| Conectividad internacional | Distancia al amarre de cable submarino más cercano, cables internacionales que llegan y amarres en 100 km | 16 |
| Competencia / saturación | Nº y MW de CPD operativos y en cartera en el municipio y en 50 km, distancia al CPD operativo más cercano, nº de hiperescala en 50 km (`src/etl/cpd_espana.py`) | 17, 7, 6 |
| Coste del suelo | Valor tasado €/m² del último trimestre (media provincial si el municipio no aparece) | 18 |
| Índice de aridez (UNEP) | Precipitación anual / evapotranspiración anual (< 0,5 = semiárido o más seco) | 8b |
| Free cooling (aprox.) | Días al año con temperatura media < 18 ºC | 8b |
| Talento y seguridad | Distancia a la universidad / parque de bomberos más cercano y nº en 30 km | 19 |
| % renovables, consumo eléctrico | Asignados desde la CCAA del municipio | 5 |
| Temperatura media / máxima, humedad, viento, días de calor | Interpolación de la estación AEMET más cercana (o media ponderada por distancia inversa de las 3 más cercanas) | 8 |
| Estrés hídrico | Índice: precipitación anual baja + muchos días de calor + lejanía a embalses | 6, 8 |
| Embalses cercanos | Nº de embalses en 30 km | 6 |
| Distancia a autopistas, puertos, aeropuertos | Distancia haversine al elemento más cercano | 6 |
| Puntos neutros, distancia a nodos | Distancia al IXP más cercano | 7 |
| Suelo industrial disponible | Nº de polígonos industriales en el municipio / en 10 km | 6 |
| Riesgo sísmico | Nº de terremotos M ≥ 3 en 50 km y magnitud máxima | 9 |
| Coste del suelo (aprox.) | Índice de alquiler del municipio (o de la provincia si falta) | 10 |
| Coste energético | Precio medio anual €/MWh y volatilidad (desviación típica horaria) × consumo previsto del CPD | 11 |
| Riesgo regulatorio / ambiental | Nº de espacios protegidos en 10 km y distancia al más cercano + puntuación regulatoria de la CCAA | 12, manual |
| Altitud y pendiente | Altitud del centroide; pendiente = máx. desnivel a 1 km / 1.000 m | 13 |

## 4. Variable objetivo (viabilidad)

"Viabilidad" no existe como dato publicado, así que se construye de dos formas complementarias:

1. **Etiqueta observada:** municipio con al menos un CPD operativo (Observatorio + PeeringDB + OSM, deduplicados, columna `tiene_cpd_operativo`) = 1; sin CPD = 0. Los CPD en construcción y planificados no cuentan para la etiqueta, pero sirven para validar: un buen modelo debería dar viabilidad alta a los municipios donde ya se están planificando. Se entrena un clasificador, y la probabilidad que predice para cada municipio se interpreta como su *viabilidad aprendida*: cuánto se parece a los sitios donde la industria ya ha decidido instalarse.
2. **Índice experto (0-100):** suma ponderada de las variables normalizadas (energía, clima, agua, conectividad, riesgo, coste). Es la base del **simulador**: cambiando los parámetros del CPD (MW, PUE, tipo de refrigeración, WUE) cambian los pesos y las restricciones. Por ejemplo, una refrigeración evaporativa penaliza mucho el estrés hídrico.

**Puntuación final:** combinación de las dos, y el informe de análisis las compara.

## 5. Limitaciones conocidas

- La demanda y el mix de REE están a nivel de CCAA, pero la **capacidad de acceso** ya es por nudo. Unos 170 de los ~940 nudos no se han podido geolocalizar por nombre (sobre todo de 66-132 kV en las islas); solo cuentan a nivel de CCAA. La capacidad de las redes de **distribución** (i-DE, e-distribución, UFD…) no está incluida.
- La capacidad de acceso cambia cada mes: el dato es una foto del momento de la descarga.
- El SNCZI solo cartografía los tramos con riesgo significativo: 0 % = "sin zona inundable cartografiada", no "riesgo nulo".
- El valor tasado solo existe para municipios > 25.000 hab.; en el resto se usa la media provincial, que sobreestima el coste en municipios rurales.
- El directorio del Observatorio no da la potencia IT de todos los CPD (≈ 40 % la tienen).
- OSM es colaborativo y su cobertura de subestaciones o CPD puede estar incompleta.
- Las estaciones de AEMET no cubren todos los municipios: se interpola por cercanía.
- La etiqueta "tiene CPD" está sesgada hacia grandes ciudades (efecto red). Por eso se combina con el índice experto.

## Notas de la última ingesta (9/11/2026)
- OSM: añadidas universidades (1.173) y bomberos (959); aeropuertos descargados correctamente en el segundo intento.
- Inundaciones (SNCZI/IDEE): el servidor WMS responde 503 de forma sostenida tras la primera petición; la variable queda pendiente y el ETL la omite con un aviso. Se reintentará más adelante.
- `data/reference/regulacion_ccaa.csv`: puntuación regulatoria **provisional** (criterio del equipo), con fuentes en la columna `fuente_url`.
