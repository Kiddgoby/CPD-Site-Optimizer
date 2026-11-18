-- =============================================================================
-- Data Warehouse CPD-Site-Optimizer (DuckDB) - esquema en estrella
--
-- Grano de la tabla de hechos: UN MUNICIPIO (código INE de 5 dígitos).
-- Las dimensiones temáticas tienen también una fila por municipio (relación
-- 1:1 con el hecho) y agrupan las ~60 variables por tema. La geografía tiene
-- jerarquía municipio -> provincia -> CCAA.
--
-- Tablas auxiliares con otro grano: fact_cpd (un CPD) y nudo_ree (un nudo de
-- la red de transporte de REE).
--
-- Lo crea y lo carga src/warehouse/build.py. El orden importa por las claves
-- foráneas: primero las dimensiones, después los hechos.
-- =============================================================================

-- ---------------------------------------------------------------- Geografía
CREATE TABLE dim_ccaa (
    cod_ccaa                   VARCHAR PRIMARY KEY,   -- código INE 01-19
    ccaa                       VARCHAR NOT NULL UNIQUE,
    puntuacion_regulatoria     TINYINT CHECK (puntuacion_regulatoria BETWEEN 1 AND 5),
    regulacion_provisional     BOOLEAN,              -- TRUE mientras el equipo no la valide
    ley_agilizacion_proyectos  VARCHAR,
    incentivos_fiscales_cpd    VARCHAR,
    plazo_tramitacion_meses    DOUBLE,
    restricciones_agua         VARCHAR,
    fuente_url                 VARCHAR,
    notas                      VARCHAR
);

CREATE TABLE dim_provincia (
    cod_provincia  VARCHAR PRIMARY KEY,              -- código INE 01-52
    provincia      VARCHAR NOT NULL,
    cod_ccaa       VARCHAR NOT NULL REFERENCES dim_ccaa (cod_ccaa)
);

CREATE TABLE dim_geografia (
    ine            VARCHAR PRIMARY KEY CHECK (length(ine) = 5),
    municipio      VARCHAR,
    cod_provincia  VARCHAR NOT NULL REFERENCES dim_provincia (cod_provincia),
    lat            DOUBLE NOT NULL,
    lon            DOUBLE NOT NULL
);

-- ------------------------------------------------------- Dimensiones temáticas
CREATE TABLE dim_red_electrica (
    ine                       VARCHAR PRIMARY KEY REFERENCES dim_geografia (ine),
    nudo_ree_cercano          VARCHAR,
    dist_nudo_ree_km          DOUBLE,
    mw_libres_nudo_cercano    DOUBLE,
    nudo_cercano_en_concurso  BOOLEAN,
    mw_libres_max_25km        DOUBLE,
    mw_libres_max_50km        DOUBLE,
    dist_nudo_50mw_km         DOUBLE,
    dist_nudo_100mw_km        DOUBLE
);

CREATE TABLE dim_competencia (
    ine                     VARCHAR PRIMARY KEY REFERENCES dim_geografia (ine),
    n_cpd_operativos_50km   INTEGER,
    mw_cpd_operativos_50km  DOUBLE,
    dist_cpd_operativo_km   DOUBLE,
    n_cpd_cartera           INTEGER,   -- en construcción + planificados
    mw_cpd_cartera          DOUBLE,
    n_cpd_cartera_50km      INTEGER,
    mw_cpd_cartera_50km     DOUBLE,
    n_hiperescala_50km      INTEGER
);

CREATE TABLE dim_clima (
    ine                   VARCHAR PRIMARY KEY REFERENCES dim_geografia (ine),
    indice_aridez         DOUBLE,     -- P/ET0; < 0,5 = semiárido o más seco
    dias_tmedia_menor_18  INTEGER     -- aproximación a días de free cooling
);

CREATE TABLE dim_socioeconomica (
    ine                        VARCHAR PRIMARY KEY REFERENCES dim_geografia (ine),
    anio_poblacion             INTEGER,
    poblacion                  INTEGER,
    poblacion_2015             INTEGER,
    crecimiento_poblacion_pct  DOUBLE,
    anio_renta                 INTEGER,
    renta_neta_persona         DOUBLE,
    renta_neta_hogar           DOUBLE,
    renta_mediana_uc           DOUBLE,
    periodo_paro               VARCHAR,
    parados_medios_12m         DOUBLE,
    tasa_paro_aprox_pct        DOUBLE,   -- parados / población total
    valor_tasado_eur_m2        DOUBLE,   -- coste del suelo (aprox.)
    valor_tasado_origen        VARCHAR   -- 'municipio' o 'media_provincia'
);

CREATE TABLE dim_riesgos (
    ine                 VARCHAR PRIMARY KEY REFERENCES dim_geografia (ine),
    pct_inundable_t100  DOUBLE,
    pct_inundable_t500  DOUBLE,
    n_sismos_50km       INTEGER,
    n_sismos_m4_50km    INTEGER,
    mag_max_50km        DOUBLE,
    dist_sismo_m4_km    DOUBLE
);

CREATE TABLE dim_conectividad (
    ine                         VARCHAR PRIMARY KEY REFERENCES dim_geografia (ine),
    amarre_cable_cercano        VARCHAR,
    dist_amarre_cable_km        DOUBLE,
    cables_intl_amarre_cercano  INTEGER,
    amarres_cable_100km         INTEGER,
    dist_universidades_km       DOUBLE,
    n_universidades_30km        INTEGER,
    dist_bomberos_km            DOUBLE,
    n_bomberos_30km             INTEGER
);

CREATE TABLE dim_entorno (
    ine                    VARCHAR PRIMARY KEY REFERENCES dim_geografia (ine),
    altitud_m              DOUBLE,
    pendiente_media_pct    DOUBLE,
    dist_embalse_km        DOUBLE,
    n_embalses_30km        INTEGER,
    dist_rio_principal_km  DOUBLE,
    dist_industrial_km     DOUBLE,
    n_industrial_10km      INTEGER,
    dist_autopista_km      DOUBLE,
    dist_puerto_km         DOUBLE,
    dist_aeropuerto_km     DOUBLE
);

-- ------------------------------------------------------------ Tabla de hechos
-- Claves hacia todas las dimensiones + la etiqueta del modelo y sus medidas.
CREATE TABLE fact_municipio (
    ine                  VARCHAR PRIMARY KEY,
    cod_provincia        VARCHAR NOT NULL REFERENCES dim_provincia (cod_provincia),
    cod_ccaa             VARCHAR NOT NULL REFERENCES dim_ccaa (cod_ccaa),
    tiene_cpd_operativo  BOOLEAN NOT NULL,   -- etiqueta observada (variable objetivo)
    n_cpd_operativos     INTEGER NOT NULL,
    mw_cpd_operativos    DOUBLE,
    FOREIGN KEY (ine) REFERENCES dim_geografia (ine),
    FOREIGN KEY (ine) REFERENCES dim_red_electrica (ine),
    FOREIGN KEY (ine) REFERENCES dim_competencia (ine),
    FOREIGN KEY (ine) REFERENCES dim_clima (ine),
    FOREIGN KEY (ine) REFERENCES dim_socioeconomica (ine),
    FOREIGN KEY (ine) REFERENCES dim_riesgos (ine),
    FOREIGN KEY (ine) REFERENCES dim_conectividad (ine),
    FOREIGN KEY (ine) REFERENCES dim_entorno (ine)
);

-- --------------------------------------------------- Tablas con otro grano
CREATE TABLE fact_cpd (
    id_cpd            INTEGER PRIMARY KEY,
    fuente            VARCHAR,
    id_fuente         VARCHAR,
    operador          VARCHAR,
    nombre            VARCHAR,
    tipo              VARCHAR,
    estado            VARCHAR,
    potencia_it_mw    DOUBLE,
    direccion         VARCHAR,
    ubicacion         VARCHAR,
    lat               DOUBLE,
    lon               DOUBLE,
    precision_geo     VARCHAR,
    url               VARCHAR,
    redes_peeringdb   DOUBLE,
    ixp_peeringdb     DOUBLE,
    ine               VARCHAR REFERENCES dim_geografia (ine),   -- vacío si no se pudo ubicar
    metodo_municipio  VARCHAR
);

CREATE TABLE nudo_ree (
    id_nudo                     INTEGER PRIMARY KEY,
    nudo                        VARCHAR,
    cod_subestacion             VARCHAR,
    ccaa                        VARCHAR,
    tension_kv                  DOUBLE,
    margen_estatico_demanda_mw  DOUBLE,
    cap_disp_cep_ch_mw          DOUBLE,
    cap_disp_cep_sh_mw          DOUBLE,
    cap_disp_no_cep_mw          DOUBLE,
    motivo_no_otorgable         VARCHAR,
    nudo_concurso               BOOLEAN,
    lat                         DOUBLE,
    lon                         DOUBLE,
    metodo_geo                  VARCHAR,
    fichero_ree                 VARCHAR
);

-- Trazabilidad: qué CSV alimentó cada tabla y cuándo
CREATE TABLE meta_carga (
    tabla        VARCHAR PRIMARY KEY,
    origen       VARCHAR,
    filas        INTEGER,
    cargado_en   TIMESTAMP
);

-- --------------------------------------------------------------------- Vistas
-- Tabla plana para el modelo de ML y el dashboard: una fila por municipio.
CREATE VIEW vw_features_municipio AS
SELECT
    f.ine, g.municipio, p.provincia, c.ccaa, g.lat, g.lon,
    f.tiene_cpd_operativo, f.n_cpd_operativos, f.mw_cpd_operativos,
    c.puntuacion_regulatoria, c.regulacion_provisional,
    r.* EXCLUDE (ine),
    k.* EXCLUDE (ine),
    cl.* EXCLUDE (ine),
    s.* EXCLUDE (ine),
    rg.* EXCLUDE (ine),
    cn.* EXCLUDE (ine),
    en.* EXCLUDE (ine)
FROM fact_municipio f
JOIN dim_geografia      g  USING (ine)
JOIN dim_provincia      p  ON p.cod_provincia = f.cod_provincia
JOIN dim_ccaa           c  ON c.cod_ccaa = f.cod_ccaa
JOIN dim_red_electrica  r  USING (ine)
JOIN dim_competencia    k  USING (ine)
JOIN dim_clima          cl USING (ine)
JOIN dim_socioeconomica s  USING (ine)
JOIN dim_riesgos        rg USING (ine)
JOIN dim_conectividad   cn USING (ine)
JOIN dim_entorno        en USING (ine);
