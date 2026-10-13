-- RAW layer objects. Run by the pipeline role at the start of every load (idempotent).
-- Names are unqualified: the connection's default database and schema (RAW) apply.
--
-- Every RAW table holds the source columns as delivered plus four metadata columns:
--   _SOURCE_FILE      file the row came from; the unit of idempotent replacement
--   _FILE_ROW_NUMBER  row position inside that file (stable key for dbt)
--   _BATCH_ID         pipeline run that loaded the row; joins to ETL_RUN_LOG
--   _LOADED_AT        when the COPY scanned the file

CREATE STAGE IF NOT EXISTS LANDING
  COMMENT = 'Internal landing stage; files are removed after a successful load';

CREATE FILE FORMAT IF NOT EXISTS FF_PARQUET TYPE = PARQUET;
CREATE FILE FORMAT IF NOT EXISTS FF_ZONES_CSV
  TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"';
CREATE FILE FORMAT IF NOT EXISTS FF_JSON TYPE = JSON;

-- Column names are the TLC names lower-cased; types are the widest seen across monthly
-- files (see src/sources.py TRIP_COLUMNS, which a unit test keeps in sync with this DDL).
CREATE TABLE IF NOT EXISTS YELLOW_TRIPS (
    VENDORID               NUMBER(38,0),
    TPEP_PICKUP_DATETIME   TIMESTAMP_NTZ,
    TPEP_DROPOFF_DATETIME  TIMESTAMP_NTZ,
    PASSENGER_COUNT        FLOAT,
    TRIP_DISTANCE          FLOAT,
    RATECODEID             FLOAT,
    STORE_AND_FWD_FLAG     VARCHAR,
    PULOCATIONID           NUMBER(38,0),
    DOLOCATIONID           NUMBER(38,0),
    PAYMENT_TYPE           NUMBER(38,0),
    FARE_AMOUNT            FLOAT,
    EXTRA                  FLOAT,
    MTA_TAX                FLOAT,
    TIP_AMOUNT             FLOAT,
    TOLLS_AMOUNT           FLOAT,
    IMPROVEMENT_SURCHARGE  FLOAT,
    TOTAL_AMOUNT           FLOAT,
    CONGESTION_SURCHARGE   FLOAT,
    AIRPORT_FEE            FLOAT,
    CBD_CONGESTION_FEE     FLOAT,
    _EXTRA_COLUMNS         VARIANT,        -- source columns the contract does not know yet
    _SOURCE_FILE           VARCHAR NOT NULL,
    _FILE_ROW_NUMBER       NUMBER(38,0) NOT NULL,
    _BATCH_ID              VARCHAR NOT NULL,
    _LOADED_AT             TIMESTAMP_LTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS TAXI_ZONE_LOOKUP (
    LOCATIONID        VARCHAR,
    BOROUGH           VARCHAR,
    ZONE              VARCHAR,
    SERVICE_ZONE      VARCHAR,
    _SOURCE_FILE      VARCHAR NOT NULL,
    _FILE_ROW_NUMBER  NUMBER(38,0) NOT NULL,
    _BATCH_ID         VARCHAR NOT NULL,
    _LOADED_AT        TIMESTAMP_LTZ NOT NULL
);

-- One row per Open-Meteo response (one month); dbt flattens the daily arrays.
CREATE TABLE IF NOT EXISTS WEATHER_DAILY_JSON (
    PAYLOAD           VARIANT,
    _SOURCE_FILE      VARCHAR NOT NULL,
    _FILE_ROW_NUMBER  NUMBER(38,0) NOT NULL,
    _BATCH_ID         VARCHAR NOT NULL,
    _LOADED_AT        TIMESTAMP_LTZ NOT NULL
);

-- One row per file per pipeline run.
CREATE TABLE IF NOT EXISTS ETL_RUN_LOG (
    BATCH_ID          VARCHAR NOT NULL,
    SOURCE_NAME       VARCHAR NOT NULL,
    SOURCE_FILE       VARCHAR NOT NULL,
    TARGET_TABLE      VARCHAR NOT NULL,
    STATUS            VARCHAR NOT NULL,   -- STARTED | SUCCESS | FAILED | SKIPPED
    SOURCE_ROW_COUNT  NUMBER(38,0),       -- rows in the local file
    LOADED_ROW_COUNT  NUMBER(38,0),       -- rows in RAW for this file after the run
    FILE_SIZE_BYTES   NUMBER(38,0),
    FILE_SHA256       VARCHAR,
    SCHEMA_NOTES      VARCHAR,            -- schema drift observed in this file, if any
    STARTED_AT        TIMESTAMP_LTZ NOT NULL,
    ENDED_AT          TIMESTAMP_LTZ,
    ERROR_MESSAGE     VARCHAR
);
