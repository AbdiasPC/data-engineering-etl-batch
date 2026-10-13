-- =============================================================================
-- One-time Snowflake setup for the NYC taxi batch ELT project.
--
-- WHO RUNS THIS: you, manually, as a user that can assume ACCOUNTADMIN
-- (Snowsight worksheet > "Run All"). The pipeline never runs admin DDL.
--
-- BEFORE RUNNING:
--   1. Generate a key pair on your machine, from the repo root:
--        openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out secrets/rsa_key.p8 -nocrypt
--        openssl rsa -in secrets/rsa_key.p8 -pubout -out secrets/rsa_key.pub
--   2. Paste the body of secrets/rsa_key.pub (the base64 text WITHOUT the
--      BEGIN/END PUBLIC KEY lines, on one line) over <PASTE_PUBLIC_KEY_BODY> below.
--      A public key is not a secret, but do not commit your edited copy anyway.
--
-- NAMES: the object names below are the defaults in .env.example. If you change
-- one here, change the matching SNOWFLAKE_* variable in .env.
--
-- The script is idempotent (IF NOT EXISTS / re-grantable) and safe to re-run.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Database, schemas, warehouse (SYSADMIN owns data objects)
-- ---------------------------------------------------------------------------
USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS NYC_TAXI
  COMMENT = 'Portfolio project: NYC taxi + weather batch ELT';

CREATE SCHEMA IF NOT EXISTS NYC_TAXI.RAW
  COMMENT = 'Source data loaded as-is by the Python pipeline';
CREATE SCHEMA IF NOT EXISTS NYC_TAXI.STAGING
  COMMENT = 'dbt staging models: renamed, cast, flagged';
CREATE SCHEMA IF NOT EXISTS NYC_TAXI.MARTS
  COMMENT = 'dbt marts: dimensions, facts, aggregates';

CREATE WAREHOUSE IF NOT EXISTS NYC_TAXI_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60            -- seconds idle before suspending
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'NYC taxi ELT: loads and dbt builds';

-- A runaway query is cancelled instead of burning credits.
ALTER WAREHOUSE NYC_TAXI_WH SET STATEMENT_TIMEOUT_IN_SECONDS = 900;

-- ---------------------------------------------------------------------------
-- 2. Resource monitor (requires ACCOUNTADMIN)
-- ---------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE RESOURCE MONITOR IF NOT EXISTS NYC_TAXI_RM
  WITH CREDIT_QUOTA = 5
       FREQUENCY = MONTHLY
       START_TIMESTAMP = IMMEDIATELY
       TRIGGERS ON 80 PERCENT DO NOTIFY
                ON 100 PERCENT DO SUSPEND            -- let running queries finish
                ON 110 PERCENT DO SUSPEND_IMMEDIATE; -- cancel running queries

ALTER WAREHOUSE NYC_TAXI_WH SET RESOURCE_MONITOR = NYC_TAXI_RM;

-- ---------------------------------------------------------------------------
-- 3. Least-privilege role and service user
-- ---------------------------------------------------------------------------
USE ROLE USERADMIN;

CREATE ROLE IF NOT EXISTS NYC_TAXI_PIPELINE
  COMMENT = 'Runs the NYC taxi ELT pipeline and dbt; scoped to NYC_TAXI only';

-- TYPE = SERVICE: no password, no interactive login; key-pair auth only.
CREATE USER IF NOT EXISTS NYC_TAXI_SVC
  TYPE = SERVICE
  DEFAULT_ROLE = NYC_TAXI_PIPELINE
  DEFAULT_WAREHOUSE = NYC_TAXI_WH
  DEFAULT_NAMESPACE = NYC_TAXI.RAW
  COMMENT = 'Service user for the NYC taxi ELT pipeline';

ALTER USER NYC_TAXI_SVC SET RSA_PUBLIC_KEY = '<PASTE_PUBLIC_KEY_BODY>';

-- ---------------------------------------------------------------------------
-- 4. Grants
-- ---------------------------------------------------------------------------
USE ROLE SECURITYADMIN;

GRANT ROLE NYC_TAXI_PIPELINE TO USER NYC_TAXI_SVC;
-- Standard hierarchy: lets SYSADMIN see and manage what the pipeline creates.
GRANT ROLE NYC_TAXI_PIPELINE TO ROLE SYSADMIN;

GRANT USAGE ON WAREHOUSE NYC_TAXI_WH TO ROLE NYC_TAXI_PIPELINE;
GRANT USAGE ON DATABASE NYC_TAXI TO ROLE NYC_TAXI_PIPELINE;

-- RAW: the pipeline creates and owns its tables, stage and file formats.
GRANT USAGE, CREATE TABLE, CREATE STAGE, CREATE FILE FORMAT
  ON SCHEMA NYC_TAXI.RAW TO ROLE NYC_TAXI_PIPELINE;

-- STAGING / MARTS: dbt creates and owns views and tables.
GRANT USAGE, CREATE TABLE, CREATE VIEW
  ON SCHEMA NYC_TAXI.STAGING TO ROLE NYC_TAXI_PIPELINE;
GRANT USAGE, CREATE TABLE, CREATE VIEW
  ON SCHEMA NYC_TAXI.MARTS TO ROLE NYC_TAXI_PIPELINE;

-- ---------------------------------------------------------------------------
-- 5. Verify (expect: one user row with RSA_PUBLIC_KEY_FP set, and the grants)
-- ---------------------------------------------------------------------------
DESC USER NYC_TAXI_SVC;
SHOW GRANTS TO ROLE NYC_TAXI_PIPELINE;

-- ---------------------------------------------------------------------------
-- TEARDOWN (destructive; run manually only when you are done with the project)
-- ---------------------------------------------------------------------------
-- USE ROLE ACCOUNTADMIN;
-- DROP DATABASE IF EXISTS NYC_TAXI;
-- DROP WAREHOUSE IF EXISTS NYC_TAXI_WH;
-- DROP RESOURCE MONITOR IF EXISTS NYC_TAXI_RM;
-- DROP USER IF EXISTS NYC_TAXI_SVC;
-- DROP ROLE IF EXISTS NYC_TAXI_PIPELINE;
