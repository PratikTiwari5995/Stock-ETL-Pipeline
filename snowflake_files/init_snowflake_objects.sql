/*
================================================================================
FILE: init_snowflake_objects.sql
PURPOSE: Initialize Snowflake warehouse, database, schemas, and tables for 
          securities pricing data using medallion architecture (RAW→CORE→DM)
EXECUTION: Run this FIRST, then load_transform_historical_data.sql
================================================================================
*/

-- WH_INGEST: Lightweight warehouse for ETL (XSMALL, auto-suspend 60s)
CREATE WAREHOUSE IF NOT EXISTS WH_INGEST
  WAREHOUSE_SIZE                = 'XSMALL'
  AUTO_SUSPEND                  = 60
  AUTO_RESUME                   = TRUE
  INITIALLY_SUSPENDED           = TRUE
  STATEMENT_TIMEOUT_IN_SECONDS  = 3600
  COMMENT                       = 'ETL ingest warehouse';

-- Database and schemas: RAW (landing) → CORE (cleansed) → DM_DIM/DM_FACT (analytics)
CREATE DATABASE IF NOT EXISTS SEC_PRICING COMMENT = 'Securities pricing data warehouse';
USE WAREHOUSE WH_INGEST;
USE DATABASE SEC_PRICING;

CREATE SCHEMA IF NOT EXISTS SEC_PRICING.RAW;      -- Landing zone (append-only)
CREATE SCHEMA IF NOT EXISTS SEC_PRICING.CORE;     -- Cleansed canonical data
CREATE SCHEMA IF NOT EXISTS SEC_PRICING.DM_DIM;   -- Dimensions
CREATE SCHEMA IF NOT EXISTS SEC_PRICING.DM_FACT;  -- Facts

-- RAW: End-of-day prices, as-is from source (audit fields included)
CREATE TABLE IF NOT EXISTS RAW.RAW_EOD_PRICES (
  TRADE_DATE   DATE,
  SYMBOL       STRING,
  OPEN         NUMBER(18,6),
  HIGH         NUMBER(18,6),
  LOW          NUMBER(18,6),
  CLOSE        NUMBER(18,6),
  VOLUME       NUMBER(38,0),
  _SRC_FILE    STRING,                            -- Source file identifier
  _INGEST_TS   TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- CORE: Deduplicated, normalized version of RAW (canonical source)
CREATE TABLE IF NOT EXISTS CORE.EOD_PRICES (
  TRADE_DATE   DATE,
  SYMBOL       STRING,
  OPEN         NUMBER(18,6),
  HIGH         NUMBER(18,6),
  LOW          NUMBER(18,6),
  CLOSE        NUMBER(18,6),
  VOLUME       NUMBER(38,0),
  LOAD_TS      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- DIM_SECURITY: Security dimension with surrogate key (SECURITY_ID)
-- One row per unique symbol; IDENTITY auto-generates SECURITY_ID
CREATE TABLE IF NOT EXISTS DM_DIM.DIM_SECURITY (
  SECURITY_ID  NUMBER IDENTITY START 1 INCREMENT 1,
  SYMBOL       STRING UNIQUE,
  IS_ACTIVE    BOOLEAN DEFAULT TRUE,
  LOAD_TS      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_DIM_SECURITY PRIMARY KEY (SECURITY_ID)
);

-- DIM_DATE: Date dimension with temporal attributes (YEAR, MONTH, QUARTER, etc.)
-- One row per unique trading date; DATE_SK = yyyymmdd format
CREATE TABLE IF NOT EXISTS DIM_DATE (
  DATE_SK       NUMBER(8,0),                      -- yyyymmdd integer surrogate key
  CAL_DATE      DATE UNIQUE,
  YEAR_NUM      NUMBER(4,0),
  QUARTER_NUM   NUMBER(1,0),
  MONTH_NUM     NUMBER(2,0),
  MONTH_NAME    VARCHAR(20),
  DAY_NUM       NUMBER(2,0),
  DAY_NAME      VARCHAR(20),
  DAY_OF_WEEK   NUMBER(1,0),                      -- 0=Sun, 6=Sat
  WEEK_OF_YEAR  NUMBER(2,0),
  IS_WEEKEND    BOOLEAN,
  CONSTRAINT PK_DIM_DATE PRIMARY KEY (DATE_SK)
);

-- FACT_DAILY_PRICE: Central fact table (grain: one row per security-date)
-- Grain: (SECURITY_ID, DATE_SK) with OHLCV metrics
CREATE TABLE IF NOT EXISTS DM_FACT.FACT_DAILY_PRICE (
  SECURITY_ID  NUMBER,                           -- FK → DIM_SECURITY
  DATE_SK      NUMBER(8,0),                       -- FK → DIM_DATE
  TRADE_DATE   DATE,                              -- Denormalized for convenience
  OPEN         NUMBER(18,6),
  HIGH         NUMBER(18,6),
  LOW          NUMBER(18,6),
  CLOSE        NUMBER(18,6),
  VOLUME       NUMBER(38,0),
  LOAD_TS      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_FACT_DAILY PRIMARY KEY (SECURITY_ID, DATE_SK)
);

-- Verify environment
SELECT current_organization_name() AS org, current_account_name() AS account_name,
       current_account() AS account_locator, current_region() AS region;
SHOW USERS;
