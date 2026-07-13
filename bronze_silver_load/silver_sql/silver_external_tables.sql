-- ============================================================================
-- 02_silver_external_tables.sql   (Databricks / Unity Catalog SQL)
-- Creates the 10 Silver EXTERNAL Delta tables.
-- Run AFTER 01_bronze_external_tables.sql, BEFORE the first Silver pipeline run.
-- ============================================================================
--
-- SILVER CONTRACT
--   Silver = a pure, deterministic function of Bronze:
--       TRIM -> UPPER -> blank-to-NULL on every string column
--       -> dedup to ONE row per business key (newest MODIFIED_TS wins)
--       -> add silver_load_ts
--   Current state only. No history, no versions, no SCD. All SCD Type 1 / Type 2
--   logic lives in the Silver -> Gold load.
--
-- WHY THIS FILE EXISTS
--   nb_bronze_silver_load writes with saveAsTable. If the table does not already
--   exist, Spark creates a MANAGED table inside the catalog's _managed folder and
--   your data silently ends up in the wrong place. Declaring the tables here with
--   an explicit LOCATION makes them EXTERNAL, and lets the notebook drop its
--   tgtLocation parameter entirely.
--
-- DIFFERENCES FROM BRONZE
--   1. bronze_ingest_date is GONE. It answered "which landing file did this row
--      come from" - a Bronze question. In Silver each subject exists exactly once.
--   2. silver_load_ts TIMESTAMP is added: when Silver was last rebuilt.
--   3. MODIFIED_TS SURVIVES, and it is not an audit column - it is functional data.
--      Gold needs the time the change ACTUALLY OCCURRED (not the load time) to date
--      its SCD Type 2 versions. Stamp effective_from with a load time instead and
--      every historical version gets dated to whenever the pipeline happened to run.
--   4. Types are still the source's types. DECIMAL stays DECIMAL, never DOUBLE -
--      widening DECIMAL(12,3) injects binary floating-point error into lab results.
--   5. Still no NOT NULL, no PK, no partitioning, no Change Data Feed.
--      Uniqueness of the business key is asserted by the notebook at load time
--      (assert total == distinct keys), which fails loudly on a wrong BusinessKey.
--
-- SQL Server -> Delta type map
--   VARCHAR(n) / CHAR(n)  -> STRING
--   INT                   -> INT
--   DATE                  -> DATE
--   DECIMAL(p,s)          -> DECIMAL(p,s)     (never DOUBLE)
--   DATETIME2             -> TIMESTAMP
-- ============================================================================

USE CATALOG clintrail_dev;
USE SCHEMA  clintrail_silver;


-- ---------------------------------------------------------------------------
-- DM  Demographics                                    business key: USUBJID
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clintrail_dev.clintrail_silver.dm (
    STUDYID         STRING,
    DOMAIN          STRING,
    USUBJID         STRING,
    SUBJID          STRING,
    SITEID          STRING,
    COUNTRY         STRING,
    RFICDTC         DATE,
    RFSTDTC         DATE,
    RFENDTC         DATE,
    AGE             INT,
    AGEU            STRING,
    SEX             STRING,
    RACE            STRING,
    ETHNIC          STRING,
    ARM             STRING,
    ARMCD           STRING,
    ACTARM          STRING,
    ACTARMCD        STRING,
    MODIFIED_TS     TIMESTAMP,
    silver_load_ts  TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@stclintraildev001.dfs.core.windows.net/dm';


-- ---------------------------------------------------------------------------
-- SV  Subject Visits                        business key: USUBJID, VISITNUM
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clintrail_dev.clintrail_silver.sv (
    STUDYID         STRING,
    DOMAIN          STRING,
    USUBJID         STRING,
    VISITNUM        DECIMAL(5,1),
    VISIT           STRING,
    SVSTDTC         DATE,
    SVENDTC         DATE,
    PROTOCOLDEV     STRING,
    MODIFIED_TS     TIMESTAMP,
    silver_load_ts  TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@stclintraildev001.dfs.core.windows.net/sv';


-- ---------------------------------------------------------------------------
-- VS  Vital Signs                              business key: USUBJID, VSSEQ
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clintrail_dev.clintrail_silver.vs (
    STUDYID         STRING,
    DOMAIN          STRING,
    USUBJID         STRING,
    VSSEQ           INT,
    VSTESTCD        STRING,
    VSTEST          STRING,
    VSORRES         DECIMAL(12,3),
    VSORRESU        STRING,
    VISITNUM        DECIMAL(5,1),
    VISIT           STRING,
    VSDTC           DATE,
    MODIFIED_TS     TIMESTAMP,
    silver_load_ts  TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@stclintraildev001.dfs.core.windows.net/vs';


-- ---------------------------------------------------------------------------
-- LB  Laboratory                               business key: USUBJID, LBSEQ
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clintrail_dev.clintrail_silver.lb (
    STUDYID         STRING,
    DOMAIN          STRING,
    USUBJID         STRING,
    LBSEQ           INT,
    LBTESTCD        STRING,
    LBTEST          STRING,
    LBCAT           STRING,
    LBORRES         DECIMAL(12,3),
    LBORRESU        STRING,
    LBORNRLO        DECIMAL(12,3),
    LBORNRHI        DECIMAL(12,3),
    LBNRIND         STRING,
    VISITNUM        DECIMAL(5,1),
    VISIT           STRING,
    LBDTC           DATE,
    MODIFIED_TS     TIMESTAMP,
    silver_load_ts  TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@stclintraildev001.dfs.core.windows.net/lb';


-- ---------------------------------------------------------------------------
-- EG  ECG                                      business key: USUBJID, EGSEQ
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clintrail_dev.clintrail_silver.eg (
    STUDYID         STRING,
    DOMAIN          STRING,
    USUBJID         STRING,
    EGSEQ           INT,
    EGTESTCD        STRING,
    EGTEST          STRING,
    EGORRES         DECIMAL(12,3),
    EGORRESU        STRING,
    VISITNUM        DECIMAL(5,1),
    VISIT           STRING,
    EGDTC           DATE,
    MODIFIED_TS     TIMESTAMP,
    silver_load_ts  TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@stclintraildev001.dfs.core.windows.net/eg';


-- ---------------------------------------------------------------------------
-- EC  Exposure as Collected                    business key: USUBJID, ECSEQ
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clintrail_dev.clintrail_silver.ec (
    STUDYID         STRING,
    DOMAIN          STRING,
    USUBJID         STRING,
    ECSEQ           INT,
    ECTRT           STRING,
    ECDOSE          DECIMAL(10,2),
    ECDOSU          STRING,
    ECROUTE         STRING,
    ECSTDTC         DATE,
    ECENDTC         DATE,
    VISITNUM        DECIMAL(5,1),
    VISIT           STRING,
    MODIFIED_TS     TIMESTAMP,
    silver_load_ts  TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@stclintraildev001.dfs.core.windows.net/ec';


-- ---------------------------------------------------------------------------
-- CM  Concomitant Medications                  business key: USUBJID, CMSEQ
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clintrail_dev.clintrail_silver.cm (
    STUDYID         STRING,
    DOMAIN          STRING,
    USUBJID         STRING,
    CMSEQ           INT,
    CMTRT           STRING,
    CMDECOD         STRING,
    CMDOSE          DECIMAL(10,2),
    CMDOSU          STRING,
    CMROUTE         STRING,
    CMSTDTC         DATE,
    CMENDTC         DATE,
    CMINDC          STRING,
    MODIFIED_TS     TIMESTAMP,
    silver_load_ts  TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@stclintraildev001.dfs.core.windows.net/cm';


-- ---------------------------------------------------------------------------
-- DS  Disposition                              business key: USUBJID, DSSEQ
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clintrail_dev.clintrail_silver.ds (
    STUDYID         STRING,
    DOMAIN          STRING,
    USUBJID         STRING,
    DSSEQ           INT,
    DSTERM          STRING,
    DSDECOD         STRING,
    DSCAT           STRING,
    DSSTDTC         DATE,
    MODIFIED_TS     TIMESTAMP,
    silver_load_ts  TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@stclintraildev001.dfs.core.windows.net/ds';


-- ---------------------------------------------------------------------------
-- TA  Trial Arms (reference)          business key: STUDYID, ARMCD, TAETORD
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clintrail_dev.clintrail_silver.ta (
    STUDYID         STRING,
    DOMAIN          STRING,
    ARMCD           STRING,
    ARM             STRING,
    TAETORD         INT,
    ETCD            STRING,
    ELEMENT         STRING,
    MODIFIED_TS     TIMESTAMP,
    silver_load_ts  TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@stclintraildev001.dfs.core.windows.net/ta';


-- ---------------------------------------------------------------------------
-- TS  Trial Summary (reference)           business key: STUDYID, TSPARMCD
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clintrail_dev.clintrail_silver.ts (
    STUDYID         STRING,
    DOMAIN          STRING,
    TSPARMCD        STRING,
    TSPARM          STRING,
    TSVAL           STRING,
    MODIFIED_TS     TIMESTAMP,
    silver_load_ts  TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@stclintraildev001.dfs.core.windows.net/ts';


-- ============================================================================
-- Verify
-- ============================================================================
SHOW TABLES IN clintrail_dev.clintrail_silver;      -- expect 10

-- Type must read EXTERNAL. If it says MANAGED, the LOCATION was ignored and your
-- data is sitting in the catalog's _managed folder instead of the silver container.
DESCRIBE TABLE EXTENDED clintrail_dev.clintrail_silver.dm;

-- Silver's contract: exactly one row per business key. The notebook asserts this
-- on every run; this is the manual version.
-- SELECT COUNT(*) AS rows, COUNT(DISTINCT USUBJID) AS subjects
-- FROM   clintrail_dev.clintrail_silver.dm;
