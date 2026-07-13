-- ============================================================================
-- gold_external_tables.sql   (Databricks / Unity Catalog SQL)
-- Gold dimensional model: 3 dimensions + 7 facts.
-- Run AFTER 02_silver_external_tables.sql.
-- ============================================================================
--
-- DIMENSION vs FACT — the rule that assigns every domain
--   Is a row an ENTITY whose attributes change, or an EVENT that happened?
--     entity -> dimension (may need SCD history)
--     event  -> fact      (immutable; append / current-state)
--
--   DM  -> dim_subject          entity, arm/site change over time      -> SCD 2
--   TA  -> dim_arm              trial-design reference                 -> SCD 1
--   TS  -> dim_trial_summary    trial-design reference                 -> SCD 1
--   SV  -> fact_subject_visit   an event (a visit occurred)            -> fact
--   VS  -> fact_vital_sign      a measurement                          -> fact
--   LB  -> fact_lab_result      a measurement                          -> fact
--   EG  -> fact_ecg             a measurement                          -> fact
--   EC  -> fact_exposure        a dosing event                         -> fact
--   CM  -> fact_conmed          a medication event                     -> fact
--   DS  -> fact_disposition     a disposition event                    -> fact
--
--   Why not SCD2 on facts (e.g. a corrected lab value)? Bronze already keeps every
--   version of every row forever, so the correction audit trail exists upstream.
--   The fact stays current-state; you do not bloat 7 large tables for something
--   Bronze gives you for free.
--
-- SURROGATE KEYS
--   Every dim and fact has a <name>_sk BIGINT GENERATED ALWAYS AS IDENTITY. Delta
--   assigns it on insert; the load never supplies it.
--
-- SCD COLUMN SETS
--   SCD 2 dim: hash_diff, start_effective_date, end_effective_date, is_active,
--              record_version, gold_load_ts
--   SCD 1 dim: hash_diff, gold_load_ts            (change detection, no history)
--   fact     : subject_sk (FK, resolved by effective-dated lookup), gold_load_ts
--
-- Types are the source's types. DECIMAL stays DECIMAL. DOMAIN / MODIFIED_TS /
-- silver_load_ts are dropped - they were Silver's concern, not Gold's.
-- ============================================================================

USE CATALOG clintrail_dev;
USE SCHEMA  clintrail_gold;


-- ###########################################################################
-- DIMENSIONS
-- ###########################################################################

-- ---------------------------------------------------------------------------
-- dim_subject   (SCD Type 2)   natural key: USUBJID
-- subject_sk is UNIQUE PER VERSION. Facts join to the version that was current
-- when the observation happened, so a March result rolls up to the March arm.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clintrail_dev.clintrail_gold.dim_subject (
    subject_sk      BIGINT GENERATED ALWAYS AS IDENTITY,
    USUBJID         STRING,
    STUDYID         STRING,
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
    hash_diff             STRING,
    start_effective_date  TIMESTAMP,   -- when this version became true
    end_effective_date    TIMESTAMP,   -- when it stopped (NULL = still active)
    is_active             BOOLEAN,     -- TRUE for the one live version per subject
    record_version        INT,         -- 1, 2, 3 ... per subject
    gold_load_ts          TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@stclintraildev001.dfs.core.windows.net/dim_subject';


-- ---------------------------------------------------------------------------
-- dim_arm   (SCD Type 1)   natural key: STUDYID, ARMCD, TAETORD
-- Trial design. Corrections overwrite in place; no history.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clintrail_dev.clintrail_gold.dim_arm (
    arm_sk          BIGINT GENERATED ALWAYS AS IDENTITY,
    STUDYID         STRING,
    ARMCD           STRING,
    ARM             STRING,
    TAETORD         INT,
    ETCD            STRING,
    ELEMENT         STRING,
    hash_diff       STRING,
    gold_load_ts    TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@stclintraildev001.dfs.core.windows.net/dim_arm';


-- ---------------------------------------------------------------------------
-- dim_trial_summary   (SCD Type 1)   natural key: STUDYID, TSPARMCD
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clintrail_dev.clintrail_gold.dim_trial_summary (
    ts_sk           BIGINT GENERATED ALWAYS AS IDENTITY,
    STUDYID         STRING,
    TSPARMCD        STRING,
    TSPARM          STRING,
    TSVAL           STRING,
    hash_diff       STRING,
    gold_load_ts    TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@stclintraildev001.dfs.core.windows.net/dim_trial_summary';


-- ###########################################################################
-- FACTS   (subject_sk resolved by effective-dated lookup on the EVENT DATE)
-- ###########################################################################

-- ---------------------------------------------------------------------------
-- fact_subject_visit   (SV)   grain: USUBJID, VISITNUM   event date: SVSTDTC
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clintrail_dev.clintrail_gold.fact_subject_visit (
    visit_sk        BIGINT GENERATED ALWAYS AS IDENTITY,
    subject_sk      BIGINT,
    STUDYID         STRING,
    USUBJID         STRING,
    VISITNUM        DECIMAL(5,1),
    VISIT           STRING,
    SVSTDTC         DATE,
    SVENDTC         DATE,
    PROTOCOLDEV     STRING,
    gold_load_ts    TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@stclintraildev001.dfs.core.windows.net/fact_subject_visit';


-- ---------------------------------------------------------------------------
-- fact_vital_sign   (VS)   grain: USUBJID, VSSEQ   event date: VSDTC
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clintrail_dev.clintrail_gold.fact_vital_sign (
    vital_sk        BIGINT GENERATED ALWAYS AS IDENTITY,
    subject_sk      BIGINT,
    STUDYID         STRING,
    USUBJID         STRING,
    VSSEQ           INT,
    VSTESTCD        STRING,
    VSTEST          STRING,
    VSORRES         DECIMAL(12,3),
    VSORRESU        STRING,
    VISITNUM        DECIMAL(5,1),
    VISIT           STRING,
    VSDTC           DATE,
    gold_load_ts    TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@stclintraildev001.dfs.core.windows.net/fact_vital_sign';


-- ---------------------------------------------------------------------------
-- fact_lab_result   (LB)   grain: USUBJID, LBSEQ   event date: LBDTC
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clintrail_dev.clintrail_gold.fact_lab_result (
    lab_sk          BIGINT GENERATED ALWAYS AS IDENTITY,
    subject_sk      BIGINT,
    STUDYID         STRING,
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
    gold_load_ts    TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@stclintraildev001.dfs.core.windows.net/fact_lab_result';


-- ---------------------------------------------------------------------------
-- fact_ecg   (EG)   grain: USUBJID, EGSEQ   event date: EGDTC
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clintrail_dev.clintrail_gold.fact_ecg (
    ecg_sk          BIGINT GENERATED ALWAYS AS IDENTITY,
    subject_sk      BIGINT,
    STUDYID         STRING,
    USUBJID         STRING,
    EGSEQ           INT,
    EGTESTCD        STRING,
    EGTEST          STRING,
    EGORRES         DECIMAL(12,3),
    EGORRESU        STRING,
    VISITNUM        DECIMAL(5,1),
    VISIT           STRING,
    EGDTC           DATE,
    gold_load_ts    TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@stclintraildev001.dfs.core.windows.net/fact_ecg';


-- ---------------------------------------------------------------------------
-- fact_exposure   (EC)   grain: USUBJID, ECSEQ   event date: ECSTDTC
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clintrail_dev.clintrail_gold.fact_exposure (
    exposure_sk     BIGINT GENERATED ALWAYS AS IDENTITY,
    subject_sk      BIGINT,
    STUDYID         STRING,
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
    gold_load_ts    TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@stclintraildev001.dfs.core.windows.net/fact_exposure';


-- ---------------------------------------------------------------------------
-- fact_conmed   (CM)   grain: USUBJID, CMSEQ   event date: CMSTDTC
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clintrail_dev.clintrail_gold.fact_conmed (
    conmed_sk       BIGINT GENERATED ALWAYS AS IDENTITY,
    subject_sk      BIGINT,
    STUDYID         STRING,
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
    gold_load_ts    TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@stclintraildev001.dfs.core.windows.net/fact_conmed';


-- ---------------------------------------------------------------------------
-- fact_disposition   (DS)   grain: USUBJID, DSSEQ   event date: DSSTDTC
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clintrail_dev.clintrail_gold.fact_disposition (
    disposition_sk  BIGINT GENERATED ALWAYS AS IDENTITY,
    subject_sk      BIGINT,
    STUDYID         STRING,
    USUBJID         STRING,
    DSSEQ           INT,
    DSTERM          STRING,
    DSDECOD         STRING,
    DSCAT           STRING,
    DSSTDTC         DATE,
    gold_load_ts    TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@stclintraildev001.dfs.core.windows.net/fact_disposition';


-- ============================================================================
-- Verify
-- ============================================================================
SHOW TABLES IN clintrail_dev.clintrail_gold;     -- expect 10 (3 dims + 7 facts)
DESCRIBE TABLE EXTENDED clintrail_dev.clintrail_gold.dim_subject;   -- Type = EXTERNAL

-- SCD2 invariant: exactly one current version per subject.
-- SELECT USUBJID, COUNT(*) FROM clintrail_dev.clintrail_gold.dim_subject
-- WHERE is_