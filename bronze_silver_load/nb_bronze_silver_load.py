# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze (Delta) -> Silver (Delta)
# MAGIC
# MAGIC One notebook, all domains. ADF: Lookup -> ForEach -> this notebook.
# MAGIC Assumes the catalog, schema, and tables already exist
# MAGIC (`00_uc_bootstrap.sql`, `02_silver_external_tables.sql`).
# MAGIC
# MAGIC **Full recompute.** Reads ALL of Bronze, cleanses, dedups, overwrites Silver.
# MAGIC No watermark, no state, so a re-run always produces an identical table — and
# MAGIC a change to the cleansing rule applies retroactively to every row.

# COMMAND ----------

from pyspark.sql import functions as F, Window

dbutils.widgets.text("domain",       "DM")
dbutils.widgets.text("srcCatalog",   "clintrail_dev")
dbutils.widgets.text("srcSchema",    "clintrail_bronze")
dbutils.widgets.text("srcTableName", "dm")
dbutils.widgets.text("tgtCatalog",   "clintrail_dev")
dbutils.widgets.text("tgtSchema",    "clintrail_silver")
dbutils.widgets.text("tgtTableName", "dm")
dbutils.widgets.text("businessKey",  "USUBJID")

domain = dbutils.widgets.get("domain")
v_bussiness_key = dbutils.widgets.get("businessKey")
# "USUBJID, VSSEQ " -> ["USUBJID", "VSSEQ"]
business_key = [c.strip() for c in v_bussiness_key.split(",")]

SOURCE = (f"{dbutils.widgets.get('srcCatalog')}"
          f".{dbutils.widgets.get('srcSchema')}"
          f".{dbutils.widgets.get('srcTableName')}")
TARGET = (f"{dbutils.widgets.get('tgtCatalog')}"
          f".{dbutils.widgets.get('tgtSchema')}"
          f".{dbutils.widgets.get('tgtTableName')}")

ORDER_COL   = "MODIFIED_TS"           # which duplicate wins
BRONZE_DATE = "bronze_ingest_date"    # tie-breaker, then dropped

print(f"{domain}: {SOURCE} -> {TARGET}  key={business_key}")

# COMMAND ----------

# Read ALL of Bronze, not one date. Silver is the current state, and to know a
# subject's current values you must see every version of them that ever landed.
df = spark.table(SOURCE)
df.count()

# COMMAND ----------

# Cleanse every string column: TRIM -> UPPER -> blank becomes NULL.
# Trim first, then test for empty, or "   " survives as a non-empty string.
# NULL-safe: upper(NULL) is NULL, and NULL = "" is NULL (not true), so the
# when() falls through to otherwise() and the NULL passes straight through.
for c, t in df.dtypes:
    if t == "string":
        cleaned = F.upper(F.trim(F.col(c)))
        df = df.withColumn(c, F.when(cleaned == "", None).otherwise(cleaned))

# COMMAND ----------

# df.count()

# COMMAND ----------

# Dedup: keep one row per business key, the newest by MODIFIED_TS.
# Tie-break on bronze_ingest_date (the BUSINESS date), never on a wall-clock load
# time — otherwise a backfilled old file, loaded later, would beat newer data.
#
# This one rule is correct for both feed shapes, which is why there's no LoadType:
#   DM (deltas)    -> newest row per USUBJID = current state
#   TS (snapshots) -> newest row per key     = the latest snapshot
w = Window.partitionBy(*business_key).orderBy(
        F.col(ORDER_COL).desc_nulls_last(),
        F.col(BRONZE_DATE).desc_nulls_last())

df = (df.withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn"))


# COMMAND ----------

# df.count()

# COMMAND ----------

# Bronze's ingest date is a Bronze concern — in Silver each subject exists once.
# MODIFIED_TS stays: it is functional data, not audit. Gold needs the time the
# change actually happened to date its SCD Type 2 versions.
df = df.drop(BRONZE_DATE).withColumn("silver_load_ts", F.current_timestamp())

# COMMAND ----------

# Full recompute, so a plain overwrite is correct and idempotent. No `path` option
# needed — 02_silver_external_tables.sql already declared LOCATION, so saveAsTable
# writes to the external table's own location.
(df.write.format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(TARGET))

# COMMAND ----------

# Silver's contract is one row per business key. Assert it, so a wrong BusinessKey
# in the control table fails here instead of quietly collapsing subjects into sites.
dbutils.notebook.exit(f"OK Exit")
