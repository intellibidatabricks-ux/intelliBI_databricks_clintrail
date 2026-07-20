# Databricks notebook source
# MAGIC %md
# MAGIC # Silver -> Gold : FACTS
# MAGIC
# MAGIC One notebook, every fact. ADF: Lookup (EntityType='fact') -> ForEach -> this
# MAGIC notebook. Runs AFTER the dimensions pipeline (it needs dim_subject to exist).
# MAGIC
# MAGIC A fact is an immutable event, so there is no SCD here — just:
# MAGIC   1. read the Silver observations (current state, already deduped),
# MAGIC   2. resolve subject_sk with an EFFECTIVE-DATED join to dim_subject,
# MAGIC   3. overwrite the fact table.
# MAGIC
# MAGIC Full overwrite = idempotent, no watermark. The subject_sk resolves the same
# MAGIC every run because the effective-dated join is deterministic.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("entityName",   "fact_lab_result")
dbutils.widgets.text("srcCatalog",   "clintrail_dev")
dbutils.widgets.text("srcSchema",    "clintrail_silver")
dbutils.widgets.text("srcTableName", "lb")
dbutils.widgets.text("tgtCatalog",   "clintrail_dev")
dbutils.widgets.text("tgtSchema",    "clintrail_gold")
dbutils.widgets.text("tgtTableName", "fact_lab_result")
dbutils.widgets.text("businessKey",  "USUBJID,LBSEQ")
dbutils.widgets.text("eventDateCol", "LBDTC")

entity        = dbutils.widgets.get("entityName")
business_key  = [c.strip() for c in dbutils.widgets.get("businessKey").split(",") if c.strip()]
event_date    = dbutils.widgets.get("eventDateCol").strip()

SOURCE = f"{dbutils.widgets.get('srcCatalog')}.{dbutils.widgets.get('srcSchema')}.{dbutils.widgets.get('srcTableName')}"
TARGET = f"{dbutils.widgets.get('tgtCatalog')}.{dbutils.widgets.get('tgtSchema')}.{dbutils.widgets.get('tgtTableName')}"
DIM_SUBJECT = f"{dbutils.widgets.get('tgtCatalog')}.{dbutils.widgets.get('tgtSchema')}.dim_subject"

# COMMAND ----------

# Carry exactly the target fact's columns, minus the ones Gold fills itself:
# the fact's own IDENTITY key (<x>_sk), the resolved subject_sk, and gold_load_ts.
MANAGED = {"subject_sk", "gold_load_ts"}
target_cols = spark.table(TARGET).columns
carried = [c for c in target_cols if c not in MANAGED and not c.endswith("_sk")]

print(f"{entity}: {SOURCE} -> {TARGET}  eventDate={event_date}")
print("carried:", carried)

# COMMAND ----------

# MAGIC %md ## Resolve subject_sk — effective-dated, NOT is_active
# MAGIC Join each observation to the dim_subject VERSION that was active on the event
# MAGIC date. Joining on `is_active = true` instead is the classic Gold bug: it
# MAGIC attributes every historical result to the subject's CURRENT arm.

# COMMAND ----------

fct = spark.table(SOURCE).select(*carried)

dim = (spark.table(DIM_SUBJECT)
            .select("subject_sk", "USUBJID", "start_effective_date", "end_effective_date"))

cond = (
    (fct["USUBJID"] == dim["USUBJID"]) &
    (fct[event_date] >= dim["start_effective_date"]) &
    ((fct[event_date] < dim["end_effective_date"]) | dim["end_effective_date"].isNull())
)

out = (fct.join(dim, cond, "left")
          .select(fct["*"], dim["subject_sk"])
          .withColumn("gold_load_ts", F.current_timestamp()))

# COMMAND ----------

# MAGIC %md ## Overwrite the fact

# COMMAND ----------

out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET)

# COMMAND ----------

# MAGIC %md ## Verify — grain is unique, and no orphan observations
# MAGIC An orphan (subject_sk IS NULL) means an observation whose subject has no
# MAGIC dim_subject version covering the event date — usually a fact dated before the
# MAGIC subject's first demographics record. Worth surfacing, not silently dropping.

# COMMAND ----------

res     = spark.table(TARGET)
total   = res.count()
unique  = res.select(*business_key).distinct().count()
orphans = res.where("subject_sk IS NULL").count()

print(f"{entity}: {total} rows, {unique} distinct {business_key}, {orphans} orphan(s)")
assert total == unique, f"GRAIN BROKEN: {total} rows but {unique} distinct keys"
if orphans:
    print(f"WARN: {orphans} fact rows did not resolve a subject_sk (event before first DM record?)")

dbutils.notebook.exit(f"OK|{entity}|{total} rows|{orphans} orphans")
