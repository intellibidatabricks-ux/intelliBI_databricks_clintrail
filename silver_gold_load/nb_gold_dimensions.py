# Databricks notebook source
# MAGIC %md
# MAGIC # Silver -> Gold : DIMENSIONS  (SCD Type 1 / 2)
# MAGIC
# MAGIC One notebook, every dimension. ADF: Lookup (EntityType='dimension') -> ForEach.
# MAGIC
# MAGIC   ScdType 1 -> MERGE upsert in place, no history.
# MAGIC   ScdType 2 -> keep history with:
# MAGIC        start_effective_date, end_effective_date, is_active, record_version.
# MAGIC
# MAGIC Silver is current-state; GOLD is the history. Each run compares the Silver
# MAGIC snapshot to Gold's active rows and records what changed.

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

dbutils.widgets.text("srcCatalog",   "clintrail_dev")
dbutils.widgets.text("srcSchema",    "clintrail_silver")
dbutils.widgets.text("srcTableName", "ta")
dbutils.widgets.text("tgtCatalog",   "clintrail_dev")
dbutils.widgets.text("tgtSchema",    "clintrail_gold")
dbutils.widgets.text("tgtTableName", "dim_arm")
dbutils.widgets.text("scdType",      "1")           # 1 | 2
dbutils.widgets.text("businessKey",  "STUDYID,ARMCD,TAETORD")

scd_type = dbutils.widgets.get("scdType").strip()
keys     = [c.strip() for c in dbutils.widgets.get("businessKey").split(",") if c.strip()]

SOURCE = f"{dbutils.widgets.get('srcCatalog')}.{dbutils.widgets.get('srcSchema')}.{dbutils.widgets.get('srcTableName')}"
TARGET = f"{dbutils.widgets.get('tgtCatalog')}.{dbutils.widgets.get('tgtSchema')}.{dbutils.widgets.get('tgtTableName')}"
CHANGE_TIME = "MODIFIED_TS"   # from Silver -> start_effective_date

# COMMAND ----------

# Figure out which columns to carry from Silver = the target's columns, minus the
# ones Gold fills itself (the _sk key and the SCD control columns).
SCD_COLS = {"hash_diff", "start_effective_date", "end_effective_date",
            "is_active", "record_version", "gold_load_ts"}
carried = [c for c in spark.table(TARGET).columns
           if c not in SCD_COLS and not c.endswith("_sk")]
attrs   = [c for c in carried if c not in keys]   # non-key attributes
# print("Total Columns: ",spark.table(TARGET).columns)
# print("Carried Columns: ",carried)
# print("Attributes: ",attrs)
# hash_diff = one fingerprint of the attributes. Change detection = compare hashes.
src = spark.table(SOURCE)
src = src.withColumn("hash_diff",
    F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in attrs]), 256))

print(f"{SOURCE} -> {TARGET}  scd={scd_type}  keys={keys}")

src.display()

# COMMAND ----------

# MAGIC %md ## SCD Type 1 — upsert in place

# COMMAND ----------

if scd_type == "1":
    incoming = src.select(*carried, "hash_diff").withColumn("gold_load_ts", F.current_timestamp())
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)

    # Explicit column map, NOT updateAll/insertAll. The target has a GENERATED ALWAYS
    # AS IDENTITY key (<x>_sk) that is absent from the source and can never be written
    # by hand; *All would try to resolve it and fail. Listing columns skips it, so
    # Delta auto-generates the key on insert.
    cols = incoming.columns          # carried + hash_diff + gold_load_ts (no _sk)
    values = {c: f"s.{c}" for c in cols}

    (DeltaTable.forName(spark, TARGET).alias("t")
       .merge(incoming.alias("s"), on)
       .whenMatchedUpdate(condition="t.hash_diff <> s.hash_diff", set=values)  # only if changed
       .whenNotMatchedInsert(values=values)
       .execute())
    dbutils.notebook.exit(f"OK|scd1|{spark.table(TARGET).count()} rows")

# COMMAND ----------

# MAGIC %md ## SCD Type 2 — two simple steps
# MAGIC 1. find keys whose data changed (or are brand new)
# MAGIC 2. **expire** their current row, then **append** the new version
# MAGIC
# MAGIC Expire first (while only old rows are active), then append the new active row,
# MAGIC so there is always exactly one active version per key.

# COMMAND ----------

# what version / hash is each key currently on? (absent = brand-new key)
current = (spark.table(TARGET).where("is_active = true")
              .select(*keys,
                      F.col("hash_diff").alias("cur_hash"),
                      F.col("record_version").alias("cur_version")))

# join incoming to current; a NULL cur_hash means new key, a different hash means changed
joined = src.join(current, keys, "left")

new_versions = (
    joined
      .where(F.col("cur_hash").isNull() | (F.col("hash_diff") != F.col("cur_hash")))
      .withColumn("record_version",       F.coalesce(F.col("cur_version"), F.lit(0)) + 1)
      .withColumn("start_effective_date",  F.coalesce(F.col(CHANGE_TIME), F.current_timestamp()))
      .withColumn("end_effective_date",    F.lit(None).cast("timestamp"))
      .withColumn("is_active",             F.lit(True))
      .withColumn("gold_load_ts",          F.current_timestamp())
      .select(*carried, "hash_diff", "record_version",
              "start_effective_date", "end_effective_date", "is_active", "gold_load_ts")
)

n_changed = new_versions.count()
print(f"{n_changed} new/changed version(s)")

# COMMAND ----------

# STEP 1 — expire the current row for every key getting a new version.
# end_effective_date = the new version's start, so the timeline has no gap.
if n_changed:
    expire = new_versions.select(*keys, F.col("start_effective_date").alias("new_start"))
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys) + " AND t.is_active = true"
    (DeltaTable.forName(spark, TARGET).alias("t")
       .merge(expire.alias("s"), on)
       .whenMatchedUpdate(set={"is_active": "false", "end_effective_date": "s.new_start"})
       .execute())

    # STEP 2 — append the new active versions.
    new_versions.write.format("delta").mode("append").saveAsTable(TARGET)

# COMMAND ----------

# MAGIC %md ## Verify — exactly one active version per key

# COMMAND ----------

dim   = spark.table(TARGET)
dupes = dim.where("is_active = true").groupBy(*keys).count().where("count > 1").count()
assert dupes == 0, f"SCD2 BROKEN: {dupes} key(s) have >1 active version"

total, active = dim.count(), dim.where("is_active = true").count()
print(f"{total} total rows, {active} active")
dbutils.notebook.exit(f"OK|scd2|{active} active / {total} total")
