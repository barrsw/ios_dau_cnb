# Databricks notebook source
# MAGIC %run ../utils/infra_utils

# COMMAND ----------

from dateutil import relativedelta
from datetime import datetime

# COMMAND ----------

PANEL_PATH = dbutils.widgets.get("input_path")
OUTPUT_PATH = dbutils.widgets.get("output_path")
EXEC_DATE = datetime.strptime(dbutils.widgets.get("date"), "%Y-%m-%d")
ENVS = generate_env_list(dbutils.widgets.get("envs"))

# COMMAND ----------

def generate_inner_vpn_suffix(d):
    return "{}/{}/{:02d}/{:02d}/*".format(PANEL_PATH, d.year, d.month, d.day)

# COMMAND ----------

PREVIOUS_DATE_PATH = generate_inner_vpn_suffix(EXEC_DATE - relativedelta.relativedelta(days=1))
EXEC_DATE_PATH = generate_inner_vpn_suffix(EXEC_DATE)
NEXT_DATE_PATH = generate_inner_vpn_suffix(EXEC_DATE + relativedelta.relativedelta(days=1))

# COMMAND ----------

print(PREVIOUS_DATE_PATH)
print(EXEC_DATE_PATH)
print(NEXT_DATE_PATH)

# COMMAND ----------

ingested_panel_df = (spark.read.parquet(PREVIOUS_DATE_PATH,EXEC_DATE_PATH,NEXT_DATE_PATH)
            .select("did", F.col("country").cast("int").alias("country"), F.col("bundle").alias("source"), "model", "cv", "osv", "v", F.col("tz").cast("int").alias("utc_offset"), F.explode("events").alias("event"))
            .select('*', "event.*")
            .withColumn("local_ts", (F.col("ts")/1000).cast("timestamp"))
            .withColumn("utc_ts", (F.col("local_ts").cast("bigint") - F.col("utc_offset")).cast("timestamp"))
            .withColumn("exec_date", F.from_utc_timestamp(F.col("utc_ts"), "EST").cast('date'))
            .filter(F.col("exec_date") == EXEC_DATE)
            .select("country", "source", "did",  "model", "cv", "osv", "v", "local_ts", "utc_ts", F.col("ts").alias("local_ts_ms"), "host", "ua")
            .orderBy("country", "source", "did"))

# COMMAND ----------

write_output(ingested_panel_df.write.format("parquet"), OUTPUT_PATH, ENVS, overwrite=True)