# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

ENVS = generate_env_list(dbutils.widgets.get("envs"))
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'

##-----INPUTS------##
SOLVED_SESSIONS_PATH = dbutils.widgets.get("solved_sessions_path")

##------OUTPUT-----##
OUTPUT_PATH = dbutils.widgets.get("output_path")


# COMMAND ----------

def make_dau(sessions_path: str) -> DataFrame:
    df = SparkDataArtifact().read_dataframe(sessions_path, spark.read.format("parquet"), debug=True, data_sources=ENVS).select("did", "country", "bundle_id", "score")
    num = (df
           .groupBy("did", "country", "bundle_id").agg(F.max("score").alias("visit_frac"))
           .groupBy("country", "bundle_id").agg(F.sum("visit_frac").alias("total_visits"))
          )
    denom = df.groupBy("country").agg(F.countDistinct("did").alias("total_users"))
    return num.join(denom, ["country"]).withColumn("dau", F.expr("total_visits/total_users"))

# COMMAND ----------

dau = make_dau(SOLVED_SESSIONS_PATH)

# COMMAND ----------

write_output(dau.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)