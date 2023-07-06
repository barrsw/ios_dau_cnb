# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

INPUT_PATH = dbutils.widgets.get("input_path")
OUTPUT_PATH = dbutils.widgets.get("output_path")
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'
ENVS = generate_env_list(dbutils.widgets.get("envs"))

# COMMAND ----------

df = SparkDataArtifact().read_dataframe(INPUT_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS)
denom = df.select("country", "source", "total_users").distinct().groupBy("country").agg(F.sum("total_users").alias("total_users"))
num = df.groupBy("country", "bundle_id").agg(F.sum("total_visits").alias("total_visits"))
dau = num.join(denom, "country").withColumn("dau", F.expr("total_visits/total_users")).select("country", "bundle_id", "total_visits", "total_users", "dau")

# COMMAND ----------

write_output(dau.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)