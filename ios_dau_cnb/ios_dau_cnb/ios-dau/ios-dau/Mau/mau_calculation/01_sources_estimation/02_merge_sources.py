# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

# MAGIC %run Repos/apps/ios-dau/utils/common_utils

# COMMAND ----------

INPUT_PATH = dbutils.widgets.get("input_path")
OUTPUT_PATH = dbutils.widgets.get("output_path")
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'
ENVS = generate_env_list(dbutils.widgets.get("envs"))

# COMMAND ----------

sources_estimations_df =  SparkDataArtifact().read_dataframe(INPUT_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS) \
    .select("source", "country", "app", "p", "avg_dau_p", "max_dau_p", "n")

# fill zero when no occurrences in the source
all_apps = sources_estimations_df.select("country", "app").distinct()
sources_estimations_df = all_apps \
    .join(sources_estimations_df.select('source', 'country', 'n').distinct(), ["country"]) \
    .join(sources_estimations_df, ['source', 'country', "app", 'n'], "left") \
    .na.fill(value=0, subset=["p", "avg_dau_p", "max_dau_p"])

estimations_df = sources_estimations_df \
    .groupBy('app', 'country') \
    .agg(w_avg("p", "n").alias("p"),
            F.sum("n").alias("n"),
            w_avg("avg_dau_p", "n").alias("avg_dau_p"),
            w_avg("max_dau_p", "n").alias("max_dau_p"))

results = estimations_df \
    .select("country", "app", "p", "avg_dau_p", "max_dau_p", "n") \
    .orderBy("country")

# COMMAND ----------

write_output(results.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)