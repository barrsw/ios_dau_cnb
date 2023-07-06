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

sources_estimations_df = SparkDataArtifact().read_dataframe(INPUT_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS) \
                         .select("country", "app", "source", "model_type", "model_date", 'trend_id', "p", "n", "anchor_p", "anchor_n", "trend", "reliability")

# take the latest model_date
sources_estimations_df = sources_estimations_df \
    .withColumn("latest_model_date", F.max("model_date").over(Window.partitionBy("country", "app", "source", "model_type", "trend_id"))) \
    .filter(F.col("model_date") == F.col("latest_model_date")) \
    .select("country", "app", "source", "model_type", 'trend_id', "p", "n", "anchor_p", "anchor_n", "trend", "reliability")

# for now we have only one model - ignore it
sources_estimations_df = sources_estimations_df \
    .drop("model_type")\
    .select("country", "app", "source", "trend_id", "p", "n", "anchor_p", "anchor_n", "trend", "reliability")

# merge diff sources
estimations_df = sources_estimations_df \
    .withColumn("stability_score", F.when(F.col("n") > F.col("anchor_n"),
                                          F.col("anchor_n")/F.col("n")).otherwise(F.col("n")/F.col("anchor_n"))) \
    .withColumn("w", F.col("n"))\
    .withColumn("w", F.col("w") / F.sum("w").over(Window.partitionBy('country', 'app', "trend_id"))) \
    .withColumn("n", F.col("w") * F.min(F.col("n")/F.col("w")).over(Window.partitionBy('country', 'app', "trend_id"))) \
    .withColumn("anchor_n", F.col("w") * F.min(F.col("anchor_n")/F.col("w")).over(Window.partitionBy('country', 'app', "trend_id"))) \
    .groupBy('country', 'app', "trend_id") \
    .agg(F.sum("n").alias("n"),
         F.sum("anchor_n").alias("anchor_n"),
         w_avg("p", "w").alias("p"),
         w_avg("anchor_p", "w").alias("anchor_p"),
         w_avg("stability_score", "w").alias("stability_score"),
         w_avg("reliability", "w").alias("reliability"),
         )\
    .filter(F.col("anchor_p") > 0) \
    .withColumn("trend", F.col("p") / F.col("anchor_p")) \
    .select("country", "app", "trend_id", "trend", "p", "n", "anchor_p", "anchor_n", "stability_score", "reliability") \
    .orderBy("country", "app")

# COMMAND ----------

write_output(estimations_df.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)