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

model_versions_sources_estimations_df = SparkDataArtifact().read_dataframe(INPUT_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS)\
                         .select("country", "app", "source", "model_type", "model_date", "p", "n", "reliability")

# fill zero when no occurrences in the source
# TODO handel reliability
all_apps = models_sources_estimations_df.select("country", "app").distinct()
full_models_sources_estimations_df = all_apps \
    .join(models_sources_estimations_df.select('country', 'source', "model_type", "model_date", 'n').distinct(), ["country"]) \
    .join(models_sources_estimations_df, ['country', "app", 'source', "model_type", "model_date", 'n'], "left") \
    .na.fill(value=0, subset=["p", "reliability"])

# take the latest model_date
app_latest_model_date_df = models_sources_estimations_df\
    .groupBy("country", "app", "source", "model_type")\
    .agg(F.max("model_date").alias("app_latest_model_date"))

full_model_sources_estimations_df = full_models_sources_estimations_df \
    .join(app_latest_model_date_df, ["country", "app", "source", "model_type"], "left") \
    .withColumn("default_latest_model_date", F.max("model_date").over(Window.partitionBy("country", "app", "source", "model_type"))) \
    .withColumn("latest_model_date", F.coalesce("app_latest_model_date", "default_latest_model_date"))\
    .filter(col("model_date") == col("latest_model_date")) \
    .select("country", "app", "source", "model_type", "p", "n", "reliability")

# TODO merge diff model_types of the same source (same users)
# for now we have only one model - ignore it
sources_estimations_df = full_model_sources_estimations_df \
    .drop("model_type") \
    .select("country", "app", "source", "p", "n", "reliability")

# merge sources (diff users)
estimations_df = sources_estimations_df \
    .groupBy('country', 'app') \
    .agg(w_avg("p", "n").alias("p"),
            F.ceil(F.sum("n")).alias("n"),
            w_avg("reliability", "n").alias("reliability"))
    .select("country", "app", "p", "n", "reliability") \
        .orderBy("country", "app") \

# COMMAND ----------

write_output(estimations_df.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)