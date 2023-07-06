# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

SOLVED_SESSIONS_PATH = dbutils.widgets.get("solved_sessions_path")
MIN_INFO = float(dbutils.widgets.get("min_info"))
OUTPUT_PATH = dbutils.widgets.get("output_path")
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'
ENVS = generate_env_list(dbutils.widgets.get("envs"))

# COMMAND ----------

df = (SparkDataArtifact().read_dataframe(SOLVED_SESSIONS_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS)
          .filter(f"info_Q >= {MIN_INFO}")
          .select("did", "country", "source", "app", "score", "model_date", "model_type", "is_main_model"))

user_app_score = (df
                    .groupBy("did", "country", "source", "app", "model_date", "model_type", "is_main_model")
                    .agg(F.max("score").alias("score")))  # take max score per app user over the day

num = (user_app_score
        .groupBy("country", "source", "app", "model_date", "model_type", "is_main_model")
        .agg(F.sum("score").alias("total_visits")))

denom = (user_app_score
            .groupBy("country", "source", "model_date", "model_type", "is_main_model")
            .agg(F.countDistinct("did").alias("total_users")))

source_raw_estimation = (num.join(denom, ["country", "source", "model_date", "model_type", "is_main_model"])
                            .filter(f"total_visits > {MIN_K_THRESHOLD}")
                            .select("country", "app", "source", "model_date", "model_type", "is_main_model",
                                    (F.col("total_visits") / F.col("total_users")).cast("double").alias("p"),
                                    F.col("total_users").alias("n"),
                                    F.lit(1).cast("float").alias("reliability"))
                            .orderBy("country", "app"))

# COMMAND ----------

write_output(source_raw_estimation.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)