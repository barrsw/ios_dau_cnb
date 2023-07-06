# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'
ENVS = generate_env_list(dbutils.widgets.get("envs"))

##-----INPUTS------##
INPUT_PATH = dbutils.widgets.get("input_path")
BUNDLE_INFO_PATH = dbutils.widgets.get("bundle_info_path")
COUNTRIES_IOS_POPULATION_PATH = dbutils.widgets.get("countries_ios_population_size")

##------OUTPUT-----##
OUTPUT_PATH = dbutils.widgets.get("output_path")

##----JOB PARAMS---##

# COMMAND ----------

df = (SparkDataArtifact().read_dataframe(INPUT_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS)
      .join(SparkDataArtifact().read_dataframe(COUNTRIES_IOS_POPULATION_PATH, spark.read.format("parquet"), debug=True), "country")
      .withColumn("active_users", (F.col("p") * F.col("population_size")))
      .join(spark.read.parquet(BUNDLE_INFO_PATH).select(F.col("bundle_id").alias("app"), F.col("id").alias("track_id")), "app", "left")
      .select("country", "app", "track_id", "active_users")
      .orderBy("country", "app"))

# COMMAND ----------

write_output(df.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)