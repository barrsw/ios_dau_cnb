# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

# MAGIC %run Repos/apps/ios-dau/utils/common_utils

# COMMAND ----------

INPUT_BASE_PATH = dbutils.widgets.get("input_base_path")
EXEC_DATE = dbutils.widgets.get("exec_date")
HISTORIC_DATES = dbutils.widgets.get("historic_dates").split(",")
OUTPUT_PATH = dbutils.widgets.get("output_path")
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'
ENVS = generate_env_list(dbutils.widgets.get("envs"))

HISTORIC_PRIOR_THRESHOLD = 100

# COMMAND ----------

estimations_df = SparkDataArtifact().read_dataframe(INPUT_BASE_PATH + add_year_month(EXEC_DATE), spark.read.format("parquet"), debug=True, data_sources=ENVS)\
    .withColumn("date", F.lit(EXEC_DATE).cast("date"))\
    .select('date', "country", "app", "p", "avg_dau_p", "n")


historic_estimations_dfs = [SparkDataArtifact().read_dataframe(INPUT_BASE_PATH + add_year_month(d), spark.read.format("parquet"), debug=True, data_sources=ENVS).withColumn("date", F.lit(d).cast("date")) for d in HISTORIC_DATES]

historic_estimations_df = reduce(DataFrame.unionByName, historic_estimations_dfs)\
    .select("date", "country", "app", "p", "avg_dau_p", "n")


# fill zero on missing (no occurrences in the panel) dates
# we want to have the best estimation we can give for each app with DAU estimation
all_apps = estimations_df.select("country", "app")\
    .unionByName(historic_estimations_df.select("country", "app"))\
    .distinct()

estimations_df = all_apps \
    .join(estimations_df.select('country', "n").distinct(), ["country"]) \
    .join(estimations_df,  ["app", 'country', "n"], "left") \
    .na.fill(value=0, subset=["p", "avg_dau_p"])\


historic_estimations_df = all_apps \
    .join(historic_estimations_df.select('date', 'country',  'n').distinct(), ["country"]) \
    .join(historic_estimations_df,  ['date', "app", 'country', "n"], "left") \
    .na.fill(value=0, subset=["p", "avg_dau_p"])

# historic prior = historic scale
# include the current date
historic_prior_df = estimations_df \
    .unionByName(historic_estimations_df) \
    .groupBy("country", "app") \
    .agg(w_avg("p", "n"),
            w_avg("avg_dau_p", "n"),
            F.avg("n").alias("n"))\

# fuse the historic prior knowledge with the estimation
# we use the old (DAU's) prior algorithm - we should re-evaluate it later
estimations_df = estimations_df \
    .select("country", "app", *[col(c).alias(f"clean_{c}") for c in ["p", "avg_dau_p", "n"]])

historic_prior_df = historic_prior_df \
    .select("app", "country", *[col(c).alias(f"historic_{c}") for c in ["p", "avg_dau_p",  "n"]])

fused_estimations_df = estimations_df\
    .join(historic_prior_df, ["country", "app"]) \
    .withColumn("historic_w", F.when(col("clean_p") * col("clean_n") <= HISTORIC_PRIOR_THRESHOLD,
                ((HISTORIC_PRIOR_THRESHOLD - col("clean_p") * col("clean_n")) / (HISTORIC_PRIOR_THRESHOLD + 1)))
                .otherwise(0)) \
    .withColumn("p",         col("clean_p")         * (1 - col("historic_w")) +         col("historic_p") * col("historic_w")) \
    .withColumn("avg_dau_p", col("clean_avg_dau_p") * (1 - col("historic_w")) + col("historic_avg_dau_p") * col("historic_w")) \
    .withColumn("n",         col("clean_n")         * (1 - col("historic_w")) +         col("historic_n") * col("historic_w"))\


results = fused_estimations_df\
    .select("country", "app", "p", "avg_dau_p", "n",
            "clean_p", "clean_avg_dau_p", "clean_n",
            "historic_p", "historic_avg_dau_p", "historic_n", "historic_w")

# COMMAND ----------

write_output(results.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)