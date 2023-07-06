# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

# MAGIC %run Repos/apps/ios-dau/utils/common_utils

# COMMAND ----------

INPUT_P_EST_PATH = dbutils.widgets.get("input_p_est_path")
INPUT_DAU_EST_BASE_PATH = dbutils.widgets.get("input_dau_est_base_path")
DATES = dbutils.widgets.get("dates").split(",")
INPUT_COUNTRIES_IOS_POP = dbutils.widgets.get("input_countries_ios_pop")
OUTPUT_PATH = dbutils.widgets.get("output_path")
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'
ENVS = generate_env_list(dbutils.widgets.get("envs"))
PROD_ENVS = generate_prod_env()

# COMMAND ----------

p_estimations_df =  SparkDataArtifact().read_dataframe(INPUT_P_EST_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS)\
    .select("country", "app", "p", "avg_dau_p", "n")

published_dau_stats_df = read_between_dates(DATES[0], DATES[1], INPUT_DAU_EST_BASE_PATH, envs=PROD_ENVS)\
.select('date', "country", "app", col("active_users").alias("dau")) \
.filter("country != 999") \
.groupBy("country", "app") \
.agg(F.avg("dau").alias("published_avg_dau"),
        F.sum("dau").alias("published_sum_dau"),
        F.max("dau").alias("published_max_dau"),
        F.count('date').alias("n_dau"))


# use binomial_mean to reduce the statistical noise on a small apps (considering our panel size)
au_estimations_df = p_estimations_df \
    .join(published_dau_stats_df, ["country", "app"]) \
    .withColumn("active_users", binomial_mean(col("p"), col("n")) * col("published_avg_dau") / col("avg_dau_p")) \
    .select("country", "app", "active_users")

stats_df = published_dau_stats_df

countries_user_population_df = SparkDataArtifact().read_dataframe(INPUT_COUNTRIES_IOS_POP, spark.read.format("parquet"), debug=True, data_sources=PROD_ENVS)\
    .select("country", "population_size")

NOISE_LIMIT =   0.0025
BUFFER_BORDER = 0.0050
aligned_au_estimations_df = stats_df \
    .join(au_estimations_df, ["country", "app"], "left") \
    .join(countries_user_population_df, ["country"]) \
    .withColumn("clean_active_users", col("active_users")) \
    .withColumn("min_border", F.col("published_max_dau"))\
    .withColumn("max_border", F.least(col("population_size"), col("published_sum_dau"))) \
    .withColumn("active_users", F
                .when((col("active_users") >= col("min_border")) & (col("active_users") <= col("max_border")), col("active_users"))
                .when( col("active_users") >  col("max_border"), col("max_border") * (1 - BUFFER_BORDER + NOISE_LIMIT - 2 * NOISE_LIMIT * F.rand(seed=0)))
                .when( col("active_users") <  col("min_border"), col("min_border") * (1 + BUFFER_BORDER - NOISE_LIMIT + 2 * NOISE_LIMIT * F.rand(seed=0)))) \
    .withColumn("active_users", F.least(F.greatest(col("active_users"), col("min_border")), col("max_border")))

results = aligned_au_estimations_df\
    .select("country", "app", "active_users", "clean_active_users", "min_border", "max_border", "published_avg_dau")

# COMMAND ----------

write_output(results.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)