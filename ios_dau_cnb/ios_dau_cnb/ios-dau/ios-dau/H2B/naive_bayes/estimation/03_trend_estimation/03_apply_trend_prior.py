# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

# MAGIC %run Repos/apps/ios-dau/utils/common_utils

# COMMAND ----------

RAW_TREND_ESTIMATION_BASE_PATH = dbutils.widgets.get("raw_trend_estimation_base_path")
ANDROID_DAU_BASE_PATH = dbutils.widgets.get("android_dau_path")
ANDROID_DATES = dbutils.widgets.get("android_dates").split(',')
MATCHES_PATH = dbutils.widgets.get("matches_path")
BUNDLE_INFO_PATH =  dbutils.widgets.get("bundle_info_path")
DATE_STR = dbutils.widgets.get("date")
AVG_SCALE_ANCHORS_DATES_STR = dbutils.widgets.get("avg_scale_anchors_dates")
AVG_SCALE_ANCHORS_DATES = AVG_SCALE_ANCHORS_DATES_STR.split(',') if AVG_SCALE_ANCHORS_DATES_STR != 'EMPTY_STRING' else []
DAY2DAY_ANCHOR_DATES = dbutils.widgets.get("day2day_anchors_dates").split(',')
OUTPUT_PATH = dbutils.widgets.get("output_path")
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'
ENVS = generate_env_list(dbutils.widgets.get("envs"))

# COMMAND ----------

trend_estimations_df = (SparkDataArtifact().read_dataframe(RAW_TREND_ESTIMATION_BASE_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS)
    .withColumn("date", F.lit(DATE_STR).cast("date"))
    .select("date", "country", "app", "trend_id", "trend", "p", "n", "anchor_p", "anchor_n", "stability_score", "reliability"))

android_dau_df = read_between_dates(start=ANDROID_DATES[0], end=ANDROID_DATES[-1], path=ANDROID_DAU_BASE_PATH) \
    .select("date", "country", F.col("app").alias("android_app_id"), "active_users")

android_to_ios_df = match_apps(MATCHES_PATH, BUNDLE_INFO_PATH)\
    .select(F.col("android_id").alias("android_app_id"), F.col("bundle_id").alias("app"))

android_dau_df = android_dau_df\
    .join(android_to_ios_df, ["android_app_id"])\
    .drop("android_app_id")

android_dau_subject_df = android_dau_df \
    .join(F.broadcast(trend_estimations_df.select("date").distinct()), "date") \
    .select("country", "app", "active_users")

android_dau_anchors_df = android_dau_df \
    .join(F.broadcast(trend_estimations_df.select("date").distinct()), "date", "leftanti") \
    .select(F.col("date").alias("anchor_date"), "country", "app", F.col("active_users").alias("anchor_active_users"))

android_dau_trends = android_dau_subject_df.select( "country", "app")\
    .unionByName(android_dau_anchors_df.select("country", "app")).distinct()\
    .crossJoin(android_dau_anchors_df.select("anchor_date").distinct())\
    .join(android_dau_subject_df, ["country", "app"], "left")\
    .join(android_dau_anchors_df, ["anchor_date", "country", "app"], "left") \
    .na.fill(value=0, subset=["active_users", "anchor_active_users"])

# day-to-day trends
android_dau_trends_est_df = android_dau_trends \
    .filter(F.col("anchor_date").isin(*DAY2DAY_ANCHOR_DATES)) \
    .withColumn("trend_id", F.struct(F.lit("DAY").alias("type"),
                                     F.col("anchor_date").cast("string").alias("anchor_dates"))) \
    .drop("anchor_date")

# avg-scale-trend
for avg_scale_anchor_dates in AVG_SCALE_ANCHORS_DATES:
    android_dau_avg_scale_trends_df = android_dau_trends \
        .filter(F.col("anchor_date").isin(*avg_scale_anchor_dates.split(","))) \
        .groupBy("country", "app") \
        .agg(F.avg("active_users").alias("active_users"),
             F.avg("anchor_active_users").alias("anchor_active_users"),
             F.count("anchor_date").alias("n_days")) \
        .filter(F.col("n_days") == len(avg_scale_anchor_dates.split(","))).drop("n_days") \
        .withColumn("trend_id", F.struct(F.lit("AVG_SCALE").alias("type"),
                                         F.lit(avg_scale_anchor_dates).alias("anchor_dates")))

    android_dau_trends_est_df = android_dau_trends_est_df.unionByName(android_dau_avg_scale_trends_df)

android_dau_trends_est_df = android_dau_trends_est_df \
    .filter(F.col("anchor_active_users") > 0) \
    .withColumn("trend", F.col("active_users") / F.col("anchor_active_users")) \
    .select("country", "app", "trend_id", F.col("trend").alias("prior_trend"), "active_users", "anchor_active_users")

PRIOR_PCT = 0

trend_estimations_df = trend_estimations_df\
    .withColumnRenamed("trend", "clean_trend")\
    .join(android_dau_trends_est_df, ["country", "app", "trend_id"], "left")\
    .withColumn("trend", F.when(F.col("prior_trend").isNotNull(),
                                (1 - PRIOR_PCT) * F.col("clean_trend") + PRIOR_PCT * F.col("prior_trend"))
                          .otherwise(F.col("clean_trend")))

trend_estimations_df = trend_estimations_df \
    .select("country", "app", "trend_id", "trend", "p", "n", "anchor_p", "anchor_n", "stability_score",
            "reliability",
            "clean_trend", "prior_trend")\
    .orderBy("country", "app")

# COMMAND ----------

write_output(trend_estimations_df.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)