# Databricks notebook source
# MAGIC %md This notebook applies factors on old MAU data between Nov 20 - Sep 22 in order to generate "New iOS Mau".

# COMMAND ----------

# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

DATA_SOURCES = [
    {"type": "s3a", "name": "sw-apps-core-data-buffer", "prefix": ""},
    {"type": "s3a", "name": "sw-apps-core-data", "prefix": ""},
    {"type": "s3a", "name": "sw-apps-core-data-buffer", "prefix": "/phoenix1"},
]

START_DATE = date(2020, 11, 1)
END_DATE = date(2022, 9, 1)

LEGACY_IOS_MAU_PATH = "similargroup/data/ios-analytics/metrics/active_users/mau/serve_model"
FACTORS_PATH = "s3://sw-apps-core-data-buffer/user/ios_dau_phoenix/ios_mau_factors/year=22/month=10/day=01"

OUTPUT_PATH = "s3://sw-apps-core-data-buffer/user/ios_dau_phoenix/ios_mau/estimation"

LAGACY_ADAPTER_PATH = "s3://sw-apps-core-data-buffer/user/ios_dau_phoenix/ios_mau/legacy_adapted_estimation"
ITUNES_PATH = "s3a://sw-df-production-internal-data/apps/app_details/app_store_bundle/year=23/month=03/day=19"

# COMMAND ----------

factors_df = spark.read.parquet(FACTORS_PATH)

# COMMAND ----------

for d in get_dates_range(START_DATE, END_DATE, mode="monthly"):
    (SparkDataArtifact().read_dataframe(LEGACY_IOS_MAU_PATH + add_year_month(d), spark.read.format("parquet"), debug=True, data_sources=DATA_SOURCES)
    .selectExpr("ios_bundle_id as app", "country", "prediction_real_number as active_users")
    .join(factors_df, ["app", "country"], "left")
    .fillna(1.0, subset=["factor"])
    .withColumn("active_users", F.col("active_users") * F.col("factor"))
    .select(
        F.col("country").cast("int").alias("country"),
        "app",
        F.col("active_users").cast("double").alias("active_users"),
        F.lit(0.0).cast("double").alias("clean_active_users"),
        F.lit(0.0).cast("double").alias("min_border"),
        F.lit(0.0).cast("double").alias("max_border"),
        )
    .write.mode("overwrite").parquet(OUTPUT_PATH + add_year_month(d))
    )

# COMMAND ----------

itunes_df = spark.read.parquet(ITUNES_PATH)

for d in get_dates_range(START_DATE, END_DATE, mode="monthly"):
    (spark.read.parquet(OUTPUT_PATH)
    .withColumnRenamed("app", "bundle_id")
    .join(itunes_df.select("bundle_id", F.col("id").alias("app")),
           "bundle_id", "left")
    .select("country", "bundle_id", "app", "active_users", F.lit(1.0).alias("confidence"))
    .orderBy("country", "bundle_id")
    .write.mode("overwrite").parquet(LAGACY_ADAPTER_PATH + add_year_month(d))
    )