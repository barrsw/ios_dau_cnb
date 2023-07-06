# Databricks notebook source
# MAGIC %md This notebook calculates factors between old iOS Mau to new iOS Mau based on active users to apply them on old MAU data in order to generate "New iOS Mau".

# COMMAND ----------

# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

DATA_SOURCES = [
    {"type": "s3a", "name": "sw-apps-core-data-buffer", "prefix": ""},
    {"type": "s3a", "name": "sw-apps-core-data", "prefix": ""},
    {"type": "s3a", "name": "sw-apps-core-data-buffer", "prefix": "/phoenix1"},
]

OVERLAP_PERIOD_START_DATE = date(2022, 10, 1)
OVERLAP_PERIOD_END_DATE = date(2022, 12, 1)

LEGACY_IOS_MAU_PATH = "similargroup/data/ios-analytics/metrics/active_users/mau/serve_model"
IOS_MAU_PATH = "similargroup/data/ios-analytics/metrics/active_users/mau/estimation"

FACTORS_OUTPUT_PATH = "s3://sw-apps-core-data-buffer/user/ios_dau_phoenix/ios_mau_factors/year=22/month=10/day=01"

# COMMAND ----------

legacy_ios_mau = (SparkDataArtifact().read_dataframe(LEGACY_IOS_MAU_PATH, spark.read.format("parquet"), debug=True, data_sources=DATA_SOURCES)
.withColumn("day", F.lit(1))
.add_date()
.filter(f"date >= '{OVERLAP_PERIOD_START_DATE}' AND date <= '{OVERLAP_PERIOD_END_DATE}'")
.selectExpr("date", "ios_bundle_id as app", "ios_bundle_id", "country", "prediction_real_number"))

ios_mau = (SparkDataArtifact().read_dataframe(IOS_MAU_PATH, spark.read.format("parquet"), debug=True, data_sources=DATA_SOURCES)
.withColumn("day", F.lit(1))
.add_date()
.filter(f"date >= '{OVERLAP_PERIOD_START_DATE}' AND date <= '{OVERLAP_PERIOD_END_DATE}'")
.selectExpr("date", "app", "country", "active_users"))

# COMMAND ----------

factors = (ios_mau
.join(legacy_ios_mau, ["app", "country", "date"], "inner")
.withColumn("factor", F.col("active_users") / F.col("prediction_real_number"))
.groupBy("app", "country")
.agg(F.avg("factor").alias("factor")))

# COMMAND ----------

factors.write.mode("overwrite").parquet(FACTORS_OUTPUT_PATH)