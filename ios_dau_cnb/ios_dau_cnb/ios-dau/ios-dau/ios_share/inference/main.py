# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

ENVS = dbutils.widgets.get("envs")
DATE_STR = dbutils.widgets.get("date")
OVERWRITE_MODE = "True"

# COMMAND ----------

YEAR_MONTH_DAY_SUFFIX = add_year_month_day(DATE_STR)
PAST_YEAR_MONTH_SUFFIX = "/year=22/month=08"
PREFIX_PATH = "/similargroup/data/ios-analytics/metrics/dau/"

##------INPUTS-----##
IOS_RATIOS_PROCESSED_PATH = PREFIX_PATH + "ios_ratios_processed" + PAST_YEAR_MONTH_SUFFIX
ANDROID_EST_PATH = "s3a://sw-apps-core-data-buffer/phoenix1/similargroup/data/android-apps-analytics/metrics/dau/p_estimations_corrected_bias/" + YEAR_MONTH_DAY_SUFFIX
##
OUTPUT_PATH = PREFIX_PATH + "dau" + YEAR_MONTH_DAY_SUFFIX

COUNTRIES = "840"

# COMMAND ----------

dbutils.notebook.run("./make_dau", 3600, arguments = {"android_est_path": ANDROID_EST_PATH, "ratios_path": IOS_RATIOS_PROCESSED_PATH, "output_path": OUTPUT_PATH, "countries": COUNTRIES, "envs": ENVS, "overwrite_mode": OVERWRITE_MODE})