# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

##------ARGS------##
DATE_STR = dbutils.widgets.get("date")
ENVS = dbutils.widgets.get("envs")

# COMMAND ----------

PREFIX_PATH = "/similargroup/data/ios-analytics/metrics/dau/"
YEAR_MONTH_DAY_SUFFIX = add_year_month_day(DATE_STR)
DATE = datetime.strptime(DATE_STR, "%Y-%m-%d").date()
OVERWRITE_MODE = 'True'

# COMMAND ----------

##------INPUTS------##
APPLY_TREND_BASE_PATH = PREFIX_PATH + "trend_estimation_applied" + YEAR_MONTH_DAY_SUFFIX
COUNTRIES_IOS_POPULATION_PATH = "/similargroup/data/ios-analytics/countries_ios_population/year=22/month=10/day=01"
BUNDLE_INFO_PATH = 's3://sw-df-production-internal-data/apps/app_details/app_store_bundle/' + (YEAR_MONTH_DAY_SUFFIX if DATE >= date(2022, 11, 27) else add_year_month_day(date(2022, 11, 27)))

##-----OUTPUTS-----##
ESTIMATION_PATH = PREFIX_PATH + "estimation" + YEAR_MONTH_DAY_SUFFIX

#------JOB PARAMS-----#

# COMMAND ----------

dbutils.notebook.run("./01_adjust_to_dau", 3600, arguments={"input_path": APPLY_TREND_BASE_PATH, 
                                                       "bundle_info_path": BUNDLE_INFO_PATH, 
                                                       "output_path": ESTIMATION_PATH,
                                                       "countries_ios_population_size": COUNTRIES_IOS_POPULATION_PATH,
                                                       "envs": ENVS, 
                                                       "overwrite_mode": OVERWRITE_MODE})