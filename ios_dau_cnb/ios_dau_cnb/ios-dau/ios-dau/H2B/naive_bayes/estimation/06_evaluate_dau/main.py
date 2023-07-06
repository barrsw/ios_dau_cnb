# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

##------ARGS------##
DATE_STR = dbutils.widgets.get("date")
ENVS = dbutils.widgets.get("envs")

# COMMAND ----------

PREFIX_PATH = "/similargroup/data/ios-analytics/metrics/dau/"
YEAR_MONTH_DAY_SUFFIX = add_year_month_day(DATE_STR)

OVERWRITE_MODE = 'True'

# COMMAND ----------

##------INPUTS------##
ESTIMATION_BASE_PATH = PREFIX_PATH + "estimation"
GA_BASE_PATH = "/similargroup/data/store-analytics/general/ga4_extractor/processed_ls/"

##-----OUTPUTS-----##
EVALUATION_PATH = PREFIX_PATH + "evaluation" + YEAR_MONTH_DAY_SUFFIX

#------JOB PARAMS-----#
COUNTRIES = "840,826,124,36"
MIN_GA = 30_000
MIN_USERS = 3
FOLDS = 5

# COMMAND ----------

dbutils.notebook.run("./01_evaluate_dau", 3600, arguments={"input_base_path": ESTIMATION_BASE_PATH, 
                                                       "ga_base_path": GA_BASE_PATH, 
                                                       "output_path": EVALUATION_PATH,
                                                       "folds": FOLDS,
                                                       "min_ga": MIN_GA,
                                                       "min_users": MIN_USERS,
                                                       "countries": COUNTRIES,
                                                       "date": DATE_STR,
                                                       "envs": ENVS, 
                                                       "overwrite_mode": OVERWRITE_MODE})