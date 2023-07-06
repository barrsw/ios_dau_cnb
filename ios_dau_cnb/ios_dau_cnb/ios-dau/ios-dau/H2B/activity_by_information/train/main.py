# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

##------INPUTS------##
DATE_STR = dbutils.widgets.get("date")
ENVS = dbutils.widgets.get("envs")

# COMMAND ----------

PREFIX_PATH = "/similargroup/data/ios-analytics/metrics/dau/"
OVERWRITE_MODE = 'True'
#-----DATES------#
YEAR_MONTH_SUFFIX = add_year_month(DATE_STR)
YEAR_MONTH = add_year_month(DATE_STR)
YEAR_MONTH_DAY_SUFFIX = add_year_month_day(DATE_STR)
DATE = datetime.strptime(DATE_STR, "%Y-%m-%d")
YEAR, MONTH, DAY = DATE.year, DATE.month, DATE.day

##------PATHS------##
MP_PATH = PREFIX_PATH + "ls" + YEAR_MONTH_DAY_SUFFIX
P_HOST_IN_BUNDLE = PREFIX_PATH + "p_host_in_bundle" + YEAR_MONTH_DAY_SUFFIX

##-----JOBS PARAMS-----##


# COMMAND ----------

dbutils.notebook.run("./01_train", 3600, arguments={"ls_path": MP_PATH,
                                                    "output_path": P_HOST_IN_BUNDLE,
                                                    "overwrite_mode": OVERWRITE_MODE,
                                                    "envs": ENVS})