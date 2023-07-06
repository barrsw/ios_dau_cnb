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
YEAR_MONTH_DAY = add_year_month_day(DATE_STR)
DATE = datetime.strptime(DATE_STR, "%Y-%m-%d")
YEAR, MONTH, DAY = DATE.year, DATE.month, DATE.day

##------PATHS------##
SESSIONS_PATH = "{}/sessions/{}".format(PREFIX_PATH, YEAR_MONTH_DAY)
P_HOST_IN_BUNDLE_PATH = "{}/p_host_in_bundle/{}".format(PREFIX_PATH, YEAR_MONTH_DAY) #TODO: take weekly train
SOLVED_SESSIONS_PATH = "{}/solved_sessions/{}".format(PREFIX_PATH, YEAR_MONTH_DAY)
DAU_PATH = "{}/dau_p/{}".format(PREFIX_PATH, YEAR_MONTH_DAY)

##-----JOBS PARAMS-----##


# COMMAND ----------

dbutils.notebook.run("./01_infer_sessions", 3600, arguments={"panel_path": SESSIONS_PATH,
                                                    "p_host_in_bundle": P_HOST_IN_BUNDLE_PATH,
                                                    "output_path": SOLVED_SESSIONS_PATH,
                                                    "country": "840",
                                                    "overwrite_mode": OVERWRITE_MODE,
                                                    "envs": ENVS})

# COMMAND ----------

dbutils.notebook.run('./02_make_dau', 3600, arguments={"solved_sessions_path": SOLVED_SESSIONS_PATH,
                                                       "output_path": DAU_PATH,
                                                       "overwrite_mode": OVERWRITE_MODE,
                                                       "envs": ENVS})