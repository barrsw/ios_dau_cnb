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
PREV_YEAR_MONTH = add_year_month(DATE - relativedelta.relativedelta(months=1))
YEAR, MONTH, DAY = DATE.year, DATE.month, DATE.day

##------PATHS------##
MODEL_PATH = "{}/model/{}".format(PREFIX_PATH, PREV_YEAR_MONTH)
SESSIONS_PATH = "{}/sessions/{}".format(PREFIX_PATH, YEAR_MONTH_DAY)
SOLVED_SESSIONS_PATH = "{}/solved_sessions/{}".format(PREFIX_PATH, YEAR_MONTH_DAY)
DAU_P_PATH = "{}/dau_p/{}".format(PREFIX_PATH, YEAR_MONTH_DAY)
DAU_BASE_PATH = "{}/dau_p".format(PREFIX_PATH)
DAU_ABS_PATH = "{}/dau_abs/{}".format(PREFIX_PATH, YEAR_MONTH_DAY)
GA_BASE_PATH = "/similargroup/data/store-analytics/general/ga4_extractor/clean_table/"

##-----JOBS PARAMS-----##
MIN_GA = 10000

# COMMAND ----------

dbutils.notebook.run('01_session_inference'
                     ,1000,
                     arguments={
                     "panel_path":SESSIONS_PATH,
                     "model_path":MODEL_PATH,
                     "output_path":SOLVED_SESSIONS_PATH,
                     "min_intersec":"0.6",
                     "overwrite_mode": OVERWRITE_MODE,
                     "envs":ENVS})

# COMMAND ----------

dbutils.notebook.run('02_make_dau',1000,
                     arguments={
                         "panel_path":SOLVED_SESSIONS_PATH,
                         "overwrite_mode": OVERWRITE_MODE,
                         "output_path":DAU_P_PATH,
                         "envs":ENVS,})

# COMMAND ----------

dbutils.notebook.run("/Repos/apps/ios-dau/make_dau_abs/prop_to_abs", 3600, arguments={"est_path": DAU_BASE_PATH, 
                                                            "ga_path": GA_BASE_PATH, 
                                                            "output_path": DAU_ABS_PATH, 
                                                            "min_ga": MIN_GA,
                                                            "date": DATE_STR,
                                                            "envs": ENVS, 
                                                            "overwrite_mode": OVERWRITE_MODE})