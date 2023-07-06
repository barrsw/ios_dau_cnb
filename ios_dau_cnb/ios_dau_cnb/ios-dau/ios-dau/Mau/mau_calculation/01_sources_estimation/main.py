# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

##------ARGS------##
DATE_STR = dbutils.widgets.get("date")
ENVS = dbutils.widgets.get("envs")

# COMMAND ----------

PREFIX_PATH = "/similargroup/data/ios-analytics/metrics/active_users/mau/"
YEAR_MONTH_DAY_SUFFIX = add_year_month_day(DATE_STR)
YEAR_MONTH_SUFFIX = add_year_month(DATE_STR)
DATE = datetime.strptime(DATE_STR, "%Y-%m-%d").date()
OVERWRITE_MODE = 'True'

# COMMAND ----------

##------INPUTS------##
SOLVED_SESSIONS_W_TRIVIAL_PATH = "/similargroup/data/ios-analytics/metrics/dau/solved_sessions_w_trivial"

##-----OUTPUTS-----##
SOURCES_RAW_ESTIMATION_PATH = PREFIX_PATH + "sources_raw_estimation" + YEAR_MONTH_SUFFIX
RAW_ESTIMATION_PATH = PREFIX_PATH + "raw_p_estimation" + YEAR_MONTH_SUFFIX

#------JOB PARAMS-----#
COUNTRIES =  get_widget_or_default('countries', "840,826,124,36")

# COMMAND ----------

dbutils.notebook.run('./01_sources_raw_estimation', arguments={"min_user_reporting_days": str(0.75 * get_days_in_month(DATE_STR)),
                                                         "input_path": SOLVED_SESSIONS_W_TRIVIAL_PATH,
                                                         "output_path": SOURCES_RAW_ESTIMATION_PATH,
                                                         "dates": ','.join([DATE_STR, str(get_last_date_in_month(DATE_STR))]),
                                                         "overwrite_mode": OVERWRITE_MODE,
                                                         "envs": ENVS
                                                        }, timeout_seconds=3600)

# COMMAND ----------

dbutils.notebook.run('./02_merge_sources', arguments={"input_path": SOURCES_RAW_ESTIMATION_PATH,
                                                   "output_path": RAW_ESTIMATION_PATH,
                                                   "overwrite_mode": OVERWRITE_MODE,
                                                   "envs": ENVS
                                                  }, timeout_seconds=3600)