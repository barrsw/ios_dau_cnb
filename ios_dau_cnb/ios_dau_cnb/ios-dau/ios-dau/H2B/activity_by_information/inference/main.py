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
LAST_SUNDAY_YEAR_MONTH_DAY_SUFFIX = add_year_month_day(get_sunday_of_n_weeks_ago(DATE_STR, 1))

##------PATHS------##
PROCESSED_PANEL_PATH = PREFIX_PATH + "sessions" + YEAR_MONTH_DAY_SUFFIX
SOLVED_SESSIONS_PATH = PREFIX_PATH + "solved_sessions" + YEAR_MONTH_DAY_SUFFIX
P_HOST_IN_BUNDLE_PATH = PREFIX_PATH + "p_host_in_bundle" + LAST_SUNDAY_YEAR_MONTH_DAY_SUFFIX
DAU_P_PER_SOURCE_PATH = PREFIX_PATH + "dau_p_per_source" + YEAR_MONTH_DAY_SUFFIX
DAU_PATH = PREFIX_PATH + "dau_p" + YEAR_MONTH_DAY_SUFFIX
DAU_BASE_PATH = PREFIX_PATH + "dau_p"

##-----JOBS PARAMS-----##
REMOVE_APPLE = get_widget_or_default('remove_apple', 'True')
MIN_INFO =  get_widget_or_default('remove_apple', 1.0)
MIN_HITS =  get_widget_or_default('remove_apple', 10)
COUNTRIES =  get_widget_or_default('countries', "840,826,124,36")

# COMMAND ----------

dbutils.notebook.run("./01_infer_sessions", 3600, arguments={"panel_path": PROCESSED_PANEL_PATH,
                                                    "p_host_in_bundle": P_HOST_IN_BUNDLE_PATH,
                                                    "output_path": SOLVED_SESSIONS_PATH,
                                                    "countries": COUNTRIES,
                                                    "overwrite_mode": OVERWRITE_MODE,
                                                    "envs": ENVS})

# COMMAND ----------

dbutils.notebook.run('./02_make_dau_per_source', arguments={"min_info": MIN_INFO ,"solved_sessions_path": SOLVED_SESSIONS_PATH, "output_path": DAU_P_PER_SOURCE_PATH, "overwrite_mode": OVERWRITE_MODE, "envs": ENVS}, timeout_seconds=3600)

# COMMAND ----------

dbutils.notebook.run('./03_merge_sources', arguments={"input_path": DAU_P_PER_SOURCE_PATH, "output_path": DAU_PATH, "overwrite_mode": OVERWRITE_MODE, "envs": ENVS}, timeout_seconds=3600)