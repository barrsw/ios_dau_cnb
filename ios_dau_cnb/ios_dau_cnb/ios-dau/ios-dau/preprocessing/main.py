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
PANEL_PATH = "s3://s3-data-collection-stream-raw-prod-us-east-1/inner_vpn/"
INGESTED_PATH = "{}/panel/{}".format(PREFIX_PATH, YEAR_MONTH_DAY)
REMOVE_BACKGROUND_PATH = "{}/background_removed/{}".format(PREFIX_PATH, YEAR_MONTH_DAY)
SESSIONIZED_PATH = "{}/sessions/{}".format(PREFIX_PATH, YEAR_MONTH_DAY)
PAINT_NON_APP_PATH = "{}/non_app_paint/{}".format(PREFIX_PATH, YEAR_MONTH_DAY)
P_BROWSING_PATH = "{}/p_browsing/".format(PREFIX_PATH)

##-----JOBS PARAMS-----##
SESSION_LENGH_MS = get_widget_or_default('session_length_ms', 10_000)
REQ_DIFF_MS = get_widget_or_default('req_diff_ms', 4950)

# COMMAND ----------

dbutils.notebook.run("./01_ingest_panel", 3600, arguments={"input_path": PANEL_PATH, "output_path": INGESTED_PATH, "date": DATE_STR, "overwrite_mode": OVERWRITE_MODE, "envs": ENVS})

# COMMAND ----------

dbutils.notebook.run("./03_sessionize_panel", 3600, arguments={"input_path": INGESTED_PATH, "output_path": SESSIONIZED_PATH, "session_length_ms": SESSION_LENGH_MS, "req_diff_ms": REQ_DIFF_MS,  "overwrite_mode": OVERWRITE_MODE, "envs": ENVS})

# COMMAND ----------

# dbutils.notebook.run("./04_paint_non_app", 3600, arguments={"input_path": SESSIONIZED_PATH,"p_browsing" : P_BROWSING_PATH, "date": DATE_STR,  "output_path": PAINT_NON_APP_PATH, "overwrite_mode": OVERWRITE_MODE, "envs": ENVS})