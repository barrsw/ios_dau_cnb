# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

##------ARGS------##
DATE_STR = dbutils.widgets.get("date")
ENVS = dbutils.widgets.get("envs")

# COMMAND ----------

PREFIX_PATH = "/similargroup/data/ios-analytics/metrics/dau/"
YEAR_MONTH_DAY_SUFFIX = add_year_month_day(DATE_STR)
PREV_YEAR_MONTH_SUFFIX = add_year_month(get_previous_month(DATE_STR))

OVERWRITE_MODE = 'True'

# COMMAND ----------

##------INPUTS------##
DAU_P_BASE_PATH = PREFIX_PATH + "dau_p"
GA_BASE_PATH = "/similargroup/data/store-analytics/general/ga4_extractor/clean_table/"

##-----OUTPUTS-----##
DAU_ABS_PATH = PREFIX_PATH + "dau_abs" + YEAR_MONTH_DAY_SUFFIX
DAU_ABS_EVALUATION_PATH = "%s/dau_abs_evaluation/%s" % (PREFIX_PATH, YEAR_MONTH_DAY_SUFFIX)

#------JOB PARAMS-----#
REMOVE_APPLE = 'True'
MIN_INFO = 1.0
MIN_HITS = 10
COUNTRIES = "840,826,124,36"
MIN_GA = 30_000
MIN_USERS = 3
FOLDS = 5

# COMMAND ----------

dbutils.notebook.run("./04_prop_to_abs", 3600, arguments={"dau_p_base_path": DAU_P_BASE_PATH, 
                                                       "ga_base_path": GA_BASE_PATH, 
                                                       "dau_abs_path": DAU_ABS_PATH,
                                                       "dau_abs_eval_path": DAU_ABS_EVALUATION_PATH,
                                                       "folds": FOLDS,
                                                       "min_ga": MIN_GA,
                                                       "min_users": MIN_USERS,
                                                       "countries": COUNTRIES,
                                                       "date": DATE_STR,
                                                       "envs": ENVS, 
                                                       "overwrite_mode": OVERWRITE_MODE})