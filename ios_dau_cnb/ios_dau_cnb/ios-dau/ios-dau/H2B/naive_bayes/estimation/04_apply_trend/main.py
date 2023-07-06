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

PREFIX_PATH = "/similargroup/data/ios-analytics/metrics/dau/"
YEAR_MONTH_DAY_SUFFIX = add_year_month_day(DATE_STR)

DATE = datetime.strptime(DATE_STR, "%Y-%m-%d").date()
OVERWRITE_MODE = 'True'

##------INPUTS------##
ESTABLISHED_SCALE_BASE_PATH = PREFIX_PATH + "established_scale"
RAW_TREND_ESTIMATION_WITH_PRIOR_BASE_PATH = PREFIX_PATH + "raw_trend_estimation_with_prior"

##-----OUTPUTS-----##
DAU_P_ESTIMATION_BASE_PATH = PREFIX_PATH + "trend_estimation_applied"


#------JOB PARAMS-----#
INIT_DATE = datetime.strptime(get_widget_or_default('init_date', '2022-09-03'), "%Y-%m-%d").date()
N_ANCHORS = int(get_widget_or_default('n_anchors', '7'))
COUNTRIES =  get_widget_or_default('countries', "840,826,124,36")

# COMMAND ----------

ANCHOR_DATE = get_sunday_of_n_weeks_ago(DATE_STR, 0) - timedelta(days=1)
ANCHOR_DATE = INIT_DATE if ANCHOR_DATE < INIT_DATE else ANCHOR_DATE
ANCHOR_DATES = [ANCHOR_DATE - timedelta(days=i) for i in range(N_ANCHORS)] # desc order
ANCHOR_DATES = [d.strftime('%Y-%m-%d') for d in ANCHOR_DATES if d >= INIT_DATE]
print("anchor_dates %s" % ANCHOR_DATES)

TREND_DATES = [DATE - timedelta(days=i) for i in range((DATE - ANCHOR_DATE).days)] # desc order
TREND_DATES = [d.strftime('%Y-%m-%d') for d in TREND_DATES if d > INIT_DATE]
print("trend_dates %s" % TREND_DATES)

# COMMAND ----------

dbutils.notebook.run('./01_apply_trend', arguments={"dau_p_estimation_base_path": DAU_P_ESTIMATION_BASE_PATH,
                                                 "raw_trend_estimation_with_prior_base_path": RAW_TREND_ESTIMATION_WITH_PRIOR_BASE_PATH,
                                                 "raw_estimation_base_path": ESTABLISHED_SCALE_BASE_PATH,
                                                 "date": DATE_STR,            
                                                 "anchors_dates": ','.join([ANCHOR_DATES[-1], ANCHOR_DATES[0]]) if ANCHOR_DATES else 'EMPTY_STRING',
                                                 "trend_dates": ','.join([TREND_DATES[-1], TREND_DATES[0]]) if TREND_DATES else 'EMPTY_STRING',
                                                 "init": str(INIT_DATE == DATE),
                                                 "envs": ENVS,
                                                 "overwrite_mode": OVERWRITE_MODE
                                                }, timeout_seconds=3*3600)