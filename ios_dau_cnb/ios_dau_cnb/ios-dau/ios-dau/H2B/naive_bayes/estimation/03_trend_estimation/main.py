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

##------INPUTS------##
SOURCES_RAW_ESTIMATION_BASE_PATH = PREFIX_PATH + "sources_raw_estimation"
ANDROID_DAU_PATH = "/phoenix1/similargroup/data/android-apps-analytics/metrics/dau/estimations_with_ww/"
MATCHES_PATH = "s3://similargroup-backup-retention/mrp/similargroup/data/android-apps-analytics/apps-matching/predict/" + add_year_month_day(get_sunday_of_n_weeks_ago(DATE, 1))
BUNDLE_INFO_PATH = 's3://sw-df-production-internal-data/apps/app_details/app_store_bundle/' + (YEAR_MONTH_DAY_SUFFIX if DATE >= date(2022, 11, 27) else add_year_month_day(date(2022, 11, 27)))

##-----OUTPUTS-----##
SOURCE_RAW_TREND_ESTIMATION = PREFIX_PATH + "sources_raw_trend_estimation" + YEAR_MONTH_DAY_SUFFIX
RAW_TREND_ESTIMATION = PREFIX_PATH + "raw_trend_estimation" + YEAR_MONTH_DAY_SUFFIX
RAW_TREND_ESTIMATION_WITH_PRIOR = PREFIX_PATH + "raw_trend_estimation_with_prior" + YEAR_MONTH_DAY_SUFFIX


#------JOB PARAMS-----#
INIT_DATE = datetime.strptime(get_widget_or_default('init_date', '2022-09-03'), "%Y-%m-%d").date()
N_ANCHORS = int(get_widget_or_default('n_anchors', '7'))

# COMMAND ----------

ANCHOR_DATES = [DATE - timedelta(days=i+1) for i in range(N_ANCHORS)] # desc order
ANCHOR_DATES = [d.strftime('%Y-%m-%d') for d in ANCHOR_DATES if d >= INIT_DATE] 
DAY2DAY_ANCHOR_DATES = ','.join(ANCHOR_DATES[:7])
ANCHOR_PATHS = ','.join([SOURCES_RAW_ESTIMATION_BASE_PATH + add_year_month_day(d) for d in ANCHOR_DATES])

IS_INIT_DATE = INIT_DATE == DATE

# COMMAND ----------

if not IS_INIT_DATE:
    dbutils.notebook.run('./01_sources_raw_trend_estimation', arguments={"sources_raw_estimation_base_path": SOURCES_RAW_ESTIMATION_BASE_PATH,
                                                    "date": DATE_STR,             
                                                    "anchors_dates": ','.join([ANCHOR_DATES[-1], ANCHOR_DATES[0]]),
                                                    "output_path": SOURCE_RAW_TREND_ESTIMATION,
                                                    "day2day_anchors_dates": DAY2DAY_ANCHOR_DATES,
                                                    "avg_scale_anchors_dates": 'EMPTY_STRING',
                                                    "envs": ENVS,
                                                    "overwrite_mode": OVERWRITE_MODE
                                                   }, timeout_seconds=3*3600)

# COMMAND ----------

if not IS_INIT_DATE:
    dbutils.notebook.run('./02_merge_sources_trend', arguments={"input_path": SOURCE_RAW_TREND_ESTIMATION,
                                                    "output_path": RAW_TREND_ESTIMATION,
                                                    "envs": ENVS,
                                                    "overwrite_mode": OVERWRITE_MODE
                                                   }, timeout_seconds=3*3600)

# COMMAND ----------

if not IS_INIT_DATE:
    dbutils.notebook.run('./03_apply_trend_prior', arguments={"raw_trend_estimation_base_path": RAW_TREND_ESTIMATION,
                                                           "date": DATE_STR,
                                                           "android_dau_path": ANDROID_DAU_PATH,
                                                           "android_dates": ','.join([ANCHOR_DATES[-1], DATE_STR]),
                                                           "matches_path": MATCHES_PATH,
                                                           "bundle_info_path": BUNDLE_INFO_PATH,
                                                           "day2day_anchors_dates": DAY2DAY_ANCHOR_DATES,
                                                           "avg_scale_anchors_dates": 'EMPTY_STRING',
                                                           "output_path": RAW_TREND_ESTIMATION_WITH_PRIOR,
                                                           "envs": ENVS,
                                                           "overwrite_mode": OVERWRITE_MODE
                                                          }, timeout_seconds=3*3600)