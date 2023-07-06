# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

##------ARGS------##
DATE_STR = dbutils.widgets.get("date")
ENVS = dbutils.widgets.get("envs")

# COMMAND ----------

PREFIX_PATH = "/similargroup/data/ios-analytics/metrics/dau/"
YEAR_MONTH_DAY_SUFFIX = add_year_month_day(DATE_STR)
YEAR_MONTH_SUFFIX = add_year_month(DATE_STR)
DATE = datetime.strptime(DATE_STR, "%Y-%m-%d").date()
OVERWRITE_MODE = 'True'

# COMMAND ----------

##------INPUTS------##
SOLVED_SESSIONS_W_TRIVIAL_PATH = PREFIX_PATH + "solved_sessions_w_trivial" + YEAR_MONTH_DAY_SUFFIX
IOS_PRIORS_PATH = "/similargroup/data/ios-analytics/ios_share/ios_priors/year=22/month=06/day=01"
COUNTRY_PRIORS_PATH = "/similargroup/data/ios-analytics/ios_share/country_priors/"
CATEGORY_COUNTRY_PRIORS_PATH = "/similargroup/data/ios-analytics/ios_share/category_country_priors/"
IOS_CATEGORY_PATH = "/similargroup/data/ios-analytics/ios_share/ios_categories/year=22/month=06/day=01"
APPS_MATCHING_PATH = "s3://similargroup-backup-retention/mrp/similargroup/data/android-apps-analytics/apps-matching/predict/" + YEAR_MONTH_SUFFIX
ITUNES_BUNDLES_PATH = 's3://sw-df-production-internal-data/apps/app_details/app_store_bundle/' + (YEAR_MONTH_DAY_SUFFIX if DATE >= date(2022, 11, 27) else add_year_month_day(date(2022, 11, 27)))
ANDROID_DAU_ADJ_PATH = "s3a://sw-apps-core-data-buffer/phoenix1/similargroup/data/android-apps-analytics/dau/bbb/countries_android_population/year=22/month=04/day=01"
ANDROID_DAU_PATH = "/phoenix1/similargroup/data/android-apps-analytics/metrics/dau/estimations_with_ww/" + YEAR_MONTH_DAY_SUFFIX

##-----OUTPUTS-----##
SOURCES_RAW_ESTIMATION_PATH = PREFIX_PATH + "sources_raw_estimation" + YEAR_MONTH_DAY_SUFFIX
RAW_ESTIMATION_PATH = PREFIX_PATH + "raw_estimation" + YEAR_MONTH_DAY_SUFFIX
IOS_SHARE_P_EST = PREFIX_PATH + "ios_share_p_estimation" + YEAR_MONTH_DAY_SUFFIX

#------JOB PARAMS-----#
MIN_INFO =  get_widget_or_default('min_info', 1.0)
COUNTRIES =  get_widget_or_default('countries', "840,826,124,36")

# COMMAND ----------

dbutils.notebook.run('./01_sources_raw_estimation', arguments={"min_info": MIN_INFO,
                                                         "solved_sessions_path": SOLVED_SESSIONS_W_TRIVIAL_PATH,
                                                         "output_path": SOURCES_RAW_ESTIMATION_PATH,
                                                         "overwrite_mode": OVERWRITE_MODE,
                                                         "envs": ENVS
                                                        }, timeout_seconds=3600)

# COMMAND ----------

dbutils.notebook.run('./02_merge_sources', arguments={"input_path": SOURCES_RAW_ESTIMATION_PATH,
                                                   "output_path": RAW_ESTIMATION_PATH,
                                                   "overwrite_mode": OVERWRITE_MODE,
                                                   "envs": ENVS
                                                  }, timeout_seconds=3600)

# COMMAND ----------

dbutils.notebook.run('./03_ios_share_p_est', arguments={"ios_priors_path": IOS_PRIORS_PATH,
                                                        "country_priors_path": COUNTRY_PRIORS_PATH,
                                                        "category_country_priors_path": CATEGORY_COUNTRY_PRIORS_PATH,
                                                        "priors_dates": "2022-06-01,2022-06-30",
                                                        "ios_category_path": IOS_CATEGORY_PATH,
                                                        "apps_matching_path": APPS_MATCHING_PATH,
                                                        "itunes_bundles_path": ITUNES_BUNDLES_PATH,
                                                        "android_dau_adj_path": ANDROID_DAU_ADJ_PATH,
                                                        "android_dau_path": ANDROID_DAU_PATH,
                                                        "date": DATE_STR,
                                                        "countries": COUNTRIES,
                                                        "output_path": IOS_SHARE_P_EST,
                                                        "overwrite_mode": OVERWRITE_MODE,
                                                        "envs": ENVS
                                                       }, timeout_seconds=3600)