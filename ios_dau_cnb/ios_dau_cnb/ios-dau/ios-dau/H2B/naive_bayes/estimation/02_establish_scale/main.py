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

LAST_SUNDAY_YEAR_MONTH_DAY_SUFFIX = add_year_month_day(get_sunday_of_n_weeks_ago(DATE_STR, 1))
SUNDAY_2_WEEKS_AGO = get_sunday_of_n_weeks_ago(DATE_STR, 2)

##------INPUTS------##
RAW_ESTIMATION_BASE_PATH = PREFIX_PATH + "raw_estimation"
RAW_ESTIMATION_PATH = PREFIX_PATH + "raw_estimation" + YEAR_MONTH_DAY_SUFFIX
IOS_SHARE_P_EST_BASE_PATH = PREFIX_PATH + "ios_share_p_estimation"
MP_SESSIONS_EVAL_PATH = PREFIX_PATH + "measure_protocol_sessions_evaluation" + LAST_SUNDAY_YEAR_MONTH_DAY_SUFFIX
MP_P_EST_PATH = "/similargroup/data/ios-analytics/measure_protocol/measure_protocol_p_estimation" + (add_year_month_day(SUNDAY_2_WEEKS_AGO) if SUNDAY_2_WEEKS_AGO >= date(2022, 11, 27) else add_year_month_day(date(2022, 11, 27)))
GA_BASE_PATH = "/similargroup/data/store-analytics/general/ga4_extractor/processed_ls/"
TOP_CHARTS_PATH = "/similargroup/data/store-analytics/iOS-app-store/top-charts" + YEAR_MONTH_DAY_SUFFIX
APPS_METADATA_PATH = "/similargroup/data/store-analytics/iOS-app-store/42matters/app-metadata/weekly/parquet" + LAST_SUNDAY_YEAR_MONTH_DAY_SUFFIX
COUNTRIES_IOS_POPULATION_PATH = "/similargroup/data/ios-analytics/countries_ios_population/year=22/month=10/day=01"

##-----OUTPUTS-----##
MERGED_ESTIMATION_PATH = PREFIX_PATH + "merged_estimations" + YEAR_MONTH_DAY_SUFFIX
MERGED_ESTIMATION_FOR_TRAIN_PATH = PREFIX_PATH + "merged_estimations_for_train" + YEAR_MONTH_DAY_SUFFIX
ESTABLISH_SCALE_PATH = PREFIX_PATH + "established_scale" + YEAR_MONTH_DAY_SUFFIX

#------JOB PARAMS-----#
COUNTRIES =  get_widget_or_default('countries', "840,826,124,36")

# COMMAND ----------

dbutils.notebook.run('./01_establish_scale', arguments={"raw_estimation_base_path": RAW_ESTIMATION_BASE_PATH,
                                                        "ios_share_p_est_base_path": IOS_SHARE_P_EST_BASE_PATH,
                                                        "mp_sessions_eval_path": MP_SESSIONS_EVAL_PATH,
                                                        "mp_p_est_path": MP_P_EST_PATH,
                                                        "ga_base_path": GA_BASE_PATH,
                                                        "top_charts_path": TOP_CHARTS_PATH,
                                                        "apps_metadata_path": APPS_METADATA_PATH,
                                                        "countries_ios_population_path": COUNTRIES_IOS_POPULATION_PATH,
                                                        "merged_estimation_path": MERGED_ESTIMATION_PATH,
                                                        "merged_estimation_for_train_path": MERGED_ESTIMATION_FOR_TRAIN_PATH,
                                                        "countries": COUNTRIES,
                                                        "date": DATE_STR,
                                                        "overwrite_mode": OVERWRITE_MODE,
                                                        "envs": ENVS
                                                        }, timeout_seconds=3600)

# COMMAND ----------

dbutils.notebook.run('./02_establish_scale', arguments={"raw_estimation_path": RAW_ESTIMATION_PATH,
                                                        "merged_estimation_path": MERGED_ESTIMATION_PATH,
                                                        "merged_estimation_for_train_path": MERGED_ESTIMATION_FOR_TRAIN_PATH,
                                                        "countries_ios_population_path": COUNTRIES_IOS_POPULATION_PATH,
                                                        "output_path": ESTABLISH_SCALE_PATH,
                                                        "date": DATE_STR,
                                                        "overwrite_mode": OVERWRITE_MODE,
                                                        "envs": ENVS
                                                        }, timeout_seconds=3600)