# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

##------INPUTS------##
DATE_STR = dbutils.widgets.get("date")
ENVS = dbutils.widgets.get("envs")

# COMMAND ----------

PREFIX_PATH = "/similargroup/data/ios-analytics/metrics/dau/"
YEAR_MONTH_SUFFIX = add_year_month(DATE_STR)
YEAR_MONTH_DAY_SUFFIX = add_year_month_day(DATE_STR)
OVERWRITE_MODE = 'True'
DATE = datetime.strptime(DATE_STR, "%Y-%m-%d").date()

##------INPUT------##
MP_PATH = "/similargroup/data/ios-analytics/measure_protocol/network_activity_adjusted/"
LAB_PATH = "/mnt/df/sw-df-staging-ios-artificial-metered-panel-output/ok"
ANDROID_PATH = "/phoenix1/similargroup/data/android-apps-analytics/metrics/dau/estimations_with_ww/"
ANDROID_ADJ_PATH = "s3a://sw-apps-core-data-buffer/phoenix1/similargroup/data/android-apps-analytics/dau/bbb/countries_android_population/year=22/month=04/day=01"
MATCHES_PATH = "s3://similargroup-backup-retention/mrp/similargroup/data/android-apps-analytics/apps-matching/predict/" + YEAR_MONTH_SUFFIX
BUNDLE_INFO_PATH = 's3://sw-df-production-internal-data/apps/app_details/app_store_bundle/' + (YEAR_MONTH_DAY_SUFFIX if DATE >= date(2022, 11, 27) else add_year_month_day(date(2022, 11, 27)))

##-----OUTPUT-----##
LS_PATH = PREFIX_PATH + "ls" + YEAR_MONTH_DAY_SUFFIX
PARAMS_PATH = PREFIX_PATH + "params" + YEAR_MONTH_DAY_SUFFIX
PRIORS_PATH = PREFIX_PATH + "priors" + YEAR_MONTH_DAY_SUFFIX
MP_SESSIONS_PATH = PREFIX_PATH + "measure_protocol_sessions" + YEAR_MONTH_DAY_SUFFIX
MP_SOLVED_SESSIONS_PATH = PREFIX_PATH + "measure_protocol_solved_sessions" + YEAR_MONTH_DAY_SUFFIX
MP_SOLVED_SESSIONS_W_TRIVIAL_PATH = PREFIX_PATH + "measure_protocol_solved_sessions_w_trivial" + YEAR_MONTH_DAY_SUFFIX
MP_SESSIONS_EVAL_PATH = PREFIX_PATH + "measure_protocol_sessions_evaluation" + YEAR_MONTH_DAY_SUFFIX
MP_P_EST_PATH = PREFIX_PATH + "measure_protocol_p_estimation" + YEAR_MONTH_DAY_SUFFIX

##-----JOBS PARAMS-----##
MEASURE_PROTOCOL_HOST = "com.measureprotocol.contributor.production"
MP_BUNDLE_BLACKLIST_JOINED = get_widget_or_default('mp_bundle_blacklist', ",".join([MEASURE_PROTOCOL_HOST, "com.apple.mobilesafari", "com.google.chrome.ios", "com.google.GoogleMobile", "org.mozilla.ios.Firefox", "com.brave.ios.browser", "com.duckduckgo.mobile.ios", "com.opera.gx", "com.opera.OperaTouch", "com.alohabrowser.alohabrowser"]))
CHECK_HOSTS = get_widget_or_default('check_hosts', ",".join(["provider-api.health.apple.com", "config-chr.health.apple.com", "weather-data.apple.com", "sequoia.cdn-apple.com"]))
MIN_INV_FREQ = get_widget_or_default('min_inv_freq', 1.0/1000)
MIN_HOST_IMP_FOR_UNIQUE = get_widget_or_default('min_host_importance_for_unique', 0.5)
MIN_HITS_FOR_UNIQUE = get_widget_or_default('min_hits_for_unique', 10)
MIN_COND_P_FOR_UNIQUE = get_widget_or_default('min_cond_p_for_unique', 0.2)
MIN_USERS = get_widget_or_default('min_users', 10) #min MP users of an app in order to be included in LS
DEFAULT_PRIOR = get_widget_or_default('default_prior', 1e-06)

##-----JOBS PARAMS FOR MP SESSIONS-----##
MIN_USERS_FOR_MP_SESSIONS = get_widget_or_default('min_users_for_mp_sessions', 3)
MIN_SESSION_LENGTH = get_widget_or_default('min_session_length', 3)
MAX_SESSION_LENGTH = get_widget_or_default('max_session_length', 10)
MIN_DISTINCT_HOSTS_PER_SESSION = get_widget_or_default('min_distinct_hosts_per_sessions', 3)
MP_WEEKS_FOR_MP_SESSIONS = get_widget_or_default('mp_weeks_for_mp_sessions', 4)
MIN_INFO =  get_widget_or_default('min_info', 1.0)
MIN_HITS =  get_widget_or_default('min_hits', 10)
MIN_COND_P = get_widget_or_default("min_cond_p", 0.002)
MIN_SCORE = get_widget_or_default("min_score", 0.000000015)
PARAMS_PATH_FOR_MP_SESSIONS = ','.join([PARAMS_PATH, DATE_STR, "NB_1", "TRUE"])

##-----JOBS PARAMS FOR MP P ESTIMATIONS-----##
MIN_USERS_FOR_MP_P_EST = get_widget_or_default('min_users_for_mp_p_est', 1)
MP_WEEKS_FOR_MP_P_EST = get_widget_or_default('mp_weeks_for_mp_p_est', 4)

REMOVE_APPLE = get_widget_or_default('remove_apple', 'True')
COUNTRIES =  get_widget_or_default('countries', "840,826,124,36")

# COMMAND ----------

dbutils.notebook.run("./01_prepare_learning_sets", 3600, {"mp_bundle_blacklist": MP_BUNDLE_BLACKLIST_JOINED, 
                                                       "check_hosts": CHECK_HOSTS,
                                                       "mp_path": MP_PATH, 
                                                       "lab_path": LAB_PATH,
                                                       "min_users": str(MIN_USERS),
                                                       "remove_apple": str(REMOVE_APPLE), 
                                                       "overwrite_mode": str(OVERWRITE_MODE),
                                                       "ls_path": LS_PATH,
                                                       "date": DATE_STR,
                                                       "envs": ENVS
                                                      }
                    )

# COMMAND ----------

dbutils.notebook.run("./02_calc_params", 3600, {
                                       "overwrite_mode": str(OVERWRITE_MODE),
                                       "envs": ENVS,
                                       "ls_path": LS_PATH,
                                       "params_path": PARAMS_PATH,
                                       "min_inv_freq": MIN_INV_FREQ,
                                       "min_host_importance_for_unique": MIN_HOST_IMP_FOR_UNIQUE,
                                       "min_hits_for_unique": MIN_HITS_FOR_UNIQUE,
                                       "min_cond_p_for_unique": MIN_COND_P_FOR_UNIQUE
                                      }
                    )

# COMMAND ----------

dbutils.notebook.run("./03_process_priors", 3600, {
                            "overwrite_mode": str(OVERWRITE_MODE),
                            "envs": ENVS,
                            "countries": COUNTRIES,
                            "matches_path": MATCHES_PATH,
                            "bundle_info_path": BUNDLE_INFO_PATH,
                            "android_path": ANDROID_PATH,
                            "android_adj_path": ANDROID_ADJ_PATH,
                            "date": DATE_STR,
                            "output_path": PRIORS_PATH
})

# COMMAND ----------

#DONT RUN THIS JOB!!! USE Production instead!
# dbutils.notebook.run("./04_mp_sessions", 3600, {
#                             "overwrite_mode": str(OVERWRITE_MODE),
#                             "envs": ENVS,
#                             "date": DATE_STR,
#                             "mp_bundle_blacklist": MEASURE_PROTOCOL_HOST,
#                             "check_hosts": CHECK_HOSTS,
#                             "mp_path": MP_PATH,
#                             "min_users": MIN_USERS_FOR_MP_SESSIONS,
#                             "min_session_length": MIN_SESSION_LENGTH,
#                             "max_session_length": MAX_SESSION_LENGTH,
#                             "min_distinct_hosts_per_sessions": MIN_DISTINCT_HOSTS_PER_SESSION,
#                             "mp_weeks": MP_WEEKS_FOR_MP_SESSIONS,
#                             "output_path": MP_SESSIONS_PATH
# })

# COMMAND ----------

#DONT RUN THIS JOB!!! USE Production instead!
# dbutils.notebook.run('../inference/01_infer_sessions', arguments={"params_path": PARAMS_PATH_FOR_MP_SESSIONS,
#                                                     "priors_path": PRIORS_PATH,
#                                                     "panel_path": MP_SESSIONS_PATH,
#                                                     "countries": "840",
#                                                     "min_hits": MIN_HITS,
#                                                     "min_info": MIN_INFO,
#                                                     "min_cond_p": MIN_COND_P,
#                                                     "min_score": MIN_SCORE,
#                                                     "remove_apple": REMOVE_APPLE,
#                                                     "solved_sessions_path": MP_SOLVED_SESSIONS_PATH,
#                                                     "envs": ENVS,
#                                                     "overwrite_mode": OVERWRITE_MODE
#                                                    }, timeout_seconds=3*3600)

# COMMAND ----------

#DONT RUN THIS JOB!!! USE Production instead!
# dbutils.notebook.run('../inference/02_add_trivial_matches', arguments={"params_path": PARAMS_PATH_FOR_MP_SESSIONS,
#                                                     "countries": "840",
#                                                     "sessions_path": MP_SESSIONS_PATH,
#                                                     "solved_sessions_path": MP_SOLVED_SESSIONS_PATH,
#                                                     "envs": ENVS,
#                                                     "overwrite_mode": OVERWRITE_MODE,
#                                                     "output_path": MP_SOLVED_SESSIONS_W_TRIVIAL_PATH
#                                                    }, timeout_seconds=3*3600)

# COMMAND ----------

#DONT RUN THIS JOB!!! USE Production instead!
# dbutils.notebook.run("./05_mp_sessions_eval", 3600, {
#                             "overwrite_mode": str(OVERWRITE_MODE),
#                             "envs": ENVS,
#                             "sessions_path": MP_SESSIONS_PATH,
#                             "solved_sessions_path": MP_SOLVED_SESSIONS_W_TRIVIAL_PATH,
#                             "output_path": MP_SESSIONS_EVAL_PATH
# })

# COMMAND ----------

#DONT RUN THIS JOB!!! USE Production instead!
# dbutils.notebook.run("./06_mp_p_est", 3600, {
#                             "overwrite_mode": str(OVERWRITE_MODE),
#                             "envs": ENVS,
#                             "date": DATE_STR,
#                             "mp_bundle_blacklist": MEASURE_PROTOCOL_HOST,
#                             "check_hosts": CHECK_HOSTS,
#                             "mp_path": MP_PATH, 
#                             "min_users": MIN_USERS_FOR_MP_P_EST,
#                             "mp_weeks": MP_WEEKS_FOR_MP_P_EST,
#                             "output_path": MP_P_EST_PATH
# })