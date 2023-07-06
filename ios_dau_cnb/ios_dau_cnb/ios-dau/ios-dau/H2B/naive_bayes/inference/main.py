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

LAST_SUNDAY_DATE = get_sunday_of_n_weeks_ago(DATE_STR, 1)
LAST_SUNDAY_YEAR_MONTH_DAY_SUFFIX = add_year_month_day(LAST_SUNDAY_DATE)

OVERWRITE_MODE = 'True'
DATE = datetime.strptime(DATE_STR, "%Y-%m-%d").date()

MODELS = [{"model_date": LAST_SUNDAY_DATE, "is_main_model": 'True'}]
if DATE >= date(2022, 8, 14):
    LAST_2_SUNDAYS_DATE = get_sunday_of_n_weeks_ago(DATE_STR, 2)
    MODELS.append({"model_date": LAST_2_SUNDAYS_DATE, "is_main_model": 'False'})

# COMMAND ----------

##------INPUTS------##
PARAMS_PATH = ';'.join([','.join([PREFIX_PATH + "params" + add_year_month_day(model['model_date']), str(model['model_date']), "NB_1", model['is_main_model']]) for model in MODELS])
PRIORS_PATH = PREFIX_PATH + "priors" + LAST_SUNDAY_YEAR_MONTH_DAY_SUFFIX
PROCESSED_PANEL_PATH = PREFIX_PATH + "sessions" + YEAR_MONTH_DAY_SUFFIX
GA_BASE_PATH = "/similargroup/data/store-analytics/general/ga4_extractor/clean_table/"

##-----OUTPUTS-----##
SOLVED_SESSIONS_PATH = PREFIX_PATH + "solved_sessions" + YEAR_MONTH_DAY_SUFFIX
SOLVED_SESSIONS_W_TRIVIAL_PATH = PREFIX_PATH + "solved_sessions_w_trivial" + YEAR_MONTH_DAY_SUFFIX
HOST_COUNT_PER_BUNDLE_PATH = PREFIX_PATH + "hosts_count_per_app" + YEAR_MONTH_DAY_SUFFIX
DAU_P_PER_SOURCE_PATH = PREFIX_PATH + "dau_p_per_source" + YEAR_MONTH_DAY_SUFFIX
DAU_PATH = PREFIX_PATH + "dau_p" + YEAR_MONTH_DAY_SUFFIX
DAU_BASE_PATH = PREFIX_PATH + "dau_p"

#------JOB PARAMS-----#
REMOVE_APPLE = get_widget_or_default('remove_apple', 'True')
MIN_INFO =  get_widget_or_default('min_info', 1.0)
MIN_HITS =  get_widget_or_default('min_hits', 10)
MIN_COND_P = get_widget_or_default("min_cond_p", 0.002)
MIN_SCORE = get_widget_or_default("min_score", 0.000000015)
COUNTRIES =  get_widget_or_default('countries', "840,826,124,36")

# COMMAND ----------

dbutils.notebook.run('./01_infer_sessions', arguments={"params_path": PARAMS_PATH,
                                                    "priors_path": PRIORS_PATH,
                                                    "panel_path": PROCESSED_PANEL_PATH,
                                                    "countries": COUNTRIES,
                                                    "min_hits": MIN_HITS,
                                                    "min_info": MIN_INFO,
                                                    "min_cond_p": MIN_COND_P,
                                                    "min_score": MIN_SCORE,
                                                    "remove_apple": REMOVE_APPLE,
                                                    "solved_sessions_path": SOLVED_SESSIONS_PATH,
                                                    "envs": ENVS,
                                                    "overwrite_mode": OVERWRITE_MODE
                                                   }, timeout_seconds=3*3600)

# COMMAND ----------

dbutils.notebook.run('./02_add_trivial_matches', arguments={"params_path": PARAMS_PATH,
                                                    "countries": COUNTRIES,
                                                    "sessions_path": PROCESSED_PANEL_PATH,
                                                    "solved_sessions_path": SOLVED_SESSIONS_PATH,
                                                    "envs": ENVS,
                                                    "overwrite_mode": OVERWRITE_MODE,
                                                    "output_path": SOLVED_SESSIONS_W_TRIVIAL_PATH
                                                   }, timeout_seconds=3*3600)

# COMMAND ----------

# dbutils.notebook.run('./03_calc_host_count_per_bundle', arguments={"input_path": SOLVED_SESSIONS_W_TRIVIAL_PATH,
#                                                     "params_path": PARAMS_PATH,
#                                                     "output_path": HOST_COUNT_PER_BUNDLE_PATH,
#                                                     "min_hits": MIN_HITS,
#                                                     "min_cond_p": MIN_COND_P,
#                                                     "remove_apple": REMOVE_APPLE,
#                                                     "envs": ENVS,
#                                                     "overwrite_mode": OVERWRITE_MODE,
#                                                    }, timeout_seconds=3*3600)