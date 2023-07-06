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
LAST_DATE_IN_MONTH = str(get_last_date_in_month(DATE_STR))
OVERWRITE_MODE = 'True'

# COMMAND ----------

##------INPUTS------##
RAW_ESTIMATION_BASE_PATH = PREFIX_PATH + "raw_p_estimation"
INPUT_COUNTRIES_iOS_POP_PATH = "/similargroup/data/ios-analytics/countries_ios_population/year=22/month=10/day=01"
INPUT_DAU_BASE_PATH = "/similargroup/data/ios-analytics/metrics/dau/estimations"
GA_DAU_BASE_PATH = "/similargroup/data/store-analytics/general/ga4_extractor/raw_data/"
GA_MAU_PATH = "s3://sw-apps-core-data-buffer/user/omri.shkedi/ga4/mau/" + DATE_STR

##-----OUTPUTS-----##
APPLY_PRIOR_PATH = PREFIX_PATH + "p_estimation_with_prior" + YEAR_MONTH_SUFFIX
MAU_ESTIMATION_PATH = PREFIX_PATH + "estimation" + YEAR_MONTH_SUFFIX
MAU_EVALUATION_PATH = PREFIX_PATH + "evaluation" + YEAR_MONTH_SUFFIX

#------JOB PARAMS-----#
COUNTRIES =  get_widget_or_default('countries', "840,826,124,36")
MAU_START_DATE = date(2022, 10, 1)
MIN_GA = 30_000
MIN_USERS = 3
FOLDS = 5
N_MONTH_LOOKBACK = 2

# COMMAND ----------

historic_dates = [DATE - relativedelta.relativedelta(months=1 + i) for i in range(int(N_MONTH_LOOKBACK))]


if any([d < MAU_START_DATE for d in historic_dates]):
    earlier_dates = [d for d in historic_dates if d < MAU_START_DATE]
    print("Found prior dates that are earlier than the start date (%s)." % MAU_START_DATE)
    print("Looking at the successive dates instead.\ndates: %s " % earlier_dates)
    successive_dates = [DATE + relativedelta.relativedelta(months=1 + i) for i in range(int(N_MONTH_LOOKBACK) - len(earlier_dates), int(N_MONTH_LOOKBACK))]
    print("Successive Dates: %s" % successive_dates)
    historic_dates = sorted([d for d in historic_dates if d >= MAU_START_DATE] + successive_dates)

historic_dates = [d.strftime("%Y-%m-%d") for d in historic_dates]

# COMMAND ----------

dbutils.notebook.run('./01_apply_prior', arguments={"input_base_path": RAW_ESTIMATION_BASE_PATH,
                                                         "output_path": APPLY_PRIOR_PATH,
                                                         "exec_date": DATE_STR,
                                                         "historic_dates": ','.join(historic_dates),
                                                         "overwrite_mode": OVERWRITE_MODE,
                                                         "envs": ENVS
                                                        }, timeout_seconds=3600)

# COMMAND ----------

dbutils.notebook.run('./02_adjust_to_mau', arguments={"input_p_est_path": APPLY_PRIOR_PATH,
                                                   "input_dau_est_base_path": INPUT_DAU_BASE_PATH,
                                                   "dates": ','.join([DATE_STR, LAST_DATE_IN_MONTH]),
                                                   "input_countries_ios_pop": INPUT_COUNTRIES_iOS_POP_PATH,
                                                   "output_path": MAU_ESTIMATION_PATH,
                                                   "overwrite_mode": OVERWRITE_MODE,
                                                   "envs": ENVS
                                                  }, timeout_seconds=3600)

# COMMAND ----------

if DATE >= date(2023, 1, 1,) and DATE <= date(2023, 2, 1):
    dbutils.notebook.run("./03_evaluate_mau", 3600, arguments={"input_path": MAU_ESTIMATION_PATH, 
                                                        "ga_dau_base_path": GA_DAU_BASE_PATH, 
                                                        "ga_mau_path": GA_MAU_PATH,
                                                        "output_path": MAU_EVALUATION_PATH,
                                                        "folds": FOLDS,
                                                        "min_ga": MIN_GA,
                                                        "min_users": MIN_USERS,
                                                        "countries": COUNTRIES,
                                                        "dates": ','.join([DATE_STR, LAST_DATE_IN_MONTH]),
                                                        "envs": ENVS, 
                                                        "overwrite_mode": OVERWRITE_MODE})