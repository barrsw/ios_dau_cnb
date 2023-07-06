# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

##------INPUTS------##
DATE_STR = dbutils.widgets.get("date")
ENVS = dbutils.widgets.get("envs")

# COMMAND ----------



# COMMAND ----------

PREFIX_PATH = "/similargroup/data/ios-analytics/metrics/dau/"
OVERWRITE_MODE = 'True'
#-----DATES------#
YEAR_MONTH_SUFFIX = add_year_month(DATE_STR)
YEAR_MONTH = add_year_month(DATE_STR)
YEAR_MONTH_DAY = add_year_month_day(DATE_STR)
DATE = datetime.strptime(DATE_STR, "%Y-%m-%d")
YEAR, MONTH, DAY = DATE.year, DATE.month, DATE.day
PREV_YEAR_MONTH = add_year_month(DATE - relativedelta.relativedelta(months=1))

##------PATHS------##
SESSIONIZED_PATH = "{}/sessions/{}".format(PREFIX_PATH, YEAR_MONTH)
BUNDLE2HOST_LAB_PATH = "/mnt/df/sw-df-staging-ios-artificial-metered-panel-output/ok"
BUNDLE2HOST_CORR_PATH = "{}/bundle2host_corr/{}".format(PREFIX_PATH, YEAR_MONTH)
APP_INFO_PATH = "s3://sw-df-production-internal-data/apps/app_details/raw/{}".format(YEAR_MONTH)
APP_INFO_PATH = APP_INFO_PATH if path_exists(APP_INFO_PATH) else "s3://sw-df-production-internal-data/apps/app_details/raw/{}".format(PREV_YEAR_MONTH)
APP_INFO_MATCHING_PATH = "{}/app_info_matching/{}".format(PREFIX_PATH, YEAR_MONTH)
CROSS_HOST2HOST_PATH = "{}/cross_host2host_corr/{}".format(PREFIX_PATH, YEAR_MONTH)
CLIQUES_PATH = "{}/cliques/{}".format(PREFIX_PATH, YEAR_MONTH)
CANDIDATES_PATH = "{}/candidates/{}s".format(PREFIX_PATH, YEAR_MONTH)
PICKLED_MODEL_PATH = "/dbfs/FileStore/user/yishay.shtrassler/ios_model_1.pkl"
PRIORS_PATH = 's3://similargroup-research/user/itzhak.dvash/ios_panel/ios_share_static_prior'
MODEL_PATH = "{}/model/{}".format(PREFIX_PATH, YEAR_MONTH)

##-----JOBS PARAMS-----##


# COMMAND ----------

# dbutils.notebook.run('01_bundle2host_corr',3000,
#                      arguments={
#                          "input_path":BUNDLE2HOST_LAB_PATH,
#                          "output_path":BUNDLE2HOST_CORR_PATH,
#                          "overwrite_mode": OVERWRITE_MODE,
#                          "envs":ENVS,
#                          "min_corr":"0.1"})

# COMMAND ----------

# dbutils.notebook.run('02_app_info_matching',3000,
#                      arguments={
#                          "input_path":APP_INFO_PATH,
#                          "output_path":APP_INFO_MATCHING_PATH,
#                          "overwrite_mode": OVERWRITE_MODE,
#                          "envs":ENVS})

# COMMAND ----------

# dbutils.notebook.run('03_cross_host2host',3000,
#                      arguments={
#                          "input_path":SESSIONIZED_PATH,
#                          "output_path":CROSS_HOST2HOST_PATH,
#                          "overwrite_mode": OVERWRITE_MODE,
#                          "envs":ENVS,
#                          "min_host_count":"1",
#                          "min_domain_count":"30"})

# COMMAND ----------

dbutils.notebook.run('04_build_cliques',3000,
                     arguments={
                         "cross_host2host_path":CROSS_HOST2HOST_PATH,
                         "bundle2host_corr_path":BUNDLE2HOST_CORR_PATH,
                         "output_path": CLIQUES_PATH,
                         "overwrite_mode": OVERWRITE_MODE,
                         "envs":ENVS,
                         "min_corr_pair":"0.8",
                         "min_corr_single":"0.8",
                         "min_corr_edge": "0.3"})

# COMMAND ----------

dbutils.notebook.run('05_candidates',3000,
                     arguments={
                         "cliques_path":CLIQUES_PATH,
                         "bundle2host_corr_path":BUNDLE2HOST_CORR_PATH,
                         "app_info_matching_path": APP_INFO_MATCHING_PATH,
                         "app_info_path":APP_INFO_PATH,
                         "output_path":CANDIDATES_PATH,
                         "overwrite_mode": OVERWRITE_MODE,
                         "envs":ENVS,
                         "min_corr_bundle": "0.3"})

# COMMAND ----------

dbutils.notebook.run('06_model',3000,
                     arguments={
                         "candidates_path":CANDIDATES_PATH,
                         "pickled_model_path":PICKLED_MODEL_PATH,
                         "overwrite_mode": OVERWRITE_MODE,
                       "share_path" :PRIORS_PATH,
                         "envs":ENVS,
                         "min_prob":"0.9",
                         "output_path":MODEL_PATH})