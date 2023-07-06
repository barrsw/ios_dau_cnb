# Databricks notebook source
# MAGIC %run /Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

#DATE_STR = dbutils.widgets.get("date")
#ENVS_STR = dbutils.widgets.get("envs")
ENVS = "master_share"
DATE_STR = "2022-08-01"
OVERWRITE_MODE = "True"

# COMMAND ----------

YEAR_MONTH_SUFFIX = add_year_month(DATE_STR)

# COMMAND ----------

PREFIX_PATH = "/similargroup/data/ios-analytics/metrics/dau/"

IOS_PRIOR_PATH = "s3://similargroup-research/user/shahar.dror/iOS_Share/intermediate/one_time/fixed_df/"
COUNTRY_PRIOR_PATH = f"s3://similargroup-research/user/shahar.dror/iOS_Share/intermediate/country_prior_df/year=22/month=06"
COUNTRY_CATEGORY_PRIOR_PATH = f"s3://similargroup-research/user/shahar.dror/iOS_Share/intermediate/category_country_prior_df/year=22/month=06"
CATEGORY_PATH = f"s3://similargroup-research/user/shahar.dror/iOS_Share/intermediate/categories_iOS/year=22/month=06"
APPS_MATCHING_PATH = "s3://similargroup-backup-retention/mrp/similargroup/data/android-apps-analytics/apps-matching/predict/{}".format(YEAR_MONTH_SUFFIX)
BUNDLE_INFO_PATH = "s3://sw-df-production-internal-data/apps/app_details/itunes_bundle/{}/day=15/".format(YEAR_MONTH_SUFFIX)
IOS_RATIOS_PROCESSED = PREFIX_PATH + "ios_ratios_processed" + YEAR_MONTH_SUFFIX

# COMMAND ----------

dbutils.notebook.run("./process_priors", 3600, arguments = {"envs": ENVS, "overwrite_mode": OVERWRITE_MODE, 
                                                            "ios_ratio_prior_path": IOS_PRIOR_PATH, 
                                                            "country_prior_path": COUNTRY_PRIOR_PATH,
                                                            "country_category_prior_path": COUNTRY_CATEGORY_PRIOR_PATH,
                                                            "category_path": CATEGORY_PATH,
                                                            "bundle_info_path": BUNDLE_INFO_PATH,
                                                            "apps_matching_path": APPS_MATCHING_PATH,
                                                            "output_path": IOS_RATIOS_PROCESSED
                                                           })