# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

# MAGIC %run Repos/apps/ios-dau/utils/common_utils

# COMMAND ----------

ENVS = generate_env_list(dbutils.widgets.get("envs"))
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'
ANDROID_PATH = dbutils.widgets.get("android_path")
ANDROID_ADJ_PATH = dbutils.widgets.get("android_adj_path")
MATCHES_PATH = dbutils.widgets.get("matches_path")
BUNDLE_INFO_PATH =  dbutils.widgets.get("bundle_info_path")
COUNTRIES = [int(country) for country in dbutils.widgets.get("countries").split(",")]
DATE = datetime.strptime(dbutils.widgets.get("date"), "%Y-%m-%d").date()
OUTPUT_PATH = dbutils.widgets.get("output_path")

# COMMAND ----------

def get_android_ps(android_path: str, android_adj_path: str, countries: List[int], d:date) -> DataFrame:
    ps = (read_between_dates(start=str(d), end=str(d+dt.timedelta(days=6)), path=android_path, days_back=0)
          .withColumn("country", F.col("country").cast("int"))
          .filter(F.col("country").isin(countries))
          .join(spark.read.parquet(android_adj_path), ['country'])
          .withColumn("p_prior", F.col("active_users") / F.col("population_size")))
    ps_avg = ps.groupBy("app", "country").agg(F.avg("p_prior").alias("p_prior"))
    return ps_avg.selectExpr("app as android_id", "p_prior", "country")

# COMMAND ----------

android_ps = get_android_ps(ANDROID_PATH, ANDROID_ADJ_PATH, COUNTRIES, DATE)
apps_matched = match_apps(MATCHES_PATH, BUNDLE_INFO_PATH)
priors = apps_matched.join(android_ps, "android_id", "inner").select(F.col("bundle_id").alias("app"), "country", "p_prior")
write_output(priors.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)