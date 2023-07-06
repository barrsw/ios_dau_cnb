# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

# MAGIC %run Repos/apps/ios-dau/utils/common_utils

# COMMAND ----------

OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'
ENVS = generate_env_list(dbutils.widgets.get("envs"))

##-----JOB INPUT-----##
IOS_PRIORS_PATH = dbutils.widgets.get("ios_priors_path")
COUNTRY_PRIORS_PATH = dbutils.widgets.get("country_priors_path")
CATEGORY_COUNTRY_PRIORS_PATH = dbutils.widgets.get("category_country_priors_path")
PRIORS_DATES = dbutils.widgets.get("priors_dates").split(",")
IOS_CATEGORY_PATH = dbutils.widgets.get("ios_category_path")
APPS_MATCHING_PATH = dbutils.widgets.get("apps_matching_path")
ITUNES_BUNDLES_PATH = dbutils.widgets.get("itunes_bundles_path")
ANDROID_DAU_ADJ_PATH = dbutils.widgets.get("android_dau_adj_path")
ANDROID_DAU_PATH = dbutils.widgets.get("android_dau_path")
DATE = dbutils.widgets.get("date")

##-----JOB OUTPUT-----##
OUTPUT_PATH = dbutils.widgets.get("output_path")

##-----JOB PARAMS-----##
COUNTRIES = [int(country) for country in dbutils.widgets.get("countries").split(",")]

# COMMAND ----------

def match_apps_with_category(app_matching: DataFrame, bundle_details: DataFrame, category: DataFrame) -> DataFrame:
    bundle_details = bundle_details.selectExpr("id", "bundle_id as app_iOS")
    matches = app_matching.selectExpr("android_id as app_Android", "ios_id as id")
    return (bundle_details
            .join(matches, "id")
            .join(category, "app_iOS", "left")
            .select("app_Android", "app_iOS", "category").distinct())

# COMMAND ----------

apps = match_apps_with_category(spark.read.parquet(APPS_MATCHING_PATH),
                      spark.read.parquet(ITUNES_BUNDLES_PATH),
                      SparkDataArtifact().read_dataframe(IOS_CATEGORY_PATH, spark.read.format("parquet"), debug=True))

priors = (SparkDataArtifact().read_dataframe(IOS_PRIORS_PATH, spark.read.format("parquet"), debug=True)
          .filter(F.col("country").isin(COUNTRIES)) # There are priors just on 840 but the priors are being used for all the countries
          .select("app_Android", "app_iOS", DOW2str("DOW").alias("DOW"), F.col("ios_ratio_pred").alias("ios_ratio")))

country_priors = (read_between_dates(PRIORS_DATES[0], PRIORS_DATES[1], COUNTRY_PRIORS_PATH)
                  .filter(F.col("country").isin(COUNTRIES))
                  .groupBy("country").agg(F.mean("country_ratio").alias("country_ratio")))

category_country_priors = (read_between_dates(PRIORS_DATES[0], PRIORS_DATES[1], CATEGORY_COUNTRY_PRIORS_PATH)
                           .filter(F.col("country").isin(COUNTRIES))
                           .groupBy("country", "category")
                           .agg(F.coalesce(F.mean("category_country_ratio"),
                                           F.mean("neighbour_category_country_ratio")).alias("category_country_ratio")))

ratios = (apps.join(country_priors) # cross-join to add the priors to all relevant countries
          .join(priors, ["app_iOS", "app_Android"], "left") # add ios share priors of 840 to all countries
          .join(category_country_priors, ["country", "category"], "left")
          .groupBy("app_Android", "app_iOS", "country", "DOW")
          .agg(F.coalesce(F.mean("ios_ratio"), F.mean("category_country_ratio"), F.mean("country_ratio")).alias("ratio")))

android_p = (SparkDataArtifact().read_dataframe(ANDROID_DAU_PATH, spark.read.format("parquet"), debug=True)
             .withColumn("date", F.lit(DATE).cast("date"))
             .withColumn("country", F.col("country").cast("int"))
             .filter(F.col("country").isin(COUNTRIES))
             .join(spark.read.parquet(ANDROID_DAU_ADJ_PATH), ['country'])
             .withColumn("p", F.col("active_users") / F.col("population_size"))
             .select("country", F.col("app").alias("app_Android"), "p", F.dayofweek("date").alias("DOW")))

result = (android_p.join(ratios, ["app_Android", "country", "DOW"])
        .select("country", F.col("app_iOS").alias("app"), F.col("ratio"), (F.col("p") * F.col("ratio")).alias("p"))
        .orderBy("country", "app"))

# COMMAND ----------

write_output(result.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)