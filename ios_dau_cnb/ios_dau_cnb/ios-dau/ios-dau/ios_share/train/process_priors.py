# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

def DOW2str(colname: str) -> Column:
    expr = f"""
    CASE 
    WHEN {colname} = 'Sun' THEN 1 
    WHEN {colname} = 'Mon' THEN 2 
    WHEN {colname} = 'Tue' THEN 3 
    WHEN {colname} = 'Wed' THEN 4 
    WHEN {colname} = 'Thu' THEN 5 
    WHEN {colname} = 'Fri' THEN 6 
    ELSE 7 
    END"""
    return F.expr(expr).alias(colname)
    
def read_app_matching(app_matching_path: str, bundle_details_path: str, category_path: str) -> DataFrame:
    
    bundle_details = spark.read.parquet(bundle_details_path).selectExpr("id", "bundle_id as app_iOS")
    matches = spark.read.parquet(app_matching_path).selectExpr("android_id as app_Android", "ios_id as id")#apple id in digits
    cats = spark.read.parquet(category_path).groupBy("app_iOS").agg(F.mode("category_iOS").alias("category"))
    joined = bundle_details.join(matches, "id").join(cats, "app_iOS", "left").select("app_Android", "app_iOS", "category").distinct()
    return joined


def make_prior_ratio_system(apps: DataFrame, priors_path: str, country_prior_path: str, country_prior_category_path: str) -> DataFrame:
    
    priors = spark.read.parquet(priors_path).filter(F.col("country").isin(COUNTRIES)).select("app_Android", "app_iOS", "country", DOW2str("DOW"), "ios_ratio_pred")
    c_priors = (spark.read.parquet(country_prior_path).filter(F.col("country").isin(COUNTRIES))
                .groupBy("country").agg(F.mean("country_ratio").alias("country_ratio"))
               )
    cc_priors = (spark.read.parquet(country_prior_category_path)
                 .filter(F.col("country").isin(COUNTRIES))
                 .groupBy("country", F.col("category_iOS").alias("category"))
                 .agg(F.coalesce(F.mean("category_country_ratio"), F.mean("neighbour_category_country_ratio")).alias("category_country_ratio"))
                )
    return (apps.join(priors, ["app_iOS", "app_Android"], "left").join(c_priors, "country", "left").join(cc_priors, ["country", "category"], "left")
            .selectExpr("app_Android", "app_iOS", "country", "DOW", "ios_ratio_pred as ios_ratio", "country_ratio", "category_country_ratio")
           )

# COMMAND ----------

COUNTRIES = [840]

# COMMAND ----------

#android_est_path = dbutils.widgets.get("android_est_path")
IOS_PRIOR_PATH = dbutils.widgets.get("ios_ratio_prior_path")
COUNTRY_PRIOR_PATH = dbutils.widgets.get("country_prior_path")
COUNTRY_CATEGORY_PRIOR_PATH = dbutils.widgets.get("country_category_prior_path")
CATEGORY_PATH = dbutils.widgets.get("category_path")
BUNDLE_INFO_PATH = dbutils.widgets.get("bundle_info_path")
APPS_MATCHING_PATH = dbutils.widgets.get("apps_matching_path")
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'
OUTPUT_PATH = dbutils.widgets.get("output_path")
ENVS = generate_env_list(dbutils.widgets.get("envs"))

# COMMAND ----------

apps = read_app_matching(APPS_MATCHING_PATH, BUNDLE_INFO_PATH, CATEGORY_PATH)
prior_ratios = make_prior_ratio_system(apps, IOS_PRIOR_PATH, COUNTRY_PRIOR_PATH, COUNTRY_CATEGORY_PRIOR_PATH)

# COMMAND ----------

ratios = prior_ratios.select("app_Android", F.col("app_iOS").alias("bundle_id"), "country", "DOW", F.coalesce("ios_ratio", "country_ratio", "category_country_ratio").alias("ratio"))

# COMMAND ----------

write_output(ratios.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)