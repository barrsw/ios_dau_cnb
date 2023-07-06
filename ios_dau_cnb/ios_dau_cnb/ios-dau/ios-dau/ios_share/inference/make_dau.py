# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

ANDROID_EST_PATH = dbutils.widgets.get("android_est_path")
RATIOS_PATH = dbutils.widgets.get("ratios_path")
OUTPUT_PATH = dbutils.widgets.get("output_path")
COUNTRIES = dbutils.widgets.get("countries").split(",")
ENVS = generate_env_list(dbutils.widgets.get("envs"))
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'

# COMMAND ----------

def main():
    and_p = (spark.read.option("basePath", ANDROID_EST_PATH.split("/year")[0]).parquet(ANDROID_EST_PATH)
             .filter(F.col("country").isin(COUNTRIES)).add_date().select("country", "app", "p", F.dayofweek("date").alias("DOW"))
            )
    ratios = SparkDataArtifact().read_dataframe(RATIOS_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS).selectExpr("app_Android as app" , "bundle_id", "country", "DOW", "ratio")

    and_ios = and_p.join(ratios, ["app", "country", "DOW"])
    output_df = and_ios.select("country", "bundle_id", F.expr("p*ratio").alias("dau"))
    write_output(output_df.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)

# COMMAND ----------

main()