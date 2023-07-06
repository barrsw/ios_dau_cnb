# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/common_utils

# COMMAND ----------

# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

ENVS = generate_env_list(dbutils.widgets.get("envs"))
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'

##-----INPUTS------##
INPUT_PATH = dbutils.widgets.get("input_path")

##------OUTPUT-----##
OUTPUT_PATH = dbutils.widgets.get('output_path')

# COMMAND ----------

df = (spark.read.json(INPUT_PATH)
              .filter("store == 'itunes'")
              .select('bundle_id', 'app_name', 'publisher_name', 'publisher_url', 'support_url', 'privacy_url')
              .dropDuplicates(subset=['bundle_id'])
             .fillna("", subset=["publisher_name", "bundle_id","publisher_url","privacy_url","app_name"]))

# COMMAND ----------

@udf(returnType=T.BooleanType())
def valid(publisher_name: str, app_name: str, bundle_id: str, url: str):
    if len(url) == 0:
        return False

    host = urlparse(url).netloc
    if len(host) < 5:
        return False

    publisher_name = publisher_name.replace(" ", "").lower()
    bundle_id = bundle_id.lower()

    p = tldextract.extract(host)
    if p.domain in publisher_name or p.domain in app_name or p.domain in bundle_id:
        return True
    return False


def app_info_bundle_host_domain(df_app_info: DataFrame) -> DataFrame:
    df_flat = (df_app_info.selectExpr("bundle_id", "publisher_name", "app_name", "publisher_url as url")
               .union(df_app_info.selectExpr("bundle_id", "publisher_name", "app_name", "privacy_url as url")))
    df_host_domain = (
        df_flat.filter(valid("publisher_name", "app_name", "bundle_id", "url"))
        .select("bundle_id", get_host("url").alias("host"))
        .distinct()
        .fillna("", subset=["host"])
        .withColumn("domain", get_domain("host")))
    return df_host_domain

# COMMAND ----------

df_host_domain = app_info_bundle_host_domain(df)

# COMMAND ----------

write_output(df_host_domain.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)