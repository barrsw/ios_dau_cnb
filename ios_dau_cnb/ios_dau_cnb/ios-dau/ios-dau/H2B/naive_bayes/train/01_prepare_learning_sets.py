# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

# MAGIC %run Repos/apps/ios-dau/utils/common_utils

# COMMAND ----------

from typing import List, Union
import pyspark.sql.functions as F
from functools import reduce

# COMMAND ----------

def read_lab(lab_path: str) -> DataFrame:
    # to do: add filter for date
    return (
        spark.read.parquet(lab_path)
        .selectExpr("bundle_id as app", "bundle_version", "host")
        .drop_duplicates()
        .filter("host <> '' AND app NOT LIKE '%com.apple%'")
        .groupBy("app", "host")
        .agg(F.count("*").alias("hits")))

# COMMAND ----------

mp_blacklist = dbutils.widgets.get("mp_bundle_blacklist").split(",")
check_hosts = dbutils.widgets.get("check_hosts").split(",")
lab_path = dbutils.widgets.get("lab_path")
ls_path = dbutils.widgets.get("ls_path")
min_users = int(dbutils.widgets.get("min_users"))
remove_apple = dbutils.widgets.get("remove_apple") == 'True'
overwrite_mode = dbutils.widgets.get("overwrite_mode") == 'True'
envs = generate_env_list(dbutils.widgets.get("envs"), True)
DATE = dbutils.widgets.get("date")
mp_paths = get_measure_protocol_dates(dbutils.widgets.get("mp_path"), DATE)

COLLECTION_APPS = ["co.ios.BlockSite", "co.ios.BlockSite.BlockSiteVPN", "com.mywot.ios", "com.mywot.ios.WotPacketTunnel"]

# COMMAND ----------

print("Measure Protocol paths:" + str(mp_paths))

# COMMAND ----------

mp = read_measure_protocol(read_and_union(mp_paths), mp_blacklist)
mp_filtered = filter_mp(mp, check_hosts, min_users).select("app", "host", "hits", F.lit(False).alias("is_lab"))
lab = read_lab(lab_path).withColumn("is_lab", F.lit(True))
ls = (mp_filtered.unionByName(lab)
          .groupBy("app", "host")
          .agg(F.sum("hits").alias("hits"), F.max("is_lab").alias("is_lab"))
          .filter(~F.col("app").isin(COLLECTION_APPS)))

# COMMAND ----------

if remove_apple:
    ls = ls.filter("host NOT LIKE '%apple%' AND host NOT LIKE '%icloud%'")

# COMMAND ----------

write_output(ls.write.format("parquet"), ls_path, envs, overwrite_mode)