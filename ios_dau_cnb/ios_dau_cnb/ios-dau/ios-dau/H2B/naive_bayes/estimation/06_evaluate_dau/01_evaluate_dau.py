# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

# MAGIC %run Repos/apps/ios-dau/utils/common_utils

# COMMAND ----------

from sklearn.model_selection import KFold
import numpy as np
from typing import Callable

# COMMAND ----------

OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'
ENVS = generate_env_list(dbutils.widgets.get("envs"))

##-----INPUTS------##
INPUT_BASE_PATH = dbutils.widgets.get("input_base_path")
GA_BASE_PATH = dbutils.widgets.get("ga_base_path")

##------OUTPUT-----##
OUTPUT_PATH = dbutils.widgets.get("output_path")

##----JOB PARAMS---##
DATE = dbutils.widgets.get("date")
FOLDS = int(dbutils.widgets.get("folds"))
MIN_GA = int(dbutils.widgets.get("min_ga"))
MIN_USERS = float(dbutils.widgets.get("min_users"))
COUNTRIES = [int(country) for country in dbutils.widgets.get("countries").split(",")]
GA4_IOS_PLATFORM = 'iOS'

# COMMAND ----------

MAPE = lambda y, yhat: np.median(np.abs(np.array(y) - np.array(yhat)) / np.array(y))
SWACC = lambda y, yhat: np.mean((np.abs(np.array(y) - np.array(yhat)) / np.array(y)) < 0.3)

@F.udf(returnType=T.StructType([T.StructField("bucket", T.ArrayType(T.StringType())),
                                T.StructField("swacc", T.ArrayType(T.FloatType())),
                                T.StructField("mape", T.ArrayType(T.FloatType())),
                                T.StructField("n_apps", T.ArrayType(T.IntegerType()))]))
def evaluate(y: List[float], yhat: List[float]):
    y = np.array(y)
    yhat = np.array(yhat)
    bucket_names, mapes, swaccs, n_apps = [], [], [], []

    bucket_names.append("-1")
    mapes.append(float(MAPE(y, yhat)))
    swaccs.append(float(SWACC(y, yhat)))
    n_apps.append(len(y))

    buckets = np.floor(np.log10(y))
    buckets_set = np.unique(buckets)
    for bucket in buckets_set:
        idx = buckets == bucket
        bucket_names.append(str(10 ** bucket))
        mapes.append(float(MAPE(y[idx], yhat[idx])))
        swaccs.append(float(SWACC(y[idx], yhat[idx])))
        n_apps.append(int(np.sum(idx)))

    return {"bucket": bucket_names, "mape": mapes, "swacc": swaccs, "n_apps": n_apps}

# COMMAND ----------

dau_df = (read_between_dates(start=DATE, end=DATE, path=INPUT_BASE_PATH, days_back=27, envs=ENVS)
         .filter(F.col("country").isin(COUNTRIES))
         .select("date", "country", "app", F.col("active_users").alias("est_active_users")))

ga_df = (read_between_dates(start=DATE, end=DATE, path=GA_BASE_PATH, days_back=27)
         .withColumn("country", F.col("country").cast("int"))
         .filter(F.col("country").isin(COUNTRIES))
         .filter(f"platform = '{GA4_IOS_PLATFORM}'")
         .groupBy("date", F.col("entity").alias("app"), "country")
         .agg(F.max("dau_final").alias("ga_active_users"))
         .select("date", "country", "app", "ga_active_users"))

# COMMAND ----------

df = (ga_df
      .join(dau_df, ["app", "country", "date"], "left")
      .fillna(0.0, ["est_active_users"])
      .groupBy("app", "country")
      .agg(F.mean("ga_active_users").alias("ga_active_users"),
           F.mean("est_active_users").alias("est_active_users"),
           F.count('*').alias("n_days"))
      .filter(f"est_active_users > 0.0 AND ga_active_users >= {MIN_GA}")
      .groupBy("country")
      .agg(F.collect_list("ga_active_users").alias("ga_active_users"),
           F.collect_list("est_active_users").alias("est_active_users"))
      .withColumn("res", evaluate("ga_active_users", "est_active_users").alias("res")))

# COMMAND ----------

results = (df
           .select("country", F.arrays_zip("res.bucket", "res.swacc", "res.mape", "res.n_apps").alias("res"))
           .withColumn("res", F.explode("res"))
           .select("country", "res.*")
           .withColumn("bucket", F.log10(F.col("bucket").cast("int")))
           .withColumn("bucket", F.when(F.expr(f"ISNULL(bucket)"), F.lit(-1)).otherwise(F.col("bucket"))))

# COMMAND ----------

write_output(results.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)