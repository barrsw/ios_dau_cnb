# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

# MAGIC %run Repos/apps/ios-dau/utils/common_utils

# COMMAND ----------

MAU_ESTIMATION_PATH = dbutils.widgets.get("input_path")
GA_DAU_BASE_PATH = dbutils.widgets.get("ga_dau_base_path")
GA_MAU_PATH = dbutils.widgets.get("ga_mau_path")
DATES = dbutils.widgets.get("dates").split(",")
OUTPUT_PATH = dbutils.widgets.get("output_path")
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'
ENVS = generate_env_list(dbutils.widgets.get("envs"))
FOLDS = int(dbutils.widgets.get("folds"))
MIN_GA = int(dbutils.widgets.get("min_ga"))
MIN_USERS = float(dbutils.widgets.get("min_users"))
COUNTRIES = [int(country) for country in dbutils.widgets.get("countries").split(",")]
GA4_IOS_PLATFORM = 'iOS'
PROD_ENVS = generate_prod_env()

# COMMAND ----------

from sklearn.model_selection import KFold
import numpy as np
from typing import Callable

MAPE = lambda y, yhat: np.median(np.abs(np.array(y) - np.array(yhat)) / np.array(y))
SWACC = lambda y, yhat: np.mean((np.abs(np.array(y) - np.array(yhat)) / np.array(y)) < 0.3)

@F.udf(returnType=T.StructType([T.StructField("bucket", T.ArrayType(T.StringType())),
                                T.StructField("swacc", T.ArrayType(T.FloatType())),
                                T.StructField("mape", T.ArrayType(T.FloatType())),
                                T.StructField("mau_dau_ratio_mape", T.ArrayType(T.FloatType())),
                                T.StructField("n_apps", T.ArrayType(T.IntegerType()))]))
def evaluate(y: List[float], yhat: List[float], ratios_y: List[float], ratios_yhat: List[float]):
    y = np.array(y)
    yhat = np.array(yhat)
    ratios_y = np.array(ratios_y)
    ratios_yhat = np.array(ratios_yhat)
    bucket_names, mapes, ratios_mapes, swaccs, n_apps = [], [], [], [], []

    bucket_names.append("-1")
    mapes.append(float(MAPE(y, yhat)))
    ratios_mapes.append(float(MAPE(ratios_y, ratios_yhat)))
    swaccs.append(float(SWACC(y, yhat)))
    n_apps.append(len(y))

    buckets = np.floor(np.log10(y))
    buckets_set = np.unique(buckets)
    for bucket in buckets_set:
        idx = buckets == bucket
        bucket_names.append(str(10 ** bucket))
        mapes.append(float(MAPE(y[idx], yhat[idx])))
        ratios_mapes.append(float(MAPE(ratios_y[idx], ratios_yhat[idx])))
        swaccs.append(float(SWACC(y[idx], yhat[idx])))
        n_apps.append(int(np.sum(idx)))

    return {"bucket": bucket_names, "mape": mapes, "mau_dau_ratio_mape": ratios_mapes, "swacc": swaccs, "n_apps": n_apps}

# COMMAND ----------

mau_df = (SparkDataArtifact().read_dataframe(MAU_ESTIMATION_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS)
            .filter(F.col("country").isin(COUNTRIES))
            .select("country", "app", F.col("active_users").alias("est_active_users"), "published_avg_dau"))

ga_mau_df = (spark.read.parquet(GA_MAU_PATH)
            .withColumn("country", F.col("country").cast("int"))
            .filter(F.col("country").isin(COUNTRIES))
            .filter(f"platform = '{GA4_IOS_PLATFORM}'")
            .select("country", "app", F.col("active_users").alias("ga_mau"), "stream_id"))

ga_avg_dau = (read_between_dates(DATES[0], DATES[1], GA_DAU_BASE_PATH, envs=PROD_ENVS)
            .selectExpr("entity as app","CAST(country as INT) as country", "streamId as stream_id", "activeUsers")
            .filter(F.col("country").isin(COUNTRIES))
            .filter(f"platform = '{GA4_IOS_PLATFORM}'")
            .groupBy("app", "country", "stream_id")
            .agg(F.avg("activeUsers").alias("ga_avg_dau")))

df = (ga_mau_df
        .join(mau_df, ["app", "country"])
        .join(ga_avg_dau, ["app", "country", "stream_id"])
        .filter(f"est_active_users > 0.0 AND ga_mau >= {MIN_GA}")
        .withColumn("ga_mau_dau_ratio", F.col("ga_mau") / F.col("ga_avg_dau"))
        .withColumn("mau_dau_ratio", F.col("est_active_users") / F.col("published_avg_dau"))
        .groupBy("country")
        .agg(F.collect_list("ga_mau").alias("ga_mau"),
            F.collect_list("est_active_users").alias("est_active_users"),
            F.collect_list("ga_mau_dau_ratio").alias("ga_mau_dau_ratio"),
            F.collect_list("mau_dau_ratio").alias("mau_dau_ratio"))
        .withColumn("res", evaluate("ga_mau", "est_active_users", "ga_mau_dau_ratio", "mau_dau_ratio"))
        .select("country", F.arrays_zip("res.bucket", "res.swacc", "res.mape", "res.mau_dau_ratio_mape", "res.n_apps").alias("res"))
        .withColumn("res", F.explode("res"))
        .select("country", "res.*")
        .withColumn("bucket", F.log10(F.col("bucket").cast("int")))
        .withColumn("bucket", F.when(F.expr(f"ISNULL(bucket)"), F.lit(-1)).otherwise(F.col("bucket"))))

# COMMAND ----------

write_output(df.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)