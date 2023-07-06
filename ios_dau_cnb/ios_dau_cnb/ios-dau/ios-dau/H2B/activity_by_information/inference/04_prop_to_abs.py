# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

from sklearn.model_selection import KFold
import numpy as np
from typing import Callable

# COMMAND ----------

OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'
ENVS = generate_env_list(dbutils.widgets.get("envs"))

##-----INPUTS------##
DAU_P_BASE_PATH = dbutils.widgets.get("dau_p_base_path")
GA_BASE_PATH = dbutils.widgets.get("ga_base_path")

##------OUTPUT-----##
DAU_ABS_PATH = dbutils.widgets.get("dau_abs_path")
DAU_ABS_EVAL_PATH = dbutils.widgets.get("dau_abs_eval_path")

##----JOB PARAMS---##
DATE = dbutils.widgets.get("date")
FOLDS = int(dbutils.widgets.get("folds"))
MIN_GA = int(dbutils.widgets.get("min_ga"))
MIN_USERS = float(dbutils.widgets.get("min_users"))
COUNTRIES = [int(country) for country in dbutils.widgets.get("countries").split(",")]

# COMMAND ----------

MAPE = lambda y, yhat: np.median(np.abs(np.array(y) - np.array(yhat)) / np.array(y))
SWACC = lambda y, yhat: np.mean((np.abs(np.array(y) - np.array(yhat)) / np.array(y)) < 0.3)

def estimate_adj_ID(x: List[float], y: List[float]):
    x,y = np.array(x), np.array(y)
    return np.median(y/x)
  
def estimate_adj_EBE(x: List[float], y: List[float], folds: int=3, loss_f: Callable[List[float], List[float]]=MAPE):
    best_cands = []
    kfold = KFold(n_splits=folds, shuffle=True)
    for train_idx, test_idx in kfold.split(x):#todo: call it validation
        adjs = y[train_idx]/x[train_idx]
        min_adj = np.percentile(adjs, 20)
        max_adj = np.percentile(adjs, 80)
        scan_beam = np.linspace(0, 1, 100)
        cands = min_adj + (max_adj - min_adj)*scan_beam
        losses = np.array([loss_f(y[test_idx], x[test_idx]*cand_adj) for cand_adj in cands])
        best_cands.append(cands[np.argmin(losses)])
        
    return np.mean(best_cands)

def evaluate(y: List[float], yhat: List[float]):
    y = np.array(y)
    yhat = np.array(yhat)
    bucket_names, mapes, swaccs, ns = [], [], [], []

    bucket_names.append("-1")
    mapes.append(float(MAPE(y, yhat)))
    swaccs.append(float(SWACC(y, yhat)))
    ns.append(len(y))

    buckets = np.floor(np.log10(y))
    buckets_set = np.unique(buckets)
    for bucket in buckets_set:
        idx = buckets==bucket
        bucket_names.append(str(10**bucket))
        mapes.append(float(MAPE(y[idx], yhat[idx])))
        swaccs.append(float(SWACC(y[idx], yhat[idx])))
        ns.append(int(np.sum(idx)))
    
    return {"bucket": bucket_names, "mape": mapes, "swacc": swaccs, "n": ns}


def find_adj_eval(ps, daus, train_valid):
    ps = np.array(ps)
    daus = np.array(daus)
    train_valid = np.array(train_valid)
    kfold = KFold(n_splits=FOLDS, random_state=1, shuffle=True)
    idx = np.array(range(len(ps)))
    preds_abs = []
    GT_obs = []
    fold_adjs = []
    for train_idx, test_idx in kfold.split(ps):
        test_idx = np.isin(idx, test_idx)
        train_idx = np.isin(idx, train_idx)
        fold_adj = estimate_adj_EBE(ps[train_idx & train_valid], daus[train_idx & train_valid])
        fold_adjs.append(fold_adj)
        assert np.sum(test_idx & train_idx)==0
        preds_abs.extend(list(fold_adj*ps[test_idx]))
        GT_obs.extend(list(daus[test_idx]))
    min_idx = int(0.2*len(fold_adjs))
    max_idx = int(0.8*len(fold_adjs))
    final_adj = np.mean(sorted(fold_adjs)[min_idx:max_idx])
    res = evaluate(GT_obs, preds_abs)
    res["adj"] = float(final_adj)
    return res

find_adj_eval_udf = F.udf(find_adj_eval, T.StructType([
    T.StructField("bucket", T.ArrayType(T.StringType())),
    T.StructField("swacc", T.ArrayType(T.FloatType())),
    T.StructField("mape", T.ArrayType(T.FloatType())),
     T.StructField("n", T.ArrayType(T.IntegerType())),
    T.StructField("adj", T.FloatType())
]))

# COMMAND ----------

ps_df = (read_between_dates(start=DATE, end=DATE, path=DAU_P_BASE_PATH, days_back=27, envs=ENVS)
         .filter(F.col("country").isin(COUNTRIES))
         .selectExpr("country", "bundle_id", "dau as pred_dau_p", "date", "total_visits"))

ga4 = (read_between_dates(start=DATE, end=DATE, path=GA_BASE_PATH, days_back=27)
        .filter(F.col("country").isin(COUNTRIES))
        .filter("platform = 'iOS'")
        .groupBy("date", F.col("entity").alias("bundle_id"), "country").agg(F.max("activeUsers").alias("activeUsers"))
        .selectExpr("country", "bundle_id", "activeUsers", "date"))

# COMMAND ----------

df = (ga4
          .join(ps_df, ["bundle_id", "country", "date"], "left")
          .fillna(0.0, ["pred_dau_p"])
          .groupBy("bundle_id", "country")
          .agg(F.mean("activeUsers").alias("dau"), F.mean("pred_dau_p").alias("p"), F.mean("total_visits").alias("n_users"), F.count('*').alias("n_days"))
          .filter(f"p > 0.0 AND dau >= {MIN_GA}")
          .withColumn("train_valid", F.expr(f"n_users >= {MIN_USERS}")))

# COMMAND ----------

results = (df.groupBy("country").agg(F.collect_list("p").alias("ps"), 
                                F.collect_list("dau").alias("daus"), 
                                F.collect_list("train_valid").alias("train_valid"))
       .select("country", find_adj_eval_udf("ps", "daus", "train_valid").alias("res")))

# COMMAND ----------

dau_abs = (SparkDataArtifact().read_dataframe(DAU_P_BASE_PATH + add_year_month_day(DATE), spark.read.format("parquet"), debug=True, data_sources=ENVS)
 .join(results, "country")
 .withColumn("adj", F.col("res")["adj"])
 .withColumn("dau_abs", F.col("adj")*F.col("dau"))
 .withColumnRenamed("dau", "dau_p")
 .select("country", "bundle_id", "total_visits", "total_users", "dau_p", "adj", "dau_abs"))

# COMMAND ----------

write_output(dau_abs.write.format("parquet"), DAU_ABS_PATH, ENVS, OVERWRITE_MODE)

# COMMAND ----------

coverage = dau_abs.filter(F.col("dau_abs")>=10).groupBy("country").agg(F.count_distinct("bundle_id").alias("coverage"))

# COMMAND ----------

write_output(results.select("country", F.arrays_zip("res.bucket", "res.swacc", "res.mape", "res.n").alias("res"))
             .withColumn("res", F.explode("res"))
             .select("country", "res.*")
             .withColumn("bucket", F.log10(F.col("bucket").cast("int")))
             .withColumn("bucket", F.when(F.expr(f"ISNULL(bucket)"), F.lit(-1)).otherwise(F.col("bucket")))
             .unionByName(coverage, True)
             .write.format("parquet"), DAU_ABS_EVAL_PATH, ENVS, OVERWRITE_MODE)