# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

from sklearn.model_selection import KFold
import numpy as np
from typing import Callable

# COMMAND ----------

DATE  = get_widget_or_default("date","2022-10-01")
LAST_SUNDAY_DATE = get_sunday_of_n_weeks_ago(DATE, 1)
LAST_SUNDAY_YEAR_MONTH_DAY_SUFFIX = add_year_month_day(LAST_SUNDAY_DATE)
YEAR_MONTH_DAY_SUFFIX = add_year_month_day(DATE)
ENVS = generate_env_list(get_widget_or_default("envs",'master_blender_scale,main'))
PREFIX_PATH =  get_widget_or_default('prefix_path',"/similargroup/data/ios-analytics/metrics/dau/")
H2B_PATH = PREFIX_PATH + '/raw_estimation'
SHARE_PATH = PREFIX_PATH + 'ios_share_p_estimation'
MP_EST_PATH = PREFIX_PATH + 'mp_dau' + LAST_SUNDAY_YEAR_MONTH_DAY_SUFFIX
GA_PATH =  get_widget_or_default('ga_path','/similargroup/data/store-analytics/general/ga4_extractor/processed_ls/')
H2B_MODEL_EVAL_PATH = PREFIX_PATH + 'measure_protocol_session_evaluation' +LAST_SUNDAY_YEAR_MONTH_DAY_SUFFIX
STORE_RANK_PATH = get_widget_or_default("store_rank_path", "/similargroup/data/store-analytics/iOS-app-store/top-charts/") + YEAR_MONTH_DAY_SUFFIX
METADATA_PATH = get_widget_or_default("metadata_path",
                                      '/similargroup/data/store-analytics/iOS-app-store/42matters/app-metadata/weekly/parquet/')+ LAST_SUNDAY_YEAR_MONTH_DAY_SUFFIX
COUNTRIES = [int(country) for country in get_widget_or_default("countries", "840,826,36,124").split(",")]
DATA_OUTPUT_PATH = get_widget_or_default("features_est_path", "/similargroup/data/ios-analytics/metrics/dau/scale_model_features_estimates" + YEAR_MONTH_DAY_SUFFIX)
DATA_TRAIN_PATH = get_widget_or_default("data_train_path", "/similargroup/data/ios-analytics/metrics/dau/scale_model_train_data" + YEAR_MONTH_DAY_SUFFIX)
NUM_DAYS = int(get_widget_or_default('num_days','28'))-1
MIN_USERS = int(get_widget_or_default('min_users','1'))
MIN_DAU = int(get_widget_or_default('min_dau','10'))
FOLDS = 5
OVERWRITE_MODE = get_widget_or_default("overwrite_mode", "True").lower() == 'true'


# COMMAND ----------

MAPE = lambda y, yhat: np.median(np.abs(np.array(y) - np.array(yhat)) / np.array(y))
SWACC = lambda y, yhat: np.mean((np.abs(np.array(y) - np.array(yhat)) / np.array(y)) < 0.3)

def estimate_adj_EBE(x: List[float], y: List[float], folds: int=3, loss_f: Callable[List[float], List[float]]=MAPE):
    best_cands = []
    kfold = KFold(n_splits=folds, shuffle=True,random_state=1)
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
    print(ps)
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

entities_fix = [['com.dutchbros.loyalty.ios','com.loyalty.dutchbros'],['com.classpass.Classpass','com.classpass.classpass']
                ,['com.shakeshack.kioskapp','com.halickman.ShackHQ'],['com.flipfit','com.flipfit.flip']
                ,['com.aisense.Otter','com.aisense.otter']]

df_entities_fix = spark.createDataFrame(entities_fix,['bundle_id','true_bundle_id'])

# COMMAND ----------

ga = (read_between_dates(DATE,DATE,GA_PATH,NUM_DAYS)
      .withColumn('country',F.col('country').astype('int'))
      .filter(F.col("country").isin(COUNTRIES))
      .filter("platform = 'iOS'")
      .select("country", "date", F.col("entity").alias("bundle_id"),F.col("dau_final").alias('activeUsers'))
      .join(df_entities_fix,'bundle_id','left')
      .withColumn('bundle_id',F.coalesce('true_bundle_id','bundle_id')).drop('true_bundle_id').filter('activeUsers > 0')
     )

df_h2b = (read_between_dates(DATE,DATE,H2B_PATH,NUM_DAYS,envs=ENVS)
          .withColumn('country',F.col('country').astype('int'))
          .filter(F.col('country').isin(COUNTRIES))
          .withColumnRenamed('app','bundle_id')
          .withColumn('users',F.expr('p*n'))
          .withColumn('dau',F.col('p'))
          .selectExpr('date','country','bundle_id','dau as dau_h2b','users')
          )


# COMMAND ----------

def adj(df,ga,col_name,min_users=0,min_ga = 20000,date=True):
    df_ = df
    if min_users == 0:
        df_ = df_.withColumn('users',F.lit(0))
    cols_on = ['country','date','bundle_id'] if date else ['country','bundle_id']
    df_joined = (ga
           .join(df_,cols_on,'left').fillna(0,subset=[col_name,'users'])
           .groupBy('country','bundle_id')
           .agg(F.mean('activeUsers').alias('dau_ga')
                ,F.mean(col_name).alias(col_name)
                ,F.mean('users').alias('users')).filter(F.col(col_name)>0)
           .withColumn('train_valid',F.col('users') >= min_users).filter(f'dau_ga > {min_ga}')
           .groupBy('country')
           .agg((find_adj_eval_udf(
               F.collect_list(col_name)
               ,F.collect_list('dau_ga')
               ,F.collect_list('train_valid'))['adj']).alias('adj')))
    return df_joined

# COMMAND ----------

adj_h2b = adj(df_h2b,ga,'dau_h2b',min_users=MIN_USERS).cache()

# COMMAND ----------

(adj_h2b
.select(F.col("country").cast("int").alias("country"), F.col("adj").alias("population_size"))
.orderBy("country")
.write.mode("overwrite").parquet("s3://sw-apps-core-data-buffer/user/omri.shkedi/ios-adjs"))