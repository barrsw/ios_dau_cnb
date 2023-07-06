# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

envs = generate_env_list(dbutils.widgets.get("envs"), True)
##-----JOB INPUT-----##
ls_path = dbutils.widgets.get("ls_path")

##-----JOB PARAMS-----##
overwrite_mode = dbutils.widgets.get("overwrite_mode") == 'True'

min_inv_freq = float(dbutils.widgets.get("min_inv_freq"))
min_host_importance_for_unique = float(dbutils.widgets.get("min_host_importance_for_unique"))
min_hits_for_unique = int(dbutils.widgets.get("min_hits_for_unique"))
min_cond_p_for_unique = float(dbutils.widgets.get("min_cond_p_for_unique"))

##-----JOB OUTPUT-----##
params_path = dbutils.widgets.get("params_path")

# COMMAND ----------

def median_arr(arrname):
    arr = F.array_sort(arrname)
    arr_size = F.size(arr)
    is_even = (arr_size%F.lit(2) == F.lit(0))
    is_odd = ~is_even
    half_idx = (arr_size/F.lit(2)).cast("int")
    return F.when(is_odd, arr[half_idx]).otherwise((arr[half_idx - 1] + arr[half_idx]) * 0.5)

def compute_host_inv_freq(raw_ls: DataFrame) -> DataFrame:
    return raw_ls.groupBy("host").agg((1/F.countDistinct('app')).alias("inv_freq")).select("host", "inv_freq")

def filter_common_hosts(inv_freq: DataFrame, raw_ls: DataFrame, min_inv_freq: float) -> DataFrame:
    return raw_ls.join(inv_freq, "host").filter(f"inv_freq >= {min_inv_freq}").select("app", "host", "hits")

def compute_host_total_hits(raw_ls: DataFrame) -> DataFrame:
    return raw_ls.groupBy("host").agg(F.sum('hits').alias("h_total_hits")).select("host", "h_total_hits")

def compute_hits_per_bundle(ls_filtered: DataFrame) -> DataFrame:
    return ls_filtered.groupBy("app").agg(F.sum("hits").alias("total_b_hits"), median_arr(F.collect_list("hits")).alias("median_b_hits"))

def compute_total_hits(ls: DataFrame) -> DataFrame:
    return ls.agg(F.sum('hits').alias("total_hits"))\
        .crossJoin(ls).select('app', 'host', 'total_hits')

def compute_cond_p(ls_filtered: DataFrame, hits_per_b: DataFrame, host_total_hits: DataFrame, total_hits: DataFrame) -> DataFrame:
    hits_per_bh = ls_filtered.groupBy("app", "host").agg(F.sum("hits").alias("bh_hits"))
    hits_per_bh_total_hits = hits_per_bh.join(total_hits, ['app', 'host'])
    return hits_per_b.join(hits_per_bh_total_hits, "app").join(host_total_hits, 'host')\
        .selectExpr("app", "host", "(h_total_hits - bh_hits)/(total_hits - total_b_hits) as cond_p", "bh_hits/median_b_hits as host_importance")
    
def calc_params(inv_freq: DataFrame, cond_p: DataFrame, ls: DataFrame, min_host_importance_for_unique: float, min_hits_for_unique: int) -> DataFrame:
    return (inv_freq
            .join(ls, "host").join(cond_p, ["host", "app"])
            .withColumn("is_unique", F.expr(f"inv_freq ==1.0 AND host_importance >= {min_host_importance_for_unique} AND cond_p > {min_cond_p_for_unique} AND hits >= {min_hits_for_unique}"))
            .select("app", "host", "cond_p", "host_importance", "inv_freq", "is_unique", "hits", "is_lab")
           )


# COMMAND ----------

ls = SparkDataArtifact().read_dataframe(ls_path, spark.read.format("parquet"), debug=True, data_sources=envs)
host_inv_freq = compute_host_inv_freq(ls)
ls_filtered = filter_common_hosts(host_inv_freq, ls, min_inv_freq)
host_total_hits = compute_host_total_hits(ls_filtered)
hits_per_b = compute_hits_per_bundle(ls_filtered)
total_hits = compute_total_hits(ls_filtered)
cond_ps = compute_cond_p(ls_filtered, hits_per_b, host_total_hits, total_hits)
params = calc_params(host_inv_freq, cond_ps, ls, min_host_importance_for_unique, min_hits_for_unique)
write_output(params.write.format("parquet"), params_path, envs, overwrite_mode)