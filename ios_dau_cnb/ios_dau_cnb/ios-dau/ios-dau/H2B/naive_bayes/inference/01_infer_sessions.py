# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

import numpy as np
from collections import Counter

# COMMAND ----------

OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'
ENVS = generate_env_list(dbutils.widgets.get("envs"), True)

##-----INPUTS------##
PARAMS_PATH = dbutils.widgets.get("params_path")
PRIORS_PATH = dbutils.widgets.get("priors_path")
PANEL_PATH = dbutils.widgets.get("panel_path")

##------OUTPUT-----##
OUTPUT_PATH = dbutils.widgets.get("solved_sessions_path")


REMOVE_APPLE = dbutils.widgets.get("remove_apple") == 'True'
MIN_HITS = int(dbutils.widgets.get("min_hits"))
MIN_INFO = float(dbutils.widgets.get("min_info"))
MIN_COND_P = float(dbutils.widgets.get("min_cond_p"))
MIN_SCORE = float(dbutils.widgets.get("min_score"))
COUNTRIES = [int(country) for country in dbutils.widgets.get("countries").split(",")]

# COMMAND ----------

PARAMS = parse_model_string(PARAMS_PATH)
SESSION_IDS = ["session_id", "did", "country", "source", "local_ts", "utc_ts"]
EPS = 1e-06
DECIMAL_CAST = "DECIMAL(38,32)"

# COMMAND ----------

def read_priors(priors_path: str) -> DataFrame:
    
    priors_collected = (SparkDataArtifact().read_dataframe(priors_path, spark.read.format("parquet"), debug=True, data_sources=ENVS)
                        .filter((F.col("country").isin(COUNTRIES))))
    return priors_collected


def read_and_adjust_ps(ps: DataFrame) -> Tuple[DataFrame, float]:

    N = ps.select("host").distinct().count()
    additive_smoothing = 1.0/N
    df = (ps.withColumn("cond_p", F.col("cond_p") + F.lit(additive_smoothing))
          .withColumn("weighted_p", (F.expr("cond_p")))
          .select("host", "app", "weighted_p"))
    return df, additive_smoothing


def filter_by_info(panel: DataFrame, params: DataFrame, min_info:float) -> DataFrame:
    
    inv_freq_by_host = params.select("host", "inv_freq").distinct()
    return (panel.join(inv_freq_by_host, ["host"], "inner")
     .groupBy("session_id", "did", "country", "source")
     .agg(F.min("local_ts").alias("local_ts"), F.min("utc_ts").alias("utc_ts"), F.expr("SUM(inv_freq*host_count)").cast(DECIMAL_CAST).alias("info_Q"), F.map_from_arrays(F.collect_list("host"), F.collect_list("host_count")).alias("hosts"))
     .filter(f"info_Q >= {min_info}"))
    
def read_panel(panel_path: str):
    return (SparkDataArtifact().read_dataframe(panel_path, spark.read.format("parquet"), debug=True, data_sources=ENVS)
                 .filter((F.col("country").isin(COUNTRIES)))
                 .groupBy("session_id", "did", "country", "source", "host")
                 .agg(F.count("host").alias("host_count"), F.min("local_ts").alias("local_ts"), F.min("utc_ts").alias("utc_ts")))


def infer_session(sessions: DataFrame, ps: DataFrame, additive_smoothing:float, priors: DataFrame, model_date: str, model_type: str, is_main_model: bool):
    
    # add all apps candidates for each session_id and host
    sessions_with_ps = (sessions.select(*SESSION_IDS, "info_Q", F.explode("hosts").alias("host", "host_count"))
                        .join(F.broadcast(ps), ["host"], "inner"))
    
    # All the hosts in a session
    hosts = sessions_with_ps.select(*SESSION_IDS, "info_Q", "host", "host_count").dropDuplicates()
    
    # All the apps in a session
    apps =  sessions_with_ps.select("session_id", "app").dropDuplicates()
    
    cands = (apps
     .join(hosts, ["session_id"]) # cross join of all the hosts and apps in a session
     .join(F.broadcast(ps), ["host", "app"], "left") # add the p
     .fillna(additive_smoothing))
    
    hosts_map = (hosts
                 .groupBy("session_id")
                 .agg(F.map_from_arrays(F.collect_list("host"), F.collect_list("host_count")).alias("hosts")))
    
    return (cands       
     .withColumn("cand_LL", F.log(F.col("host_count") + 1) * F.log(F.col("weighted_p"))).cast(DECIMAL_CAST))
     .groupBy(*SESSION_IDS, "info_Q", "app")
     .agg(F.sum("cand_LL").alias("cand_LL"))
     .join(priors, ["app", "country"], "left")
     .withColumn("log_p_prior", (F.coalesce(F.log(F.col("p_prior")), F.log(F.lit(EPS)))))
     .withColumn("cand_LL_post", -F.col("cand_LL") + (F.col("log_p_prior").cast(DECIMAL_CAST)))
     .withColumn("cand_L", F.col("cand_LL").cast(DECIMAL_CAST))
     .withColumn("cand_L_sum", F.sum("cand_L").over(Window.partitionBy(*SESSION_IDS, "info_Q")).cast(DECIMAL_CAST))
     .withColumn("score", (F.col("cand_L")/F.col("cand_L_sum")).cast(DECIMAL_CAST))
    #  .filter(f"score >= {MIN_SCORE}")
     .join(hosts_map, ["session_id"])
     .select("session_id", "did", "country", "source", "local_ts", "utc_ts", "info_Q", "app", "hosts", "score", F.lit(model_date).cast("date").alias("model_date"), F.lit(model_type).alias("model_type"), F.lit(is_main_model).cast("boolean").alias("is_main_model")))

# COMMAND ----------

def calc_infer_sessions_per_model(params_path: str, panel_df: DataFrame, priors: DataFrame, model_date: str, model_type: str, is_main_model: bool) -> DataFrame:
    params = read_NB_params(params_path, MIN_HITS, REMOVE_APPLE, ENVS)
    # filtered_params = params.filter(f"cond_p >= {MIN_COND_P}")
    filtered_params = params
    ps, additive_smoothing = read_and_adjust_ps(filtered_params)
    filtered_panel = filter_by_info(panel_df, params, MIN_INFO)
    return infer_session(filtered_panel, ps, additive_smoothing, priors, model_date, model_type, is_main_model)

# COMMAND ----------

panel = read_panel(PANEL_PATH)
priors = read_priors(PRIORS_PATH)
dfs = [calc_infer_sessions_per_model(param['model_path'], panel, priors, param['model_date'], param['model_type'], param['is_main_model']) for param in PARAMS]
unioned_models = (reduce(DataFrame.unionByName, dfs)
                  .orderBy("country", "app"))

# COMMAND ----------

write_output(unioned_models.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)