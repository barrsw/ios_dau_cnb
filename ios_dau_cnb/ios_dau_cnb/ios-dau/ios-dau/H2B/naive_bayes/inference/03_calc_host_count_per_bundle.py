# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'
ENVS = generate_env_list(dbutils.widgets.get("envs"))

##-----INPUTS------##
INPUT_PATH = dbutils.widgets.get("input_path")
PARAMS_PATH = dbutils.widgets.get("params_path")

##------OUTPUT-----##
OUTPUT_PATH = dbutils.widgets.get("output_path")


REMOVE_APPLE = dbutils.widgets.get("remove_apple") == 'True'
MIN_HITS = int(dbutils.widgets.get("min_hits"))
MIN_COND_P = float(dbutils.widgets.get("min_cond_p"))

# COMMAND ----------

PARAMS = parse_model_string(PARAMS_PATH)

# COMMAND ----------

def read_params_per_mode(path: str, model_date: str, model_type: str, is_main_model: bool):
    return (read_NB_params(path, MIN_HITS, REMOVE_APPLE, ENVS, MIN_COND_P)
            .select("host", "app", "inv_freq", "cond_p").distinct()
            .withColumn("model_date", F.lit(model_date).cast("date"))
            .withColumn("model_type", F.lit(model_type))
            .withColumn("is_main_model", F.lit(is_main_model).cast("boolean")))

# COMMAND ----------

params_dfs = [read_params_per_mode(param['model_path'], param['model_date'], param['model_type'], param['is_main_model']) for param in PARAMS]
params = reduce(DataFrame.unionByName, params_dfs)

host_count = (SparkDataArtifact().read_dataframe(INPUT_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS)
            .withColumn("max_score", F.max("score").over(Window.partitionBy("session_id", "did", "country", "source", "local_ts", "utc_ts", "app", "model_date", "model_type", "is_main_model")))
            .filter("max_score == score")
            .select("app", "country", "source", F.explode("hosts").alias("host", "host_count"), "score", "model_date", "model_type", "is_main_model")
            .groupBy("country", "app", "source", "host", "model_date", "model_type", "is_main_model")
            .agg(F.sum(F.log(F.col("host_count") + 1)).alias("host_count"))
            .join(F.broadcast(params), ["host", "app", "model_date", "model_type", "is_main_model"], "left")
            .orderBy("country", "app"))

# COMMAND ----------

write_output(host_count.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)