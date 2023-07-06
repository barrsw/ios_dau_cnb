# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'
ENVS = generate_env_list(dbutils.widgets.get("envs"), True)

##-----INPUTS------##
PARAMS_PATH = dbutils.widgets.get("params_path")
SESSIONS_PATH = dbutils.widgets.get("sessions_path")
SOLVED_SESSIONS_PATH = dbutils.widgets.get("solved_sessions_path")
##------OUTPUT-----##
OUTPUT_PATH = dbutils.widgets.get("output_path")

COUNTRIES = [int(country) for country in dbutils.widgets.get("countries").split(",")]
MAX_INFO = 1000
DECIMAL_CAST = "DECIMAL(38,32)"

# COMMAND ----------

PARAMS = parse_model_string(PARAMS_PATH)

# COMMAND ----------

def solve_trivial(params_path:str, sessions: DataFrame, model_date: str, model_type: str, is_main_model: bool) -> DataFrame:
    uniques = SparkDataArtifact().read_dataframe(params_path, spark.read.format("parquet"), True, data_sources=ENVS).filter("is_unique = 'true'").select("host", "app")
    return sessions.join(uniques, "host").selectExpr("session_id", "did", "country", "source", "local_ts", "utc_ts", "app", "map(host, 1) as hosts", f"CAST(1.0 as {DECIMAL_CAST}) as score", f"CAST({MAX_INFO} as {DECIMAL_CAST}) as info_Q", f"CAST('{model_date}' as date) as model_date", f"'{model_type}' as model_type", f"CAST({is_main_model} as boolean) as is_main_model")


# COMMAND ----------

sessions = SparkDataArtifact().read_dataframe(SESSIONS_PATH, spark.read.format("parquet"), True, data_sources=ENVS).filter((F.col("country").isin(COUNTRIES)))

trivial_dfs =  [solve_trivial(param['model_path'], sessions, param['model_date'], param['model_type'], param['is_main_model']) for param in PARAMS]
trivials_df = reduce(DataFrame.unionByName, trivial_dfs)

solved_sessions = SparkDataArtifact().read_dataframe(SOLVED_SESSIONS_PATH, spark.read.format("parquet"), True, data_sources=ENVS)
unioned_df = (solved_sessions.unionByName(trivials_df, True)
              .withColumn("max_info_Q", F.max("info_Q").over(Window.partitionBy("session_id", "did", "country", "source", "local_ts", "utc_ts", "app", "score", "model_date", "model_type", "is_main_model")))
              .filter("max_info_Q == info_Q")
              .select("session_id", "did", "country", "source", "local_ts", "utc_ts", "info_Q", "app", "hosts", "score", "model_date", "model_type", "is_main_model")
              .orderBy("country", "app"))

# COMMAND ----------

write_output(unioned_df.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)