# Databricks notebook source
# MAGIC %run ../utils/infra_utils

# COMMAND ----------

SESSION_PATH = dbutils.widgets.get("input_path")
P_BROWSING_PATH = dbutils.widgets.get("p_browsing")
OUTPUT_PATH = dbutils.widgets.get("output_path")
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'
ENVS = generate_env_list(dbutils.widgets.get("envs"))
DATE_STR = dbutils.widgets.get("date")

# COMMAND ----------

def paint_sessions(sessions_df: DataFrame, p_browsing_df: DataFrame, mode_join: str = 'inner') -> DataFrame:
    
    df_joined = sessions_df.join(p_browsing_df, 'host', mode_join)
    
    df_agg = df_joined.groupBy('session_id').agg(F.percentile_approx('p_browsing',0.5).alias('median_p_browsing'),F.mean('p_browsing').alias('mean_p_browsing'))
    
    return sessions_df.join(df_agg, 'session_id', 'left')

# COMMAND ----------

date_p_browsing = get_sunday_of_n_weeks_ago(datetime.strptime(DATE_STR, "%Y-%m-%d").date(), 1)
YEAR_MONTH_DAY_P_BROWSING = add_year_month_day(date_p_browsing)

full_p_browsing_path = "{}/{}".format(P_BROWSING_PATH, YEAR_MONTH_DAY_P_BROWSING)

# COMMAND ----------

df_sessions = SparkDataArtifact().read_dataframe(SESSION_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS)

df_p_browsing = SparkDataArtifact().read_dataframe(full_p_browsing_path, spark.read.format("parquet"), debug=True, data_sources=ENVS)

df_paint = paint_sessions(df_sessions, df_p_browsing)

# COMMAND ----------

write_output(df_writer = df_paint.write.format("parquet"), path = OUTPUT_PATH, envs = ENVS, overwrite = OVERWRITE_MODE)