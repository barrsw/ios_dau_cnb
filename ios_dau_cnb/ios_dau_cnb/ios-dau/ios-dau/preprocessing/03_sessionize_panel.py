# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

PANEL_PATH = dbutils.widgets.get("input_path")
OUTPUT_PATH = dbutils.widgets.get("output_path")
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'
ENVS = generate_env_list(dbutils.widgets.get("envs"))
SESSION_LEN = int(dbutils.widgets.get("session_length_ms"))
REQ_DIFF_MS = int(dbutils.widgets.get("req_diff_ms"))

# COMMAND ----------

panel = SparkDataArtifact().read_dataframe(PANEL_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS)

# COMMAND ----------

win = Window.partitionBy('did', 'country', 'source', "model", "cv", "osv", "v").orderBy('local_ts_ms')
    
df0 = (panel
        .withColumn('prev_ts', F.lag('local_ts_ms', 1).over(win))
        .withColumn('delta_ts', F.col('local_ts_ms')-F.col('prev_ts'))
        .withColumn('indicator', (F.col('delta_ts').isNull() | (F.col('delta_ts')>REQ_DIFF_MS)).cast('int'))
        .withColumn('group', F.sum(F.col('indicator')).over(win.rangeBetween(Window.unboundedPreceding, 0))))

df1 = (df0
       .groupBy('did', 'country', 'source', 'group', "model", "cv", "osv", "v")
       .agg(F.min('local_ts_ms').alias('session_start_ts'),
            F.max('local_ts_ms').alias('session_end_ts'))
       .withColumn('session_duration', F.col('session_end_ts') - F.col('session_start_ts'))
       .withColumn('max_session_group', (F.col('session_duration')/SESSION_LEN).cast('int'))
       .select('did', 'country', 'source', 'group', "model", "cv", "osv", "v", 'max_session_group'))

panel_w_sessions = (df0.join(df1, ['did', 'country', 'source', 'group', "model", "cv", "osv", "v"], 'left')
        .withColumn('copy_delta_ts', F.when(F.col('indicator')==1, None).otherwise(F.col('delta_ts')))
        .withColumn('delta_rank', F.rank().over(Window.partitionBy('did', 'country', 'source', "model", "cv", "osv", "v").orderBy(F.col('copy_delta_ts').desc())))
        .withColumn('session_cut_indicator', (F.col('delta_rank')<=F.col('max_session_group')).cast('int'))
        .withColumn('final_indicator', F.greatest('indicator', 'session_cut_indicator'))
        .withColumn('group', F.sum(F.col('final_indicator')).over(win.rangeBetween(Window.unboundedPreceding, 0)))
        .withColumn('session_id', F.md5(F.concat('group', 'did', 'country', 'source', "model", "cv", "osv", "v")))
        .select("country", "source", "did", "session_id", "model", "cv", "osv", "v", "local_ts", "utc_ts", "local_ts_ms", "host", "ua")
        .orderBy("country", "source", "session_id"))

# COMMAND ----------

write_output(panel_w_sessions.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)