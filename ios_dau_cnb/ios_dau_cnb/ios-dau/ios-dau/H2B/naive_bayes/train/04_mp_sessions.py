# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

# MAGIC %run Repos/apps/ios-dau/utils/common_utils

# COMMAND ----------

OVERWITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'
ENVS = generate_env_list(dbutils.widgets.get("envs"))
DATE = dbutils.widgets.get("date")
MP_WEEKS = int(dbutils.widgets.get("mp_weeks"))

##-----JOB INPUT-----##
MP_PATHS = get_paths_for_mp(dbutils.widgets.get("mp_path"), DATE, MP_WEEKS)

##-----JOB OUTPUT-----##
OUTPUT_PATH = dbutils.widgets.get("output_path")

##-----JOB PARAMS-----##
MIN_USERS = int(dbutils.widgets.get("min_users"))
MIN_SESSION_LENGTH = int(dbutils.widgets.get("min_session_length"))
MAX_SESSION_LENGTH = int(dbutils.widgets.get("max_session_length"))
MIN_DISTINCT_HOSTS_PER_SESSION = int(dbutils.widgets.get("min_distinct_hosts_per_sessions"))
MP_BLACKLIST = dbutils.widgets.get("mp_bundle_blacklist").split(",")
CHECK_HOSTS = dbutils.widgets.get("check_hosts").split(",")

USA = 840

# COMMAND ----------

def get_mp_sessions(mp: DataFrame) -> DataFrame:
    win_first = Window.partitionBy('app', 'did', 'date').orderBy("first_time_stamp")
    win_last = Window.partitionBy('app', 'did', 'date').orderBy(F.desc("timestamp"))

    df_sessions = (mp
                   .withColumn('diff_prev_first', (F.col("first_time_stamp").cast("timestamp") - F.lag("first_time_stamp").over(win_first).cast("timestamp")).cast('int'))
                   .withColumn('diff_next_last', (F.col("timestamp").cast("timestamp") - F.lag("timestamp").over(win_last).cast("timestamp")).cast('int'))
                   .withColumn('session_number_first', F.sum(F.when(F.col('diff_prev_first') > MAX_SESSION_LENGTH, 1).otherwise(0)).over(win_first))
                   .withColumn('session_number_last', F.sum(F.when(F.col('diff_next_last') < -MAX_SESSION_LENGTH, 1).otherwise(0)).over(win_last)))

    df_first_sessions = (df_sessions
                         .filter(F.col('session_number_first') == 0)
                         .withColumn('session_type', F.lit('first_session')).withColumn('ts', F.col('first_time_stamp')))
    df_last_sessions = (df_sessions
                        .filter(F.col('session_number_last') == 0)
                        .withColumn('session_type', F.lit('last_session')).withColumn('ts', F.col('timestamp')))

    df_union = (df_first_sessions.unionByName(df_last_sessions)
                .withColumn('session_id', F.md5(F.concat_ws("", "app", 'did', 'date', 'session_type')))
                .select("session_id", "did", "app", "host", "ts"))

    df_length = (df_union.groupBy('session_id')
                 .agg(F.count('host').alias('hits'),
                       (F.max('ts').cast('timestamp') - F.min('ts').cast('timestamp')).cast('int').alias('session_length'),
                      F.min('ts').alias('session_timestamp')))
    return df_union.join(df_length, 'session_id')

# COMMAND ----------

print("Measure Protocol paths:" + str(MP_PATHS))

# COMMAND ----------

mp_df = read_measure_protocol(read_and_union(MP_PATHS), MP_BLACKLIST)
mp_filtered = filter_mp(mp_df, CHECK_HOSTS, MIN_USERS)
mp_sessions = get_mp_sessions(mp_filtered)

# COMMAND ----------

adjusted_mp_sessions = (mp_sessions
     .filter(f"session_length < {MIN_SESSION_LENGTH}")
     .groupBy("session_id", "did", "app", "host", "session_timestamp")
     .agg(F.sum("hits").alias("hits"))
     .withColumn("local_ts", F.to_timestamp(F.col("session_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
     .withColumn("utc_ts", F.substring(F.col("session_timestamp"), 0, 23).cast("timestamp"))
     .withColumn("max_host", F.count("host").over(Window.partitionBy("session_id", "app", "did")))
     .filter(F.col("max_host") >= MIN_DISTINCT_HOSTS_PER_SESSION)
     .select(F.lit(USA).alias("country"),
             F.lit("-1").alias("source"),
             "did",
             "session_id",
             F.lit("-1").alias("model"),
             F.lit("-1").alias("cv"),
             F.lit("-1").alias("osv"),
             F.lit("-1").alias("v"),
             "local_ts",
             "utc_ts",
             "host",
             F.lit("-1").alias("ua"),
             "app")
     .orderBy("country", "source", "session_id"))

# COMMAND ----------

write_output(adjusted_mp_sessions.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWITE_MODE)