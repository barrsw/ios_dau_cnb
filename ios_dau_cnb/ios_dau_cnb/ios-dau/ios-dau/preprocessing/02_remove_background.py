# Databricks notebook source
# MAGIC %run ../utils/infra_utils

# COMMAND ----------

PANEL_PATH = dbutils.widgets.get("panel_path")
OUTPUT_PATH = dbutils.widgets.get("output_path")
EXEC_DATE = dbutils.widgets.get("date")
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'
COUNTRIES = dbutils.widgets.get("countries")
REMOVE_APPLE = dbutils.widgets.get("remove_apple_hosts") == 'True'
BLACKLIST_HOSTS = dbutils.widgets.get("panel_path")
ENVS = generate_env_list(dbutils.widgets.get("envs"))

# COMMAND ----------

panel_df = SparkDataArtifact().read_dataframe(PANEL_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS)
blacklist_hosts = spark.read.parquet(BLACKLIST_HOSTS)

# COMMAND ----------

schema = T.ArrayType(
    T.StructType(
        [T.StructField("session_id", T.StringType(),True),
         T.StructField("session_type", T.IntegerType(),True),
         T.StructField("session_start", T.IntegerType(),True),
         T.StructField("session_end", T.IntegerType(),True),
         T.StructField('hosts',T.ArrayType(
             T.StructType(
                 [T.StructField("host", T.StringType(),True)
                 ])))
                      ]))

def create_session_dict(session):
    session_type = 0 if len(session['payload']) < 3 else 1
    return {'session_type': session_type, 'session_start': session['window'], 'session_end': session['window'] + 1, 'hosts': session['payload']}

def merge_sessions(s_1, s_2):
    return {'session_type': s_1['session_type'], 'session_start': s_1['session_start'], 'session_end': s_2['session_end'], 'hosts': s_1['hosts'] + s_2['hosts']}

def foreground_background_split(arr):
    MAX_DIFF = 60
    res = [create_session_dict(arr[0])]
    for window_cell in arr:
        prev_cell = res[-1]
        current_cell = create_session_dict(window_cell)
        merge_similar_close_sessions_cond = prev_cell['session_type'] == current_cell['session_type'] and prev_cell['session_end'] > current_cell['session_start'] - MAX_DIFF
        if merge_similar_close_sessions_cond:
            res[-1] = merge_sessions(prev_cell, current_cell)
        else:
            res.append(current_cell)
    return res

def condense_sessions(arr):
    MERGE_BE_TO_FE_LIMIT = 5
    arr_length = len(arr)
    i = 1
    res = []
    while i < arr_length - 1:
        prev_cell = arr[i - 1]
        current_cell = arr[i]
        next_cell = arr[i + 1]
        cond_1 = prev_cell['session_type'] == 1 and current_cell['session_type'] == 0
        cond_2 = prev_cell['session_end'] - MERGE_BE_TO_FE_LIMIT > current_cell['session_start']
        if cond_1 and cond_2:
            new_session = merge_sessions(prev_cell, current_cell)
            res.append(new_session)
            arr[i] = new_session
        else:
            res.append(prev_cell)
        
        i+=1
    
    return res
            
            
        
def fe_be_sessions(arr, did):
    try:
        res = foreground_background_split(arr)
#         res = condense_sessions(res)
        for i in range(len(res)):
            res[i]['session_id'] = str(did) + "_" + str(i)
        return res
    except:
        return []

        
        
        
        
fe_be_sessions_udf = F.udf(fe_be_sessions, schema)

# COMMAND ----------

df = (spark.read.parquet("s3://sw-apps-core-data-buffer/ios_dau_phoenix/master_preprocessing/similargroup/data/ios-analytics/metrics/dau/panel/year=22/month=11/day=01/")
      .withColumn("window", (F.col("ts") / 1000).cast("int"))
      .withColumn("payload", F.struct("host"))
      .groupBy("did", "country","window")
      .agg(F.collect_list("payload").alias("payload"), F.max("tz").alias("tz"))
      .groupBy("did","country")
      .agg(F.sort_array(F.collect_list(F.struct("window", "payload"))).alias("one_sec_sessions"), F.max("tz").alias("tz"))
      .withColumn("res", fe_be_sessions_udf("one_sec_sessions", "did"))
      .filter(F.size(F.col("res")) >= 1)
      .withColumn("events", F.explode("res"))
      .select("did","country", "tz", "events.session_type","events.session_id",
              (F.col("events.session_start").cast(T.LongType())*F.lit(1000)).alias("ts"), F.explode("events.hosts").alias("host"))
      .withColumn("host", F.col("host.host"))
      .filter(F.col("session_type") == 1)
      .drop("session_type")
      .join(blacklist_hosts, "host", "left_anti")
      .filter(F.col("country").isin(countries))
      .select("did", "country", "ts", "host"))

# COMMAND ----------

if remove_apple:
    df = df.filter("host NOT LIKE '%apple.com'")

# COMMAND ----------

write_output(df.write.format("parquet"), OUTPUT_PATH, ENVS, overwrite=True)