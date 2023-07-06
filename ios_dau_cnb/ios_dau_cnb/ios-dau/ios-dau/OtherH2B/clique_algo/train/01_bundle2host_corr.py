# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/common_utils

# COMMAND ----------

# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

ENVS = generate_env_list(dbutils.widgets.get("envs"))
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'

##-----INPUTS------##
INPUT_PATH = dbutils.widgets.get("input_path")

##------OUTPUT-----##
OUTPUT_PATH = dbutils.widgets.get('output_path')

##------PARAMS-----##
MIN_CORR = float(dbutils.widgets.get("min_corr"))

# COMMAND ----------

schema = ['device_id', 'ts', 'bundle_id', 'host']
df =(spark.read.parquet(INPUT_PATH)
                    .filter("month < 12")
                    .select(schema)
                    .withColumn('ts', F.unix_timestamp('ts',format="yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").astype(T.IntegerType()))
                    .filter("host NOT LIKE '%.apple.com'")
                    .filter(F.col("bundle_id") != "UserEventAgent").filter('host is not null and host != ""'))

# COMMAND ----------

SESSION_PARTITION_COLS =["device_id", "bundle_id"]

# COMMAND ----------

sessions_df = sessionize(df,SESSION_PARTITION_COLS)

# COMMAND ----------

corr_df = correlate(sessions_df , x_col='host', y_col='bundle_id', unit_cols=['session_id'])

# COMMAND ----------

write_output(corr_df.filter(f'correlation > {MIN_CORR}').withColumn('domain',get_domain('host')).write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)