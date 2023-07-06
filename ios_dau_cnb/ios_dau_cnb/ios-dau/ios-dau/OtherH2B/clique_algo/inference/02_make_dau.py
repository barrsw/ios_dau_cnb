# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/common_utils

# COMMAND ----------

# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

ENVS = generate_env_list(dbutils.widgets.get("envs"))
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'

##-----INPUTS------##
PANEL_PATH = dbutils.widgets.get("panel_path")

##------OUTPUT-----##
OUTPUT_PATH = dbutils.widgets.get('output_path')


# COMMAND ----------

df_panel = SparkDataArtifact().read_dataframe(PANEL_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS)

# COMMAND ----------

df_max_user_app = df_panel.selectExpr('country', 'did', 'session_id','bundle_id','score').groupBy('country', 'did','bundle_id').agg(F.max('score').alias('user'))

total_users = df_max_user_app.groupBy('country').agg(F.count_distinct('did').alias('total_users'))

df_dau = df_max_user_app.groupBy('country','bundle_id').agg(F.sum('user').alias('users'))

df_final_dau = df_dau.join(total_users,['country']).withColumn('dau',F.expr('users/total_users'))

# COMMAND ----------

write_output(df_final_dau.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)