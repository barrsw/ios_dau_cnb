# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/common_utils

# COMMAND ----------

# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

ENVS = generate_env_list(dbutils.widgets.get("envs"))
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'

##-----INPUTS------##
PANEL_PATH = dbutils.widgets.get("panel_path")
INFERENCE_TABLE_PATH = dbutils.widgets.get("model_path")

##------OUTPUT-----##
OUTPUT_PATH = dbutils.widgets.get('output_path')

##------PARAMS-----##
MIN_INTERSEC = dbutils.widgets.get("min_intersec")

# COMMAND ----------

panel = SparkDataArtifact().read_dataframe(PANEL_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS)

# COMMAND ----------

df_model = SparkDataArtifact().read_dataframe(INFERENCE_TABLE_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS)
                   

reference_table = df_model.select('clique_id','bundle_id','score','model_predict')

cliques = df_model.select('clique_id','clique').distinct().select('clique_id', F.size('clique').alias('n_hosts'), F.explode('clique').alias('host'))


# COMMAND ----------

pivot_columns = ['country', 'did', 'session_id']
sessions_cliques = (panel
             .join(cliques ,'host')
             .groupBy('session_id','clique_id','n_hosts')
             .agg(F.count_distinct('host').alias('intersection'))
             .filter(F.expr(f'intersection/n_hosts >= {MIN_INTERSEC}'))
                    .select('session_id','clique_id').distinct())


panel_sessions = (panel
                  .groupBy('country', 'did', 'session_id')
                  .agg(F.min('ts').alias('ts')
                       , F.collect_set('host').alias('hosts')))

final_inference = (panel_sessions
                   .join(sessions_cliques, 'session_id')
                   .join(reference_table, 'clique_id'))

# COMMAND ----------

write_output(final_inference.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)