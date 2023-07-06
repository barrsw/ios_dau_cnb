# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/common_utils

# COMMAND ----------

# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

import pickle
import pandas as pd

# COMMAND ----------

ENVS = generate_env_list(dbutils.widgets.get("envs"))
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'

##-----INPUTS------##
FEATURES_PATH = dbutils.widgets.get("candidates_path")
MODEL_PATH = dbutils.widgets.get("pickled_model_path")
SHARE_PATH = dbutils.widgets.get("share_path")

##------OUTPUT-----##
OUTPUT_PATH = dbutils.widgets.get('output_path')

##------PARAMS-----##
MIN_PROB = float(dbutils.widgets.get("min_prob"))

# COMMAND ----------

df_fetures = SparkDataArtifact().read_dataframe(FEATURES_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS).distinct()
model = pickle.load(open(MODEL_PATH, 'rb'))

# COMMAND ----------

@F.pandas_udf(returnType=T.FloatType())
def predict_pandas_udf(*features):
    
    X = pd.concat(features, axis=1).values

    y = model.predict_proba(X) 
    
    return pd.Series(y[:,1])

# COMMAND ----------

features_cols = ['bundles', 'hosts', 'domains',
       'app_info_hosts_Y', 'app_info_hosts_score', 'app_info_domains_Y',
       'app_info_domains_score', 'panel_hosts_Y', 'panel_hosts_score',
       'panel_domains_Y', 'panel_domains_score', 'max_corr', 'match_bundle',
       'match_publisher','match_app_name']
df_inference = df_fetures.withColumn('predict',predict_pandas_udf(*features_cols)).select('clique_id','bundle_id','bundles','clique','predict')

# COMMAND ----------

share_table = spark.read.parquet(SHARE_PATH).filter('country == 840').selectExpr('app as bundle_id','prior')

win = Window.partitionBy('clique_id')

inference_with_score = (df_inference.filter(f'predict >= {MIN_PROB}')
                        .join(share_table,'bundle_id','left')
                        .withColumn('min_prior',F.min('prior').over(win))
                       .fillna(1.0,subset=['min_prior'])
                       .withColumn('prior',F.coalesce('prior','min_prior'))
                       .withColumn('sum_priors',F.sum('prior').over(win))
                       .withColumn('score',F.expr('prior/sum_priors'))
                       .selectExpr('clique_id','clique','bundle_id','score','predict as model_predict')
                       )

# COMMAND ----------

write_output(inference_with_score.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)