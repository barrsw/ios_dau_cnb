# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

OVERWITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'
ENVS = generate_env_list(dbutils.widgets.get("envs"))

##-----JOB INPUT-----##
MP_SESSIONS_PATH = dbutils.widgets.get("sessions_path")
MP_SOLVED_SESSIONS = dbutils.widgets.get("solved_sessions_path")

##-----JOB OUTPUT-----##
OUTPUT_PATH = dbutils.widgets.get("output_path")

##-----JOB PARAMS-----##
EPS = 1e-06

# COMMAND ----------

sessions = SparkDataArtifact().read_dataframe(MP_SESSIONS_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS)
solved_sessions = SparkDataArtifact().read_dataframe(MP_SOLVED_SESSIONS, spark.read.format("parquet"), debug=True, data_sources=ENVS)

# COMMAND ----------

preds_user = (solved_sessions
              .withColumnRenamed("app", "pred_app")
              .groupBy("did", "pred_app").agg(F.max("score").alias("score"), F.sum("score").alias("pred_n_sessions")) #level of certainty that did opened bundle_id over the period
              .groupBy("pred_app").agg(F.sum("score").alias("pred_n_users"), F.sum("pred_n_sessions").alias("pred_n_sessions"))#sum of estimated visits over the period
              )

truth_user = (sessions
              .groupBy("app").agg(F.count_distinct('did').alias('true_n_users'), F.count_distinct("session_id").alias("true_n_sessions")))

result = (preds_user.join(truth_user, preds_user.pred_app == truth_user.app, "outer")
     .withColumn("app", F.coalesce("app", "pred_app"))
     .fillna(EPS)
     .select("app", "true_n_users", "true_n_sessions", F.expr("LOG(pred_n_users/true_n_users)").alias("n_users_err"), F.expr("LOG(pred_n_sessions/true_n_sessions)").alias("n_sessions_err"))
     .filter("n_users_err IS NOT NULL")
     .orderBy("app"))

# COMMAND ----------

write_output(result.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWITE_MODE)