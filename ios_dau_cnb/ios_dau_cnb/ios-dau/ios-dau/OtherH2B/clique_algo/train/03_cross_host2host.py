# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/common_utils

# COMMAND ----------

# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

ENVS = generate_env_list(dbutils.widgets.get("envs"))
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'

##-----INPUTS------##
PANEL_PATH = dbutils.widgets.get("input_path")

##------OUTPUT-----##
OUTPUT_PATH = dbutils.widgets.get('output_path')

##------PARAMS-----##
MIN_HOST_COUNT = int(dbutils.widgets.get("min_host_count"))
MIN_DOMAIN_COUNT = int(dbutils.widgets.get("min_domain_count"))

# COMMAND ----------

panel = SparkDataArtifact().read_dataframe(PANEL_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS).select("session_id", "host").distinct()
panel_counts = panel.groupBy("host").count().filter(f"count > {MIN_HOST_COUNT}")
panel = panel.join(panel_counts, "host")

# COMMAND ----------

def filter_by_n_domains(cross_corrs, n):
    blacklist = (
        cross_corrs.filter("correlation > 0.3")
        .select("host", "target", get_domain("target").alias("target_domain"))
        .groupBy("host")
        .agg(F.count_distinct("target_domain").alias("n_domains"))
        .filter(f"n_domains>{n}"))
    return cross_corrs.join(blacklist, "host", "leftanti").join(
        blacklist.withColumnRenamed("host", "target"), "target", "leftanti")

# COMMAND ----------

cross_panel = panel.join(panel.selectExpr("host as target","session_id"), "session_id").filter("host != target")
cross_corrs = correlate(cross_panel, "host", "target", ["session_id"]).filter( 'correlation >  0.1')
cross_corrs_clean = filter_by_n_domains(cross_corrs, MIN_DOMAIN_COUNT).filter("host < target")
write_output(cross_corrs_clean.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)