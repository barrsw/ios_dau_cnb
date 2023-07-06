# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/common_utils

# COMMAND ----------

# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

ENVS = generate_env_list(dbutils.widgets.get("envs"))
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'

##-----INPUTS------##
MP_PATH = dbutils.widgets.get("mp_path")

##------OUTPUT-----##
P_HOST_IN_BUNDLE = dbutils.widgets.get("output_path")

# COMMAND ----------

LOW_LIM_ACCEPTANCE_THR = 0.001
NUM_BUNDLES_CANDIDATES_IN_SESSION = 4

# COMMAND ----------

check_hosts = ["provider-api.health.apple.com", "config-chr.health.apple.com", "weather-data.apple.com", "sequoia.cdn-apple.com"]
def filter_mp(mp, check_hosts, min_users = 10):
    blacklist = (
        mp.filter(F.col("host").isin(check_hosts))
        .groupBy("bundle_id")
        .agg(F.count_distinct("host").alias("n"))
        .filter("n>=2 AND bundle_id NOT LIKE '%com.apple%'")
        .select("bundle_id")
    )
    whitelist = (
        mp.groupBy("bundle_id")
        .agg(F.count_distinct("user_id").alias("n_users"))
        .filter(f"n_users >= {min_users}")
        .select("bundle_id")
    )
    return (
        mp.join(blacklist, "bundle_id", "leftanti")
        .join(whitelist, "bundle_id")
        .drop("user_id")
    )

# COMMAND ----------

mp = spark.read.parquet(MP_PATH)
#--------------------------#
#calculate prior of host:
#--------------------------#
ph = (mp
      .groupby("host")
      .agg(F.sum("hits").alias("hits_of_host"), 
           F.count("bundle_id").alias("num_bundles_host_participates")
          )#over all bundle_id
      .withColumn("confidence", F.lit(1.0)/(F.lit(1.0) + F.pow((F.col("num_bundles_host_participates")/F.lit(NUM_BUNDLES_CANDIDATES_IN_SESSION)), F.lit(4.0))))
      .withColumn("total_host_portion", F.col("confidence") * F.col("hits_of_host"))
     )

##----------------------------------------#
#calculate probability of host in  bundle:
#----------------------------------------#
p_host_in_bundle = (mp
 .join(ph, ["host"])
 .withColumn("host_portion_per_bundle", F.col("confidence") * F.col("hits")) #hits - per bundle
 .groupby("bundle_id", "host", "total_host_portion")
 .agg(F.sum("host_portion_per_bundle").alias("host_portion_per_bundle"))
 .filter(F.col("host_portion_per_bundle")/F.col("total_host_portion") > F.lit(LOW_LIM_ACCEPTANCE_THR))
)

write_output(p_host_in_bundle.write.format("parquet"), P_HOST_IN_BUNDLE, ENVS, OVERWRITE_MODE)