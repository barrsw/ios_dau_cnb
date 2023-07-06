# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/common_utils

# COMMAND ----------

# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

ENVS = generate_env_list(dbutils.widgets.get("envs"))
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'

##-----INPUTS------##
LS_PATH = dbutils.widgets.get("ls_path")

##------OUTPUT-----##
OUTPUT_PATH = dbutils.widgets.get("output_path")

# COMMAND ----------

#INFERENCE STAGE:
#[(3.291, 99.9%), (2.807, 99.5%), (2.576, 99%), (1.96, 95%)]:
#-----------------------------------------------------------#
ACCEPTANCE_CONFIDENCE_INTERVAL = 1.96 
SMALL_BUNDLE_ACCEPTANCE_CONFIDENCE_INTERVAL = 0.7
LOW_ACCEPTANCE_THR = 0.001

# COMMAND ----------

mp = SparkDataArtifact().read_dataframe(LS_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS)

#--------------------------#
#calculate prior of bundle:
#--------------------------#

pb = (mp.groupby("bundle_id")
      .agg(F.sum("hits").alias("bundle_hits"),
           F.size(F.collect_set("host")).alias("total_distinct_hosts_in_bundle")
          )
      .withColumn("dummy", F.lit(1.0))
      .withColumn("total_hits", F.sum("bundle_hits").over(Window.partitionBy("dummy")))
      .withColumn("prior_bundle_hits", F.col("bundle_hits") / F.col("total_hits"))
     )


#--------------------------#
#calculate prior of host:
#--------------------------#
ph = (mp.groupby("host").agg(F.sum("hits").alias("hits_of_host"))
     .withColumn("dummy", F.lit(1.0))
     .withColumn("total_hits", F.sum("hits_of_host").over(Window.partitionBy("dummy")))
     .withColumn("p_h", F.col("hits_of_host")/F.col("total_hits"))
     )

#----------------------------------------#
#calculate probability of host in  bundle:
#----------------------------------------#

p_host_in_bundle = (mp
 .join(pb.select("bundle_id", "bundle_hits", "prior_bundle_hits", "total_distinct_hosts_in_bundle"), ['bundle_id'])
 .join(ph.select("host", "hits_of_host", "p_h"), ['host'])
 .withColumn("p_of_h_given_b", F.col("hits") / F.col("bundle_hits")) #calc: p_of_h_given_b
 .withColumn("p_of_b_h", F.when(F.col("prior_bundle_hits")*F.col("p_of_h_given_b") <=F.lit(1.0), F.col("prior_bundle_hits")*F.col("p_of_h_given_b")).otherwise(F.lit(1.0)))
 .withColumn("p_of_b_given_h", F.col("p_of_b_h")/F.col("p_h"))
 .withColumn("mutual_information", F.col("p_of_h_given_b")*(F.log("p_of_h_given_b") - F.log("p_h")))
 .filter(F.col("p_of_b_given_h") > F.lit(LOW_ACCEPTANCE_THR))
)

write_output(p_host_in_bundle.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)