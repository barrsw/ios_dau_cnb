# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/common_utils

# COMMAND ----------

# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

ENVS = generate_env_list(dbutils.widgets.get("envs"))
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'

##-----INPUTS------##
PANEL_PATH = dbutils.widgets.get("panel_path")
P_HOST_IN_BUNDLE = dbutils.widgets.get("p_host_in_bundle")

##------OUTPUT-----##
OUTPUT_PATH = dbutils.widgets.get('output_path')

##------PARAMS-----##
COUNTRY = int(dbutils.widgets.get("country"))

# COMMAND ----------

import numpy as np

# COMMAND ----------

df_panel = SparkDataArtifact().read_dataframe(PANEL_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS).filter(f"country = {COUNTRY}")
p_host_in_bundle = SparkDataArtifact().read_dataframe(P_HOST_IN_BUNDLE, spark.read.format("parquet"), debug=True, data_sources=ENVS)

# COMMAND ----------

def get_bundle_probs(row):
    r = row.asDict()
    set_of_hosts = r["hosts_set_per_bundle"]
    num = 0.0
    den = 0.0
    max_single = 0.0
    for host_tuple in set_of_hosts:
        split = host_tuple.split(",")
        num += float(split[1])
        den += float(split[2])
        max_single = np.maximum(max_single, float(split[1])/float(split[2]))
    r["posterior"] = float(np.maximum(float(num/den), max_single))
    return Row(**r)

# COMMAND ----------

window3 = Window.partitionBy("country", "did", "session_id", "bundle_id").orderBy("avg_session").rowsBetween(-1, 1)
window5 = Window.partitionBy("country", "did", "session_id", "bundle_id").orderBy("avg_session").rowsBetween(-2, 2)

# COMMAND ----------

full_panel = (df_panel
               .join(F.broadcast(p_host_in_bundle), ["host"]))

calc_avg_session =  (full_panel.groupby("country", "did", "session_id") #same for all ("country", "did", "session_id")
                   .agg(F.mean("ts").alias("avg_session"),  
                        F.size(F.collect_set("bundle_id")).alias("num_bundles_in_session"))
                   .withColumn("redundancy", F.log("num_bundles_in_session"))                                         
                  )  #same for all ("country", "did", "session_id")

#-------------------------------------------------------------------#
#from here, reduce to ("country", "did", "session_id", "bundle_id"):#
#-------------------------------------------------------------------#
calc_probabilities_v1 = (full_panel
                    .select("country", 
                            "did", 
                            "session_id", 
                            "bundle_id",  
                            "host", 
                            "total_host_portion", 
                            "host_portion_per_bundle")
                    #distinct host-bundle pairs in session:
                    .dropDuplicates(["country", 
                                     "did", 
                                     "session_id", 
                                     "bundle_id", 
                                     "host", 
                                     "total_host_portion", 
                                     "host_portion_per_bundle"]
                                   )
                    #over all hosts in "country", "did", "session_id", "bundle_id":
                    .withColumn("posterior_by_single", F.col("host_portion_per_bundle")/F.col("total_host_portion"))
                    .groupby("country", "did", "session_id", "bundle_id")     
                    .agg(F.sum("host_portion_per_bundle").alias("exected_hits_given_bundle"),
                         F.sum("total_host_portion").alias("exected_total_hits"),
                         F.max("posterior_by_single").alias("max_single")
                        )
                    .join(calc_avg_session, ["country", "did", "session_id"])
                    .withColumn("posterior", F.col("exected_hits_given_bundle")/F.col("exected_total_hits"))
                    .withColumn("posterior", F.when(F.col("posterior")>F.col("max_single"), F.col("posterior")).otherwise(F.col("max_single")))
                    .withColumn("order_in_session", 
                                F.row_number()
                                .over(Window.partitionBy("country", "did", "session_id")
                                                    .orderBy(F.col("posterior").desc())
                                     )
                               )  
                   )

calc_probabilities_v2 = (full_panel
                    .select("country", 
                            "did", 
                            "session_id", 
                            "bundle_id",  
                            "host", 
                            "total_host_portion", 
                            "host_portion_per_bundle")
                    #distinct host-bundle pairs in session:
                    .dropDuplicates(["country", 
                                     "did", 
                                     "session_id", 
                                     "bundle_id", 
                                     "host", 
                                     "total_host_portion", 
                                     "host_portion_per_bundle"]
                                   )
                    .join(calc_avg_session, ["country", "did", "session_id"])
                    .withColumn("host_tuple", 
                                F.concat(F.concat(F.concat(F.concat(F.col("host"), 
                                                                    F.lit(",")), 
                                                           F.col("host_portion_per_bundle")), 
                                                  F.lit(",")), 
                                         F.col("total_host_portion")
                                        )
                               )
                    .withColumn("wider_window", F.collect_set("host_tuple").over(window3))
                    .groupby("country", "did", "session_id", "bundle_id", "avg_session")
                    .agg(F.first("wider_window").alias("hosts_set_per_bundle"))
                    .rdd.map(get_bundle_probs).toDF()
                    .withColumn("order_in_session", 
                                F.row_number().over(Window.partitionBy("country", "did", "session_id")
                                                    .orderBy(F.col("posterior").desc()))
                               ) 
                   )

calc_probabilities_v3 = (full_panel
                    .select("country", 
                            "did", 
                            "session_id", 
                            "bundle_id",  
                            "host", 
                            "total_host_portion", 
                            "host_portion_per_bundle")
                    #distinct host-bundle pairs in session:
                    .dropDuplicates(["country", 
                                     "did", 
                                     "session_id", 
                                     "bundle_id", 
                                     "host", 
                                     "total_host_portion", 
                                     "host_portion_per_bundle"]
                                   )
                    .join(calc_avg_session, ["country", "did", "session_id"])
                    .withColumn("host_tuple", 
                                F.concat(F.concat(F.concat(F.concat(F.col("host"), 
                                                                    F.lit(",")), 
                                                           F.col("host_portion_per_bundle")), 
                                                  F.lit(",")), 
                                         F.col("total_host_portion")
                                        )
                               )
                    .withColumn("wider_window", F.collect_set("host_tuple").over(window5))
                    .groupby("country", "did", "session_id", "bundle_id", "avg_session")
                    .agg(F.first("wider_window").alias("hosts_set_per_bundle"))
                    .rdd.map(get_bundle_probs).toDF()
                    .withColumn("order_in_session", 
                                F.row_number().over(Window.partitionBy("country", "did", "session_id")
                                                    .orderBy(F.col("posterior").desc()))
                               ) 
                   ) 

# COMMAND ----------

output_df = (calc_probabilities_v1
    .join(calc_probabilities_v2
          .selectExpr("country", "did", "session_id", "bundle_id", "avg_session", "posterior as posterior3"), 
          ["country", "did", "session_id", "bundle_id", "avg_session"]
         )
   .join(calc_probabilities_v3
          .selectExpr("country", "did", "session_id", "bundle_id", "avg_session", "posterior as posterior5"), 
          ["country", "did", "session_id", "bundle_id", "avg_session"]
         )
   .withColumn("find_max", F.when(F.col("posterior") > F.col("posterior3"), F.col("posterior")).otherwise(F.col("posterior3")))
   .withColumn("score", F.when(F.col("find_max") > F.col("posterior5"), F.col("find_max")).otherwise(F.col("posterior5"))))

# COMMAND ----------

write_output(output_df.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)