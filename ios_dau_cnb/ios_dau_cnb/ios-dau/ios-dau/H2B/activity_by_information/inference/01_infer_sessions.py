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
COUNTRIES = dbutils.widgets.get("countries").split(",")

# COMMAND ----------

#DECODE STAGE:
#-----------------------------------------------------------#
THR_INTEGRITY_FOR_FOR_DECODE = 0.38
THR_ACTIVITY_FOR_FOR_DECODE = 0.7

# COMMAND ----------

df_panel = SparkDataArtifact().read_dataframe(PANEL_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS).filter((F.col("country").isin(COUNTRIES)))
p_host_in_bundle = SparkDataArtifact().read_dataframe(P_HOST_IN_BUNDLE, spark.read.format("parquet"), debug=True, data_sources=ENVS)

# COMMAND ----------

full_panel = (df_panel
               .join(F.broadcast(p_host_in_bundle), ["host"]))

calc_avg_session =  (full_panel.groupby("country", "did", "source", "session_id") #same for all ("country", "did", "session_id")
                   .agg(F.mean("ts").alias("avg_session"),  F.size(F.collect_set("bundle_id")).alias("num_bundles_in_session"))
                   .withColumn("redundancy", F.log("num_bundles_in_session")))

#same for all ("country", "did", "session_id")

#-------------------------------------------------------------------#
#from here, reduce to ("country", "did", "session_id", "bundle_id"):#
#-------------------------------------------------------------------#
calc_probabilities = (full_panel
                    .select("country", 
                            "did", 
                            "source",
                            "host", 
                            "session_id", 
                            "bundle_id",
                            "prior_bundle_hits",
                            "mutual_information", 
                            "p_of_b_h", 
                            "p_of_b_given_h", 
                            "total_distinct_hosts_in_bundle"
                           )
                    #distinct host-bundle pairs in session:
                    .dropDuplicates(["country", 
                                     "did", 
                                     "source",
                                     "host", 
                                     "session_id", 
                                     "bundle_id", 
                                     "prior_bundle_hits",
                                     "mutual_information", 
                                     "p_of_b_h", 
                                     "p_of_b_given_h", 
                                     "total_distinct_hosts_in_bundle"]
                                   )
                    #over all hosts in "country", "did", "session_id", "bundle_id":
                    .groupby("country", "did", "source", "session_id", "bundle_id", "prior_bundle_hits", "total_distinct_hosts_in_bundle")     
                    .agg(F.sum("mutual_information").alias("mutual_information"),
                         F.sum("p_of_b_h").alias("bundle_prob_by_panel"),
                         F.max("p_of_b_given_h").alias("max_p_b_given_h"),
                         F.size(F.collect_set("host")).alias("num_distinct_hosts_in_session")
                        )
                    .join(calc_avg_session, ["country", "did", "source", "session_id"])
                    #-----------------#
                    #analyze activity:#
                    #-----------------#
                    .withColumn("host_activity_for_bundle", F.col("num_distinct_hosts_in_session") / F.col("total_distinct_hosts_in_bundle"))
                    .withColumn("mutual_information_bh", F.col("prior_bundle_hits") * F.when(F.col("mutual_information")<F.lit(0.0), F.lit(0))
                                .otherwise(F.col("mutual_information"))
                               ) #("country", "did", "session_id", "bundle_id")
                     .withColumn("total_mutual_information", F.sum("mutual_information_bh").over(Window.partitionBy("country", "did", "source", "session_id"))) #same for all ("country", "did", "session_id")
                     .withColumn("prob_of_activity_in_session", F.col("mutual_information_bh") / (F.lit(1e-20) + F.col("total_mutual_information")))  #("country", "did", "session_id", "bundle_id")            
                    .withColumn("shannon_of_activity_in_session", -F.log(F.col("prob_of_activity_in_session")))                                       #("country", "did", "session_id", "bundle_id")
                    .withColumn("relative_shanon", F.col("shannon_of_activity_in_session") / F.col("redundancy"))                                     #("country", "did", "session_id", "bundle_id")
                    .withColumn("ent_of_activity_in_session", F.col("shannon_of_activity_in_session") * F.col("prob_of_activity_in_session"))         #("country", "did", "session_id", "bundle_id")
                    .withColumn("entropy", F.sum(F.col("ent_of_activity_in_session")).over(Window.partitionBy("country", "did", "source", "session_id")))       
                    .withColumn("activity_in_session", F.lit(1.0)-F.col("entropy")/F.col("redundancy"))  
                    .withColumn("score", F.when((F.col("activity_in_session") > F.lit(THR_ACTIVITY_FOR_FOR_DECODE)),
                                                 F.col("max_p_b_given_h")                                                                             #activity in ("country", "did", "session_id", "bundle_id")
                                               )
                                          .otherwise(F.lit(0.0))                                                                                      #no activity detected
                              )
                   )

# COMMAND ----------

write_output(calc_probabilities.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)