# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/common_utils

# COMMAND ----------

# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

ENVS = generate_env_list(dbutils.widgets.get("envs"))
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'

##-----INPUTS------##
CROSS_CORRS_PATH = dbutils.widgets.get("cross_host2host_path")
BUNDLE_HOST_PATH = dbutils.widgets.get("bundle2host_corr_path")

##------OUTPUT-----##
OUTPUT_PATH = dbutils.widgets.get('output_path')

##------PARAMS-----##
MIN_CORR_PAIR = float(dbutils.widgets.get("min_corr_pair"))
MIN_CORR_SINGLE = float(dbutils.widgets.get("min_corr_single"))
MIN_CORR_EDGE = float(dbutils.widgets.get("min_corr_edge"))

# COMMAND ----------

def find_cliques(data: List[Row], origin: str, target: str) -> DataFrame:
    """
    Returns dataframe with cliques of hosts based on a list of tuples with correlation between $host and $target

    OUTPUT
    Dataframe with
      $clique: array<string>
      $clique_id: string
    """
    
    graph = nx.Graph()
    for row in data:
        graph.add_edge(row[origin], row[target])
    cliques = []
    for i, c in enumerate(nx.find_cliques(graph)):
        cliques.append((c,))
        if i % 10_000 == 0:
            print(i, end=" |")
    return spark.createDataFrame(cliques, "clique array<string>").withColumn("clique_id", F.md5(F.concat_ws("", "clique")))

# COMMAND ----------

def update_cliques(cliques, host_corrs, lab_corrs, min_corr_pair, min_corr_single):
    blacklist = cliques.select(F.explode("clique").alias("host")).distinct()
    pairs = (host_corrs.filter(f"correlation > {min_corr_pair}")
             .join(blacklist, "host", "left_anti")
             .join(blacklist.withColumnRenamed("host", "target"), "target", "left_anti")
             .selectExpr("ARRAY(host, target) as clique")
             .withColumn("clique_id", F.md5(F.concat_ws("", "clique"))))
    blacklist_updated = blacklist.union(pairs.select(F.explode("clique").alias("host")).distinct())
    singles = (lab_corrs.filter(f"correlation > {min_corr_single}")
               .select("host")
               .distinct()
               .join(blacklist_updated, "host", "left_anti")
               .selectExpr("ARRAY(host) as clique")
               .withColumn("clique_id", F.md5(F.concat_ws("", "clique"))))
    
    return pairs.unionByName(singles).unionByName(cliques)

# COMMAND ----------

df_cross_corrs = SparkDataArtifact().read_dataframe(CROSS_CORRS_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS).filter(f'correlation > {MIN_CORR_EDGE}')

df_bundle2_host_corrs = SparkDataArtifact().read_dataframe(BUNDLE_HOST_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS)

# COMMAND ----------

cliques = find_cliques(df_cross_corrs.select("host", "target").collect(), "host", "target").filter(F.size("clique") > 2)

full_cliques = update_cliques(cliques, df_cross_corrs,df_bundle2_host_corrs , MIN_CORR_PAIR, MIN_CORR_SINGLE)

# COMMAND ----------

write_output(full_cliques.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)