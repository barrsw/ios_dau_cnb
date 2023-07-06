# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/common_utils

# COMMAND ----------

# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

ENVS = generate_env_list(dbutils.widgets.get("envs"))
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'

##-----INPUTS------##
CLIQUES_PATH = dbutils.widgets.get("cliques_path")
BUNDLE_HOST_PATH = dbutils.widgets.get("bundle2host_corr_path")
BUNDLE_APP_INFO_PATH = dbutils.widgets.get("app_info_matching_path")
APP_INFO_PATH = dbutils.widgets.get("app_info_path")

##------OUTPUT-----##
OUTPUT_PATH = dbutils.widgets.get('output_path')

##------PARAMS-----##
MIN_CORR_BUNDLE = float(dbutils.widgets.get("min_corr_bundle"))

# COMMAND ----------

def match_clique_bundle(df_cliques:DataFrame, df_bundles:DataFrame, col_joined: str, source_name: str) -> DataFrame:
    df_x = df_cliques.groupBy('clique_id').agg(F.count_distinct(col_joined).alias('X'))
    df_y = df_bundles.groupBy('bundle_id').agg(F.count_distinct(col_joined).alias('Y'))
    df_xy = df_cliques.join(df_bundles, col_joined).groupBy('bundle_id', 'clique_id').agg(F.count_distinct(col_joined).alias('XY'))
    df_ret = (df_xy.join(df_x, ['clique_id'])
                     .join(df_y, ['bundle_id'])
                     .selectExpr('*', 'XY / (X+Y-XY) as score')
                     .selectExpr('bundle_id', 'clique_id', f'X as {source_name}_{col_joined}s_X', f'Y as {source_name}_{col_joined}s_Y', f'score as {source_name}_{col_joined}s_score'))
    return df_ret

def max_corr(df_panel:DataFrame, df_cliques:DataFrame)->DataFrame:
    return df_cliques.join(df_panel,'host').groupBy('bundle_id','clique_id').agg(F.max('correlation').alias('max_corr'))


@udf(returnType= T.FloatType())
def match_bundle(bundle_id, fingerprint):
    for host in fingerprint:
        p = tldextract.extract(host)
        if p.domain in bundle_id.lower():
            return 1.0
    return 0.0



@udf(returnType= T.FloatType())
def match_publisher(publisher_name, fingerprint):
    publisher_name = publisher_name.replace(" ", "")
    for host in fingerprint:
        p = tldextract.extract(host)
        if p.domain in publisher_name.lower():
            return 1.0
    return 0.0
  
@udf(returnType= T.FloatType())
def match_app_name(app_name, fingerprint):
    app_name = app_name.replace(" ", "")
    for host in fingerprint:
        p = tldextract.extract(host)
        if p.domain in app_name.lower():
            return 1.0
    return 0.0


# COMMAND ----------

df_app_info = (spark.read.json(APP_INFO_PATH).filter("store == 'itunes'")
              .select('bundle_id', 'app_name', 'publisher_name')
              .dropDuplicates(subset=['bundle_id']))

app_info_hosts = SparkDataArtifact().read_dataframe(BUNDLE_APP_INFO_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS)

panel_hosts = SparkDataArtifact().read_dataframe(BUNDLE_HOST_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS).filter(f'correlation > {MIN_CORR_BUNDLE}')

cliques = SparkDataArtifact().read_dataframe(CLIQUES_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS)

cliques_flat = cliques.withColumn('host',F.explode('clique')).withColumn('domain',get_domain('host'))

# COMMAND ----------

cliques_bundle_app_info_hosts = match_clique_bundle(cliques_flat, app_info_hosts,'host', 'app_info')
cliques_bundle_app_info_domain = match_clique_bundle(cliques_flat, app_info_hosts,'domain', 'app_info')
cliques_bundle_panel_hosts = match_clique_bundle(cliques_flat, panel_hosts,'host', 'panel')
cliques_bundle_panel_domain = match_clique_bundle(cliques_flat, panel_hosts,'domain', 'panel')

# COMMAND ----------

df_max_corr = max_corr(panel_hosts,cliques_flat)

# COMMAND ----------

candidates = cliques_bundle_app_info_domain.select('bundle_id', 'clique_id').unionByName(cliques_bundle_panel_domain.select('bundle_id', 'clique_id')).distinct()

# COMMAND ----------

df_features = (candidates
               .withColumn('bundles',F.count('bundle_id').over(Window.partitionBy('clique_id')))
               .join(cliques_bundle_app_info_hosts,['bundle_id', 'clique_id'], 'left_outer')
               .join(cliques_bundle_app_info_domain,['bundle_id', 'clique_id'], 'left_outer')
               .join(cliques_bundle_panel_hosts,['bundle_id', 'clique_id'], 'left_outer')
               .join(cliques_bundle_panel_domain,['bundle_id', 'clique_id'], 'left_outer')
               .join(df_max_corr, ['bundle_id', 'clique_id'], 'left_outer')
               .join(df_app_info.select('bundle_id', 'publisher_name','app_name'), ['bundle_id'])
               .join(cliques, ['clique_id'])
               .select('clique_id', 'bundle_id', 'bundles'
                       ,F.size('clique').alias('hosts')
                       ,F.coalesce('app_info_domains_X','panel_domains_X').alias('domains'),
                      'app_info_hosts_Y', 'app_info_hosts_score', 'app_info_domains_Y', 'app_info_domains_score', 
                            'panel_hosts_Y', 'panel_hosts_score', 'panel_domains_Y', 'panel_domains_score', 
                            'max_corr'
                       ,match_bundle('bundle_id','clique').alias('match_bundle')
                       ,match_publisher('publisher_name','clique').alias('match_publisher'),match_app_name('app_name','clique').alias('match_app_name'), 'clique')
               .distinct().fillna(-1.0))

# COMMAND ----------

write_output(df_features.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)