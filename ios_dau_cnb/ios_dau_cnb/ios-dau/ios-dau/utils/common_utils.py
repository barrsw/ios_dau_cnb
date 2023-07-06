# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame,  Column, Row
from typing import *
import pyspark.sql.types as T
from urllib.parse import urlparse
from hashlib import  md5   

# COMMAND ----------

def correlate(df: DataFrame, x_col: str, y_col: str, unit_cols: List[str]) -> DataFrame:
    agg = lambda df, vars_, units, agg_name: df.groupBy(vars_).agg(F.count_distinct(*units).alias(agg_name))
    
    joint_vars = [x_col, y_col]
    joint_vars_name = '_'.join(joint_vars)
    
    x = agg(df, [x_col], unit_cols, 'x')
    y = agg(df, [y_col], unit_cols, 'y')
    xy = agg(df, joint_vars, unit_cols, 'xy')
    N = df.select(unit_cols).distinct().count()
    return (xy.join(x, x_col).join(y, y_col).selectExpr(x_col, y_col, f'{N} as N', f'x/{N} as px', f'y/{N} as py', f'xy/{N} as pxy')
     .withColumn("correlation", F.expr('((pxy - (px * py)) / (SQRT(px * (1-px)) * SQRT(py * (1-py))))'))
    )

# COMMAND ----------

def sessionize(df:DataFrame, partition_identifiers: List[str], timedelta_in_secs: int = 600):
    
    assert isinstance(df.schema["ts"].dataType, T.IntegerType), "Must include an int time column 'ts'"
    
    w = Window.partitionBy(partition_identifiers).orderBy("ts")
    return (df.withColumn("timedelta", (F.col("ts") - F.lag("ts").over(w)))
     .withColumn("new_session", F.when(F.expr(f"(timedelta IS NULL) OR (timedelta > {timedelta_in_secs})"), 1).otherwise(0))
     .withColumn("session_no", F.when(F.expr("new_session==1"), F.row_number().over(w)).otherwise(None))
     .withColumn("session_no", F.last("session_no", ignorenulls=True).over(w))
     .withColumn("session_id", F.concat_ws("-", *partition_identifiers, "session_no"))
     .drop("session_no", "new_session", "timedelta")
    )

# COMMAND ----------

@udf(returnType= T.StringType())
def get_host(url):
    ret = urlparse(url).netloc
    return ret if ret else ''


@udf(returnType= T.StringType())
def get_domain(host):
    p = tldextract.extract(host)
    return f'{p.domain}.{p.suffix}'

# COMMAND ----------

w_avg = lambda v, w: (F.sum(F.expr(v) * F.expr(w)) / F.sum(F.when(F.expr(v).isNotNull(), F.expr(w)))).alias(v)
binomial_mean = lambda p, n: (p * n + 1) / (n + 2)

# COMMAND ----------

def match_apps(matching_path: str, bundle_info_path: str) -> DataFrame:
    w = Window.partitionBy("bundle_id", "id").orderBy(F.desc("count"))
    name2id = spark.read.parquet(bundle_info_path).groupBy("bundle_id", "id").count().withColumn("rank", F.rank().over(w)).filter("rank = 1").selectExpr("bundle_id", "id as ios_id")
    w = Window.partitionBy("android_id", "ios_id").orderBy(F.desc("count"))
    ios2and = spark.read.parquet(matching_path).filter("label == 1").groupBy("android_id", "ios_id").count().withColumn("rank", F.rank().over(w)).filter("rank = 1").selectExpr("android_id", "ios_id")
    return name2id.join(ios2and, "ios_id").select("android_id", "bundle_id")

# COMMAND ----------

def read_measure_protocol(mp_df: DataFrame, blacklist: List[str]) -> DataFrame:
    return (mp_df.selectExpr(
        "bundle_id as app",
        "domain as host",
        "hits",
        "domain_classification",
        "context_verification_type",
        "user_id as did",
        "first_time_stamp",
        "timestamp",
        "date",
        "country")
            .filter(
        "app IS NOT NULL AND host IS NOT NULL AND domain_classification <> '2.0' AND context_verification_type == '0.0'")
            .filter(~F.col("app").isin(blacklist))
            .drop("domain_classification", "context_verification_type"))

def filter_mp(mp: DataFrame, check_hosts: List[str], min_users: int) -> DataFrame:
    blacklist = (
        mp.filter(F.col("host").isin(check_hosts))
        .groupBy("app")
        .agg(F.count_distinct("host").alias("n"))
        .filter("n>=2 AND app NOT LIKE '%com.apple%'")
        .select("app"))
    whitelist = (
        mp.groupBy("app")
        .agg(F.count_distinct("did").alias("n_users"))
        .filter(f"n_users >= {min_users}")
        .select("app"))
    return (
        mp.join(blacklist, "app", "leftanti")
        .join(whitelist, "app"))

# COMMAND ----------

def DOW2str(colname: str) -> Column:
    expr = f"""
    CASE 
    WHEN {colname} = 'Sun' THEN 1 
    WHEN {colname} = 'Mon' THEN 2 
    WHEN {colname} = 'Tue' THEN 3 
    WHEN {colname} = 'Wed' THEN 4 
    WHEN {colname} = 'Thu' THEN 5 
    WHEN {colname} = 'Fri' THEN 6 
    ELSE 7 
    END"""
    return F.expr(expr)