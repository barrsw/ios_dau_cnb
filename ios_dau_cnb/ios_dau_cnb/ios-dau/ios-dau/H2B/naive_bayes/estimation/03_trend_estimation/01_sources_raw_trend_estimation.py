# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

SOURCE_RAW_EST_BASE_PATH = dbutils.widgets.get("sources_raw_estimation_base_path")
DATE = datetime.strptime(dbutils.widgets.get("date"), "%Y-%m-%d").date()
ANCHORS_DATES = dbutils.widgets.get("anchors_dates").split(',')
DAY2DAY_ANCHOR_DATES = dbutils.widgets.get("day2day_anchors_dates").split(',')
AVG_SCALE_ANCHORS_DATES_STR = dbutils.widgets.get("avg_scale_anchors_dates")
AVG_SCALE_ANCHORS_DATES = AVG_SCALE_ANCHORS_DATES_STR.split(',') if AVG_SCALE_ANCHORS_DATES_STR != 'EMPTY_STRING' else []
OUTPUT_PATH = dbutils.widgets.get("output_path")
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'
ENVS = generate_env_list(dbutils.widgets.get("envs"))

# COMMAND ----------

key_cols = ["country", "app", "source", "model_type", "model_date"]

# COMMAND ----------

subject_df = SparkDataArtifact().read_dataframe(SOURCE_RAW_EST_BASE_PATH + add_year_month_day(DATE), spark.read.format("parquet"), debug=True, data_sources=ENVS) \
              .select(*key_cols, "p", "n", "reliability")

anchor_df = (read_between_dates(start=ANCHORS_DATES[0], end=ANCHORS_DATES[-1], path=SOURCE_RAW_EST_BASE_PATH, envs=ENVS)
             .select(F.col("date").alias("anchor_date"), *key_cols, F.col("p").alias("anchor_p"),
                     F.col("n").alias("anchor_n"), F.col("reliability").alias("anchor_reliability")))

# fill zero p when missing in date
anchor_n_df = anchor_df.select("anchor_date", *[c for c in key_cols if c != "app"], 'anchor_n').distinct()

all_dates = anchor_df.select("anchor_date").distinct()
anchor_df = anchor_df.select(key_cols).unionByName(subject_df.select(key_cols)).distinct() \
    .crossJoin(all_dates) \
    .join(anchor_df.drop("anchor_n"), [*key_cols, "anchor_date"], "left") \
    .na.fill(value=0, subset=["anchor_p"])

# Fill zero anchor-p / subject-p when one of them is missing
subject_n_df = subject_df.select(*[c for c in key_cols if c != "app"], 'n').distinct()

est_df = subject_df.drop("n") \
    .join(anchor_df, on=key_cols, how="full") \
    .join(subject_n_df, [c for c in key_cols if c != "app"]) \
    .join(anchor_n_df, [*[c for c in key_cols if c != "app"], 'anchor_date']) \
    .na.fill(value=0, subset=["anchor_p", "p"])

# day-to-day trends
trends_est_df = est_df \
    .filter(F.col("anchor_date").isin(*DAY2DAY_ANCHOR_DATES)) \
    .withColumn("trend_id", F.struct(F.lit("DAY").alias("type"),
                                     F.col("anchor_date").cast("string").alias("anchor_dates"))) \
    .drop("anchor_date")

# avg-scale-trend
for avg_scale_anchor_dates in AVG_SCALE_ANCHORS_DATES:
    avg_scale_trends_df = est_df \
        .filter(F.col("anchor_date").isin(*avg_scale_anchor_dates.split(","))) \
        .groupBy(key_cols) \
        .agg(F.avg("p").alias("p"),
             F.avg("reliability").alias("reliability"),
             F.ceil(F.avg("n")).alias("n"),
             F.avg("anchor_p").alias("anchor_p"),
             F.ceil(F.avg("anchor_n")).alias("anchor_n"),
             F.avg("anchor_reliability").alias("anchor_reliability"),
             F.count("anchor_date").alias("n_days")) \
        .filter(F.col("n_days") == len(avg_scale_anchor_dates.split(","))).drop("n_days") \
        .withColumn("trend_id", F.struct(F.lit("AVG_SCALE").alias("type"),
                                         F.lit(avg_scale_anchor_dates).alias("anchor_dates")))

    trends_est_df = trends_est_df.unionByName(avg_scale_trends_df)

result = trends_est_df \
        .filter((F.col('p') > 0) | (F.col("anchor_p") > 0)) \
        .withColumn("trend", F.col("p") / F.col("anchor_p")) \
        .withColumn("reliability", F.least(F.col("reliability"), F.col("anchor_reliability")))\
        .select(*key_cols, "trend_id", "p", "n", "anchor_p", "anchor_n", "trend", "reliability") \
        .orderBy("country", "app")

# COMMAND ----------

write_output(result.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)