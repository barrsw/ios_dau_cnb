# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

# MAGIC %run Repos/apps/ios-dau/utils/common_utils

# COMMAND ----------

INPUT_PATH = dbutils.widgets.get("input_path")
MIN_USER_REPORTING_DAYS = float(dbutils.widgets.get("min_user_reporting_days"))
DATES = dbutils.widgets.get("dates").split(",")
OUTPUT_PATH = dbutils.widgets.get("output_path")
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'
ENVS = generate_env_list(dbutils.widgets.get("envs"))
PROD_ENVS = generate_prod_env()

# COMMAND ----------

inputs_df = read_between_dates(DATES[0], DATES[1], INPUT_PATH, envs=PROD_ENVS) \
    .select("date", "source", "country", "app", "did", "score", "model_date", "model_type", "is_main_model").distinct()

# take the latest model_date
app_latest_model_date_df = inputs_df \
    .groupBy("date", "source", "country", "app", "did", "model_type") \
    .agg(F.max("model_date").alias("app_latest_model_date"))

inputs_df_latest_model = inputs_df \
    .join(app_latest_model_date_df, ["date", "source", "country", "app", "did", "model_type"], "left") \
    .withColumn("default_latest_model_date", F.max("model_date").over(Window.partitionBy("date", "country", "app", "source", "model_type"))) \
    .withColumn("latest_model_date", F.coalesce("app_latest_model_date", "default_latest_model_date")) \
    .filter(col("model_date") == col("latest_model_date")) \
    .select("date", "source", "country", "app", "did", "score")

# take only users that were active a reasonable period of time that may represent the "entire" period
period_total_n_days_df = inputs_df_latest_model.select("date").distinct()
user_total_active_days_df = inputs_df_latest_model.select("date", "source", "country", "did").distinct() \
    .groupBy("source", "country", "did") \
    .agg(F.count("date").alias("user_total_n_reporting_days"))

# take only users that were active a reasonable period of time that may represent the "entire" period
user_total_active_days_df = user_total_active_days_df\
    .crossJoin(period_total_n_days_df.selectExpr("COUNT(date) as period_total_n_days"))\
    .filter(col("user_total_n_reporting_days") >= MIN_USER_REPORTING_DAYS)

filtered_inputs_df = inputs_df_latest_model\
    .join(user_total_active_days_df, ["source", "country", "did"])

user_app_score = (filtered_inputs_df
                    .groupBy("date", "did", "country", "source", "app")
                    .agg(F.max("score").alias("score")))  # take max score per app user over the day

user_app_score_unique = (user_app_score
                            .join(user_total_active_days_df, ["source", "country", "did"]))

# Evaluate PAU-P
pau_k_df = user_app_score_unique \
    .groupBy('source', 'country', 'app', 'did') \
    .agg(F.max("score").alias("score")) \
    .groupBy('source', 'country', 'app') \
    .agg(F.sum("score").alias("k"))

pau_n_df = user_app_score_unique \
    .groupBy('source', 'country') \
    .agg(F.countDistinct("did").alias("n"))

pau_df = pau_k_df.join(pau_n_df, ['source', 'country'])\
    .withColumn("p", col("k") / col("n")).drop("k")

# Calculate AVG-DAU

# For users that have missing days,
# evaluate the probability to open each app (by their exiting days)
# P = (user-seen-app-days) / (user-total-existing-days)
# and fill their missing days with this probability
user_app_p_df = user_app_score_unique \
    .filter(col("user_total_n_reporting_days") != col("period_total_n_days")) \
    .groupBy('source', 'country', 'did', 'app') \
    .agg(F.sum("score").alias("sum_score_pau")) \
    .join(user_total_active_days_df, ['source', 'country', 'did']) \
    .withColumn("engagement_p", col("sum_score_pau") / col("user_total_n_reporting_days"))

users_across_dates_df = user_app_score_unique.select("date", "country", "did", "source").distinct().withColumn("is_panel", F.lit(True))
daily_dau_k_df = user_app_score_unique \
    .join(user_app_p_df.crossJoin(period_total_n_days_df), ['date', 'source', 'country', 'app', 'did'], "full") \
    .join(users_across_dates_df, ["date", "country", "did", "source"], "full") \
    .na.fill(value=0, subset=["score"]) \
    .na.fill(value=False, subset=["is_panel"]) \
    .withColumn("engagement", F.when(col("is_panel"), col("score")).otherwise(col("engagement_p"))) \
    .groupBy('date', 'source', 'app', 'country') \
    .agg(F.sum("engagement").alias("k"))

# use PAU-N - we assume that all users are reporting the entire period
# Notice - we filled missing users' reports with User-App 'engagement_p'
daily_dau_df = daily_dau_k_df \
    .join(pau_n_df.select('source', 'country', 'n'),  ['source', 'country']) \
    .withColumn("p", col("k") / col("n"))

# fill zero when no occurrences in the date
# Although it's a bit heavy,
# Notice, I'm not using the SUM/N_Days as AVG, I do fill the "missing" dates with their panel-size (n)
# to allow weighting by the panel-size on each day (try to avoid "bad" days)
daily_dau_df = daily_dau_df \
    .select("source", "country", "app").distinct() \
    .crossJoin(period_total_n_days_df) \
    .join(daily_dau_df, ["date", "source", "country", "app"], "left") \
    .na.fill(value=0, subset=["p", "k"])

# use the actual (raw) number of users in the panel for AVG-DAU - weighted-avg (relay more on days with more data)
daily_dau_raw_n_df = user_app_score_unique \
    .groupBy('date', 'source', 'country') \
    .agg(F.countDistinct("did").alias("raw_daily_n"))

avg_dau_df = daily_dau_df \
    .join(daily_dau_raw_n_df, ['date', 'source', 'country'])\
    .groupBy('source', 'country', "app") \
    .agg(w_avg("p", "raw_daily_n").alias("avg_dau_p"),
            F.max("p").alias("max_dau_p"))

results = pau_df \
    .join(avg_dau_df, ['source', 'country', 'app'])\
    .select("source", "country", "app", "p", "avg_dau_p", "max_dau_p", "n") \
    .orderBy("country", "app")

# COMMAND ----------

write_output(results.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)