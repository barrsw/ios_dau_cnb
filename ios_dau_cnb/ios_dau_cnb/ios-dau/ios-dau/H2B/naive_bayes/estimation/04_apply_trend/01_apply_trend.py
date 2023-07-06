# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

# MAGIC %run Repos/apps/ios-dau/utils/common_utils

# COMMAND ----------

RAW_TREND_ESTIMATION_WITH_PRIOR_BASE_PATH = dbutils.widgets.get("raw_trend_estimation_with_prior_base_path")
RAW_ESTIMATION_BASE_PATH = dbutils.widgets.get("raw_estimation_base_path")
DAU_P_ESTIMATION_BASE_PATH = dbutils.widgets.get("dau_p_estimation_base_path")
DATE_STR = dbutils.widgets.get("date")
ANCHORS_DATES_STR = dbutils.widgets.get("anchors_dates")
ANCHORS_DATES = ANCHORS_DATES_STR.split(',') if ANCHORS_DATES_STR != 'EMPTY_STRING' else []
TREND_DATES_STR = dbutils.widgets.get("trend_dates")
TREND_DATES = TREND_DATES_STR.split(',') if TREND_DATES_STR != 'EMPTY_STRING' else []
INIT = dbutils.widgets.get("init") == 'True'
OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'
ENVS = generate_env_list(dbutils.widgets.get("envs"))

# COMMAND ----------

if INIT:
    # First day, we can't have trends, yield the estimation from the panel
    final_p_by_trends = SparkDataArtifact().read_dataframe(RAW_ESTIMATION_BASE_PATH + add_year_month_day(DATE_STR), spark.read.format("parquet"), debug=True, data_sources=ENVS) \
        .select("country", "app", "p", "n", F.col("p").alias("raw_p"), F.col("p").alias("eff_p"),
                F.lit(None).alias("anchors").cast("array<struct<trend_id:struct<type:string,anchor_dates:string>,trend:double,p:double,w:double>>"))
else:
    # eff_p represent the actual p that produced the trend,
    # we keep it as an indicator for the confidence of the estimation
    trends_estimations_df = read_between_dates(start=TREND_DATES[0], end=TREND_DATES[-1], path=RAW_TREND_ESTIMATION_WITH_PRIOR_BASE_PATH, envs=ENVS) \
        .select("date", "trend_id", "country", "app", "trend",
                F.col("p").alias("trend_eff_p"), F.col("anchor_p").alias("trend_eff_anchor_p"),
                F.col("n").alias("trend_eff_n"), F.col("anchor_n").alias("trend_eff_anchor_n"))

    raw_estimations_df = read_between_dates(start=TREND_DATES[0], end=TREND_DATES[-1], path=RAW_ESTIMATION_BASE_PATH, envs=ENVS) \
        .select("date", "country", "app", F.col("p").alias("raw_p"), F.col("n").alias("raw_n"))

    anchors_estimations_df = read_between_dates(start=ANCHORS_DATES[0], end=ANCHORS_DATES[-1], path=DAU_P_ESTIMATION_BASE_PATH, envs=ENVS) \
        .withColumnRenamed("date", "anchor_date") \
        .select("anchor_date", "country", "app", F.col("p").alias("anchor_p"), F.col("n").alias("anchor_n"), F.col("eff_p").alias("anchor_eff_p"))

    # today's estimation depends on past estimation/s (feedback loop)
    # we must have All the past estimations ready before we can calculate the current subject estimation
    # we can handle missing estimations (between the anchors and today) by calculating each missing day
    # before we start today's calculation
    # example:
    # subject_i = t_i * avg(i-1, i-2, i-3)
    # For evaluation the subject of (day 0) we must have its anchors (days -3,-2,-1)
    # and the trends leading from those anchors to it
    # when we have only the days -6,-5,-4 (and the needed trends) we can evaluate each of missing days iteratively
    # (one by one)
    #        |    -6     |    -5     |    -4     |    -3     |    -2     |    -1     |     0     |
    # init:  | anchor1/3 | anchor2/3 | anchor3/3 | missing1  | missing2  | missing3  |  Subject  |
    # iter1: | anchor1/3 | anchor2/3 | anchor3/3 |     X     |   ----    |   ----    |  Subject  |
    # iter2: |   ----    | anchor1/3 | anchor2/3 | anchor3/3 |     X     |   ----    |  Subject  |
    # iter3: |   ----    |   ----    | anchor1/3 | anchor2/3 | anchor3/3 |     X     |  Subject  |
    # iter4: |   ----    |   ----    |   ----    | anchor1/3 | anchor2/3 | anchor3/3 |  Subject  |
    missing_exec_dates_to_calc = sorted([x["date"] for x in trends_estimations_df.select("date").distinct().collect()])
    print(missing_exec_dates_to_calc)
    for i, exec_date in enumerate(missing_exec_dates_to_calc):
        print(f"working on {'a missing' if len(missing_exec_dates_to_calc) == i+1 else 'the subject'} date {exec_date}")
        # take the trends leading from the anchors to the exec_date
        trend_estimation_df = trends_estimations_df \
            .filter(F.col("date") == exec_date)

        # get the day-to-day anchors and their mathing trends (leading to the exec_date)
        d2d_ests = trend_estimation_df \
            .filter(F.col("trend_id.type") == "DAY") \
            .withColumn("anchor_date", F.col("trend_id.anchor_dates").cast("date")) \
            .join(anchors_estimations_df, ["country", "app", "anchor_date"]) \
            .select("date", "country", "app", "trend_id", "trend", "anchor_p", "anchor_n",
                        "trend_eff_p", "trend_eff_anchor_p", "trend_eff_n", "trend_eff_anchor_n",
                        "anchor_eff_p")

        # get the avg-scale anchors and their mathing trends (leading to the exec_date)
        # evaluate the anchors by the AVG of the anchors' days
        avg_scale_ests = trend_estimation_df \
            .filter(F.col("trend_id.type") == "AVG_SCALE") \
            .withColumn("n_days", F.size(F.split("trend_id.anchor_dates", ","))) \
            .withColumn("anchor_date",  F.explode(F.split("trend_id.anchor_dates", ",").cast("array<date>"))) \
            .join(anchors_estimations_df, ["country", "app", "anchor_date"]) \
            .groupBy("date", "country", "app", "trend", "trend_id",  "trend_eff_p", "trend_eff_anchor_p", "trend_eff_n", "trend_eff_anchor_n", "n_days")\
            .agg((F.sum("anchor_p")/F.col("n_days")).alias("anchor_p"),
                 F.avg("anchor_n").alias("anchor_n"),
                 (F.sum("anchor_eff_p")/F.col("n_days")).alias("anchor_eff_p"),
                 F.count("anchor_date").alias("_n_days")) \
            .select("date", "country", "app", "trend_id", "trend", "anchor_p", "anchor_n",
                        "trend_eff_p", "trend_eff_anchor_p", "trend_eff_n", "trend_eff_anchor_n",
                        "anchor_eff_p")

        # evaluate each p by applying the trend on its anchor
        # here we could have many estimations (could be different) for the p,
        # evaluated from different anchors with different trends
        ps_by_trends_df = d2d_ests.unionByName(avg_scale_ests) \
            .filter(F.col("anchor_p") > 0)\
            .withColumn("p", F.col("anchor_p") * F.col("trend"))

        # we can have one or more trends leading to the same subject.
        # by having multiple trends (from different anchors) leading to the same subject (exec_date)
        # we could have different estimations for the subject (with different confidence)
        final_p_by_trends = ps_by_trends_df \
            .withColumn("w", F.sqrt((F.col("anchor_eff_p") * F.col("anchor_n")) * (F.col("trend_eff_anchor_p") * F.col("trend_eff_anchor_n")))) \
            .withColumn("w", F.col("w") / F.sum("w").over(Window.partitionBy("date", 'country', 'app'))) \
            .groupBy("date", "country", "app") \
            .agg(w_avg("p", "w").alias("p"),
                 w_avg("trend_eff_p", "w").alias("trend_eff_p"),
                 F.ceil(w_avg("trend_eff_n", "w")).alias("trend_eff_n"),
                 w_avg("trend_eff_anchor_p", "w").alias("trend_eff_anchor_p"),
                 F.ceil(w_avg("trend_eff_anchor_n", "w")).alias("trend_eff_anchor_n"),
                 F.sort_array(F.collect_list(F.struct("trend_id", "trend", "p", "w"))).alias("anchors")) \
            .filter(F.round(F.col("trend_eff_anchor_p") * F.col("trend_eff_anchor_n")) > 0) \
            .select("date", "country", "app", "p", F.col("trend_eff_n").alias("n"), F.col("trend_eff_p").alias("eff_p"), "anchors")

        # when we don't have trends (the app didn't appear in any of the anchors)
        # yield the estimation from the panel.
        # reset the estimation as it appears in the panel,
        # we can't match it with its past without at least one trend vector.
        final_p_by_trends = final_p_by_trends \
            .join(raw_estimations_df.filter(F.col("date") == exec_date), ["date", "country", "app"], "full") \
            .withColumn("p", F.coalesce(F.col("p"), F.col("raw_p"))) \
            .withColumn("n", F.ceil(F.coalesce(F.col("n"), F.col("raw_n")))) \
            .withColumn("eff_p", F.coalesce(F.col("eff_p"), F.col("raw_p")))

        # push the calculated estimation to the anchors, for calculating the next estimation in the pipe
        anchors_estimations_df = final_p_by_trends\
                .select(F.col("date").alias("anchor_date"), "country", "app", F.col("p").alias("anchor_p"), F.col("n").alias("anchor_n"), F.col("eff_p").alias("anchor_eff_p"))\
                .unionByName(anchors_estimations_df)

        if i % 4 == 3:
            # When there are many days to fill between the anchors and the subject
            # the execution plan may grow too large - checkpoint will truncate the logical plan
            anchors_estimations_df = anchors_estimations_df.localCheckpoint()

# COMMAND ----------

final_p_by_trends = (final_p_by_trends
                     .select("country", "app", "p", "n", "raw_p", "eff_p", "anchors")
                     .orderBy("country", "app"))

# COMMAND ----------

write_output(final_p_by_trends.write.format("parquet"), DAU_P_ESTIMATION_BASE_PATH + add_year_month_day(DATE_STR), ENVS, OVERWRITE_MODE)