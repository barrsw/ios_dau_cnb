# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

# MAGIC %run Repos/apps/ios-dau/utils/common_utils

# COMMAND ----------

OVERWITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'
ENVS = generate_env_list(dbutils.widgets.get("envs"))
DATE = dbutils.widgets.get("date")
MP_WEEKS = int(dbutils.widgets.get("mp_weeks"))

##-----JOB INPUT-----##
MP_PATHS = get_paths_for_mp(dbutils.widgets.get("mp_path"), DATE, MP_WEEKS)

##-----JOB OUTPUT-----##
OUTPUT_PATH = dbutils.widgets.get("output_path")

##-----JOB PARAMS-----##
MIN_USERS = int(dbutils.widgets.get("min_users"))
MP_BLACKLIST = dbutils.widgets.get("mp_bundle_blacklist").split(",")
CHECK_HOSTS = dbutils.widgets.get("check_hosts").split(",")

FIRST_TS = 'first_ts'
LAST_TS = 'last_ts'

# COMMAND ----------

def preprocess_timestamp(mp: DataFrame) -> DataFrame:
    # Cut local TZ
    df = mp.withColumn("first_time_stamp", F.substring("first_time_stamp", 0, 23))
    df = df.withColumn("timestamp", F.substring("timestamp", 0, 23))

    # Cast original timestamps
    df = df.withColumn("first_time_stamp", F.col("first_time_stamp").cast("timestamp"))
    df = df.withColumn("timestamp", F.col("timestamp").cast("timestamp"))

    # Rename timestamp columns
    df = df.withColumnRenamed('first_time_stamp', FIRST_TS)
    df = df.withColumnRenamed('timestamp', LAST_TS)

    return df


def user_life_interval(mp: DataFrame, day: str) -> DataFrame:
    if day == FIRST_TS:
        # Find the first & last intervals (24 hours window), last seen timestamp
        user_life_first = mp.groupBy('date', 'did', 'country').agg(F.expr(f'MIN({day}) as start_interval'), F.expr(
            f'MIN({day}) + INTERVAL 24 HOURS as end_interval'),
                                                                        F.expr(f'MAX({LAST_TS}) as last_seen'))

        # Filter users with activity duration >= 24 hours
        user_life_first_filtered = user_life_first.filter('last_seen >= end_interval')
        return user_life_first_filtered

    else:
        # Find the first & last intervals (24 hours window), first seen timestamp
        user_life_last = mp.groupBy('date', 'did', 'country').agg(F.expr(f'MAX({day}) as end_interval'), F.expr(
            f'MAX({day}) - INTERVAL 24 HOURS as start_interval'),
                                                                       F.expr(f'MIN({FIRST_TS}) as first_seen'))

        # Filter users with activity duration >= 24 hours
        user_life_last_filtered = user_life_last.filter('first_seen <= start_interval')
        return user_life_last_filtered


def clean_overlap_last(user_life_last: DataFrame, user_life_first: DataFrame) -> DataFrame:
    conds = [user_life_last.date == user_life_first.date, user_life_last.did == user_life_first.did,
             user_life_last.country == user_life_first.country,
             user_life_last.start_interval > user_life_first.end_interval]

    return user_life_last.join(user_life_first, conds, 'leftsemi')


def full_events(mp: DataFrame, user_life: DataFrame, day: str) -> DataFrame:
    conds = [user_life.date == mp.date, user_life.did == mp.did, user_life.country == mp.country,
             user_life.end_interval >= mp.first_ts]
    if day == LAST_TS:
        conds = [user_life.date == mp.date, user_life.did == mp.did, user_life.country == mp.country,
                 user_life.start_interval <= mp.last_ts]

    events = mp.join(user_life, on=conds).drop(user_life.date).drop(user_life.did).drop(user_life.country)
    return events.groupBy('app', 'date', 'country').agg(F.count_distinct('did').alias('activities'))


def dau_raw(user_life: DataFrame, events_df: DataFrame, day: str):
    active_users = user_life.groupBy('date', 'country').agg(F.count_distinct('did').alias('active_users'))
    events_active_users = events_df.join(active_users, ['date', 'country'])
    dau_t = events_active_users.withColumn('dau', F.col('activities') / F.col('active_users'))
    return dau_t.withColumn('day_type', F.lit(day))


def dau_weighted(merged_dau: DataFrame) -> DataFrame:
    # Fill missing bundle data with zeros
    dates = merged_dau.select("date", "day_type").distinct()
    app_country = merged_dau.select("app", "country").distinct()
    dates_by_app_country = app_country.join(dates)
    final_dau_full = dates_by_app_country.join(merged_dau, ["app", "date", "day_type", "country"],
                                               "left").fillna(0.0)

    # Calculate avg dau
    weighted_dau = final_dau_full.groupBy("app", "country").agg(F.avg("dau").alias("p"))

    return weighted_dau


# COMMAND ----------

print("Measure Protocol paths:" + str(MP_PATHS))

# COMMAND ----------

mp_df = read_measure_protocol(read_and_union(MP_PATHS), MP_BLACKLIST)
mp_filtered = filter_mp(mp_df, CHECK_HOSTS, MIN_USERS)
mp = preprocess_timestamp(mp_filtered)

user_life_first = user_life_interval(mp, FIRST_TS)
user_life_last = user_life_interval(mp, LAST_TS)
user_life_last = clean_overlap_last(user_life_last, user_life_first)
first_events = full_events(mp, user_life_first, FIRST_TS)
last_events = full_events(mp, user_life_last, LAST_TS)

first_dau = dau_raw(user_life_first, first_events, FIRST_TS)
last_dau = dau_raw(user_life_last, last_events, LAST_TS)
merged_dau = first_dau.union(last_dau)
final_dau = dau_weighted(merged_dau).orderBy("country", "app")

# COMMAND ----------

write_output(final_dau.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWITE_MODE)