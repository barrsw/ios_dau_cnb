# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

import pandas as pd
from sklearn.model_selection import KFold, GridSearchCV, cross_val_predict, train_test_split
from sklearn.ensemble import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder, VectorIndexer, IndexToString

# COMMAND ----------

RAW_ESTIMATION_BASE_PATH = dbutils.widgets.get("raw_estimation_base_path")
IOS_SHARE_P_EST_BASE_PATH = dbutils.widgets.get("ios_share_p_est_base_path")
MP_SESSIONS_EVAL_PATH = dbutils.widgets.get("mp_sessions_eval_path")
MP_P_EST_PATH = dbutils.widgets.get("mp_p_est_path")
GA_BASE_PATH = dbutils.widgets.get("ga_base_path")
TOP_CHARTS_PATH = dbutils.widgets.get("top_charts_path")
APPS_METADATA_PATH = dbutils.widgets.get("apps_metadata_path")
COUNTRIES_IOS_POPULATION_PATH = dbutils.widgets.get("countries_ios_population_path")
MERGED_ESTIMATION_PATH = dbutils.widgets.get("merged_estimation_path")
MERGED_ESTIMATION_FOR_TRAIN_PATH = dbutils.widgets.get("merged_estimation_for_train_path")
DATE = dbutils.widgets.get("date")
COUNTRIES = [int(country) for country in dbutils.widgets.get("countries").split(",")]

OVERWRITE_MODE = dbutils.widgets.get("overwrite_mode") == 'True'
ENVS = generate_env_list(dbutils.widgets.get("envs"))

# COMMAND ----------

# Wrong bundle_ids in GA
fixed_bundle_ids = [['com.dutchbros.loyalty.ios', 'com.loyalty.dutchbros'],
                    ['com.classpass.Classpass', 'com.classpass.classpass'],
                    ['com.shakeshack.kioskapp', 'com.halickman.ShackHQ'],
                    ['com.flipfit', 'com.flipfit.flip'],
                    ['com.aisense.Otter', 'com.aisense.otter']]
#### VARS ####
MIN_USERS = 1
MIN_GA = 30000
FOLDS = 5
MIN_DAU = 10
MIN_GA_TRAIN = 15000
MIN_GA_EVAL = 30000

CROSS_VALIDATION_MODE = "CROSS_VALIDATION"
MODE = CROSS_VALIDATION_MODE
GA4_IOS_PLATFORM = 'iOS'
STORE_RANK_IPHONE_DEVICE = 'iphone'
TOP_SELLING_FREE = 'topselling_free'

#### FEATURES COLUMNS ####
numeric_cols = [
    'averageUserRating',
    'hasInAppPurchases',
    'userRatingCount',
    'rank',
    'true_n_users',
    'true_n_sessions',
    'n_users_err',
    'n_sessions_err',
    'raw_est_indicator',
    'ios_share_est_indicator',
    'mp_est_indicator',
    'avg_est',
    'raw_est_ratio',
    'ios_share_est_ratio',
    'mp_est_ratio']

categorical_cols = ['category', 'country', 'contentAdvisoryRating']
categorical_cols_idx = [col + "_idx" for col in categorical_cols]
features_cols = categorical_cols_idx + numeric_cols

#### MODEL VARS ####
MIN_SAMPLES_SPLIT = [2, 5, 10, 15, 20, 50]
MAX_SAMPLES = [None, 0.1, 0.2, 0.3, 0.5, 0.7, 0.9, 0.999]
MAX_DEPTH = [2, 3, 4, 5, 8, 10, 15, 20, 25]
rf_model = RandomForestClassifier(n_jobs=-1, random_state=1)
model_params = {"min_samples_split": MIN_SAMPLES_SPLIT, "max_samples": MAX_SAMPLES, "max_depth": MAX_DEPTH}
cv = GridSearchCV(rf_model, model_params, n_jobs=-1, scoring="neg_mean_absolute_error")

# COMMAND ----------

def create_store_features(store_ranks_path: str, apps_metadata_path: str,
                          countries: List[str]) -> DataFrame:
    store_ranks = (SparkDataArtifact().read_dataframe(store_ranks_path, spark.read.format("parquet"), debug=True)
                   .withColumn('country', F.col('country_numeric').cast('int'))
                   .filter(F.col('country').isin(countries))
                   .filter(f"mode == '{TOP_SELLING_FREE}' and device_type == '{STORE_RANK_IPHONE_DEVICE}'")
                   .select('id', 'country', 'category', 'rank'))

    apps_metadata = (SparkDataArtifact().read_dataframe(apps_metadata_path, spark.read.format("parquet"), debug=True)
                     .select(F.col("bundleId").alias("app"),
                             F.col("trackId").alias("id"),
                             "averageUserRating",
                             "contentAdvisoryRating",
                             F.col("hasInAppPurchases").cast("int").alias("hasInAppPurchases"),
                             F.col("primaryGenreId").alias("category"),
                             "userRatingCount"))

    countries_df = (spark.createDataFrame([[country] for country in countries], ["country"])
                    .withColumn('country', F.col('country').astype('int')))
    return (apps_metadata
            .join(countries_df)
            .join(store_ranks, ['id', 'country', 'category'], 'left')
            .fillna(-1, ['rank'])
            .select("country", "app", "rank", "averageUserRating", "contentAdvisoryRating", "hasInAppPurchases",
                    "category", "userRatingCount"))


def find_adj(df: DataFrame, ga: DataFrame, col_name: str, min_users: int = 0, min_ga: int = MIN_GA,
             join_by_date: bool = True):
    join_cols = ['country', 'date', 'app'] if join_by_date else ['country', 'app']
    if min_users == 0:
        df = df.withColumn('k', F.lit(0))
    return (ga
            .join(df, join_cols, "left")
            .fillna(0, subset=[col_name, 'k'])
            .groupBy("country", "app")
            .agg(F.mean('ga_active_users').alias('ga_active_users'),
                 F.mean(col_name).alias(col_name),
                 F.mean('k').alias('k')).filter(F.col(col_name) > 0)
            .filter(F.col('k') >= min_users)
            .filter(f'ga_active_users > {min_ga}')
            .withColumn("adj", F.col("ga_active_users") / F.col(col_name))
            .groupBy("country")
            .agg(F.percentile_approx("adj", 0.5).cast("int").alias("adj")))


def merge_features(df: DataFrame, store_features: DataFrame, mp_sessions_eval: DataFrame, min_dau):
    return (df
            .join(store_features, ['country', 'app'], 'left')
            .join(mp_sessions_eval, 'app', 'left')
            .withColumn('raw_est_indicator', F.when(F.col('raw_est_active_users') > min_dau, 1).otherwise(0))
            .withColumn('ios_share_est_indicator', F.when(F.col('ios_share_active_users') > min_dau, 1).otherwise(0))
            .withColumn('mp_est_indicator', F.when(F.col('mp_active_users') > min_dau, 1).otherwise(0))
            .withColumn('avg_est',
                        (F.col('raw_est_active_users') + F.col('ios_share_active_users') + F.col('mp_active_users')) / (
                                F.col('raw_est_indicator') + F.col('ios_share_est_indicator') + F.col(
                            'mp_est_indicator')))
            .withColumn('raw_est_ratio',
                        F.col('raw_est_active_users') / (F.col('ios_share_active_users') + F.col('mp_active_users')))
            .withColumn('ios_share_est_ratio',
                        F.col('ios_share_active_users') / (F.col('raw_est_active_users') + F.col('mp_active_users')))
            .withColumn('mp_est_ratio',
                        F.col('mp_active_users') / (F.col('ios_share_active_users') + F.col('raw_est_active_users'))))


# Picks the active_users that is closer to the GA
@F.udf(returnType=T.IntegerType())
def make_labels(ga_active_users, raw_est_active_users, ios_share_active_users, mp_active_users, min_dau=MIN_DAU):
    cand = []
    if raw_est_active_users > min_dau:
        cand.append((abs(ga_active_users - raw_est_active_users), 0))
    if ios_share_active_users > min_dau:
        cand.append((abs(ga_active_users - ios_share_active_users), 1))
    if mp_active_users > min_dau:
        cand.append((abs(ga_active_users - mp_active_users), 2))
    if len(cand) == 0:
        return None
    return min(cand)[1]


@F.pandas_udf(returnType=T.ArrayType(T.FloatType()))
def predict_pandas_udf(*features):
    x = pd.concat(features, axis=1).values
    y = cv.predict_proba(x)

    return pd.Series(pd.DataFrame(y).values.tolist())

def apply_model(data: DataFrame, train_data: DataFrame) -> DataFrame:
    indexers = StringIndexer(inputCols=categorical_cols, outputCols=categorical_cols_idx, handleInvalid="skip")
    indexers = indexers.fit(data)

    data_idx = (indexers.transform(data)
                .fillna(-1, features_cols))
    data_train_idx = (indexers.transform(train_data)
                      .fillna(-1, features_cols)
                      .filter((F.col("raw_est_active_users") > MIN_DAU) & (F.col("ga_active_users") > MIN_GA_TRAIN)))

    pdf_train = data_train_idx.toPandas()
    X = pdf_train[features_cols]
    Y = pdf_train.label
    cv.fit(X, Y)

    if MODE == CROSS_VALIDATION_MODE:
        pdf_train['prob'] = pd.DataFrame(cross_val_predict(RandomForestClassifier(random_state=1, **cv.best_params_), X, Y, n_jobs=-1,
                                                           method='predict_proba')).values.tolist()
        prob_train = spark.createDataFrame(pdf_train[['app', 'country', 'prob', 'ga_active_users']])
        data_with_prob = (data_idx.filter('raw_est_indicator == 1').join(prob_train, ['app', 'country'], 'left')
        .withColumn('prob(raw_est,ios_share_est,mp_est)',
                    predict_pandas_udf(*features_cols)).withColumn(
            'prob(raw_est,ios_share_est,mp_est)', F.coalesce('prob', 'prob(raw_est,ios_share_est,mp_est)')))

    else:
        data_with_prob = (data_idx
                          .withColumn('prob(raw_est,ios_share_est,mp_est)', predict_pandas_udf(*features_cols))
                          .withColumn("ga_active_users", F.lit(None).cast("double")) # Keep the same scheme as with CROSS_VALIDATION_MODE
                          )

    return data_with_prob

# COMMAND ----------

store_features = create_store_features(TOP_CHARTS_PATH, APPS_METADATA_PATH, COUNTRIES)

fixed_bundle_ids_df = spark.createDataFrame(fixed_bundle_ids, ['app', 'fixed_app'])

ga_df = (read_between_dates(start=DATE, end=DATE, path=GA_BASE_PATH, days_back=27)
         .withColumn("country", F.col("country").cast("int"))
         .filter(F.col("country").isin(COUNTRIES))
         .filter(f"platform = '{GA4_IOS_PLATFORM}'")
         .select("country", "date", F.col("entity").alias("app"), F.col("dau_final").alias("ga_active_users"))
         .filter(F.col("ga_active_users") > 0)
         .join(fixed_bundle_ids_df, 'app', 'left')
         .withColumn('app', F.coalesce('fixed_app', 'app')).drop('fixed_app'))

raw_estimation = (read_between_dates(start=DATE, end=DATE, path=RAW_ESTIMATION_BASE_PATH, days_back=27, envs=ENVS)
                  .select("date", "country", "app", F.col("p").alias("raw_est_p"), "n",
                          (F.col("p") * F.col("n")).alias("k")))

N_DAYS = raw_estimation.select("date").distinct().count()

ios_share = (read_between_dates(start=DATE, end=DATE, path=IOS_SHARE_P_EST_BASE_PATH, days_back=27, envs=ENVS)
             .select("date", "country", "app", F.col("p").alias("ios_share_p")))

mp_p_est = (SparkDataArtifact().read_dataframe(MP_P_EST_PATH, spark.read.format("parquet"), debug=True)
            .select("country", "app", F.col("p").alias("mp_p")))

mp_sessions_eval = (SparkDataArtifact().read_dataframe(MP_SESSIONS_EVAL_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS)
                    .select("app", "true_n_users", "true_n_sessions", "n_users_err", "n_sessions_err"))

raw_estimation_w_adj = (raw_estimation
                        .join(find_adj(raw_estimation, ga_df, "raw_est_p", min_users=MIN_USERS), "country")
                        .select("date", "country", "app",
                                (F.col("raw_est_p") * F.col("adj")).alias("raw_est_active_users"),
                                F.col("adj").alias("raw_est_adj")))

ios_share_w_adj = (ios_share
                   .join(find_adj(ios_share, ga_df, "ios_share_p"), "country")
                   .select("date", "country", "app",
                           (F.col("ios_share_p") * F.col("adj")).alias("ios_share_active_users"),
                           F.col("adj").alias("ios_share_adj")))

mp_est_w_adj = (mp_p_est
                .join(find_adj(mp_p_est, ga_df, "mp_p", join_by_date=False), "country")
                .select("country", "app",
                        (F.col("mp_p") * F.col("adj")).alias("mp_active_users"),
                        F.col("adj").alias("mp_adj")))

raw_estimation_avg_active_users = (raw_estimation_w_adj
                                   .groupBy('country', 'app')
                                   .agg(F.max('raw_est_adj').alias('raw_est_adj'),
                                        F.count("date").alias("days_count"),
                                        F.min('date').alias('first_seen'),
                                        (F.sum('raw_est_active_users') / N_DAYS).alias('raw_est_active_users')))

ios_share_avg_active_users = (ios_share_w_adj
                              .groupBy('country', 'app')
                              .agg(F.max('ios_share_adj').alias('ios_share_adj'),
                                   (F.sum('ios_share_active_users') / N_DAYS).alias('ios_share_active_users')))

joined_estimations = (raw_estimation_avg_active_users
                      .join(ios_share_avg_active_users, ["country", "app"], "outer")
                      .join(mp_est_w_adj, ["country", "app"], "outer")
                      .fillna(0, subset=["raw_est_active_users", "ios_share_active_users", "mp_active_users"])
                      .select("country", "app", "raw_est_active_users", "raw_est_adj",
                              "ios_share_active_users", "ios_share_adj", "mp_active_users", "mp_adj", "days_count", "first_seen"))

joined_estimations_for_train = (ga_df
                                .join(raw_estimation_w_adj, ['country', 'app', 'date'], 'left')
                                .join(ios_share_w_adj, ['country', 'app', 'date'], 'left')
                                .fillna(0, ['raw_est_active_users', 'ios_share_active_users'])
                                .groupBy('country', 'app')
                                .agg(F.mean('ga_active_users').alias('ga_active_users'),
                                     F.mean('raw_est_active_users').alias('raw_est_active_users'),
                                     F.mean('ios_share_active_users').alias('ios_share_active_users'),
                                     F.max('raw_est_adj').alias('raw_est_adj'),
                                     F.max('ios_share_adj').alias('ios_share_adj'))
                                .join(mp_est_w_adj.select('country', 'app', 'mp_active_users', 'mp_adj'),
                                      ['country', 'app'],
                                      'left')
                                .fillna(0, subset=['mp_active_users'])
                                .select("country", "app", "ga_active_users", "raw_est_active_users", "raw_est_adj",
                                        "ios_share_active_users", "ios_share_adj", "mp_active_users", "mp_adj"))

joined_estimations_w_features = merge_features(joined_estimations, store_features, mp_sessions_eval, MIN_DAU).orderBy("country", "app")
joined_estimations_for_train_w_features = merge_features(joined_estimations_for_train, store_features,
                                                         mp_sessions_eval, MIN_DAU).orderBy("country", "app")

# COMMAND ----------

write_output(joined_estimations_w_features.write.format("parquet"), MERGED_ESTIMATION_PATH, ENVS, OVERWRITE_MODE)

# COMMAND ----------

write_output(joined_estimations_for_train_w_features.write.format("parquet"), MERGED_ESTIMATION_FOR_TRAIN_PATH, ENVS, OVERWRITE_MODE)