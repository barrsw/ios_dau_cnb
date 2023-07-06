# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

import pandas as pd
from sklearn.model_selection import KFold, GridSearchCV, cross_val_predict, train_test_split
from sklearn.ensemble import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder, VectorIndexer, IndexToString

# COMMAND ----------

RAW_ESTIMATION_PATH = dbutils.widgets.get("raw_estimation_path")
MERGED_ESTIMATION_PATH = dbutils.widgets.get("merged_estimation_path")
MERGED_ESTIMATION_FOR_TRAIN_PATH = dbutils.widgets.get("merged_estimation_for_train_path")
COUNTRIES_IOS_POPULATION_PATH = dbutils.widgets.get("countries_ios_population_path")
OUTPUT_PATH = dbutils.widgets.get("output_path")
DATE = dbutils.widgets.get("date")
OUTPUT_PATH = dbutils.widgets.get("output_path")

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

raw_estimation = (SparkDataArtifact().read_dataframe(RAW_ESTIMATION_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS)
                  .select("country", "app", F.col("p").alias("raw_est_p"), "n",
                          (F.col("p") * F.col("n")).alias("k")))

joined_estimations_w_features = (SparkDataArtifact().read_dataframe(MERGED_ESTIMATION_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS))

joined_estimations_for_train_w_features = (SparkDataArtifact().read_dataframe(MERGED_ESTIMATION_FOR_TRAIN_PATH, spark.read.format("parquet"), debug=True, data_sources=ENVS))

labeled_train_data = (joined_estimations_for_train_w_features
                      .withColumn("label",
                                  make_labels("ga_active_users", "raw_est_active_users", "ios_share_active_users",
                                              "mp_active_users")))
modeled_data = apply_model(joined_estimations_w_features, labeled_train_data)

modeled_data_normalized = (modeled_data
                       .withColumn("raw_est_model_p", F.col('prob(raw_est,ios_share_est,mp_est)').getItem(0))
                       .withColumn("ios_share_est_model_p", F.col('prob(raw_est,ios_share_est,mp_est)').getItem(1))
                       .withColumn("mp_est_model_p", F.col('prob(raw_est,ios_share_est,mp_est)').getItem(2))
                       .withColumn('sum_prob',
                                   (F.col('raw_est_indicator') * F.col('raw_est_model_p')) +
                                   (F.col('ios_share_est_indicator') * F.col("ios_share_est_model_p")) +
                                   (F.col('mp_est_indicator') * F.col('mp_est_model_p')))
                       .withColumn('blended_active_users',
                                   ((F.col('raw_est_indicator') * F.col('raw_est_model_p') * F.col(
                                       'raw_est_active_users')) +
                                    (F.col('ios_share_est_indicator') * F.col('ios_share_est_model_p') * F.col(
                                        'ios_share_active_users')) +
                                    (F.col('mp_est_indicator') * F.col('mp_est_model_p') * F.col(
                                        'mp_active_users')))
                                   / F.col('sum_prob')))

population_size = SparkDataArtifact().read_dataframe(COUNTRIES_IOS_POPULATION_PATH, spark.read.format("parquet"), debug=True)

results = (modeled_data_normalized.join(population_size, 'country')
           .filter(F.datediff(F.lit(DATE).cast("date"), F.col("first_seen")) == (28 - 1))  # Remove app-country that we don't have raw_est on them because they came from ios_share/mp or new apps
           .filter(F.col("days_count") >= 1)
           .withColumn('scale_factor', ((F.col("blended_active_users") / F.col("population_size")) / (F.col("raw_est_active_users") / F.col("raw_est_adj"))))
           .join(raw_estimation, ["country", "app"], "left")
           .fillna(0, subset=['raw_est_p'])
           .withColumn("p", F.col("raw_est_p") * F.col("scale_factor"))
           .select("country", "app", "p", "n", "scale_factor", "raw_est_active_users", "raw_est_adj","ios_share_active_users", "ios_share_adj", "mp_active_users", "mp_adj",
                   "ga_active_users", "days_count","raw_est_model_p", "ios_share_est_model_p", "mp_est_model_p", "blended_active_users")
           .orderBy("country", "app"))

# COMMAND ----------

write_output(results.write.format("parquet"), OUTPUT_PATH, ENVS, OVERWRITE_MODE)