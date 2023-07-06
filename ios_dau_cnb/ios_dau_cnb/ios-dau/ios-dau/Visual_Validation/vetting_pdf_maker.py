# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

import re
from pyspark.sql.functions import col
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
import pycountry
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.ticker as ticker
import matplotlib.patches as patches
from matplotlib.offsetbox import (TextArea, DrawingArea, OffsetImage,
                                  AnnotationBbox)
from matplotlib.dates import SUNDAY, MonthLocator, WeekdayLocator, DateFormatter

ymd_to_date = lambda : F.concat(F.lit("20"), col("year"), F.lit("-"), col("month"), F.lit("-"), col("day")).cast("date")
w_avg = lambda v, w: (F.sum(F.expr(v) * F.expr(w)) / F.sum(F.when(F.expr(v).isNotNull(), F.expr(w)))).alias(v)

# COMMAND ----------

START_DATE = date(2022, 9, 3)
END_DATE = date(2023, 5, 8)

VERSION = "dau_ios_v5"
BASE_PATH = f"s3a://sw-apps-core-data-buffer/user/ios_dau_phoenix/pdfs/{VERSION}"
COMMON_PATH = f"s3a://sw-apps-core-data-buffer/user/ios_dau_phoenix/pdfs/dau_ios/common"

COUNTRIES_LIST = [840, 826]

# COMMAND ----------

# MAGIC %md
# MAGIC #APPS SELECTION

# COMMAND ----------

TOP_APPS_LIST = [
    "com.tinyspeck.chatlyio",
    "com.google.Gmail",
    "com.amazon.Amazon",
    "com.stopandshop.mobile",
    "com.brainly.us",
    "com.mattel163.uno",
    "net.manta.comic",
    "com.sezzle.sezzlemobile",
    "com.adoreme.qmobile",
    "com.ebay.iphone",
    "com.walmart.electronics",
    "com.yourcompany.PPClient",
]

# COMMAND ----------

VIP_APPS = spark.read.parquet("s3://sw-apps-core-data-buffer/utils/top_queried_android_apps_by_customers_matched_to_ios")

# COMMAND ----------

# select panel's app by buckets
BLACKLIST_APPS = ["PLACE_HOLDER"] # when BLACKLIST_APPS the join on it filters all apps
MAX_BUCKET_APPS = 10
TOP_ACTIVE_USERS = 100

metric_apps_df = spark.read.parquet("s3a://sw-apps-core-data-buffer/similargroup/data/ios-analytics/metrics/dau/estimation")\
    .filter(col('country').isin(COUNTRIES_LIST)) \
    .withColumn("date", ymd_to_date())\
    .filter((col("date") == START_DATE) | (col("date") == END_DATE)  | (col("date") == '2023-03-01'))\
    .filter(F.lower(col("app")).rlike("|".join(BLACKLIST_APPS)) == False)\
    .filter(~col("app").isin(*TOP_APPS_LIST))\
    .groupBy("country", "app").agg(F.avg("active_users").alias("active_users"))\
    .withColumn("bucket_size", F.round(F.log10("active_users")))

top_active_users_apps = metric_apps_df\
    .withColumn("rank", F.row_number().over(Window.partitionBy("country").orderBy("active_users")))\
    .filter(col("rank") <= TOP_ACTIVE_USERS)\
    .select("country", "app")

apps_by_buckets = metric_apps_df \
    .withColumn("rank", F.row_number().over(Window.partitionBy("country", "bucket_size").orderBy(F.randn(seed=0))))\
    .filter(col("rank") <= MAX_BUCKET_APPS)\
    .select("country", "app")

metric_apps_df = top_active_users_apps.unionByName(apps_by_buckets).distinct()
    
metric_apps_df.orderBy("country").write.mode("overwrite").parquet(f"{COMMON_PATH}/metric_apps_df")
metric_apps_df = spark.read.parquet(f"{COMMON_PATH}/metric_apps_df")

# COMMAND ----------

# GA signals
MAX_GA_APPS = 200
PLATFORM = "iOS"


ga_apps_df = (read_between_dates(str(START_DATE), str(END_DATE), "similargroup/data/store-analytics/general/ga4_extractor/processed_ls")
    .filter(col('country').isin(COUNTRIES_LIST))
    .filter(col("platform") == PLATFORM)
    .filter((col("date") == START_DATE) | (col("date") == END_DATE)  | (col("date") == '2023-02-01'))
    .select("date", "country", col("entity").alias("app"), col("dau_ga").alias("active_users"))
    .filter(~col("app").isin(*TOP_APPS_LIST))
    .filter("active_users >= 0")
    .groupBy("country", "app")
    .agg(F.count("date").alias("n_days"), 
         F.avg("active_users").alias("avg_active_users"))
    .withColumn("rank", F.row_number().over(Window.partitionBy("country").orderBy(F.desc("avg_active_users"))))
    .filter(col("rank") <= MAX_GA_APPS))



ga_apps_df.orderBy("country").write.mode("overwrite").parquet(f"{COMMON_PATH}/ga_apps_df")
ga_apps_df = spark.read.parquet(f"{COMMON_PATH}/ga_apps_df")

# COMMAND ----------

#By Store Rank
TOP_N_RANKS = 7

store_ranks_df = (SparkDataArtifact().read_dataframe('/similargroup/data/store-analytics/iOS-app-store/top-charts/' + add_year_month_day(END_DATE), spark.read.format("parquet"), debug=True)
            .filter(col('country_numeric').isin(COUNTRIES_LIST))\
            .filter('mode == "topselling_free" and device_type == "iphone"')
            .filter(f'rank <= {TOP_N_RANKS}')
            .join(spark.read.parquet("s3a://sw-df-production-internal-data/apps/app_details/app_store_bundle/" + add_year_month_day(str(END_DATE))),'id') \
            .selectExpr('country_numeric as country', 'bundle_id as app'))

store_ranks_df.orderBy("country").write.mode("overwrite").parquet(f"{COMMON_PATH}/store_ranks_df")
store_ranks_df = spark.read.parquet(f"{COMMON_PATH}/store_ranks_df")

# COMMAND ----------

ga_apps_df = spark.read.parquet(f"{COMMON_PATH}/ga_apps_df")
metric_apps_df = spark.read.parquet(f"{COMMON_PATH}/metric_apps_df")
store_ranks_df = spark.read.parquet(f"{COMMON_PATH}/store_ranks_df")
top_apps_df = metric_apps_df.select("country").distinct()\
    .crossJoin(spark.createDataFrame(data=[[app] for app in TOP_APPS_LIST], schema="app string"))


selected_apps = metric_apps_df.withColumn("is_vip", F.lit(False))\
       .unionByName(top_apps_df.withColumn("is_vip", F.lit(False)))\
       .unionByName(ga_apps_df.select("country", "app").distinct().withColumn("is_vip", F.lit(False)))\
       .unionByName(VIP_APPS.withColumn("is_vip", F.lit(True)))\
       .unionByName(store_ranks_df.withColumn("is_vip", F.lit(False)))\
       .groupBy("country", "app").agg(F.max("is_vip").alias("is_vip")) # prefer country-apps with is_vip == True

selected_apps.orderBy("country").write.mode("overwrite").parquet(f"{COMMON_PATH}/selected_apps")
selected_apps = spark.read.parquet(f"{COMMON_PATH}/selected_apps")

# COMMAND ----------

apps_info_df = SparkDataArtifact().read_dataframe('/similargroup/scraping/mobile/app-info/' + add_year_month_day(END_DATE) + '/store=1/', spark.read.format("avro"), debug=True) \
            .selectExpr('id','title' ,'author' ,"installs", "maincategory", "description") \
            .join(spark.read.parquet("s3a://sw-df-production-internal-data/apps/app_details/app_store_bundle/" + add_year_month_day(str(END_DATE))),'id') \
            .selectExpr('bundle_id as app','title' ,'author' ,"installs", "maincategory", "description") \
            .join(selected_apps, ["app"])

apps_info_df.orderBy("app").write.mode("overwrite").parquet(f"{COMMON_PATH}/apps_info")
apps_info_df = spark.read.parquet(f"{COMMON_PATH}/apps_info")

# COMMAND ----------

selected_apps = spark.read.parquet(f"{COMMON_PATH}/selected_apps")

selected_apps.filter("country = 840").display()
selected_apps.filter("country = 826").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Prepare Signals

# COMMAND ----------

# MAGIC %md
# MAGIC ## GA Signal

# COMMAND ----------

# GA signals
selected_apps = spark.read.parquet(f"{COMMON_PATH}/selected_apps")

PLATFORM = "iOS"
ga_signals_df = read_between_dates(str(START_DATE), str(END_DATE), "similargroup/data/store-analytics/general/ga4_extractor/processed_ls")\
    .filter(col('country').isin(COUNTRIES_LIST))\
    .filter(col("platform") == PLATFORM)\
    .filter((col("date") >= START_DATE) & (col("date") <= END_DATE))\
    .select("date", "country", col("entity").alias("app"), col("dau_ga").alias("ga_dau"))\
    .join(F.broadcast(selected_apps), ["country", "app"])

ga_signals_df.orderBy("country").write.mode("overwrite").partitionBy("date").parquet(f"{COMMON_PATH}/ga_signals")
ga_signals_df = spark.read.parquet(f"{COMMON_PATH}/ga_signals")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final DAU Signal

# COMMAND ----------

final_dau_signals_df = spark.read.parquet("s3a://sw-apps-core-data-buffer/similargroup/data/ios-analytics/metrics/dau/estimation")\
   .filter(col('country').isin(COUNTRIES_LIST))\
   .withColumn("date", ymd_to_date())\
   .filter((col("date") >= START_DATE) & (col("date") <= END_DATE))\
   .join(selected_apps, ["country", "app"])\
   .select("date", "country", "app", "active_users")\
   .join(F.broadcast(selected_apps), ["country", "app"])

final_dau_signals_df.orderBy("country").write.mode("overwrite").parquet(f"{COMMON_PATH}/final_dau_signals")
final_dau_signals_df = spark.read.parquet(f"{COMMON_PATH}/final_dau_signals")

# COMMAND ----------

# MAGIC %md
# MAGIC ##DataAI Signal

# COMMAND ----------

d_ai_signals_df = spark.read.parquet("s3://sw-apps-core-data-buffer/utils/dai/ios_dau/")\
   .select(col("date").cast("date"), col("country").cast("int"),col("bundle_id").alias("app"), col("data_ai_dau").cast("int"))\
   .filter(col('country').isin(COUNTRIES_LIST))\
   .filter((col("date") >= START_DATE) & (col("date") <= END_DATE))\
   .join(F.broadcast(selected_apps), ["country", "app"])\


d_ai_signals_df.orderBy("country").write.mode("overwrite").parquet(f"{BASE_PATH}/d_ai_signals")
d_ai_signals_df = spark.read.parquet(f"{BASE_PATH}/d_ai_signals")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Android Signal

# COMMAND ----------

def match_apps(matching_df: DataFrame, bundle_info: DataFrame) -> DataFrame:
    w = Window.partitionBy("bundle_id", "id").orderBy(F.desc("count"))
    name2id = bundle_info.groupBy("bundle_id", "id").count().withColumn("rank", F.rank().over(
        w)).filter("rank = 1").selectExpr("bundle_id", "id as ios_id")
    w = Window.partitionBy("android_id", "ios_id").orderBy(F.desc("count"))
    ios2and = matching_df.filter("label == 1").groupBy("android_id", "ios_id").count().withColumn(
        "rank", F.rank().over(w)).filter("rank = 1").selectExpr("android_id", "ios_id")
    return name2id.join(ios2and, "ios_id").select("android_id", "bundle_id")

android_to_ios_df = match_apps(
    SparkDataArtifact().read_dataframe('/similargroup/data/android-apps-analytics/apps-matching/predict' + add_year_month_day(get_sunday_of_n_weeks_ago(str(END_DATE), 1)), spark.read.format("parquet"), debug=True),
    spark.read.parquet("s3a://sw-df-production-internal-data/apps/app_details/app_store_bundle/" + add_year_month_day(str(END_DATE)))) \
        .select(col("android_id").alias("android_app_id"), col("bundle_id").alias("app"))

# COMMAND ----------

android_dau_signals_df = spark.read.parquet("s3a://sw-apps-core-data-buffer/phoenix1/similargroup/data/android-apps-analytics/metrics/dau/estimations_with_ww")\
   .filter(col('country').isin(COUNTRIES_LIST))\
   .withColumn("date", ymd_to_date())\
   .filter((col("date") >= START_DATE) & (col("date") <= END_DATE))\
   .withColumnRenamed("app", "android_app_id")\
   .join(android_to_ios_df, ["android_app_id"])\
   .join(F.broadcast(selected_apps), ["country", "app"])\
   .select("date", "country", "app", col("active_users").alias("android_dau"))


android_dau_signals_df.orderBy("country").write.mode("overwrite").parquet(f"{COMMON_PATH}/android_dau_signals")
android_dau_signals_df = spark.read.parquet(f"{COMMON_PATH}/android_dau_signals")

# COMMAND ----------

# MAGIC %md
# MAGIC ##ALL Signals

# COMMAND ----------

ga_signals_df = spark.read.parquet(f"{COMMON_PATH}/ga_signals")
android_dau_signals_df = spark.read.parquet(f"{COMMON_PATH}/android_dau_signals")
d_ai_signals_df = spark.read.parquet(f"{BASE_PATH}/d_ai_signals")
final_dau_signals_df = spark.read.parquet(f"{COMMON_PATH}/final_dau_signals")
                                     
                                     
all_dau_signals_df = final_dau_signals_df\
    .join(ga_signals_df,["date", "country", "app"], "full")\
    .join(android_dau_signals_df,["date", "country", "app"], "full")\
    .join(d_ai_signals_df,["date", "country", "app"], "full")\
    .select("date", "country", "app", "active_users", "ga_dau", "android_dau", "data_ai_dau")

all_dau_signals_df.orderBy("country").write.mode("overwrite").parquet(f"{BASE_PATH}/all_dau_signals")
all_dau_signals_df = spark.read.parquet(f"{BASE_PATH}/all_dau_signals")

all_dau_signals_df.filter("country = 840").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Establish Scale

# COMMAND ----------

raw_estimation_df = spark.read.parquet(f"s3a://sw-apps-core-data-buffer/similargroup/data/ios-analytics/metrics/dau/raw_estimation")\
.filter(col('country').isin(COUNTRIES_LIST))\
.withColumn("date", ymd_to_date())\
.filter((col("date") >= START_DATE) & (col("date") <= END_DATE))\
.join(F.broadcast(selected_apps), ["country", "app"])\
.select("date", "country", "app", col("p").alias("raw_p"), (col("p") * col("n")).alias("k"), "n")



established_scale_df = spark.read.parquet(f"s3a://sw-apps-core-data-buffer/similargroup/data/ios-analytics/metrics/dau/established_scale")\
.filter(col('country').isin(COUNTRIES_LIST))\
.withColumn("date", ymd_to_date())\
.filter((col("date") >= START_DATE) & (col("date") <= END_DATE))\
.join(F.broadcast(selected_apps), ["country", "app"])\
.select("date", "country", "app", col("p").alias("established_scale_p"))



apply_trend_estimation_df = spark.read.parquet(f"s3a://sw-apps-core-data-buffer/similargroup/data/ios-analytics/metrics/dau/trend_estimation_applied")\
.filter(col('country').isin(COUNTRIES_LIST))\
.withColumn("date", ymd_to_date())\
.filter((col("date") >= START_DATE) & (col("date") <= END_DATE))\
.join(F.broadcast(selected_apps), ["country", "app"])\
.select("date", "country", "app", col("p").alias("tss_p"))



raw_estimation_df\
    .join(established_scale_df, ["date", "country", "app"], "full")\
    .join(apply_trend_estimation_df, ["date", "country", "app"], "full")\
    .na.fill(value=0, subset=["raw_p", "tss_p", "established_scale_p"])\
    .orderBy("country", "app")\
    .write.mode("overwrite").parquet(f"{BASE_PATH}/est_scale_signals")

est_scale_signals_df = spark.read.parquet(f"{BASE_PATH}/est_scale_signals")

# COMMAND ----------

# MAGIC %md ##Prior Signal

# COMMAND ----------

p_estimation_with_prior_df = spark.read.parquet(f"s3a://sw-apps-core-data-buffer/similargroup/data/ios-analytics/metrics/dau/p_estimation_with_prior")\
.filter(col('country').isin(COUNTRIES_LIST))\
.withColumn("date", ymd_to_date())\
.filter((col("date") >= START_DATE) & (col("date") <= END_DATE))\
.join(F.broadcast(selected_apps), ["country", "app"])


p_estimation_with_prior_df.orderBy("country", "app")\
    .write.mode("overwrite").parquet(f"{BASE_PATH}/p_estimation_with_prior")

p_estimation_with_prior_df = spark.read.parquet(f"{BASE_PATH}/p_estimation_with_prior")

# COMMAND ----------

# MAGIC %md
# MAGIC #DEMO

# COMMAND ----------

app = 'com.facebook.Facebook'
country_code = 840


all_dau_signals_df = spark.read.parquet(f"{BASE_PATH}/all_dau_signals")
est_scale_signals_df = spark.read.parquet(f"{BASE_PATH}/est_scale_signals")
p_estimation_with_prior_df = spark.read.parquet(f"{BASE_PATH}/p_estimation_with_prior")
apps_info_df = spark.read.parquet(f"{COMMON_PATH}/apps_info")

all_dau_signals_pdf = all_dau_signals_df.filter(col("country") == country_code).filter(col("app") == app).toPandas()
est_scale_signals_pdf = est_scale_signals_df.filter(col("country") == country_code).filter(col("app") == app).toPandas()
p_estimation_with_prior_pdf = p_estimation_with_prior_df.filter(col("country") == country_code).filter(col("app") == app).toPandas()
app_info_pdf = apps_info_df.filter(col("app") == app).toPandas()

# COMMAND ----------

def plot_app(all_dau_signals_pdf,
             app_info_pdf,
             est_scale_signals_pdf,
             p_estimation_with_prior_pdf,
             display=False, figsize=(20,6)):
    
    all_dau_signals_pdf = all_dau_signals_pdf.sort_values('date',ascending=True)
    
    N_PLOTS = 5#7
    height_ratios =[1 for _ in range(N_PLOTS - 1)]
    H_SPACE = (6/figsize[1]) * 0.3
    annotate_dy = (6 / figsize[1])*0.05
    DATES = all_dau_signals_pdf["date"].values.tolist()
    MAX_DATE = max(DATES)
    MIN_DATE = min(min(DATES), MAX_DATE - timedelta(days=1))

    fig, axs = plt.subplots(N_PLOTS, 1, figsize=(figsize[0], 1.5*sum(height_ratios)*(figsize[1] + H_SPACE)), gridspec_kw={'height_ratios':  [0.1, *height_ratios]})
    fig.subplots_adjust(hspace=H_SPACE)
    sundays_locator = WeekdayLocator(SUNDAY)
    
    # Title
    ax = axs[0]
    ax.set_axis_off()
    app_id = all_dau_signals_pdf["app"].values[0]
    country_code = all_dau_signals_pdf["country"].values[0]
    country = pycountry.countries.get(numeric=f"{int(country_code):03}")
    country_title = f"{country.name}-{country.numeric}-{country.alpha_3}" if int(country_code) != 999 else "WW"        
    if app_info_pdf.size > 0:
        app_title = app_info_pdf["title"].values[0].encode("utf-8").decode("utf-8","ignore") if app_info_pdf["title"].values[0] else None
        app_category = app_info_pdf["maincategory"].values[0] 
        app_store_downloads = app_info_pdf["installs"].values[0]
        title_text = f"{app_title}{f' - ({app_category})' if app_category else ''}\n{app_id}\n{country_title}"
        if app_info_pdf["is_vip"].values[0]:
            img = plt.imread('/dbfs/FileStore/AppsEngagement/eye.png', format='png')
            #ax.imshow(img)
            imagebox = OffsetImage(img, zoom=0.5)
            imagebox.image.axes = ax
            ab = AnnotationBbox(imagebox,
                                xy = [0.3, 0.55],
                                frameon=False,
                                xybox=(700., 80.),
                                xycoords='data',
                                boxcoords="offset points",
                                pad=0.5)
            ax.add_artist(ab)
    else:
        title_text = f"{app_id}\n{country_title}"
    ax.text(0.5,0.5, title_text, size=24, ha="center")
    
    #####  Final DAU Signal  #####
    
    all_dau_signals_pdf = all_dau_signals_pdf.sort_values('date',ascending=True)
    ax = axs[1]
    ax.plot(all_dau_signals_pdf["date"], all_dau_signals_pdf["active_users"], marker=".",color="orange", label="active_users")
    ax.plot(all_dau_signals_pdf["date"], all_dau_signals_pdf["ga_dau"], marker=".",color="green", label="ga")
    ax.fill_between(all_dau_signals_pdf["date"], all_dau_signals_pdf["ga_dau"]*0.7,all_dau_signals_pdf["ga_dau"]*1.3,alpha=.1,color="green",  label="sw_acc_interval")
    ax.plot(all_dau_signals_pdf["date"], all_dau_signals_pdf["android_dau"], marker="",color="gray", label="android_dau")
    ax.plot(all_dau_signals_pdf["date"], all_dau_signals_pdf["data_ai_dau"], marker=".",color="red", label="data_ai_dau")

    ax.axvline(x=datetime(2022, 10, 1), color="lime", linestyle='dashed', label="starting_point")
    ax.axvline(x=datetime(2023, 3, 5), color="brown", linestyle='dashed', label="init_FIFO_weekly_mp")
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.set_title(f'Final DAU Signal')
    ax.set_xlabel("Date")
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%Y-%m-%d"))
    ax.xaxis.set_major_locator(sundays_locator)
    ax.yaxis.set_major_formatter(ticker.EngFormatter())
    ax.set_xlim([MIN_DATE, MAX_DATE])
    ax.grid(True)
    plt.setp(ax.get_xticklabels(), rotation=30, ha='right')
    
    #####  Final DAU Signal - Rescaled  #####
    ax = axs[2]
    app_signals_pdf_proj = all_dau_signals_pdf.copy()
    app_signals_pdf_proj["active_users"] = app_signals_pdf_proj["active_users"] / np.mean(app_signals_pdf_proj["active_users"].values[:14])
    for c in ["android_dau", "data_ai_dau",  "ga_dau"]:
        _df =  app_signals_pdf_proj[(app_signals_pdf_proj[c] > 0) & (app_signals_pdf_proj["active_users"] > 0)]
        if _df.size > 0:
            normalizing_factor = np.mean(_df["active_users"].values[:14]/ _df[c].values[:14], axis=0)
            app_signals_pdf_proj[c] = app_signals_pdf_proj[c] * normalizing_factor

    ax.plot(app_signals_pdf_proj["date"], app_signals_pdf_proj["active_users"], marker=".",color="orange", label="active_users")
    ax.plot(app_signals_pdf_proj["date"], app_signals_pdf_proj["ga_dau"], marker=".",color="green", label="ga")
    ax.plot(app_signals_pdf_proj["date"], app_signals_pdf_proj["android_dau"], marker="",color="gray", label="android_dau")
    ax.plot(app_signals_pdf_proj["date"], app_signals_pdf_proj["data_ai_dau"], marker=".",color="red", label="data_ai_dau")

    ax.axvline(x=datetime(2022, 10, 1), color="lime", linestyle='dashed', label="starting_point")
    ax.axvline(x=datetime(2023, 3, 5), color="brown", linestyle='dashed', label="init_FIFO_weekly_mp")
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.set_title(f'Final DAU Signal - Rescaled')
    ax.set_xlabel("Date")
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%Y-%m-%d"))
    ax.xaxis.set_major_locator(sundays_locator)
    ax.set_xlim([MIN_DATE, MAX_DATE])    
    ax.grid(True)
    plt.setp(ax.get_xticklabels(), rotation=30, ha='right')
    
    #####  Users Signal  #####
    ax = axs[3]
    est_scale_signals_pdf = est_scale_signals_pdf.sort_values('date',ascending=True)

    ax.plot(est_scale_signals_pdf["date"], est_scale_signals_pdf["k"], marker=".",color="orange", label="users")

    ax.axvline(x=datetime(2022, 10, 1), color="lime", linestyle='dashed', label="starting_point")
    ax.axvline(x=datetime(2023, 3, 5), color="brown", linestyle='dashed', label="init_FIFO_weekly_mp")
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.set_title(f'Users(k)')
    ax.set_xlabel("Date")
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%Y-%m-%d"))
    ax.xaxis.set_major_locator(sundays_locator)
    ax.set_xlim([MIN_DATE, MAX_DATE])
    ax.grid(True)
    plt.setp(ax.get_xticklabels(), rotation=30, ha='right')
    
    #####  P estimations Signals  #####
    ax = axs[4]
    est_scale_signals_pdf = est_scale_signals_pdf.sort_values('date',ascending=True)

    ax.plot(est_scale_signals_pdf["date"], est_scale_signals_pdf["raw_p"], marker=".",color="orange", label="raw_p")
    ax.plot(est_scale_signals_pdf["date"], est_scale_signals_pdf["established_scale_p"], marker=".",color="red", label="established_scale_p")
    ax.plot(est_scale_signals_pdf["date"], est_scale_signals_pdf["tss_p"], marker=".",color="blue", label="tss_p")
    
    ax.axvline(x=datetime(2022, 10, 1), color="lime", linestyle='dashed', label="starting_point")
    ax.axvline(x=datetime(2023, 3, 5), color="brown", linestyle='dashed', label="init_FIFO_weekly_mp")
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.set_title(f'P estimations Signals')
    ax.set_xlabel("Date")
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%Y-%m-%d"))
    ax.xaxis.set_major_locator(sundays_locator)
    ax.set_xlim([MIN_DATE, MAX_DATE])
    ax.grid(True)
    plt.setp(ax.get_xticklabels(), rotation=30, ha='right')
    
    # if p_estimation_with_prior_pdf.size > 0:
    #     #####  priors Signals  #####
    #     ax = axs[5]
    #     p_estimation_with_prior_pdf = p_estimation_with_prior_pdf.sort_values('date',ascending=True)
    #     p_estimation_with_prior_pdf.loc[p_estimation_with_prior_pdf.historic_patterns_signal == 0.0, "historic_patterns_signal"] = None
    #     ax.plot(p_estimation_with_prior_pdf["date"], p_estimation_with_prior_pdf["clean_p"], marker=".",color="orange", label="tss_p")
    #     ax.plot(p_estimation_with_prior_pdf["date"], p_estimation_with_prior_pdf["trend_signal"],linestyle='dashed', marker="",color="gray", label="trend_signal")
    #     ax.plot(p_estimation_with_prior_pdf["date"], p_estimation_with_prior_pdf["historic_patterns_signal"], marker="",color="red", label="historic_patterns_signal")
    #     ax.plot(p_estimation_with_prior_pdf["date"], p_estimation_with_prior_pdf["p"], marker=".",color="blue", label="p_posterior")
    #     ax.plot(p_estimation_with_prior_pdf["date"], p_estimation_with_prior_pdf["smooth_p"], marker=".",color="indigo", label="smooth_p")
    #     # ax.plot(p_estimation_with_prior_pdf["date"], p_estimation_with_prior_pdf["android_patterns_signal"], marker="",color="green", label="android_patterns_signal")

    #     ax.axvline(x=datetime(2022, 10, 1), color="lime", linestyle='dashed', label="starting_point")
    #     ax.axvline(x=datetime(2023, 3, 5), color="brown", linestyle='dashed', label="init_FIFO_weekly_mp")
    #     ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    #     ax.set_title(f'Priors Signals')
    #     ax.set_xlabel("Date")
    #     ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%Y-%m-%d"))
    #     ax.xaxis.set_major_locator(sundays_locator)
    #     ax.set_xlim([MIN_DATE, MAX_DATE])
    #     ax.grid(True)
    #     plt.setp(ax.get_xticklabels(), rotation=30, ha='right')

    #     #####  Priors Weights  #####
    #     ax = axs[6]
    #     p_estimation_with_prior_pdf = p_estimation_with_prior_pdf.sort_values('date',ascending=True)

    #     android_w = p_estimation_with_prior_pdf["android_weight"] * p_estimation_with_prior_pdf["patterns_weight"]
    #     historical_w = p_estimation_with_prior_pdf["patterns_weight"] - android_w
    #     smooth_w = p_estimation_with_prior_pdf["smooth_weight"]

    #     ax.axvline(x=datetime(2022, 10, 1), color="lime", linestyle='dashed', label="starting_point")
    #     ax.axvline(x=datetime(2023, 3, 5), color="brown", linestyle='dashed', label="init_FIFO_weekly_mp")
    #     ax.set_title(f"Priors Weights")
    #     ax.stackplot(p_estimation_with_prior_pdf["date"] ,android_w, historical_w, smooth_w, labels=["android_w","historical_w", "smooth_w"])
    #     ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    #     ax.set_xlabel("Date")
    #     ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%Y-%m-%d"))
    #     ax.xaxis.set_major_locator(sundays_locator)
    #     ax.set_xlim([MIN_DATE, MAX_DATE])
    #     ax.set_ylim([0, 1])
    #     ax.grid(True)
    #     plt.setp(ax.get_xticklabels(), rotation=30, ha='right')
    
    if not display:
        plt.close(fig)
    return fig

# COMMAND ----------

plot_app(all_dau_signals_pdf,
         app_info_pdf,
         est_scale_signals_pdf,
         p_estimation_with_prior_pdf,
         display=True, figsize=(25,3))
x=1

# COMMAND ----------

def plot_country_size(country_signal_df, display=False, figsize=(20,6)):


    DATES = country_signal_df["date"].values.tolist()
    MAX_DATE = max(DATES)
    MIN_DATE = min(min(DATES), MAX_DATE - timedelta(days=1))
    sundays_locator = WeekdayLocator(SUNDAY)

    fig = plt.figure(figsize=figsize)
    ax =  fig.gca()

    pdf = country_signal_df.sort_values('date',ascending=True)
    
    ax.axvline(x=datetime(2022, 10, 1), color="lime", linestyle='dashed', label="starting_point")
    ax.axvline(x=datetime(2023, 3, 5), color="brown", linestyle='dashed', label="init_FIFO_weekly_mp")
    ax.set_title(f"N Signal")
    ax.plot(pdf["date"].drop_duplicates() ,pdf["n"] , label="n")
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.set_xlabel("Date")
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%Y-%m-%d"))
    ax.xaxis.set_major_locator(sundays_locator)
    ax.set_xlim([MIN_DATE, MAX_DATE])
    ax.grid(True)
    plt.setp(ax.get_xticklabels(), rotation=30, ha='right')
    if not display:
        plt.close(fig)
    return fig

plot_country_size(est_scale_signals_pdf)


# COMMAND ----------

# MAGIC %md
# MAGIC #PDFS

# COMMAND ----------

file_dir = f"user/ios_dau_phoenix/pdfs/{VERSION}-fw"
dbutils.fs.mkdirs(f'dbfs:/FileStore/{file_dir}/')
import warnings
# dont warn on missing characters in the font
warnings.filterwarnings("ignore",message=r'([\S\s]*Glyph[\S\s]*missing from current font[\S\s]*)')


all_dau_signals_df = spark.read.parquet(f"{BASE_PATH}/all_dau_signals")
est_scale_signals_df = spark.read.parquet(f"{BASE_PATH}/est_scale_signals")
p_estimation_with_prior_df = spark.read.parquet(f"{BASE_PATH}/p_estimation_with_prior")
apps_info_df = spark.read.parquet(f"{COMMON_PATH}/apps_info")

live_signals = (all_dau_signals_df
                .select("app", "country").distinct()
                .join(est_scale_signals_df.select("app", "country").distinct(),
                      ["app", "country"]))
#keep apps with raw_estimation/tss
all_dau_signals_df = all_dau_signals_df.join(live_signals, ["country", "app"])


for country_code in COUNTRIES_LIST:
    print("Running on country", country_code)
    country = pycountry.countries.get(numeric=f"{int(country_code):03}")
    filename = f'dau_metric_{country.numeric}_{country.alpha_3}_{VERSION}.pdf'
    print(f"creating {filename}")
    with PdfPages(f'/dbfs/FileStore/{file_dir}/{filename}') as pdf:
        fig, axs = plt.subplots(figsize=(20,2))
        axs.set_axis_off()
        country_title = f"{country.name}-{country.numeric}-{country.alpha_3}" if int(country_code) != 999 else "999-WW"
        axs.text(0.5,0.5,country_title , size=24, ha="center")
        pdf.savefig(fig)
        
        fig = plot_country_size(est_scale_signals_df.filter(col("country") == country_code).groupBy("date", "country").agg(F.max("n").alias("n")).toPandas())
        pdf.savefig(fig)

        _signals_pdf = all_dau_signals_df.filter(col("country") == country_code).toPandas()
        _est_scale_signals_pdf = est_scale_signals_df.filter(col("country") == country_code).toPandas()
        _p_estimation_with_prior_pdf = p_estimation_with_prior_df.filter(col("country") == country_code).toPandas()
        _apps_info_pdf = apps_info_df.filter(col("country") == country_code).toPandas()

        app_list = _signals_pdf[["app", "active_users"]]\
            .groupby("app")\
            .agg(active_users=("active_users","mean"), count=("active_users","count"))\
            .sort_values(by='active_users', ascending=False)\
            .index.values
        app_list = list(app_list)
        #for app in app_list[:200] + app_list[-20:-1]:
        for app in app_list:
            app_signals_pdf = _signals_pdf[_signals_pdf["app"] == app]
            app_est_scale_signals_pdf = _est_scale_signals_pdf[_est_scale_signals_pdf["app"] == app]
            app_info_pdf = _apps_info_pdf[_apps_info_pdf["app"] == app]
            app_p_estimation_with_prior_pdf = _p_estimation_with_prior_pdf[_p_estimation_with_prior_pdf["app"] == app]
            app_fig = plot_app(app_signals_pdf, app_info_pdf, app_est_scale_signals_pdf,app_p_estimation_with_prior_pdf,display=False, figsize=(20,3))
            pdf.savefig(app_fig)  
    plt.close("all")
    url = f'https://similarweb-databricks-df.cloud.databricks.com/files/{file_dir}/{filename}/?o=1245118320249363' 
    displayHTML(f'<a href="{url}">Download {filename}</a>')