# Databricks notebook source
# MAGIC %run /Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

from matplotlib.backends.backend_pdf import PdfPages
from functools import reduce
import matplotlib.pyplot as plt
import matplotlib
import pycountry
import matplotlib.ticker as ticker
from pyspark.sql.functions import col
import pandas as pd

# COMMAND ----------

dbutils.widgets.text('start_date','')
dbutils.widgets.text('end_date','')
dbutils.widgets.text('envs','')
dbutils.widgets.text('compare_env','')

# COMMAND ----------

START_DATE  = dbutils.widgets.get("start_date")
END_DATE  = dbutils.widgets.get("end_date")
ENVS = generate_env_list(dbutils.widgets.get("envs"))
COMPARE_ENV = generate_env_list(dbutils.widgets.get("compare_env")) if dbutils.widgets.get("compare_env") !='' else ''
IOS_DAU_PATH = '/similargroup/data/ios-analytics/metrics/dau/estimation/'
GA_PATH = '/similargroup/data/store-analytics/general/ga4_extractor/processed_ls/'
DATA_AI_PATH = 's3://similargroup-research/user/yishay.shtrassler/dai_for_pdfs'
ANDROID_PATH = '/phoenix1/similargroup/data/android-apps-analytics/metrics/dau/estimations_with_ww/'
IOS_CONV_PATH = 's3://sw-df-production-internal-data/apps/app_details/app_store_bundle/' + (add_year_month(END_DATE) if datetime.strptime(END_DATE, "%Y-%m-%d").date() >= date(2022, 11, 27) else add_year_month_day(date(2022, 11, 27)))
MP_PATH = "/similargroup/data/ios-analytics/measure_protocol/adjusted/"
APPS_MATCHING_PATH = '/similargroup/data/android-apps-analytics/apps-matching/predict/' + add_year_month_day(get_sunday_of_n_weeks_ago(END_DATE, 1))
STORE_RANK_PATH = '/similargroup/data/store-analytics/iOS-app-store/top-charts/' + add_year_month_day(END_DATE)
APP_INFO_PATH = '/similargroup/scraping/mobile/app-info/' + add_year_month_day(END_DATE) + '/store=1/'
COUNTRIES = [840, 826]
TOP_N_RANKS = 7

primary_env = dbutils.widgets.get("envs").split(',')[0]

# COMMAND ----------

df_ios_dau = read_between_dates(start=START_DATE, end=END_DATE, path=IOS_DAU_PATH,  envs=ENVS).filter(F.col("country").isin(COUNTRIES))
if COMPARE_ENV != '':
    df_compare_dau = read_between_dates(start=START_DATE, end=END_DATE, path=IOS_DAU_PATH,  envs=COMPARE_ENV).filter(F.col("country").isin(COUNTRIES))
else:
    df_compare_dau = None
df_ga  = (read_between_dates(start=START_DATE, end=END_DATE, path=GA_PATH)
          .filter(F.col("country").isin(COUNTRIES))
          .filter("platform = 'iOS'")
          .select("country", "date", F.col("entity").alias("app"), F.col("dau_final").alias("activeUsers")))

df_android = read_between_dates(start=START_DATE, end=END_DATE, path=ANDROID_PATH).withColumnRenamed('app','android_id').filter(F.col("country").isin(COUNTRIES))


df_data_ai = (spark.read.parquet(DATA_AI_PATH)
              .filter(F.col('country').isin(COUNTRIES)).filter(F.col('date').between(START_DATE,END_DATE))
              .withColumnRenamed("bundle_id", "app"))


# df_mp = SparkDataArtifact().read_dataframe(MP_PATH, spark.read.format("parquet"), debug=True).filter(F.col('date').between(START_DATE,END_DATE))
# df_mp_total = df_mp.groupBy('country','date').agg(F.countDistinct('user_id').alias('total_users'))
# df_mp_wau = (df_mp.groupBy('country','date','bundle_id')
#              .agg(F.countDistinct('user_id').alias('users'))
#              .join(df_mp_total, ['country','date'])
#              .withColumn('p_mp', F.expr('users / total_users'))
#              .withColumn('mp_wau', F.col('p_mp')*150e6))


df_ios_conv = spark.read.parquet(IOS_CONV_PATH)
df_apps_matching = SparkDataArtifact().read_dataframe(APPS_MATCHING_PATH, spark.read.format("parquet"), debug=True).selectExpr('android_id','ios_id as id')
df_bundle_id_app = df_ios_conv.join(df_apps_matching,'id','left').withColumnRenamed("bundle_id", "app")
df_ranks = (SparkDataArtifact().read_dataframe(STORE_RANK_PATH, spark.read.format("parquet"), debug=True)
            .filter(F.col('country_numeric').isin(COUNTRIES))
            .filter('mode == "topselling_free" and device_type == "iphone"')
            .selectExpr('country_numeric as country', 'id', 'category', 'rank'))

# COMMAND ----------

def get_bundles_for(df_ios_dau,df_ranks,df_ga,df_data_ai,df_compare_dau,df_bundle_id_app):
    top_ranks_bundles = df_ranks.filter(f'rank <= {TOP_N_RANKS}').select('country','id').distinct().join(df_bundle_id_app,'id')

    ga_bundles = df_ga.filter('activeUsers >8000').select('country','app').distinct().join(df_bundle_id_app,'app','left')
    
    dai_bundles = df_data_ai.select('country','app').distinct().join(df_bundle_id_app,'app','left')

    buckets_dau = (df_ios_dau.groupBy('country','app').agg(F.avg("active_users").alias("dau"))\
        .withColumn("size_bucket", F.round(F.log10("dau")))\
        .withColumn("rank", F.row_number().over(Window.partitionBy("size_bucket",'country').orderBy(F.randn(seed=0))))\
        .filter(col("rank") <= 15)\
        .select("app",'country').join(df_bundle_id_app,'app','left'))
    
    if df_compare_dau:
        buckets_compare_dau = (df_compare_dau.groupBy('country','app').agg(F.avg("active_users").alias("dau"))\
        .withColumn("size_bucket", F.round(F.log10("dau")))\
        .withColumn("rank", F.row_number().over(Window.partitionBy("size_bucket",'country').orderBy(F.randn(seed=0))))\
        .filter(col("rank") <= 15)\
        .select("app",'country').join(df_bundle_id_app,'app','left'))
    
    df_ret = top_ranks_bundles.unionByName(ga_bundles).unionByName(buckets_dau)
    
    final_df_ret = df_ret.unionByName(buckets_compare_dau) if df_compare_dau else df_ret
    
    return final_df_ret.distinct()

# COMMAND ----------

bundles = get_bundles_for(df_ios_dau,df_ranks,df_ga,df_data_ai,df_compare_dau,df_bundle_id_app)

# COMMAND ----------

def union_all_signals(df_ios_dau, df_data_ai, df_ga, df_android, df_compare_dau):
    df_dau = df_ios_dau.selectExpr('country','app','date','active_users as dau_est').join(bundles,['country','app'])
    df_data_ai = df_data_ai.join(bundles,['country','app'])
    #df_mp_ = df_mp_wau.join(bundles,['country','bundle_id'])
    df_ga = df_ga.withColumnRenamed('activeUsers', 'ga_dau').join(bundles,['country','app'])
    df_and = df_android.withColumnRenamed('active_users','android_dau').join(bundles,['country','android_id'])
    if df_compare_dau:
        df_comp = df_compare_dau.selectExpr('country','app','date','active_users as compare_dau').join(bundles,['country','app'])
    cols_join = ['id','country','app','android_id','date']
    
    df_ret = df_dau.join(df_data_ai,cols_join,'outer').join(df_ga,cols_join,'outer').join(df_and,cols_join,'outer')#.join(df_mp_,cols_join,'outer')
    
    final_df_ret = df_ret.join(df_comp,cols_join,'outer') if df_compare_dau else df_ret
    
    return final_df_ret

# COMMAND ----------

df_signals = union_all_signals(df_ios_dau, df_data_ai, df_ga, df_android,
                               #df_mp_wau,
                               df_compare_dau)

# COMMAND ----------


def plot_app(signals_pdf ,app_info_pdf,display=False,figsize=(20,3)):
    signals_pdf['date'] = pd.to_datetime(signals_pdf['date'])
    signals_pdf = signals_pdf.sort_values('date',ascending=False)
    figsize=figsize
    N_PLOTS = 2 
    H_SPACE = (6/figsize[1]) * 0.3
    DATES = signals_pdf["date"].values.tolist()

    fig, axs = plt.subplots(N_PLOTS, 1, figsize=(figsize[0],N_PLOTS*(figsize[1] + H_SPACE)), gridspec_kw={'height_ratios': [0.1, *[1 for _ in range(N_PLOTS-1)]]})
    fig.subplots_adjust(hspace=H_SPACE)
    
    ax = axs[0]
    ax.set_axis_off()
    app_id = signals_pdf["app"].values[0]
    country = pycountry.countries.get(numeric=f"{int(840):03}")
    country_title = f"{country.name}-{country.numeric}-{country.alpha_3}" if int(840) != 999 else "WW"
    if app_info_pdf.size > 0:
        app_title = app_info_pdf["title"].values[0].encode("utf-8").decode("utf-8","ignore") if app_info_pdf["title"].values[0] else None
        app_category = app_info_pdf["maincategory"].values[0] 
        app_store_downloads = app_info_pdf["installs"].values[0]
        title_text = f"{app_title}{f' - ({app_category})' if app_category else ''}\n{app_id}\n{country_title}"
    else:
        title_text = app_id
    ax.text(0.5,0.5, title_text, size=24, ha="center")


    ax = axs[1]
    est_pdf = signals_pdf[~signals_pdf["dau_est"].isnull()]
    ax.plot(est_pdf["date"], est_pdf["dau_est"], marker=".",color="blue", label=f'{primary_env}_dau')
    
    ga_pdf = signals_pdf[~signals_pdf["ga_dau"].isnull()]
    if len(ga_pdf)>0:
        ax.plot(ga_pdf["date"], ga_pdf["ga_dau"], marker=".",color="g", label="dau_ga")

    android_pdf = signals_pdf[~signals_pdf["android_dau"].isnull()]
    if len(android_pdf)>0:
        ax.plot(android_pdf["date"], android_pdf["android_dau"], marker=".",color="red", label="android_dau")
        
#     mp_pdf = signals_pdf[~signals_pdf["mp_wau"].isnull()]
#     if len(mp_pdf)>0:
#         ax.plot(mp_pdf["date"], mp_pdf["mp_wau"], marker=".",color="orange", label="mp_wau")
    
    df_data_ai = signals_pdf[~signals_pdf["data_ai_dau"].isnull()]
    if len(df_data_ai)>0:
        ax.plot(df_data_ai["date"], df_data_ai["data_ai_dau"], marker=".",color="black", label="data_ai_dau")
    
    if df_compare_dau:
        compare_pdf = signals_pdf[~signals_pdf["compare_dau"].isnull()]
        if len(compare_pdf)>0:
            ax.plot(compare_pdf["date"], compare_pdf["compare_dau"], marker=".",color="brown", label=f'{dbutils.widgets.get("compare_env")}_dau')
    
    
    ax.legend(loc=1)
    ax.set_title(f'IOS_dau')
    ax.set_xlabel("Date")
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%Y-%m-%d"))
    ax.yaxis.set_major_formatter(ticker.EngFormatter())
    

        
    if not display:
        plt.close(fig)
    return fig

# COMMAND ----------

app_info = (SparkDataArtifact().read_dataframe(APP_INFO_PATH, spark.read.format("avro"), debug=True)
            .selectExpr('id','title' ,'author' ,"installs", "maincategory", "description")
            .join(bundles,'id','left_semi'))

# COMMAND ----------

df_dau = read_between_dates(start=START_DATE, end=START_DATE, path=IOS_DAU_PATH,  envs=ENVS).filter(F.col("country").isin(COUNTRIES)).select('app','country',F.col("active_users").alias('dau_abs'))
bundles_dau = bundles.join(df_dau,['country','app'],'left')

# COMMAND ----------

envs_str = f'{primary_env}' + (f'-{dbutils.widgets.get("compare_env")}' if df_compare_dau else '')
file_dir = f'user/ios_dau_phoenix/pdfs/{envs_str}'
dbutils.fs.mkdirs(f'dbfs:/FileStore/{file_dir}/')
import warnings
# dont warn on missing characters in the font
warnings.filterwarnings( "ignore",message=r'([\S\s]*Glyph[\S\s]*missing from current font[\S\s]*)')
# Stop printing SettingWithCopyWarning
pd.options.mode.chained_assignment = None

signals_df = df_signals

apps_info_pdf = app_info.toPandas()

for country_code in COUNTRIES:
    country = pycountry.countries.get(numeric=f"{int(country_code):03}")
    filename = f'ios_dau_{country.numeric}_{country.alpha_3}-{primary_env}-{dbutils.widgets.get("compare_env")}.pdf'
    print(f"creating /dbfs/FileStore/{file_dir}/{filename}")
    with PdfPages(f'/dbfs/FileStore/{file_dir}/{filename}') as pdf:
        
        _signals_pdf = signals_df.filter(f'country == {country_code}').withColumn('data_ai_dau',F.col('data_ai_dau').cast('float')).toPandas()
        _signals_pdf['data_ai_dau'] = pd.to_numeric(_signals_pdf['data_ai_dau'])
        
        app_list = bundles_dau.filter(f'country == {country_code}').select('app','id','dau_abs').orderBy(F.desc('dau_abs')).collect()
        for app in app_list:
             
            app_signals_pdf = _signals_pdf[_signals_pdf["app"] == app[0]]
            if len(app_signals_pdf['dau_est'].dropna().values) == 0:
                continue
            app_info_pdf = apps_info_pdf[apps_info_pdf["id"] == app[1]]
            app_fig = plot_app(app_signals_pdf, app_info_pdf, display=False)
            pdf.savefig(app_fig) 
            
    plt.close("all")
    url = f'https://similarweb-databricks-df.cloud.databricks.com/files/{file_dir}/{filename}/?o=1245118320249363' 
    displayHTML(f'<a href="{url}">Download {filename}</a>')