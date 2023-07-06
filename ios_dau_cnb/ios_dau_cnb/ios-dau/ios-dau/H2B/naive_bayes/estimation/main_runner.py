# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/parallel_notebook_runner

# COMMAND ----------

Notebook.runner(notebook_name = './01_sources_estimation/main',
                arguments={"envs": "master_NB"},
                start_date=date(2022,8,7), 
                end_date=date(2022,10,31),
                timeout=3600*5,
                parallel_runs=10)

# COMMAND ----------

Notebook.runner(notebook_name = './02_establish_scale/main',
                arguments={"envs": "master_NB,main"},
                start_date=date(2022,9,3), 
                end_date=date(2022,10,31),
                timeout=3600*5,
                parallel_runs=10)

# COMMAND ----------

Notebook.runner(notebook_name = './03_trend_estimation/main',
                arguments={"envs": "master_NB"},
                start_date=date(2022,9,3), 
                end_date=date(2022,10,31),
                timeout=3600*5,
                parallel_runs=10)

# COMMAND ----------

Notebook.runner(notebook_name = './04_apply_trend/main',
                arguments={"envs": "master_NB,main"},
                start_date=date(2022,9,3), 
                end_date=date(2022,9,3),
                timeout=3600*5,
                parallel_runs=10)
Notebook.runner(notebook_name = './04_apply_trend/main',
                arguments={"envs": "master_NB,main"},
                start_date=date(2022,9,4), 
                end_date=date(2022,9,10),
                timeout=3600*5,
                parallel_runs=10)
Notebook.runner(notebook_name = './04_apply_trend/main',
                arguments={"envs": "master_NB,main"},
                start_date=date(2022,9,11), 
                end_date=date(2022,10,31),
                timeout=3600*5,
                parallel_runs=7,
                parallelization_mode='bulk')

# COMMAND ----------

Notebook.runner(notebook_name = './05_adjust_to_dau/main',
                arguments={"envs": "master_NB"},
                start_date=date(2022,9,3), 
                end_date=date(2022,10,31),
                timeout=3600*5,
                parallel_runs=10)

# COMMAND ----------

Notebook.runner(notebook_name = './06_evaluate_dau/main',
                arguments={"envs": "master_NB"},
                start_date=date(2022,10,1), 
                end_date=date(2022,10,31),
                timeout=3600*5,
                parallel_runs=5)

# COMMAND ----------

calc_accuracy(spark.read.parquet("s3a://sw-apps-core-data-buffer//ios_dau_phoenix/master_NB//similargroup/data/ios-analytics/metrics/dau//evaluation//year=22/month=10/")).display()

# COMMAND ----------

calc_coverage(spark.read.parquet("s3a://sw-apps-core-data-buffer//ios_dau_phoenix/master_NB//similargroup/data/ios-analytics/metrics/dau//estimation//year=22/month=10/")).display()