# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/parallel_notebook_runner

# COMMAND ----------

Notebook.runner(notebook_name = './main',
                arguments={"envs": "h2b_train_cnb"},
                start_date=date(2022,11,1), 
                end_date=date(2023,4,1),
                timeout=3600*5,
                parallel_runs=5)

# COMMAND ----------

There are failed runs on those dates: 2023-01-23, 2023-01-24
