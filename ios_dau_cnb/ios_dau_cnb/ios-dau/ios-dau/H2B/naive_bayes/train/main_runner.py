# Databricks notebook source
# MAGIC %run /Repos/apps/ios-dau/utils/parallel_notebook_runner

# COMMAND ----------

Notebook.runner(notebook_name = './main',
              arguments={"envs": "h2b_train_cnb_debug"},
              start_date=date(2022,11,1), 
              end_date=date(2023,2,1),
              mode='weekly',
              timeout=3600*5,
              parallel_runs=5)

# COMMAND ----------

