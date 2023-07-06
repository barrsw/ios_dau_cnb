# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/parallel_notebook_runner

# COMMAND ----------

Notebook.runner(notebook_name = './01_sources_estimation/main',
                arguments={"envs": "mau_model_0"},
                start_date=date(2022,10,1), 
                end_date=date(2023,3,1),
                timeout=3600*5,
                mode="monthly",
                parallel_runs=2)

# COMMAND ----------

Notebook.runner(notebook_name = './02_adjust_to_mau/main',
                arguments={"envs": "mau_model_0"},
                start_date=date(2022,10,1), 
                end_date=date(2023,3,1),
                timeout=3600*5,
                mode="monthly",
                parallel_runs=10)