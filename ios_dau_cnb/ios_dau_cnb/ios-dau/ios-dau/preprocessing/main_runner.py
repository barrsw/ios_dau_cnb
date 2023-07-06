# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/parallel_notebook_runner

# COMMAND ----------

# MAGIC %md ##Usage
# MAGIC
# MAGIC Each experimental run should have a separate `envs`. Multiple `envs` can be passed by comma-separation. The first `env` is the output env. "master" cannot be the first `env`, because it should represent the most up-to-date code everybody agreed upon.
# MAGIC
# MAGIC `envs` naming should follow the `{branch_name}_{algorithm_name},{another_branch_name}_{another_algorithm_name}` convention. The experiment name should be either "master" if this is the most up-to-date version as informative as possible.

# COMMAND ----------

Notebook.runner(notebook_name='./main',
       arguments={"envs": "master_preprocessing"},
       start_date=date(2022,8,7), 
       end_date=date(2022,10,31),
       timeout=3600*5,
       retry=1,
       parallel_runs=5)