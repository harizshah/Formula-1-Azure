# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.keys
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

formuladl_account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'harizformula1-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dl.dfs.core.windows.net",
    formuladl_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@harizformula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@harizformula1dl.dfs.core.windows.net/circuit.csv"))

# COMMAND ----------


