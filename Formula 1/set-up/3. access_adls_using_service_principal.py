# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Service Principal
# MAGIC Steps to follow
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/ password for the Application
# MAGIC 3. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake.

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.harizformula1dl.dfs.core.windows.net",
    "n88HPW/v1NhHO0xZkmMp80FOfxe+Tty0mrxM9ckvXdA21f+pRw1JLIIX+ObYYbsx0ku+8InpoW64+AStVyTNZA=="
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@harizformula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@harizformula1dl.dfs.core.windows.net/circuit.csv"))

# COMMAND ----------


