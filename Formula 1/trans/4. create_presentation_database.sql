-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "abfss://presentation@harizformula1dl.dfs.core.windows.net/"

-- COMMAND ----------

DROP SCHEMA f1_presentation CASCADE

-- COMMAND ----------


