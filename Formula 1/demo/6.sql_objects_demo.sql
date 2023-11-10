-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Lesson Objectives
-- MAGIC 1. Spark SQL Documentation
-- MAGIC 2. Create database memo
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. SHOW Command
-- MAGIC 5. DESCRIBE command
-- MAGIC 6. Find the current database

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo_race_results_python")

-- COMMAND ----------

USE demo;
SHOW Tables;

-- COMMAND ----------

DESC demo_race_results_python

-- COMMAND ----------

DESC EXTENDED demo_race_results_python

-- COMMAND ----------

SELECT *
FROM demo_race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

CREATE TABLE race_results_sql
AS
SELECT *
FROM demo_race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

DESC EXTENDED demo_race_results_python

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Learning Objectives
-- MAGIC 1. Create external table using Python
-- MAGIC 2. Create external table using SQL
-- MAGIC 3. Effect of dropping in external table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo_race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED demo_race_results_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP)
USING PARQUET
LOCATION "/mnt/formula1dl/presentation/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.demo_race_results_ext_py WHERE race_year = 2020;

-- COMMAND ----------

SELECT COUNT(1) FROM demo.race_results_ext_sql

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Views on tables
-- MAGIC Learning Objectives
-- MAGIC 1. Create Temp View
-- MAGIC 2. Create Global Temp View
-- MAGIC 3. Create Permanent View

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW v_race_results
AS
SELECT *
FROM demo.demo_race_results_python
WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM global_temp.v_race_results;

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results
AS
SELECT *
FROM demo.demo_race_results_python
WHERE race_year = 2012;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

SELECT * FROM demo.pv_race_results;

-- COMMAND ----------


