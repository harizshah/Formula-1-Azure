-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

USE hariz_f1_processed

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SELECT * 
FROM drivers
LIMIT 10;

-- COMMAND ----------

DESC drivers

-- COMMAND ----------

SELECT *
FROM drivers
WHERE nationality = 'British'
AND dob >= '1990-01-01'

-- COMMAND ----------

SELECT name, dob
FROM drivers
WHERE (nationality = 'British'
and dob >= '1990-01-01')
OR nationality = 'Indian'
ORDER BY dob DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####### SQL Simple Functions

-- COMMAND ----------

USE hariz_f1_processed

-- COMMAND ----------

SELECT *, concat(driver_ref, '-', code ) AS new_driver_ref
FROM drivers

-- COMMAND ----------

SELECT *, SPLIT(name, '')[0] forename, SPLIT(name, '')[1] surname
FROM drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####### SQL Simple Functions

-- COMMAND ----------

SELECT *, date_add(dob, 1)
FROM drivers

-- COMMAND ----------

SELECT MAX(dob)
FROM drivers

-- COMMAND ----------

SELECT nationality, COUNT(*)
FROM drivers
GROUP BY nationality
HAVING count(*) > 100
ORDER BY nationality;

-- COMMAND ----------

SELECT nationality, name, dob, RANK() OVER (PARTITION BY nationality ORDER BY dob DESC) AS age_rank
FROM drivers
ORDER BY nationality, age_rank

-- COMMAND ----------


