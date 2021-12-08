-- Databricks notebook source
USE CATALOG main;
-- namespace notation: catalog.db.table

-- COMMAND ----------

-- frank_db is re-used
-- CREATE database frank_db;
USE frank_db;

-- COMMAND ----------

CREATE TABLE motorbikes  (id int, year int, make string, model string);

-- COMMAND ----------

INSERT INTO motorbikes VALUES  (0, 1985, "Yamaha","XT500"), (1, 2001, "Honda","Africa Twin"), (2, 2010, "Triumph","Bonneville") 

-- COMMAND ----------

SELECT * FROM motorbikes;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Delta Sharing Share

-- COMMAND ----------

CREATE share frank_bikes

-- COMMAND ----------

ALTER share frank_bikes ADD TABLE motorbikes;

-- COMMAND ----------

SHOW ALL IN share frank_bikes;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Recipient

-- COMMAND ----------

CREATE RECIPIENT frank2;

-- COMMAND ----------

GRANT SELECT ON SHARE frank_bikes TO RECIPIENT frank; 
-- REVOKE SELECT ON SHARE frank_bikes FROM RECIPIENT frank; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Check for live data - Update

-- COMMAND ----------

UPDATE motorbikes SET year = 1999 where id==2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cleanup

-- COMMAND ----------

USE CATALOG main;

-- COMMAND ----------

DROP TABLE  IF EXISTS frank_db.motorbikes;
-- don't drop the schema aka database: DROP SCHEMA IF EXISTS frank_db;


-- COMMAND ----------

DROP SHARE IF EXISTS frank_bikes

-- COMMAND ----------

DROP RECIPIENT frank
