# Databricks notebook source
spark

# COMMAND ----------

# MAGIC %md
# MAGIC #### let's have a fresh start

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop database if exists fmcars CASCADE;
# MAGIC -- drop table if exists customers_parquet;
# MAGIC -- drop table if exists customers_delta;

# COMMAND ----------

# MAGIC %md
# MAGIC #### create resources

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC create database fmcars;
# MAGIC use fmcars;

# COMMAND ----------

# MAGIC %md
# MAGIC ###File / Table Abstraction

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /FileStore/frank.munz/mock

# COMMAND ----------

# read data from a directory full of JSON datasets with multiple datasets per file 

customers = spark.read.format("json").load("dbfs:/FileStore/frank.munz/mock")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Work with Spark DF / tables

# COMMAND ----------

customers.filter("car_make == 'Pontiac' ").show()

# COMMAND ----------

# then save as parquet. 
customers.write.format("parquet").saveAsTable("customers_parquet")


# COMMAND ----------

# also save as delta
customers.write.format("delta").saveAsTable("customers_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from customers_delta where car_make == "Pontiac"

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables 

# COMMAND ----------

# MAGIC %md
# MAGIC ## No UPDATE or MERGE for Apache Parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE customers_delta SET email = "hello@world.com" WHERE car_make = "Audi"

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from customers_delta ;

# COMMAND ----------

# MAGIC %md
# MAGIC SQL delete

# COMMAND ----------

# MAGIC %sql 
# MAGIC delete from customers_delta where  car_make == "Ford";
# MAGIC select count(*) from customers_delta;

# COMMAND ----------

# MAGIC %md
# MAGIC SQL update

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE customers_parquet SET email = "hello@world.com" WHERE car_make = "Audi"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time Travel and Versioning

# COMMAND ----------

# MAGIC %sql 
# MAGIC UPDATE customers_delta SET email = "hello1@world.com" WHERE car_make = "Audi";
# MAGIC UPDATE customers_delta SET email = "hello2@world.com" WHERE car_make = "Audi";
# MAGIC UPDATE customers_delta SET email = "hello3@world.com" WHERE car_make = "Audi";

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from customers_delta version as of 2 where car_make = "Audi"

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC describe history customers_delta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Restore a table to earlier version

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table customers_delta  TO VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers_delta where car_make = "Audi"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema enforcement

# COMMAND ----------

cd = spark.table("customers_delta")
cp = spark.table("customers_parquet")

# COMMAND ----------

cd.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC insert into table customers_delta values ("fmcar",2000,"x@x.org","test",9999,"dummy","extra")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ##Streaming

# COMMAND ----------

streamingDf = spark.readStream.format("rate").load()

# COMMAND ----------

streamwriter = streamingDf.selectExpr("value as id").writeStream.format("delta").option("checkpointLocation", "/tmp/checkpoint").start("/tmp/delta-tablefm")

# COMMAND ----------

streamwriter.status


# COMMAND ----------

df = spark.read.format("delta").load("/tmp/delta-tablefm")


# COMMAND ----------

df.count()

# COMMAND ----------

reader = spark.readStream.format("delta").load("/tmp/delta-tablefm").writeStream.format("console").start()

# COMMAND ----------

streamwriter2 = streamingDf.selectExpr("value as id").writeStream.format("delta").option("checkpointLocation", "/tmp/checkpoint2").start("/tmp/delta-tablefm")
