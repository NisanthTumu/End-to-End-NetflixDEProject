# Databricks notebook source
# MAGIC %sql
# MAGIC create schema netflix_catalog.net_schema;

# COMMAND ----------

checkpoint_loc = "abfss://silver@netflixprojectdlnis.dfs.core.windows.net/checkpoint"

# COMMAND ----------



# COMMAND ----------

df = spark.readStream.format("cloudfiles")\
.option("cloudFiles.format", "csv")\
.option("cloudFiles.schemaLocation", checkpoint_loc)\
.load("abfss://raw@netflixprojectdlnis.dfs.core.windows.net")

# COMMAND ----------

df.display()

# COMMAND ----------

df.writeStream.option("checkpointLocation", checkpoint_loc)\
    .trigger(processingTime='10 seconds')\
    .start("abfss://bronze@netflixprojectdlnis.dfs.core.windows.net/netflix_titles")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

