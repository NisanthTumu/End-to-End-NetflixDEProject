# Databricks notebook source
dbutils.widgets.text("sourcefolder","netflix_directors")
dbutils.widgets.text("targetfolder","netflix_directors")

# COMMAND ----------

var_src_folder = dbutils.widgets.get("sourcefolder")
var_tgt_folder = dbutils.widgets.get("targetfolder")

# COMMAND ----------

df= spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load(f"abfss://bronze@netflixprojectdlnis.dfs.core.windows.net/{var_src_folder}")

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("delta")\
    .mode("append")\
    .option("path",f"abfss://silver@netflixprojectdlnis.dfs.core.windows.net/{var_tgt_folder}")\
    .save()
