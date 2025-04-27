# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("delta")\
    .option("header",True)\
    .option("inferSchema",True)\
    .load("abfss://bronze@netflixprojectdlnis.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn("duration_type", split(col("duration"), " ")[1])
    

# COMMAND ----------

df = df.withColumn("duration", split(col("duration"), " ")[0])

# COMMAND ----------

df = df.withColumn("short_title",split(col("title"),":")[0])

# COMMAND ----------

df = df.withColumn("duration", col("duration").cast(IntegerType()))\
    .withColumn("release_year", col("release_year").cast(IntegerType()))

# COMMAND ----------

df = df.fillna({"duration" : 0})

# COMMAND ----------

df = df.withColumn("type_flag",when(col("type") == "TV Show", 1)\
   .when(col("type") == "Movie", 2)\
      .otherwise(0))

# COMMAND ----------

from pyspark.sql import Window

# COMMAND ----------

df = df.withColumn("duaration_ranking", dense_rank().over(Window.orderBy(col('duration').desc())))

# COMMAND ----------

df_aggregated = df.groupBy("type").agg(
    count("*").alias("count"))

display(df_aggregated)

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .option("path","abfss://silver@netflixprojectdlnis.dfs.core.windows.net/netflix_titles")\
    .save()