# Databricks notebook source
files = [
    {
        "sourcefolder" : "netflix_diretors",
        "targetfolder" : "netflix_directors",
    },
    {
        "sourcefolder" : "netflix_cast",
        "targetfolder" : "netflix_cast",
    },{
        "sourcefolder" : "netflix_countries",
        "targetfolder" : "netflix_countries",
    },{
        "sourcefolder" : "netflix_category",
        "targetfolder" : "netflix_category",
    }
]

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "my_arr", value = files)