# Databricks notebook source
var = dbutils.jobs.taskValues.get(taskKey="weekday_lookup",key="weekoutput")

# COMMAND ----------

print(var)