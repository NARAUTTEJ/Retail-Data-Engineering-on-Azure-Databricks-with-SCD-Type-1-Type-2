# Databricks notebook source
dataset = [
    {
        "file_name" : "orders"
    },
    {
        "file_name" : "products"
    },
    {
        "file_name" : "customers"
    }
]

# COMMAND ----------

dbutils.jobs.taskValues.set("output_dataset",dataset)