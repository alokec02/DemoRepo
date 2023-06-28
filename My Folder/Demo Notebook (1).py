# Databricks notebook source
print("Hello World!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Hello world from SQL!"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Title 1
# MAGIC ## Title 2
# MAGIC ### Title 3
# MAGIC
# MAGIC | user_id | user_name |
# MAGIC |---------|-----------|
# MAGIC |  1      |   Adam    |
# MAGIC |  2      |   Sarah   |
# MAGIC |  3      |   John    |

# COMMAND ----------

# MAGIC %run ./Setup

# COMMAND ----------

print(Full_name)

# COMMAND ----------

# MAGIC %fs ls '/databricks-datasets'

# COMMAND ----------

dbutils

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

files = dbutils.fs.ls('/databricks-datasets')
display(files)

# COMMAND ----------


