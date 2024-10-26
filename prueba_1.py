# Databricks notebook source
# MAGIC %md
# MAGIC ### Análisis exploratorio de datos

# COMMAND ----------

print("Hello world")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

dl_location = 'abfss://dataengineering@bidatarepositoryg3yhinca.dfs.core.windows.net/'  #Agregar la dirección del datalake
raw_location = dl_location + 'RAW/'
print(raw_location)

# COMMAND ----------

dbutils.fs.ls(raw_location) #Permite buscar los archivos

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Inspeccionamos cómo se ven los archivos del data lake

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Primero exploremos cómo se ven las carpetas que cargamos en el data lake

# COMMAND ----------

dbutils.fs.ls(raw_location) #Permite buscar los archivos
