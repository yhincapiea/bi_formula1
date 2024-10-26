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

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora inspeccionemos algúna carpeta aleatoria, Data_5 por ejemplo

# COMMAND ----------

dbutils.fs.ls(f"{raw_location}/circuits.csv")  #Agregar la ruta de la carpeta para cargar lo que aparece en la subcarpeta

# COMMAND ----------

# MAGIC %md
# MAGIC Tomamos algún archivo aleatorio para hacer una exploración de datos
# MAGIC
# MAGIC
# MAGIC Cargamos la información dentro en un dataframe de spark

# COMMAND ----------

df = (
    spark
    .read
    .format('csv')
    .option('header', True)
    .load("abfss://dataengineering@bidatarepositoryg3yhinca.dfs.core.windows.net/RAW/circuits.csv") #Añadir la ruta para cargar el dataframe de unos de los archivos 
)

display(df)

# COMMAND ----------

print("Hello Camilo")

# COMMAND ----------


