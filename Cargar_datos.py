# Databricks notebook source
# MAGIC %md
# MAGIC ## Librerias ##

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
import uuid

# COMMAND ----------

# MAGIC %md
# MAGIC ## Variables del proyecto ##

# COMMAND ----------

# Variables de ubicación de archivos
dl_location = 'abfss://dataengineering@bidatarepositoryg3jvegas.dfs.core.windows.net/'
raw_location = dl_location + 'RAW/'

uc_location_aumented = 'formula1.aumented.' ## unity catalog

# Otras variables
date_format = 'dd/MM/yyyy'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema para cada tabla ##

# COMMAND ----------

## Tabla circuits
df_circuits_schema = StructType(fields=[
    StructField('circuitId', IntegerType(), False),
    StructField('circuitRef', StringType(), False),
    StructField('name', StringType(), False),
    StructField('location', StringType(), True),
    StructField('country', StringType(), True),
    StructField('lat', FloatType(), True),
    StructField('lng', FloatType(), True),
    StructField('alt', IntegerType(), True),
    StructField('url', StringType(), False)
])

# COMMAND ----------

## Tabla circuits
df_circuits_schema = StructType(fields=[
    StructField('circuitId', IntegerType(), False),
    StructField('circuitRef', StringType(), False),
    StructField('name', StringType(), False),
    StructField('location', StringType(), True),
    StructField('country', StringType(), True),
    StructField('lat', FloatType(), True),
    StructField('lng', FloatType(), True),
    StructField('alt', IntegerType(), True),
    StructField('url', StringType(), False)
])

# COMMAND ----------

## Tabla drivers
df_drivers_schema = StructType(fields=[
    StructField('driverId', IntegerType(), False),
    StructField('driverRef', StringType(), False),
    StructField('number', IntegerType(), True),
    StructField('code', StringType(), True),
    StructField('forename', StringType(), False),
    StructField('surname', StringType(), False),
    StructField('dob', DateType(), True),
    StructField('nationality', StringType(), True),
    StructField('url', StringType(), False)
])

# COMMAND ----------

## Tabla pit_stops
df_pit_stops_schema = StructType(fields=[
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), False),
    StructField('stop', IntegerType(), False),
    StructField('lap', IntegerType(), False),
    StructField('time', TimestampType(), False),
    StructField('duration', StringType(), True),
    StructField('milliseconds', IntegerType(), True)
])

# COMMAND ----------

## Tabla races
df_races_schema = StructType(fields=[
    StructField('raceId', IntegerType(), False),
    StructField('year', IntegerType(), False),
    StructField('round', IntegerType(), False),
    StructField('circuitId', IntegerType(), False),
    StructField('name', StringType(), False),
    StructField('date', DateType(), False),
    StructField('time', TimestampType(), True),
    StructField('url', StringType(), True),
    StructField('fp1_date', DateType(), True),
    StructField('fp1_time', TimestampType(), True),
    StructField('fp2_date', DateType(), True),
    StructField('fp2_time', TimestampType(), True),
    StructField('fp3_date', DateType(), True),
    StructField('fp3_time', TimestampType(), True),
    StructField('quali_date', DateType(), True),
    StructField('quali_time', TimestampType(), True),
    StructField('sprint_date', DateType(), True),
    StructField('sprint_time', TimestampType(), True)
])

# COMMAND ----------

# Tabla results
df_results_schema = StructType(fields=[
    StructField('resultId', IntegerType(), False),
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), False),
    StructField('constructorId', IntegerType(), False),
    StructField('number', IntegerType(), True),
    StructField('grid', IntegerType(), False),
    StructField('position', IntegerType(), True),
    StructField('positionText', StringType(), False),
    StructField('positionOrder', IntegerType(), False),
    StructField('points', FloatType(), False),
    StructField('laps', IntegerType(), False),
    StructField('time', StringType(), True),
    StructField('milliseconds', IntegerType(), True),
    StructField('fastestLap', IntegerType(), True),
    StructField('rank', IntegerType(), True),
    StructField('fastestLapTime', StringType(), True),
    StructField('fastestLapSpeed', StringType(), True),
    StructField('statusId', IntegerType(), False)
])

# COMMAND ----------

## Tabla lap_times
df_lap_times_schema = StructType(fields=[
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), False),
    StructField('lap', IntegerType(), False),
    StructField('position', IntegerType(), True),
    StructField('time', StringType(), True),
    StructField('milliseconds', IntegerType(), True)
])

# COMMAND ----------

## Tabla qualifying
df_qualifying_schema = StructType(fields=[
    StructField('qualifyId', IntegerType(), False),
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), False),
    StructField('constructorId', IntegerType(), False),
    StructField('number', IntegerType(), False),
    StructField('position', IntegerType(), True),
    StructField('q1', StringType(), True),
    StructField('q2', StringType(), True),
    StructField('q3', StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1.tracking
# MAGIC COMMENT 'En este schema va a ir guardada la informacion agregada'
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS formula1.tracking.summary(
# MAGIC     TRANSACTION_DATE TIMESTAMP,
# MAGIC     FOLDER_NAME STRING,
# MAGIC     UUID STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1.aumented
# MAGIC COMMENT 'En este schema va a ir guardada la informacion agregada'
# MAGIC ;

# COMMAND ----------

folders = dbutils.fs.ls(raw_location)
print(folders)

# COMMAND ----------

folders2 = dbutils.fs.ls(raw_location + 'lap_times')
print(folders2)

# COMMAND ----------

folders3 = dbutils.fs.ls(raw_location + 'qualifying')
print(folders3)

# COMMAND ----------

raw_folders = dbutils.fs.ls(raw_location)
raw_folders = [folder[0] for folder in raw_folders]
raw_folders

# COMMAND ----------

raw_folders2 = dbutils.fs.ls(raw_location + 'lap_times')
raw_folders2 = [folder[0] for folder in raw_folders2]
raw_folders2

# COMMAND ----------

raw_folders3 = dbutils.fs.ls(raw_location + 'qualifying')
raw_folders3 = [folder[0] for folder in raw_folders3]
raw_folders3

# COMMAND ----------


