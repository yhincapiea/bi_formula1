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

## Tabla constructors
df_constructors_schema = StructType(fields=[
    StructField('constructorId', IntegerType(), False),
    StructField('constructorRef', StringType(), True),
    StructField('name', StringType(), True),
    StructField('nationality', StringType(), True),
    StructField('url', StringType(), True)
])

# COMMAND ----------

## Tabla drivers
df_drivers_schema = StructType(
    fields=[
        StructField('driverId', IntegerType(), True),
        StructField('driverRef', StringType(), True),
        StructField('number', IntegerType(), True),
        StructField('code', StringType(), True),
        StructField("name", StructType([
        StructField("forename", StringType(), True),
        StructField("surname", StringType(), True)]), True),
        StructField('dob', StringType(), True),
        StructField('nationality', StringType(), True),
        StructField('url', StringType(), True)
    ]
)

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
    StructField('date', StringType(), False),
    StructField('time', StringType(), True),
    StructField('url', StringType(), True)
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
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1.plata
# MAGIC COMMENT 'En este schema va a ir guardada la informacion agregada'
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1.oro
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

# MAGIC %md
# MAGIC ## Funciones

# COMMAND ----------

def writing_info(df: DataFrame, catalogo: str, table_name: str):
    df.write \
    .mode("append") \
    .format("delta") \
    .saveAsTable(f"{catalogo}.{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga de datos

# COMMAND ----------

# Cargar datos circuits
df_circuits = (
    spark
    .read
    .format('csv')
    .schema(df_circuits_schema)
    .option('header', True)
    .load(f'{raw_location}/circuits.csv')
)

display(df_circuits)

# COMMAND ----------

writing_info(df_circuits,'formula1.oro','circuits')

# COMMAND ----------

# Caregar datos constructors
df_constructors = (
    spark
    .read
    .format('json')
    .schema(df_constructors_schema)
    .option('header', True)
    .load(f'{raw_location}/constructors.json')
)

display(df_constructors)

# COMMAND ----------

writing_info(df_constructors,'formula1.oro','constructors')

# COMMAND ----------

# Cargar datos drivers
df_drivers = (
    spark
    .read
    .format('json')
    .schema(df_drivers_schema)
    .option('header', True)
    .load(f'{raw_location}/drivers.json')
)

display(df_drivers)

# COMMAND ----------

# Seleccionar y renombrar las columnas deseadas
df_drivers = df_drivers.select(
    col("driverId"),
    col("driverRef"),
    col("number"),
    col("code"),
    col("name.forename").alias("forename"),
    col("name.surname").alias("surname"),
    col("dob"),
    col("nationality"),
    col("url")
)
display(df_drivers)

# COMMAND ----------

df_drivers = df_drivers.withColumn("dob", to_date("dob", "yyyy-MM-dd")) 
display(df_drivers)

# COMMAND ----------

from pyspark.sql.functions import coalesce, lit
from pyspark.sql.functions import when

# Reemplazar los valores nulos en la columna 'number' con 0
df_drivers = df_drivers.withColumn("number", coalesce("number", lit(0)))

# Reemplazar valores '\N' en la columna 'code' con 'sin registro'
df_drivers = df_drivers.withColumn("code", when(df_drivers["code"] == "\\N", "Sin registro").otherwise(df_drivers["code"]))

display(df_drivers)

# COMMAND ----------

writing_info(df_drivers,'formula1.plata','drivers')

# COMMAND ----------

# Cargar datos races
df_races = (
    spark
    .read
    .format('csv')
    .schema(df_races_schema)
    .option('header', True)
    .load(f'{raw_location}/races.csv')
)

display(df_races)

# COMMAND ----------

df_races = df_races.withColumn("date", to_date("date", "yyyy-MM-dd")) ## columna "date" a formato fecha
df_races = df_races.withColumn("time", when(df_races["time"] == "\\N", "00:00:00").otherwise(df_races["time"])) ## remplazar \N por 00:00:00
df_races = df_races.withColumn("time", expr("make_timestamp(year(date), month(date), day(date), split(time, ':')[0], split(time, ':')[1], split(time, ':')[2])")) ## columna "time" a formato tiempo
display(df_races)

# COMMAND ----------

writing_info(df_races,'formula1.plata','races')

# COMMAND ----------

# Cargar datos qualifying
df_qualifying = (
    spark
    .read
    .format('json')
    .schema(df_qualifying_schema)
    .option('multiline', True)
    .load(f'{raw_location}/qualifying/*.json')
)

display(df_qualifying)

# COMMAND ----------

from pyspark.sql.functions import when

df_qualifying = df_qualifying.withColumn("q1", when(df_qualifying["q1"] == "\\N", "Sin registro").otherwise(df_qualifying["q1"]))
df_qualifying = df_qualifying.withColumn("q2", when(df_qualifying["q2"] == "\\N", "Sin registro").otherwise(df_qualifying["q2"]))
df_qualifying = df_qualifying.withColumn("q3", when(df_qualifying["q3"] == "\\N", "Sin registro").otherwise(df_qualifying["q3"]))

display(df_qualifying)

# COMMAND ----------

writing_info(df_qualifying,'formula1.plata','qualifying')

# COMMAND ----------

# Cargar datos results
df_results = (
    spark
    .read
    .format('json')
    .schema(df_results_schema)
    .option('header', True)
    .load(f'{raw_location}/results.json')
)

display(df_results)

# COMMAND ----------

from pyspark.sql.functions import when, col

# Reemplazar el valor '\N' con 'No registra' en todas las columnas
df_results = df_results.select(
    [when(col(c) == "\\N", "No registra").otherwise(col(c)).alias(c) for c in df_results.columns]
)

df_results = df_results.fillna(-1)
display(df_results)

# COMMAND ----------

writing_info(df_results,'formula1.plata','results')

# COMMAND ----------

# Cargar datos pit stops
df_pit_stops = (
    spark
    .read
    .format('json')
    .schema(df_pit_stops_schema)
    .option('multiline', True)
    .load(f'{raw_location}/pit_stops.json')
)

display(df_pit_stops)

# COMMAND ----------

writing_info(df_pit_stops,'formula1.oro','pit_stops')

# COMMAND ----------

# Cargar datos lap times
df_lap_times = (
    spark
    .read
    .format('csv')
    .schema(df_lap_times_schema)
    .option('header', True)
    .load(f'{raw_location}/lap_times/*.csv')
)

display(df_lap_times)

# COMMAND ----------

writing_info(df_lap_times,'formula1.oro','lap_times')

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS formula1.oro.drivers;
# MAGIC DROP TABLE IF EXISTS formula1.oro.qualifying;
# MAGIC DROP TABLE IF EXISTS formula1.oro.races;
# MAGIC DROP TABLE IF EXISTS formula1.oro.results;

# COMMAND ----------


