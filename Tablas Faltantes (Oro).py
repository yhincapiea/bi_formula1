# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
import uuid

# COMMAND ----------

# Variables de ubicación de archivos
silver = 'formula1.plata.'
gold = 'formula1.oro.'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.oro.circuits

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.oro.constructors

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.oro.lap_times

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.oro.pit_stops

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS formula1.oro.drivers AS
# MAGIC SELECT * FROM formula1.plata.drivers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.oro.drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS formula1.oro.qualifying AS
# MAGIC SELECT * FROM formula1.plata.qualifying;
# MAGIC
# MAGIC SELECT * FROM formula1.oro.qualifying;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS formula1.oro.races AS
# MAGIC SELECT * FROM formula1.plata.races;
# MAGIC
# MAGIC SELECT * FROM formula1.oro.races;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS formula1.oro.results AS
# MAGIC SELECT * FROM formula1.plata.results;
# MAGIC
# MAGIC SELECT * FROM formula1.oro.results;

# COMMAND ----------

# df_seasons_schema = StructType(
#   fields=[
#     StructField('year', IntegerType(),True),
#     StructField('url', StringType(), True)
# ])

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS formula1.oro.seasons AS
# MAGIC SELECT 
# MAGIC   year, url
# MAGIC FROM
# MAGIC   formula1.oro.races;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.oro.seasons

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS formula1.oro.driverStandings AS
# MAGIC SELECT 
# MAGIC   ROW_NUMBER() OVER (ORDER BY raceId, driverId) AS driverStandingsId,
# MAGIC   raceId,
# MAGIC   driverId,
# MAGIC   points,
# MAGIC   position,
# MAGIC   positionText
# MAGIC
# MAGIC FROM
# MAGIC   formula1.oro.results;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.oro.driverStandings

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS formula1.oro.constructorStandings AS
# MAGIC SELECT 
# MAGIC   ROW_NUMBER() OVER (ORDER BY raceId, constructorId) AS constructorStandingsId,
# MAGIC   raceId,
# MAGIC   constructorId,
# MAGIC   points,
# MAGIC   position,
# MAGIC   positionText
# MAGIC
# MAGIC FROM
# MAGIC   formula1.oro.results;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.oro.constructorStandings

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS formula1.oro.constructorResults AS
# MAGIC SELECT 
# MAGIC   ROW_NUMBER() OVER (ORDER BY raceId, constructorId) AS constructorResultsId,
# MAGIC   raceId,
# MAGIC   constructorId,
# MAGIC   points
# MAGIC FROM
# MAGIC   formula1.oro.results;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.oro.constructorResults

# COMMAND ----------


