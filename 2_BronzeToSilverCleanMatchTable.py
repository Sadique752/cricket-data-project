# Databricks notebook source
# DBTITLE 1,Importing Libraries
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Read the bronze table
bronze_df = spark.table('workspace.default.cricket_bronze_current_matches')

raw_json = bronze_df.select("raw_json").collect()[0]['raw_json']
api_data = json.loads(raw_json)

matches = api_data.get("data",[])

print("Total Matches Found: ", len(matches))
print(matches[0] if len(matches) > 0 else "NO MATCHES FOUND")

# COMMAND ----------

# DBTITLE 1,Extract Only the Useful Fields
silver_rows = []

for match in matches:
    teams=match.get("teams",[])
    score=match.get("score",[])

    team_1=teams[0] if len(teams)>0 else None
    team_2=teams[1] if len(teams)>1 else None

    score_1 = None
    score_2 = None

    if len(score)>0:
        s1 = score[0]
        #Formating it in real score display example 180/5 in 20 overs
        score_1 = f"{s1.get('r',0)}/{s1.get('w', 0)} in {s1.get('o', 0)} overs"
    
    if len(score)>1:
        s2 = score[1]
        score_2 = f"{s2.get('r',0)}/{s2.get('w', 0)} in {s2.get('o', 0)} overs"

    silver_rows.append({
        "match_id": match.get("id"),
        "match_name": match.get("name"),
        "match_type": match.get("matchType"),
        "status": match.get("status"),
        "venue": match.get("venue"),
        "date": match.get("date"),
        "date_time_gmt": match.get("dateTimeGMT"),
        "team_1": team_1,
        "team_2": team_2,
        "score_1": score_1,
        "score_2": score_2,
        "series_id": match.get("series_id"),
        "match_started": match.get("matchStarted"),
        "match_ended": match.get("matchEnded"),
        "fantasy_enabled": match.get("fantasyEnabled"),
        "has_squad": match.get("hasSquad")
    })
print('Silver rows prepared:', len(silver_rows))

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType
)

silver_schema = StructType([
    StructField("match_id",       StringType(),  nullable=False),
    StructField("match_name",     StringType(),  nullable=True),
    StructField("match_type",     StringType(),  nullable=True),
    StructField("status",         StringType(),  nullable=True),
    StructField("venue",           StringType(),  nullable=True),
    StructField("date",            StringType(),  nullable=True),
    StructField("date_time_gmt",  StringType(),  nullable=True),
    StructField("team_1",          StringType(),  nullable=True),
    StructField("team_2",          StringType(),  nullable=True),
    StructField("score_1",         StringType(),  nullable=True),
    StructField("score_2",         StringType(),  nullable=True),
    StructField("series_id",       StringType(),  nullable=True),
    StructField("match_started",  BooleanType(), nullable=True),
    StructField("match_ended",     BooleanType(), nullable=True),
    StructField("fantasy_enabled", BooleanType(), nullable=True),
    StructField("has_squad",       BooleanType(), nullable=True),
])

# Create the silver DataFrame with the explicit schema
silver_df = spark.createDataFrame(data=silver_rows, schema=silver_schema)\
    .withColumn("match_date",to_date(col("date")))\
    .withColumn("loaded_at",current_timestamp())

display(silver_df)

# COMMAND ----------

silver_df.write\
    .format('delta')\
    .mode('overwrite')\
    .option('overwriteSchema','true')\
    .saveAsTable('workspace.default.cricket_silver_current_matches')

print("Silver Table created Successfully")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.default.cricket_silver_current_matches

# COMMAND ----------

