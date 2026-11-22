# Databricks notebook source
# DBTITLE 1,Importing Libraries
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Read the Silver Layer
silver_df = spark.table('workspace.default.cricket_silver_current_matches')

display(silver_df)

# COMMAND ----------

# DBTITLE 1,GOLD Analytics 1: Match type Distribution
gold_match_type_df = silver_df.groupBy('match_type').agg(count('*').alias('Total_Matches'))

display(gold_match_type_df)

# COMMAND ----------

# DBTITLE 1,GOLD Analytics 2: Venue-wise Match count
gold_venue_df = silver_df.groupBy('venue').agg(count('*').alias('Total_Matches'))

display(gold_venue_df)

# COMMAND ----------

# DBTITLE 1,GOLD Analytics 3: Team Wise Match Count
team1_df = silver_df.select(col("team_1").alias("team"))
team2_df = silver_df.select(col("team_2").alias("team"))

all_team_df = team1_df.union(team2_df)
gold_team_df = all_team_df.groupBy('team')\
    .agg(count('*').alias("matches_played"))

display(gold_team_df)

# COMMAND ----------

# DBTITLE 1,Final Analytics Queries
#Match Overview

display(spark.sql("""SELECT count(*) as Total_matches,
count(distinct match_type) as total_match_types,
count(distinct venue) as total_venues
FROM workspace.default.cricket_silver_current_matches"""))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) as Total_matches,
# MAGIC count(distinct match_type) as total_match_types,
# MAGIC count(distinct venue) as total_venues
# MAGIC FROM workspace.default.cricket_silver_current_matches

# COMMAND ----------

