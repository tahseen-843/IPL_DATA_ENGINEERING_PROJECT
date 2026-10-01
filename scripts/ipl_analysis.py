# IPL Data Engineering Project
# Author: Tahseen Nadaf

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# -----------------------------------------
# 1. Spark Session
# -----------------------------------------
spark = SparkSession.builder \
    .appName("IPL Data Engineering Project") \
    .getOrCreate()

# -----------------------------------------
# 2. Read Data (Local for GitHub)
# -----------------------------------------
matches = spark.read.csv("data/matches.csv", header=True, inferSchema=True)
deliveries = spark.read.csv("data/deliveries.csv", header=True, inferSchema=True)

# -----------------------------------------
# 3. Data Cleaning
# -----------------------------------------
matches = matches.dropDuplicates().dropna(subset=["id"])
deliveries = deliveries.dropDuplicates().dropna(subset=["match_id"])

# -----------------------------------------
# 4. Join Data
# -----------------------------------------
ipl_data = deliveries.join(
    matches,
    deliveries.match_id == matches.id,
    "inner"
)

# -----------------------------------------
# 5. Top Batsmen
# -----------------------------------------
top_batsman = deliveries.groupBy("batsman") \
    .agg(sum("batsman_runs").alias("total_runs")) \
    .orderBy(col("total_runs").desc())

# -----------------------------------------
# 6. Top Bowlers
# -----------------------------------------
top_bowlers = deliveries.filter(col("player_dismissed").isNotNull()) \
    .groupBy("bowler") \
    .agg(count("player_dismissed").alias("wickets")) \
    .orderBy(col("wickets").desc())

# -----------------------------------------
# 7. Team Wins
# -----------------------------------------
team_wins = matches.groupBy("winner") \
    .agg(count("*").alias("wins")) \
    .orderBy(col("wins").desc())

# -----------------------------------------
# 8. Strike Rate
# -----------------------------------------
strike_rate = ipl_data.groupBy("batsman") \
    .agg(
        (sum("batsman_runs") / count("ball") * 100).alias("strike_rate")
    ) \
    .orderBy(col("strike_rate").desc())

# -----------------------------------------
# 9. Player of Match
# -----------------------------------------
player_of_match = matches.groupBy("season", "player_of_match") \
    .agg(count("*").alias("awards")) \
    .orderBy("season", col("awards").desc())

# -----------------------------------------
# 10. Most Centuries
# -----------------------------------------
runs_per_match = deliveries.groupBy("match_id", "batsman") \
    .agg(sum("batsman_runs").alias("runs"))

centuries = runs_per_match.filter(col("runs") >= 100)

most_centuries = centuries.groupBy("batsman") \
    .agg(count("*").alias("centuries")) \
    .orderBy(col("centuries").desc())

# -----------------------------------------
# 11. Best Player Per Team
# -----------------------------------------
team_best = matches.groupBy("winner", "player_of_match") \
    .agg(count("*").alias("awards"))

windowSpec = Window.partitionBy("winner").orderBy(col("awards").desc())

best_player_each_team = team_best.withColumn(
    "rank",
    row_number().over(windowSpec)
).filter(col("rank") == 1).drop("rank")

# -----------------------------------------
# 12. Save Outputs
# -----------------------------------------
top_batsman.write.mode("overwrite").csv("output/top_batsman", header=True)
top_bowlers.write.mode("overwrite").csv("output/top_bowlers", header=True)
team_wins.write.mode("overwrite").csv("output/team_wins", header=True)
strike_rate.write.mode("overwrite").csv("output/strike_rate", header=True)
player_of_match.write.mode("overwrite").csv("output/player_of_match", header=True)
most_centuries.write.mode("overwrite").csv("output/most_centuries", header=True)
best_player_each_team.write.mode("overwrite").csv("output/best_player_each_team", header=True)

print("All outputs saved successfully")