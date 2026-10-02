import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# -----------------------------------------
# Load sample output files
# -----------------------------------------
batsmen = pd.read_csv("output/top_batsmen.csv")
bowlers = pd.read_csv("output/top_bowlers.csv")
centuries = pd.read_csv("output/most_centuries.csv")

# -----------------------------------------
# 1. Top Batsmen Graph
# -----------------------------------------
plt.figure()
plt.bar(batsmen["batsman"][:10], batsmen["total_runs"][:10])
plt.xticks(rotation=45)
plt.title("Top 10 Batsmen by Runs")
plt.xlabel("Player")
plt.ylabel("Runs")
plt.tight_layout()
plt.savefig("images/top_batsmen.png")

# -----------------------------------------
# 2. Top Bowlers Graph
# -----------------------------------------
plt.figure()
plt.bar(bowlers["bowler"][:10], bowlers["wickets"][:10])
plt.xticks(rotation=45)
plt.title("Top 10 Bowlers by Wickets")
plt.xlabel("Bowler")
plt.ylabel("Wickets")
plt.tight_layout()
plt.savefig("images/top_bowlers.png")

# -----------------------------------------
# 3. Most Centuries Graph
# -----------------------------------------
plt.figure()
plt.bar(centuries["batsman"], centuries["century_count"])
plt.xticks(rotation=45)
plt.title("Most Centuries in IPL")
plt.xlabel("Player")
plt.ylabel("Centuries")
plt.tight_layout()
plt.savefig("images/centuries.png")

print("Graphs saved in images folder")