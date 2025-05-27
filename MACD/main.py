from RSI import RSIPlot
import matplotlib.pyplot as plt
import pandas as pd

from db import MongoDbContext

# region RSI Plot Example
prices = pd.Series([45, 46, 47, 46, 45, 44, 43,
                    42, 43, 44, 45, 46, 47, 48, 49])

# 初始化 RSIPlot 並加載數據
rsi_plot = RSIPlot(prices)
rsi_plot.load()
rsi_plot.plot("RSI Plot")
rsi_plot1 = RSIPlot(prices)
rsi_plot1.load()
rsi_plot1.plot("RSI Plot1")
plt.show()
# endregion


db = MongoDbContext("localhost", "bingo")
table = "bingo_accum_times"
queryKey = {}

querys = db.Find(table, queryKey).sort("DrawTerm", -1)
results = list(querys)
results = results[:30]
resultAsc = sorted(results, key=lambda x: x["DrawTerm"])
