import pandas as pd
import matplotlib.pyplot as plt
from typing import List, Dict
from IPlot import IPlot
from db import MongoDbContext

# region example
# # 假設有一個 DataFrame 包含日期和收盤價
# # 示例數據
# data = {
#     'Date': pd.date_range(start='2025-01-01', periods=100),
#     'Close': [i + (i * 0.02) for i in range(100)]
# }
# df = pd.DataFrame(data)

# # 計算快速和慢速 EMA
# df['EMA12'] = df['Close'].ewm(span=12, adjust=False).mean()
# df['EMA26'] = df['Close'].ewm(span=26, adjust=False).mean()

# # 計算 MACD 和信號線
# df['MACD'] = df['EMA12'] - df['EMA26']
# df['Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()

# # # 計算柱狀圖
# # df['Histogram'] = df['MACD'] - df['Signal']

# output_file = 'MACD_Output.xlsx'
# df.to_excel(output_file, index=False)
# print(f"DataFrame 已保存到 {output_file}")


# # 繪製圖表
# plt.figure(figsize=(12, 6))

# # # 繪製收盤價
# # plt.subplot(2, 1, 1)
# # plt.plot(df['Date'], df['Close'], label='Close Price', color='blue')
# # plt.title('Stock Price and MACD')
# # plt.legend()

# # 繪製 MACD 和信號線
# plt.subplot(2, 1, 2)
# plt.plot(df['Date'], df['MACD'], label='MACD', color='green')
# plt.plot(df['Date'], df['Signal'], label='Signal Line', color='red')
# # plt.bar(df['Date'], df['Histogram'],
# #         label='Histogram', color='gray', alpha=0.5)
# plt.legend()

# plt.tight_layout()
# plt.show()
# endregion


class MACDPlot(IPlot):

    @property
    def isMACDBig(self):
        return self._df['isMACDBig'].iloc[-1]

    def __init__(self, result, date_field, close_field):
        self._date_field = date_field  # 初始化為 None
        self._close_field = close_field  # 初始化為 None
        self._result = result
        self._df = None  # 初始化為 None
        pass

    def load(self):
        data = {'Date': [], 'Close': []}

        for index, result in enumerate(self._result):
            data['Date'].append(result[self._date_field])
            data['Close'].append(result[self._close_field])

        self._df = pd.DataFrame(data)

        # 計算快速和慢速 EMA
        self._df['EMA12'] = self._df['Close'].ewm(span=12, adjust=False).mean()
        self._df['EMA26'] = self._df['Close'].ewm(span=26, adjust=False).mean()

        # 計算 MACD 和信號線
        self._df['MACD'] = self._df['EMA12'] - self._df['EMA26']
        self._df['Signal'] = self._df['MACD'].ewm(span=9, adjust=False).mean()
        self._df['isMACDBig'] = self._df['MACD'] > self._df['Signal']
        print(self._df)

    def plot(self, name):
        # 繪製圖表
        plt.figure(figsize=(8, 5))
        plt.plot(self._df['Date'], self._df['MACD'],
                 label='MACD', color='green')
        plt.plot(self._df['Date'], self._df['Signal'],
                 label='Signal Line', color='red')
        plt.legend()
        plt.title(f"MACD Plot ({self._close_field})")
        plt.legend()
        # plt.tight_layout()
        pass


# 主程式
db = MongoDbContext("localhost", "bingo")
table = "bingo_accum_times"
queryKey = {}

querys = db.Find(table, queryKey).sort("DrawTerm", -1)
results = list(querys)
results = results[:30]
resultAsc = sorted(results, key=lambda x: x["DrawTerm"])
print("")


# ball_field = 'Ball' + str(i).zfill(2)
ball_field69 = 'Ball69'
ball_field70 = 'Ball70'

macd_plot69 = MACDPlot(
    result=resultAsc, date_field='DrawTerm', close_field=ball_field69)
macd_plot69.load()
macd_plot69.plot(name=ball_field69)
macd_plot70 = MACDPlot(
    result=resultAsc, date_field='DrawTerm', close_field=ball_field70)
macd_plot70.load()
macd_plot70.plot(name='Ball70')
plt.show()

# ballFields = []
# # 繪製第一個視窗的圖表
# idxStart = 1
# idxEnd = 40
# for i in range(idxStart, idxEnd):
#     # fig1, ax1 = plt.subplots(figsize=(12, 6))  # 第一個視窗
#     ball_field = 'Ball' + str(i).zfill(2)
#     macd_plot1 = MACDPlot()
#     isShow = macd_plot1.getShow(resultAsc, date_field='DrawTerm',
#                                 close_field=ball_field)
#     if isShow:
#         ballFields.append(ball_field)

# ballFieldEnd = ballFields[-1]
# for ballField in ballFields:
#     fig1, ax1 = plt.subplots(figsize=(12, 6))
#     macd_plot1 = MACDPlot()
#     macd_plot1.plot(ax1, resultAsc, date_field='DrawTerm',
#                     close_field=ballField)
#     plt.tight_layout()
#     if ballField != ballFieldEnd:
#         plt.show(block=False)  # 顯示第一個視窗，但不阻塞程式執行
#     else:
#         plt.show()

# print("")

# region 圖表 example

# fig1, ax1 = plt.subplots(figsize=(12, 6))  # 第一個視窗
# macd_plot1 = MACDPlot()
# macd_plot1.plot(ax1, resultAsc, date_field='DrawTerm', close_field='Ball69')
# plt.tight_layout()
# plt.show(block=False)  # 顯示第一個視窗，但不阻塞程式執行

# # # 繪製第二個視窗的圖表
# fig2, ax2 = plt.subplots(figsize=(12, 6))  # 第二個視窗
# macd_plot2 = MACDPlot()
# macd_plot2.plot(ax2, resultAsc, date_field='DrawTerm', close_field='Ball70')
# plt.tight_layout()
# plt.show(block=False)  # 顯示第二個視窗

# fig3, ax3 = plt.subplots(figsize=(12, 6))  # 第二個視窗
# macd_plot3 = MACDPlot()
# macd_plot3.plot(ax3, resultAsc, date_field='DrawTerm', close_field='Ball71')
# plt.tight_layout()
# plt.show()  # 顯示第二個視窗


# ## 多個子圖
# import matplotlib.pyplot as plt
# import numpy as np

# # 創建數據
# x = np.linspace(0, 10, 100)
# y1 = np.sin(x)
# y2 = np.cos(x)
# y3 = np.tan(x)

# # 創建多個子圖
# fig, axs = plt.subplots(3, 1, figsize=(8, 12))  # 3 行 1 列的子圖

# # 繪製第一張圖
# axs[0].plot(x, y1, label='sin(x)', color='blue')
# axs[0].set_title('Sine Function')
# axs[0].legend()

# # 繪製第二張圖
# axs[1].plot(x, y2, label='cos(x)', color='green')
# axs[1].set_title('Cosine Function')
# axs[1].legend()

# # 繪製第三張圖
# axs[2].plot(x, y3, label='tan(x)', color='red')
# axs[2].set_title('Tangent Function')
# axs[2].legend()

# # 調整布局並顯示圖表
# plt.tight_layout()
# plt.show()

# ## 多個視窗
# # 第一個視窗
# plt.figure()
# plt.plot(x, y1, label='sin(x)')
# plt.title('Sine Function')
# plt.legend()
# plt.show()

# # 第二個視窗
# plt.figure()
# plt.plot(x, y2, label='cos(x)')
# plt.title('Cosine Function')
# plt.legend()
# plt.show()
# endregion
