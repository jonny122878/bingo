import pandas as pd
import matplotlib.pyplot as plt
from typing import List, Dict
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


class MACDPlot:
    def __init__(self):
        pass

    def getShow(self, results, date_field: str, close_field: str) -> bool:
        data = {'Date': [], 'Close': []}

        for index, result in enumerate(results):
            data['Date'].append(result[date_field])
            data['Close'].append(result[close_field])

        df = pd.DataFrame(data)

        # 計算快速和慢速 EMA
        df['EMA12'] = df['Close'].ewm(span=12, adjust=False).mean()
        df['EMA26'] = df['Close'].ewm(span=26, adjust=False).mean()

        # 計算 MACD 和信號線
        df['MACD'] = df['EMA12'] - df['EMA26']
        df['Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
        df['isMACDBig'] = df['MACD'] > df['Signal']
        return df['isMACDBig'].iloc[-1]

    def plot(self, ax, results, date_field: str, close_field: str):
        """
        繪製 MACD 圖表
        :param ax: matplotlib 的軸對象，用於繪製圖表
        :param results: 資料來源 (list of dict)
        :param date_field: 日期欄位名稱
        :param close_field: 收盤價欄位名稱
        """
        data = {'Date': [], 'Close': []}

        for index, result in enumerate(results):
            data['Date'].append(result[date_field])
            data['Close'].append(result[close_field])

        df = pd.DataFrame(data)

        # 計算快速和慢速 EMA
        df['EMA12'] = df['Close'].ewm(span=12, adjust=False).mean()
        df['EMA26'] = df['Close'].ewm(span=26, adjust=False).mean()

        # 計算 MACD 和信號線
        df['MACD'] = df['EMA12'] - df['EMA26']
        df['Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
        df['isMACDBig'] = df['MACD'] > df['Signal']

        # 繪製圖表
        ax.plot(df['Date'], df['MACD'], label='MACD', color='green')
        ax.plot(df['Date'], df['Signal'], label='Signal Line', color='red')
        ax.legend()
        ax.set_title(f"MACD Plot ({close_field})")


        # 主程式
db = MongoDbContext("localhost", "bingo")
table = "bingo_accum_times"
queryKey = {}

querys = db.Find(table, queryKey).sort("DrawTerm", -1)
results = list(querys)
results = results[:30]
resultAsc = sorted(results, key=lambda x: x["DrawTerm"])
print("")

# 將 results 加載到 DataFrame
# df = pd.DataFrame(results)

# # 將 DataFrame 導出為 Excel 文件
# output_file = 'MACD_times.xlsx'
# df.to_excel(output_file, index=False, encoding='utf-8')

ballFields = []
# 繪製第一個視窗的圖表
idxStart = 1
idxEnd = 40
for i in range(idxStart, idxEnd):
    # fig1, ax1 = plt.subplots(figsize=(12, 6))  # 第一個視窗
    ball_field = 'Ball' + str(i).zfill(2)
    macd_plot1 = MACDPlot()
    isShow = macd_plot1.getShow(resultAsc, date_field='DrawTerm',
                                close_field=ball_field)
    if isShow:
        ballFields.append(ball_field)
    # plt.tight_layout()
    # if i != idxEnd and isShow:
    #     plt.show(block=False)  # 顯示第一個視窗，但不阻塞程式執行
    # elif isShow:
    #     plt.show()
ballFieldEnd = ballFields[-1]
for ballField in ballFields:
    fig1, ax1 = plt.subplots(figsize=(12, 6))
    macd_plot1 = MACDPlot()
    macd_plot1.plot(ax1, resultAsc, date_field='DrawTerm',
                    close_field=ballField)
    plt.tight_layout()
    if ballField != ballFieldEnd:
        plt.show(block=False)  # 顯示第一個視窗，但不阻塞程式執行
    else:
        plt.show()

print("")
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
