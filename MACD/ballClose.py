import pandas as pd
import matplotlib.pyplot as plt

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

# # 計算柱狀圖
# df['Histogram'] = df['MACD'] - df['Signal']

# # 繪製圖表
# plt.figure(figsize=(12, 6))

# # 繪製收盤價
# plt.subplot(2, 1, 1)
# plt.plot(df['Date'], df['Close'], label='Close Price', color='blue')
# plt.title('Stock Price and MACD')
# plt.legend()

# # 繪製 MACD 和信號線
# plt.subplot(2, 1, 2)
# plt.plot(df['Date'], df['MACD'], label='MACD', color='green')
# plt.plot(df['Date'], df['Signal'], label='Signal Line', color='red')
# plt.bar(df['Date'], df['Histogram'],
#         label='Histogram', color='gray', alpha=0.5)
# plt.legend()

# plt.tight_layout()
# plt.show()
# endregion
