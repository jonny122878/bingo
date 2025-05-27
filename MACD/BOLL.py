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

# # 設定移動平均的窗口大小
# window = 20

# # 計算中軌線（SMA）
# df['SMA'] = df['Close'].rolling(window=window).mean()

# # 計算標準差
# df['STD'] = df['Close'].rolling(window=window).std()

# # 計算上軌線和下軌線
# df['Upper'] = df['SMA'] + (2 * df['STD'])
# df['Lower'] = df['SMA'] - (2 * df['STD'])

# # 繪製布林帶圖表
# plt.figure(figsize=(12, 6))
# plt.plot(df['Date'], df['Close'], label='Close Price', color='blue')
# plt.plot(df['Date'], df['SMA'], label='Middle Band (SMA)', color='orange')
# plt.plot(df['Date'], df['Upper'], label='Upper Band', color='green')
# plt.plot(df['Date'], df['Lower'], label='Lower Band', color='red')

# # 填充布林帶區域
# plt.fill_between(df['Date'], df['Upper'], df['Lower'], color='gray', alpha=0.2)

# plt.title('Bollinger Bands')
# plt.legend()
# plt.tight_layout()
# plt.show()
# endregion
