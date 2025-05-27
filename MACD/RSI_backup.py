import pandas as pd
import matplotlib.pyplot as plt


def calculate_rsi(prices, window=14):
    # 計算每日價格變動
    delta = prices.diff()

    # 分別計算上漲值和下跌值
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    # 計算平均上漲值和平均下跌值
    avg_gain = gain.rolling(window=window, min_periods=1).mean()
    avg_loss = loss.rolling(window=window, min_periods=1).mean()

    # 計算 RS 和 RSI
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    return rsi


# 範例數據
prices = pd.Series([45, 46, 47, 46, 45, 44, 43,
                   42, 43, 44, 45, 46, 47, 48, 49])

# 計算 RSI
rsi = calculate_rsi(prices)
print("RSI 指標：")
print(rsi)

# 繪製折線圖
plt.figure(figsize=(8, 5))
plt.plot(rsi, marker='o', linestyle='-', color='b', label='Data Line')
plt.title("Line Chart of Series", fontsize=14)
plt.xlabel("Index", fontsize=12)
plt.ylabel("Value", fontsize=12)
plt.grid(True, linestyle='--', alpha=0.6)
plt.legend()
plt.show()
