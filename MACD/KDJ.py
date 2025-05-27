import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# 創建模擬數據
np.random.seed(42)
data = {
    'Date': pd.date_range(start='2023-01-01', periods=50),
    'Close': np.random.randint(100, 200, 50)
}
df = pd.DataFrame(data)

# 計算 KDJ 指標


def calculate_kdj(df, n=9):
    df['Low_N'] = df['Close'].rolling(window=n).min()
    df['High_N'] = df['Close'].rolling(window=n).max()
    df['RSV'] = (df['Close'] - df['Low_N']) / \
        (df['High_N'] - df['Low_N']) * 100

    df['K'] = df['RSV'].ewm(com=2).mean()
    df['D'] = df['K'].ewm(com=2).mean()
    df['J'] = 3 * df['K'] - 2 * df['D']
    return df


# 計算 KDJ
df = calculate_kdj(df)

# 繪製 KDJ 指標
plt.figure(figsize=(10, 6))
plt.plot(df['Date'], df['K'], label='K', color='blue')
plt.plot(df['Date'], df['D'], label='D', color='orange')
plt.plot(df['Date'], df['J'], label='J', color='green')
plt.title('KDJ Indicator')
plt.xlabel('Date')
plt.ylabel('Value')
plt.legend()
plt.grid(True, linestyle='--', alpha=0.6)
plt.tight_layout()
plt.show()
