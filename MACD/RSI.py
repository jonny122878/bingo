import unittest
import pandas as pd
import matplotlib.pyplot as plt
from IPlot import IPlot


class RSIPlot(IPlot):
    @property
    def rsi_df(self):
        return self._rsi_df

    @property
    def last_rsi_level(self):
        return self._rsi_df['level'].iloc[-1]

    def __init__(self, prices: pd.Series):
        self.prices = prices
        self._rsi_df = None  # 初始化為 None

    def __calculate_rsi(self):
        window = len(self.prices)  # 將 window 設為 prices 的長度
        # 計算每日價格變動
        delta = self.prices.diff()

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

    def load(self):
        rsi = self.__calculate_rsi()
        self._rsi_df = pd.DataFrame({'rsi': rsi})  # 將 rsi 加入公共屬性 rsi_df
        self._rsi_df['level'] = self._rsi_df['rsi'].apply(
            lambda x: 'high' if x > 70 else 'low' if x < 30 else 'normal'
        )
        print("RSI 指標：")
        print(self._rsi_df)

    def plot(self, name):
        plt.figure(figsize=(8, 5))
        plt.plot(self._rsi_df['rsi'], marker='o', linestyle='-',
                 color='b', label='Data Line')
        plt.title(name, fontsize=14)
        plt.xlabel("Index", fontsize=12)
        plt.ylabel("Value", fontsize=12)
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.legend()
        # plt.show()


class RSIPlotTest(unittest.TestCase):
    def test_last_rsi_level_is_normal(self):
        """
        測試最後輸出的 RSI 等級值是否為 'normal'，low超賣，high超買
        """
        # 測試數據，確保 RSI 值在 30 和 70 之間
        prices = pd.Series([45, 46, 47, 46, 45, 44, 43,
                           42, 43, 44, 45, 46, 47, 48, 49])

        # 初始化 RSIPlot 並加載數據
        rsi_plot = RSIPlot(prices)
        rsi_plot.load()

        # 驗證 last_rsi_level 是否為 'normal'
        self.assertEqual(rsi_plot.last_rsi_level, 'normal')


if __name__ == '__main__':
    try:
        suite = unittest.TestSuite()
        suite.addTest(RSIPlotTest('test_last_rsi_level_is_normal'))
        runner = unittest.TextTestRunner(verbosity=2)
        runner.run(suite)
    except SystemExit:
        pass
