import unittest
from LineChart import LineChart
import matplotlib.pyplot as plt

class TestLineChart(unittest.TestCase):
    def Test_plot(self):
        x = [1, 3, 2, 5, 4]
        chart = LineChart()
        chart.plot(x)

    def Test_rand_unit_plot(self):
        group_arr = [3,4,5,6,7,8,9,10]
        # group_arr = [3]
        for group in group_arr:
            import os
            import pandas as pd
            dir_path = rf"C:\Programs\test_data\rand8\take{group}"
            excel_files = [f for f in os.listdir(dir_path) if f.endswith('.xlsx') or f.endswith('.xls')]
            excel_files.sort()  # 從小到大排序
            print(f"Excel files in take{group}:", excel_files)
            arr = []
            for fname in excel_files:
                fpath = os.path.join(dir_path, fname)
                df = pd.read_excel(fpath, sheet_name=0, header=None)
                # A2 is row 1, col 0 (0-based index)
                try:
                    val = df.iat[1, 0]
                    arr.append(float(val))
                except Exception as e:
                    print(f"Error reading {fname}: {e}")
            print(f"Collected float array for take{group}:", arr)
            chart = LineChart()
            plt.figure()  # 新增: 每個 group 一個新 figure
            chart.plot(arr, title=f"take{group}", show_block=False)  # 傳 show_block 參數
        plt.show()  # 統一顯示所有圖表
        pass
if __name__ == "__main__": 
    try:
        suite = unittest.TestSuite()
        # suite.addTest(TestLineChart('Test_plot'))
        suite.addTest(TestLineChart('Test_rand_unit_plot'))
        runner = unittest.TextTestRunner(verbosity=2)
        runner.run(suite)
    except SystemExit:
        pass
