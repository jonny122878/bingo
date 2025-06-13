import unittest
from LineChart import LineChart

class TestLineChart(unittest.TestCase):
    def Test_plot(self):
        x = [1, 3, 2, 5, 4]
        chart = LineChart()
        chart.plot(x)

    def Test_group8_plot(self):
        import os
        import pandas as pd
        dir_path = r"C:\Programs\test_data\take6"
        excel_files = [f for f in os.listdir(dir_path) if f.endswith('.xlsx') or f.endswith('.xls')]
        excel_files.sort()  # 從小到大排序
        print("Excel files in test_group_3:", excel_files)
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
        print("Collected float array:", arr)
        chart = LineChart()
        chart.plot(arr)
        pass
if __name__ == "__main__": 
    try:
        suite = unittest.TestSuite()
        # suite.addTest(TestLineChart('Test_plot'))
        suite.addTest(TestLineChart('Test_group8_plot'))
        runner = unittest.TextTestRunner(verbosity=2)
        runner.run(suite)
    except SystemExit:
        pass
