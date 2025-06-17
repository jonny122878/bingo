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
            dir_path = rf"C:\Programs\test_data\rand246\take{group}"
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

    def Test_excel_unit_plot(self):
        """
        遍歷指定根目錄下的所有子目錄，將每個子目錄的 Excel 檔案清單輸出到一個 Excel 檔案，欄位為 directory、excel_file、excel（完整路徑）、average（PercentAverage Sheet A2）。
        並在 Sheet2 輸出 group by excel_file（不含副檔名） 的 average mean。
        """
        import os
        import pandas as pd
        root_dir = r"C:\Programs\test_data"
        result = []
        for dirpath, dirnames, filenames in os.walk(root_dir):
            excel_files = [f for f in filenames if f.endswith('.xlsx') or f.endswith('.xls')]
            for fname in excel_files:
                excel_path = os.path.join(dirpath, fname)
                average = None
                try:
                    df_avg = pd.read_excel(excel_path, sheet_name='PercentAverage', header=None)
                    average = df_avg.iat[1, 0]
                except Exception as e:
                    print(f"Error reading PercentAverage from {excel_path}: {e}")
                result.append({'directory': dirpath, 'excel_file': fname, 'excel': excel_path, 'average': average})
        df = pd.DataFrame(result, columns=['directory', 'excel_file', 'excel', 'average'])
        # group 欄位為 excel_file 去除副檔名
        df['group'] = df['excel_file'].apply(lambda x: os.path.splitext(x)[0])
        group_mean = df.groupby('group', as_index=False)['average'].mean()
        group_mean.rename(columns={'average': 'average_mean'}, inplace=True)
        output_path = os.path.join(root_dir, 'excel_file_list.xlsx')
        with pd.ExcelWriter(output_path) as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
            group_mean.to_excel(writer, index=False, sheet_name='Sheet2')
        print(f"Excel file list exported to: {output_path}")

if __name__ == "__main__": 
    try:
        suite = unittest.TestSuite()
        # suite.addTest(TestLineChart('Test_plot'))
        # suite.addTest(TestLineChart('Test_rand_unit_plot'))
        suite.addTest(TestLineChart('Test_excel_unit_plot'))
        runner = unittest.TextTestRunner(verbosity=2)
        runner.run(suite)
    except SystemExit:
        pass
