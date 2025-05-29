import unittest
from TimesAlgorithm import TimesAlgorithm
import pandas as pd

class TestTimesAlgorithm(unittest.TestCase):
    def Test_LoadData(self):
        algo = TimesAlgorithm()
        algo.IsToExcel = True  # 不輸出到 Excel
        takeColumns = ["79", "80"]
        algo.TakeColumns = takeColumns
        # 新增測試資料
        inputs = [
            ["04","09","11","28","35","41","42","43","44","45","46","49","50","52","57","63","66","68","74","80"],
            ["08","14","23","24","25","30","31","32","41","43","44","50","53","57","62","63","64","68","69","72"],
            ["03","11","12","13","17","22","24","31","34","40","45","48","55","59","61","68","71","72","78","80"]
        ]
        algo.LoadData(inputs)
        # 1. declare DataFrame dfExcepted
        dfExcepted = pd.DataFrame({
            "79": [1, 2, 3],
            "80": [0, 1, 0]
        })
        print(algo.DfExport)
        print(dfExcepted)
        # 2. 驗證 algo.DfExport 和 dfExcepted 是否相等
        pd.testing.assert_frame_equal(algo.DfExport.reset_index(drop=True), dfExcepted.reset_index(drop=True))

if __name__ == '__main__':
    try:
        suite = unittest.TestSuite()
        suite.addTest(TestTimesAlgorithm('Test_LoadData'))
        runner = unittest.TextTestRunner(verbosity=2)
        runner.run(suite)
    except SystemExit:
        pass
