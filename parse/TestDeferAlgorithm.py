import unittest
from DeferAlgorithm import DeferAlgorithm

class TestDeferAlgorithm(unittest.TestCase):
    def Test_LoadData(self):
        algo = DeferAlgorithm()
        # 新增測試資料
        inputs = [
            ["04","09","11","28","35","41","42","43","44","45","46","49","50","52","57","63","66","68","74","80"],
            ["08","14","23","24","25","30","31","32","41","43","44","50","53","57","62","63","64","68","69","72"],
            ["03","11","12","13","17","22","24","31","34","40","45","48","55","59","61","68","71","72","78","80"]
        ]
        algo.LoadData(inputs)
        # No assertion needed as method is pass

if __name__ == "__main__":
    unittest.main()
