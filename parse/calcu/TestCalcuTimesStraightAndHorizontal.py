import unittest
import pandas as pd
from .CalcuTimesStraightAndHorizontal import CalcuTimesStraightAndHorizontal
from parse.TimesAlgorithm import TimesAlgorithm
from parse.ball_mark.HorizontalBallMark import HorizontalBallMark
from parse.ball_mark.StraightBallMark import StraightBallMark

class TestCalcuTimesStraightAndHorizontal(unittest.TestCase):
    def test_calcu(self):
        # Test_LoadDataByStraight variant
        algo_straight = TimesAlgorithm()
        algo_straight.ExcelPath = r"C:\Programs\test_data\TimesAlgorithmByStraight.xlsx"
        algo_straight.BallMark = StraightBallMark()
        algo_straight.TakeColumns = ["01S", "07S"]
        inputs = [
            ["04","09","11","28","35","41","42","43","44","45","46","49","50","52","57","63","66","68","74","80"],
            ["08","14","23","24","25","30","31","32","41","43","44","50","53","57","62","63","64","68","69","72"],
            ["03","11","12","13","17","22","24","31","34","40","45","48","55","59","61","68","71","72","78","80"]
        ]
        algo_straight.LoadData(inputs)
        dfStraight = algo_straight.DfExport

        # Test_LoadDataByHorizontal variant
        algo_horizontal = TimesAlgorithm()
        algo_horizontal.ExcelPath = r"C:\Programs\test_data\TimesAlgorithmByHorizontal.xlsx"
        algo_horizontal.BallMark = HorizontalBallMark()
        algo_horizontal.TakeColumns = ["41H", "71H"]
        algo_horizontal.LoadData(inputs)
        dfHorizontal = algo_horizontal.DfExport

        calcu = CalcuTimesStraightAndHorizontal()
        calcu.Calcu(dfStraight, dfHorizontal)
        print(dfStraight)
        print(dfHorizontal)

if __name__ == '__main__':
    try:
        suite = unittest.TestSuite()
        suite.addTest(TestCalcuTimesStraightAndHorizontal('test_calcu'))
        runner = unittest.TextTestRunner(verbosity=2)
        runner.run(suite)
    except SystemExit:
        pass
