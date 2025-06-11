import unittest
from StraightBallMark import StraightBallMark

class TestStraightBallMark(unittest.TestCase):

    def Test_loadStds(self):
        mark = StraightBallMark()
        stds = mark.loadStds()
        # 檢查 BallGroup sort 屬性
        self.assertEqual(list(stds.keys()), ['01S', '02S', '03S', '04S', '05S', '06S', '07S', '08S', '09S', '10S'])

    def Test_ballToMark(self):
        mark = StraightBallMark()
        self.assertEqual(mark.ballToMark("01"), "01S")
        self.assertEqual(mark.ballToMark("11"), "01S")
        self.assertEqual(mark.ballToMark("71"), "01S")
        self.assertEqual(mark.ballToMark("02"), "02S")
        self.assertEqual(mark.ballToMark("12"), "02S")
        self.assertEqual(mark.ballToMark("72"), "02S")
        self.assertEqual(mark.ballToMark("10"), "10S")
        self.assertEqual(mark.ballToMark("20"), "10S")
        self.assertEqual(mark.ballToMark("80"), "10S")
        self.assertEqual(mark.ballToMark("99"), "")

    def Test_markToBalls(self):
        mark = StraightBallMark()
        self.assertEqual(mark.markToBalls("01S"), ['01', '11', '21', '31', '41', '51', '61', '71'])
        self.assertEqual(mark.markToBalls("02S"), ['02', '12', '22', '32', '42', '52', '62', '72'])
        self.assertEqual(mark.markToBalls("10S"), ['10', '20', '30', '40', '50', '60', '70', '80'])
        self.assertEqual(mark.markToBalls("not-exist"), [])

if __name__ == "__main__": 
    try:
        suite = unittest.TestSuite()
        suite.addTest(TestStraightBallMark('Test_loadStds'))
        suite.addTest(TestStraightBallMark('Test_ballToMark'))
        suite.addTest(TestStraightBallMark('Test_markToBalls'))
        runner = unittest.TextTestRunner(verbosity=2)
        runner.run(suite)
    except SystemExit:
        pass
