import unittest
from StraightBallMark import StraightBallMark

class TestStraightBallMark(unittest.TestCase):

    def Test_loadStds(self):
        mark = StraightBallMark()
        stds = mark.loadStds()
        # 檢查 BallGroup sort 屬性
        self.assertEqual([b.sort for b in stds], ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10'])

    def Test_ballToMark(self):
        mark = StraightBallMark()
        self.assertEqual(mark.ballToMark("01"), "01")
        self.assertEqual(mark.ballToMark("11"), "01")
        self.assertEqual(mark.ballToMark("71"), "01")
        self.assertEqual(mark.ballToMark("02"), "02")
        self.assertEqual(mark.ballToMark("12"), "02")
        self.assertEqual(mark.ballToMark("72"), "02")
        self.assertEqual(mark.ballToMark("10"), "10")
        self.assertEqual(mark.ballToMark("20"), "10")
        self.assertEqual(mark.ballToMark("80"), "10")
        self.assertEqual(mark.ballToMark("99"), "")

    def Test_markToBalls(self):
        mark = StraightBallMark()
        self.assertEqual(mark.markToBalls("01"), ['01', '11', '21', '31', '41', '51', '61', '71'])
        self.assertEqual(mark.markToBalls("02"), ['02', '12', '22', '32', '42', '52', '62', '72'])
        self.assertEqual(mark.markToBalls("10"), ['10', '20', '30', '40', '50', '60', '70', '80'])
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
