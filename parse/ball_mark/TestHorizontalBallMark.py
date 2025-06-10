import unittest
from HorizontalBallMark import HorizontalBallMark

class TestHorizontalBallMark(unittest.TestCase):

    def Test_loadStds(self):
        mark = HorizontalBallMark()
        stds = mark.loadStds()
        # 檢查 BallGroup sort 屬性
        self.assertEqual([b.sort for b in stds], ['01', '11', '21', '31', '41', '51', '61', '71'])

    def Test_ballToMark(self):
        mark = HorizontalBallMark()
        self.assertEqual(mark.ballToMark("01"), "01")
        self.assertEqual(mark.ballToMark("10"), "01")
        self.assertEqual(mark.ballToMark("11"), "11")
        self.assertEqual(mark.ballToMark("20"), "11")
        self.assertEqual(mark.ballToMark("21"), "21")
        self.assertEqual(mark.ballToMark("30"), "21")
        self.assertEqual(mark.ballToMark("31"), "31")
        self.assertEqual(mark.ballToMark("40"), "31")
        self.assertEqual(mark.ballToMark("41"), "41")
        self.assertEqual(mark.ballToMark("50"), "41")
        self.assertEqual(mark.ballToMark("51"), "51")
        self.assertEqual(mark.ballToMark("60"), "51")
        self.assertEqual(mark.ballToMark("61"), "61")
        self.assertEqual(mark.ballToMark("70"), "61")
        self.assertEqual(mark.ballToMark("71"), "71")
        self.assertEqual(mark.ballToMark("80"), "71")
        self.assertEqual(mark.ballToMark("99"), "")

    def Test_markToBalls(self):
        mark = HorizontalBallMark()
        self.assertEqual(mark.markToBalls("01"), ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10'])
        self.assertEqual(mark.markToBalls("11"), ['11', '12', '13', '14', '15', '16', '17', '18', '19', '20'])
        self.assertEqual(mark.markToBalls("21"), ['21', '22', '23', '24', '25', '26', '27', '28', '29', '30'])
        self.assertEqual(mark.markToBalls("31"), ['31', '32', '33', '34', '35', '36', '37', '38', '39', '40'])
        self.assertEqual(mark.markToBalls("41"), ['41', '42', '43', '44', '45', '46', '47', '48', '49', '50'])
        self.assertEqual(mark.markToBalls("51"), ['51', '52', '53', '54', '55', '56', '57', '58', '59', '60'])
        self.assertEqual(mark.markToBalls("61"), ['61', '62', '63', '64', '65', '66', '67', '68', '69', '70'])
        self.assertEqual(mark.markToBalls("71"), ['71', '72', '73', '74', '75', '76', '77', '78', '79', '80'])
        self.assertEqual(mark.markToBalls("not-exist"), [])

if __name__ == "__main__": 
    try:
        suite = unittest.TestSuite()
        suite.addTest(TestHorizontalBallMark('Test_loadStds'))
        suite.addTest(TestHorizontalBallMark('Test_ballToMark'))
        suite.addTest(TestHorizontalBallMark('Test_markToBalls'))
        runner = unittest.TextTestRunner(verbosity=2)
        runner.run(suite)
    except SystemExit:
        pass
