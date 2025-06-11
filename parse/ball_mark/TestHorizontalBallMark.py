import unittest
from HorizontalBallMark import HorizontalBallMark

class TestHorizontalBallMark(unittest.TestCase):

    def Test_loadStds(self):
        mark = HorizontalBallMark()
        stds = mark.loadStds()
        # 檢查 keys
        self.assertEqual(list(stds.keys()), ['01H', '11H', '21H', '31H', '41H', '51H', '61H', '71H'])

    def Test_ballToMark(self):
        mark = HorizontalBallMark()
        self.assertEqual(mark.ballToMark("01"), "01H")
        self.assertEqual(mark.ballToMark("10"), "01H")
        self.assertEqual(mark.ballToMark("11"), "11H")
        self.assertEqual(mark.ballToMark("20"), "11H")
        self.assertEqual(mark.ballToMark("21"), "21H")
        self.assertEqual(mark.ballToMark("30"), "21H")
        self.assertEqual(mark.ballToMark("31"), "31H")
        self.assertEqual(mark.ballToMark("40"), "31H")
        self.assertEqual(mark.ballToMark("41"), "41H")
        self.assertEqual(mark.ballToMark("50"), "41H")
        self.assertEqual(mark.ballToMark("51"), "51H")
        self.assertEqual(mark.ballToMark("60"), "51H")
        self.assertEqual(mark.ballToMark("61"), "61H")
        self.assertEqual(mark.ballToMark("70"), "61H")
        self.assertEqual(mark.ballToMark("71"), "71H")
        self.assertEqual(mark.ballToMark("80"), "71H")
        self.assertEqual(mark.ballToMark("99"), "")

    def Test_markToBalls(self):
        mark = HorizontalBallMark()
        self.assertEqual(mark.markToBalls("01H"), ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10'])
        self.assertEqual(mark.markToBalls("11H"), ['11', '12', '13', '14', '15', '16', '17', '18', '19', '20'])
        self.assertEqual(mark.markToBalls("21H"), ['21', '22', '23', '24', '25', '26', '27', '28', '29', '30'])
        self.assertEqual(mark.markToBalls("31H"), ['31', '32', '33', '34', '35', '36', '37', '38', '39', '40'])
        self.assertEqual(mark.markToBalls("41H"), ['41', '42', '43', '44', '45', '46', '47', '48', '49', '50'])
        self.assertEqual(mark.markToBalls("51H"), ['51', '52', '53', '54', '55', '56', '57', '58', '59', '60'])
        self.assertEqual(mark.markToBalls("61H"), ['61', '62', '63', '64', '65', '66', '67', '68', '69', '70'])
        self.assertEqual(mark.markToBalls("71H"), ['71', '72', '73', '74', '75', '76', '77', '78', '79', '80'])
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
