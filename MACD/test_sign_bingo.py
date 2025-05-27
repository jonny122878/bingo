import unittest
from sign_bingo import SingleBingoMatch
from sign_bingo import BatchBingoMatch


class TestSingleBingoMatch(unittest.TestCase):
    def test_equal(self):
        match1 = SingleBingoMatch(Balls=["B1", "B2"], MatchBalls=[
                                  "M1", "M2"], Profit=100)
        match2 = SingleBingoMatch(Balls=["B1", "B2"], MatchBalls=[
                                  "M2", "M1"], Profit=100)
        self.assertEqual(match1, match2)

    def test_not_equal_balls(self):
        match1 = SingleBingoMatch(Balls=["B1", "B2"], MatchBalls=[
                                  "M1", "M2"], Profit=100)
        match2 = SingleBingoMatch(Balls=["B3", "B4"], MatchBalls=[
                                  "M1", "M2"], Profit=100)
        self.assertNotEqual(match1, match2)

    def test_not_equal_matchballs(self):
        match1 = SingleBingoMatch(Balls=["B1", "B2"], MatchBalls=[
                                  "M1", "M2"], Profit=100)
        match2 = SingleBingoMatch(Balls=["B1", "B2"], MatchBalls=[
                                  "M3", "M4"], Profit=100)
        self.assertNotEqual(match1, match2)

    def test_not_equal_profit(self):
        match1 = SingleBingoMatch(Balls=["B1", "B2"], MatchBalls=[
                                  "M1", "M2"], Profit=100)
        match2 = SingleBingoMatch(Balls=["B1", "B2"], MatchBalls=[
                                  "M1", "M2"], Profit=200)
        self.assertNotEqual(match1, match2)


class TestBatchBingoMatch(unittest.TestCase):
    def test_equal(self):
        match1 = SingleBingoMatch(["M3", "M4"], ["B3", "B4"], 100)
        match2 = SingleBingoMatch(["B3", "B4"], ["M3", "M4"], 100)
        batch1 = BatchBingoMatch([match1, match2], 300)
        batch2 = BatchBingoMatch([match1, match2], 300)  # Same as batch1
        self.assertEqual(batch1, batch2)

    def test_not_equal_results(self):
        match1 = SingleBingoMatch(["B3", "B4"], ["M1", "M2"], 100)
        match2 = SingleBingoMatch(["B3", "B4"], ["M1", "M2"], 200)
        batch1 = BatchBingoMatch([match1], 100)
        batch2 = BatchBingoMatch([match2], 200)
        self.assertNotEqual(batch1, batch2)

    def test_not_equal_sumprofit(self):
        match1 = SingleBingoMatch(["B1", "B2"], ["M1", "M2"], 100)
        match2 = SingleBingoMatch(["B3", "B4"], ["M3", "M4"], 200)
        batch1 = BatchBingoMatch([match1, match2], 300)
        batch2 = BatchBingoMatch([match1, match2], 400)  # Different SumProfit
        self.assertNotEqual(batch1, batch2)


if __name__ == '__main__':
    try:
        suite = unittest.TestSuite()
        # suite.addTest(TestSingleBingoMatch('test_equal'))
        # suite.addTest(TestSingleBingoMatch('test_not_equal_balls'))
        # suite.addTest(TestSingleBingoMatch('test_not_equal_matchballs'))
        # suite.addTest(TestSingleBingoMatch('test_not_equal_profit'))
        # suite.addTest(TestBatchBingoMatch('test_not_equal_results'))
        # suite.addTest(TestBatchBingoMatch('test_not_equal_sumprofit'))
        suite.addTest(TestBatchBingoMatch('test_equal'))
        runner = unittest.TextTestRunner(verbosity=2)
        runner.run(suite)
    except SystemExit:
        pass
