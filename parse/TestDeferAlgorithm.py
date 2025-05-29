import unittest
from pandas import DataFrame
from DeferAlgorithm import DeferAlgorithm

class TestDeferAlgorithm(unittest.TestCase):
    def test___init__(self):
        algo = DeferAlgorithm()
        self.assertIsInstance(algo, DeferAlgorithm)

    def test_calcu(self):
        algo = DeferAlgorithm()
        result = algo.calcu([])
        self.assertIsInstance(result, DataFrame)

if __name__ == "__main__":
    unittest.main()
