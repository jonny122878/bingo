import unittest
from DeferAlgorithm import DeferAlgorithm

class TestDeferAlgorithm(unittest.TestCase):
    def Test_LoadData(self):
        algo = DeferAlgorithm()
        algo.LoadData([])
        # No assertion needed as method is pass

if __name__ == "__main__":
    unittest.main()
