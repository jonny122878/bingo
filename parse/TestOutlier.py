import unittest
from Outlier import Outlier

class TestOutlier(unittest.TestCase):
    def Test_calcu(self):
        data = [10, 12, 12, 13, 12, 14, 13, 100]
        excepted = [10, 100]
        outlier = Outlier()
        result = outlier.calcu(data)
        self.assertEqual(result, excepted)

if __name__ == "__main__": 
    try:
        suite = unittest.TestSuite()
        suite.addTest(TestOutlier('Test_calcu'))
        runner = unittest.TextTestRunner(verbosity=2)
        runner.run(suite)
    except SystemExit:
        pass
