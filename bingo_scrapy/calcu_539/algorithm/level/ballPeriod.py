from db.db import MSSQLDbContext
from datetime import datetime
import unittest
from unittest.mock import Mock


class BallGroup539:

    @property
    def period(self) -> str:
        return self._period

    @property
    def ball2Ds(self) -> list:
        return self._ball2Ds

    @period.setter
    def period(self, value: str):
        self._period = value

    @ball2Ds.setter
    def ball2Ds(self, value: list):
        self._ball2Ds = value

    def __init__(self) -> None:
        self._period = ''
        self._ball2Ds = []
        pass

    def __eq__(self, other):
        if not isinstance(other, BallGroup539):
            return False
        return self._period == other._period and self._ball2Ds == other._ball2Ds

    def __hash__(self) -> int:
        return hash((self._period, tuple(tuple(row) for row in self._ball2Ds)))
    pass


class DividePeriod:
    def __init__(self):
        pass

    def _split_array(self, arr: list, divisor: int):
        return [arr[i:i + divisor] for i in range(0, len(arr), divisor)]

    def _mapBallGroup539(self, rows: list) -> list[BallGroup539]:

        dates = list(map(lambda row: row['lotteryDate'], rows))
        max_date = max(dates)
        min_date = min(dates)
        str_max_date = max_date.strftime("%Y%m%d")
        str_min_date = min_date.strftime("%Y%m%d")
        ballGroup539 = BallGroup539()
        ballGroup539.period = f"{str_min_date}-{str_max_date}"
        ballGroup539.ball2Ds = list(
            map(lambda row: row['drawNumberSize'].split(','), rows))
        return ballGroup539
        print('')
        pass

    def calcu(self, rows: list, divisor: int) -> list[BallGroup539]:
        quotient = len(rows) // divisor
        remainder = len(rows) % divisor
        if remainder > 0:
            remainder = 1
        carry = quotient + remainder
        split_arrays = self._split_array(rows, divisor)
        ballGroup539s = list(
            map(lambda split_array: self._mapBallGroup539(split_array), split_arrays))
        return ballGroup539s
        pass
    pass


class TestDividePeriod(unittest.TestCase):
    def test_calcu(self):
        dbContext = MSSQLDbContext({'server': 'wpdb2.hihosting.hinet.net', 'user': 'p89880749_p89880749',
                                    'password': 'Jonny1070607!@#$%', 'database': 'p89880749_test'})
        # Mock the select method to return fixed rows
        dbContext.select = Mock(return_value=[
            {'drawNumberSize': '01,02,03,04,05',
                'lotteryDate': datetime(2024, 10, 1)},
            {'drawNumberSize': '11,12,13,14,15',
                'lotteryDate': datetime(2024, 10, 2)},
            {'drawNumberSize': '21,22,23,24,25',
                'lotteryDate': datetime(2024, 10, 3)},
            {'drawNumberSize': '31,32,33,34,35',
                'lotteryDate': datetime(2024, 10, 4)},
            # Add more mock rows as needed
        ])
        rows = dbContext.select(
            'select top 400 drawNumberSize,lotteryDate from Daily539 ORDER BY period')
        dp = DividePeriod()
        divide = 2
        actual = dp.calcu(rows, divide)
        firstBallGroup539 = BallGroup539()
        firstBallGroup539.period = '20241001-20241002'
        firstBallGroup539.ball2Ds = [
            ['01', '02', '03', '04', '05'], ['11', '12', '13', '14', '15']]
        endBallGroup539 = BallGroup539()
        endBallGroup539.period = '20241003-20241004'
        endBallGroup539.ball2Ds = [
            ['21', '22', '23', '24', '25'], ['31', '32', '33', '34', '35']]
        excepteds = [firstBallGroup539, endBallGroup539]
        self.assertEqual(excepteds, actual)
    pass


if __name__ == '__main__':
    print('')
    pass
    # region test case

    try:
        suite = unittest.TestSuite()
        # path = 'C:/Programs/bingo/bingo_scrapy/calcu_539/test_data'
        # # suite.addTest(TestTimesCalcu('test_calcu_ball_df', path=path))
        # # suite.addTest(TestTimesCalcu('test_calcu_mark_df', path=path))
        # suite.addTest(TestDeferCalcu('test_calcu_mark_df', path=path))
        # suite.addTest(TestDeferCalcu('test_calcu_ball_df', path=path))

        # not ok
        suite.addTest(TestDividePeriod('test_calcu'))
        # suite.addTest(TestContinueCalcu('test_calcu_mark_df'))
        # suite.addTest(TestDeferCalcu('test_to_csv_df'))
        # suite.addTest(TestDeferCalcu('test_calcu_3mean'))
        runner = unittest.TextTestRunner(verbosity=2)
        runner.run(suite)
    except SystemExit:
        pass

    # endregion
