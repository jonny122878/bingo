import pandas as pd
from typing import List
import unittest
from unittest.mock import Mock


class WeekInfo:
    @property
    def start(self) -> int:
        return self._start

    @start.setter
    def start(self, value: int):
        self._start = value

    @property
    def end(self) -> int:
        return self._end

    @end.setter
    def end(self, value: int):
        self._end = value

    def __init__(self, start: int, end: int) -> None:
        self._start = start
        self._end = end
        pass

    def __eq__(self, other):
        if not isinstance(other, WeekInfo):
            return False
        return self._start == other._start and self._end == other._end

    def __hash__(self) -> int:
        return hash(self._start, self._end)


class Week:
    def __init__(self) -> None:
        pass

    def calcu(self, df: pd.DataFrame, high_point: str, low_point: str, level: str, order: str) -> List[WeekInfo]:
        """
        high_point:波峰高點等級名稱
        low_point:波谷低點等級名稱
        level:要計算週期欄位名稱
        order:要計算週期排序欄位名稱
        """
        # Find the indices of high and low points
        high_indices = df[df[level] == high_point].index
        low_indices = df[df[level] == low_point].index

        # Calculate the ranges
        ranges = []
        for high_idx in high_indices:
            for low_idx in low_indices:
                start = df.loc[high_idx, order]
                end = df.loc[low_idx, order]
                week = WeekInfo(start, end)
                is_start_exist = any(r.start <= start and r.end >=
                                     start for r in ranges)
                is_end_exist = any(
                    r.start <= end and r.end >= end for r in ranges)
                if not (is_start_exist or is_end_exist):
                    ranges.append(week)

        return ranges


class TestWeek(unittest.TestCase):
    def test_calcu_cycle_overlap(self):
        # region arrange
        week = Week()
        # endregion

        # region act
        data = {
            'order': [1, 2, 3, 4, 5, 6, 7, 8],
            'level': ['Q1', 'Q2', 'Q3', 'Q1', 'Q4', 'Q1', 'Q3', 'Q4']
        }
        df = pd.DataFrame(data)
        high_point = 'Q1'
        low_point = 'Q4'
        level = 'level'
        order = 'order'
        actual = week.calcu(df, high_point, low_point, level, order)
        # endregion

        # region assert
        excepted = [WeekInfo(6, 8), WeekInfo(1, 5)]
        self.assertCountEqual(actual, excepted)
        # endregion
    pass

    def test_calcu_high_point_contin(self):
        # region arrange
        week = Week()
        # endregion

        # region act
        data = {
            'order': [1, 2, 3, 4, 5, 6, 7, 8],
            'level': ['Q1', 'Q1', 'Q3', 'Q1', 'Q4', 'Q1', 'Q3', 'Q4']
        }
        df = pd.DataFrame(data)
        high_point = 'Q1'
        low_point = 'Q4'
        level = 'level'
        order = 'order'
        actual = week.calcu(df, high_point, low_point, level, order)
        # endregion

        # region assert
        expected = [WeekInfo(6, 8), WeekInfo(1, 5)]
        self.assertCountEqual(actual, expected)
        # endregion


if __name__ == '__main__':
    try:
        suite = unittest.TestSuite()
        suite.addTest(TestWeek('test_calcu_cycle_overlap'))
        suite.addTest(TestWeek('test_calcu_high_point_contin'))
        runner = unittest.TextTestRunner(verbosity=2)
        runner.run(suite)
    except SystemExit:
        pass
