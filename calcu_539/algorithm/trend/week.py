import pandas as pd
from typing import List


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

    def __init__(self) -> None:
        self._start = 0
        self._end = 0
        pass


class Week:
    def __init__(self) -> None:
        pass

    def calcu(self, df: pd.DataFrame, high_point: str, low_point: str, level: str, order: str) -> List[WeekInfo]:

        # Find the indices of high and low points
        high_indices = df[df[level] == high_point].index
        low_indices = df[df[level] == low_point].index

        # Calculate the ranges
        ranges = []
        for high_idx, low_idx in zip(high_indices, low_indices):
            start = df.loc[high_idx, order]
            end = df.loc[low_idx, order]
            week = WeekInfo()
            week.start = start
            week.end = end
            is_start_exist = any(
                r.start <= start and r.end >= start for r in ranges)
            is_end_exist = any(r.start <= end and r.end >= end for r in ranges)
            if not (is_start_exist or is_end_exist):
                ranges.append(week)

        return ranges
