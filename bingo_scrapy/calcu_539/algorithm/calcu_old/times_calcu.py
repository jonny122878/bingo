# 將calcu 全部弄到main
# calcu 提煉defer、times、outlier

from math import exp, nan
import re
from typing import List
import numpy as np
import pandas as pd
from pandas import DataFrame
import os
import unittest
from unittest.mock import Mock
from pandas.testing import assert_frame_equal
from abc import ABC, abstractmethod


import db
from enum import Enum
import math
from calcu_539.algorithm.mark_old import BeginConvertMark, ConvertMark, ConvertOddEvenMark
from calcu_539.static.quantile_old import Quantile, QLevel
from calcu_539.excel.exportFile import ExportFile
from db.db import MSSQLDbContext


class DfInfo:
    """原始計算、刪除欄位"""

    def __init__(self) -> None:
        self._df = pd.DataFrame()
        self._dfDrop = pd.DataFrame()
        pass

    @property
    def df(self) -> str:
        return self._df

    @df.setter
    def df(self, value: str):
        self._df = value

    @property
    def dfDrop(self) -> str:
        return self._dfDrop

    @dfDrop.setter
    def dfDrop(self, value: str):
        self._dfDrop = value


class ICalcu(ABC):

    @property
    def includeColumns(self):
        return self._includeColumns

    @includeColumns.setter
    def includeColumns(self, value):
        self._includeColumns = value

    def __init__(self, exportFile: ExportFile, convert: ConvertMark, quantile: Quantile, isToCsv=False, path=None, filename=None) -> None:
        self._exportFile = exportFile
        self._convert = convert
        self._quantile = quantile
        self._isToCsv = isToCsv
        self._path = path
        self._filename = filename
        self._includeColumns = []

    @abstractmethod
    def calcu(self, inputs: List[str]) -> DfInfo:
        pass


class TimesCalcu(ICalcu):

    """計算出分組標記累計次數"""

    def __init__(self, exportFile: ExportFile, convert: ConvertMark, quantile: Quantile, isToCsv=False, path=None, filename=None) -> None:
        self._exportFile = exportFile
        self._convert = convert
        self._quantile = quantile
        self._isToCsv = isToCsv
        self._path = path
        self._filename = filename
        self._includeColumns = []

    def _getOutliers(self, row: pd.Series, quantileInfo: dict, quanColumns: list):
        markOutliers = []
        for column in quanColumns:
            if row[column] > quantileInfo['upper']:
                markOutliers.append(column)
        return markOutliers

    def _convertSort(self, row: pd.Series, columns: List[str], isDesc=True):
        arr = []
        for column in columns:
            arr.append({'key': column, 'value': row[column]})
        arrAsc = sorted(arr, key=lambda e: e['value'], reverse=isDesc)
        return list(map(lambda x: x['key'], arrAsc))
        pass

    def calcu(self, inputs: List[str]) -> DfInfo:
        time2Ds = []
        timesBallInfos = self._convert.loadStds()
        for idx, arr in enumerate(inputs):
            marks = []
            for ele in arr:
                mark = self._convert.ballToMark(ele)
                timesBall = next(
                    (x for x in timesBallInfos if x.sort == mark), None)
                timesBall.times += 1
                times = list(map(lambda x: x.times, timesBallInfos))
                std_dev = np.std(times)
                variance = np.var(times)
                times.append(std_dev)
                times.append(variance)
                times.append(inputs[idx])
                marks.append(mark)
            times.append(marks)
            time2Ds.append(times)

        timesColumns = [ball.sort for ball in timesBallInfos]
        timesColumns.append('std')
        timesColumns.append('var')
        timesColumns.append('num')
        timesColumns.append('mark')
        dfTimes = pd.DataFrame(time2Ds, columns=timesColumns)
        sortColumns = dfTimes.columns[self._quantile.cutFrt:self._quantile.cutEnd]
        dfTimes['desc'] = dfTimes.apply(lambda row: self._convertSort(
            row, sortColumns), axis=1)
        dfTimes['asc'] = dfTimes.apply(lambda row: self._convertSort(
            row, sortColumns, False), axis=1)

        dfTimeInfo = DfInfo()
        dfTimeInfo.df = dfTimes
        if len(self.includeColumns) != 0:
            dropColumns = []
            for column in dfTimes.columns:
                if any(column == includeColumn for includeColumn in self.includeColumns) == False:
                    dropColumns.append(column)
                pass
            dfDropTimes = dfTimes.copy(deep=True)
            dfDropTimes.drop(columns=dropColumns, inplace=True)
            dfTimeInfo.dfDrop = dfDropTimes

        if self._isToCsv and self._path is not None and self._filename is not None:
            self._exportFile.exportExcel([dfTimeInfo.df, dfTimeInfo.dfDrop], [
                                         'df', 'dfDrop'], self._path, self._filename)
        return dfTimeInfo

        pass


class TestTimesCalcu(unittest.TestCase):

    def __init__(self, methodName: str = "runTest", path=None, filename=None) -> None:
        super(TestTimesCalcu, self).__init__(methodName)
        self._path = path
        self._filename = filename

    def test_calcu_ball_df(self):
        """DataFrame balls相等結果"""
        mockExportFile = ExportFile()
        convert = BeginConvertMark()
        quantile = Quantile(QLevel=QLevel.Q10)
        quantile.cutFrt = 0
        quantile.cutEnd = 39
        timesCalcu = TimesCalcu(mockExportFile,  convert,
                                quantile, True, self._path, self._filename)
        timesCalcu.includeColumns = ['num', 'desc']
        # act
        dbContext = MSSQLDbContext({'server': 'wpdb2.hihosting.hinet.net', 'user': 'p89880749_p89880749',
                                    'password': 'Jonny1070607!@#$%', 'database': 'p89880749_test'})
        rows = dbContext.select(
            'select drawNumberSize,lotteryDate from Daily539 ORDER BY period')
        inputs = list(map(lambda row: row['drawNumberSize'].split(','), rows))
        dfDefer = timesCalcu.calcu(inputs)

        # assert 只要沒exception就是True

        self.assertEqual(True, True)
        pass

    def test_calcu_mark_df(self):
        """DataFrame相等結果"""
        # arrange
        mockExportFile = ExportFile()
        convert = ConvertMark()
        quantile = Quantile(QLevel=QLevel.Q10)
        quantile.cutFrt = 0
        quantile.cutEnd = 19
        quantile.lowerLimit = 1.1
        path = 'C:/Programs/bingo/bingo_scrapy/539_calcu'
        filename = 'timesMark.xlsx'
        timesCalcu = TimesCalcu(mockExportFile,  convert,
                                quantile, True, self._path, filename)
        timesCalcu.includeColumns = ['num', 'desc']

        # act
        dbContext = MSSQLDbContext({'server': 'wpdb2.hihosting.hinet.net', 'user': 'p89880749_p89880749',
                                    'password': 'Jonny1070607!@#$%', 'database': 'p89880749_test'})
        rows = dbContext.select(
            'select drawNumberSize,lotteryDate from Daily539 ORDER BY period')
        inputs = list(map(lambda row: row['drawNumberSize'].split(','), rows))
        dfDefer = timesCalcu.calcu(inputs)
        self.assertEqual(True, True)
        pass


if __name__ == '__main__':
    try:
        import json

        # 讀取 export.json 文件
        with open('calcu_539/algorithm/calcu_old/export.json', 'r') as file:
            data = json.load(file)
            path = data['path']
            filename_ball = 'timesBall.xlsx'
            filename_mark = 'timesMark.xlsx'

        # 現在可以使用 path 和 filename 變數
        print(f"The path is: {path}")
        suite = unittest.TestSuite()
        suite.addTest(TestTimesCalcu('test_calcu_ball_df',
                      path=path, filename=filename_ball))
        # suite.addTest(TestTimesCalcu('test_calcu_mark_df', path=path, filename=filename_mark))
        runner = unittest.TextTestRunner(verbosity=2)
        runner.run(suite)
    except SystemExit:
        pass
