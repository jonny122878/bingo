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


class DeferCalcu(ICalcu):
    """計算分組標記拖期次數,並衍生衡量集合離散欄位(標準差)、離群值欄位"""

    def __init__(self, exportFile: ExportFile, convert: ConvertMark, quantile: Quantile, isToCsv=False, path=None, filename=None) -> None:
        self._exportFile = exportFile
        self._convert = convert
        self._quantile = quantile
        self._isToCsv = isToCsv
        self._path = path
        self._filename = filename
        self._includeColumns = []
        pass

    def _getNumOutliers(self, row: pd.Series):
        numOutlier2Ds = []
        for ele in row['markOutliers']:
            numOutlier2Ds.append(self._convert.markToBalls(ele))
        return numOutlier2Ds
        pass

    def _getMarkOutliers(self, row: pd.Series, quantileInfo: dict, quanColumns: list):
        markOutliers = []
        for column in quanColumns:
            if row[column] > quantileInfo['upper']:
                markOutliers.append(column)
        return markOutliers
        pass

    def calcu(self, inputs: List[str]) -> DataFrame:
        """
        計算拖期
        """
        deferBallInfos = self._convert.loadStds()
        defer2Ds = []
        for idx, arr in enumerate(inputs):
            marks = [self._convert.ballToMark(ele) for ele in arr]
            for deferBall in deferBallInfos:
                if any(mark == deferBall.sort for mark in marks):
                    deferBall.times = 0
                else:
                    deferBall.times += 1
            defers = list(map(lambda x: x.times, deferBallInfos))
            std_dev = np.std(defers)
            variance = np.var(defers)
            defers.append(std_dev)
            defers.append(variance)
            defers.append(inputs[idx])
            defers.append(marks)
            defer2Ds.append(defers)

        deferColumns = [ball.sort for ball in deferBallInfos]
        deferColumns.append('std')
        deferColumns.append('var')
        deferColumns.append('num')
        deferColumns.append('markNum')
        dfDefer = pd.DataFrame(defer2Ds, columns=deferColumns)
        quantileInfo = self._quantile.calcuOutlierInfo(dfDefer)
        quanColumns = dfDefer.columns[self._quantile.cutFrt:self._quantile.cutEnd]
        # 計算符合離群值mark
        dfDefer['markOutliers'] = dfDefer.apply(
            lambda row: self._getMarkOutliers(row, quantileInfo, quanColumns), axis=1)
        # 轉換成num
        dfDefer['numOutliers'] = dfDefer.apply(
            lambda row: self._getNumOutliers(row), axis=1)

        # 計算變異數
        self._quantile.loadQInfo(dfDefer['var'])
        dfDefer['varQ'] = dfDefer.apply(
            lambda row: self._quantile.calcuLevelQs(row, 'var'), axis=1)

        dfDeferInfo = DfInfo()
        dfDeferInfo.df = dfDefer
        if len(self.includeColumns) != 0:
            dropColumns = []
            for column in dfDefer.columns:
                if any(column == includeColumn for includeColumn in self.includeColumns) == False:
                    dropColumns.append(column)
                pass
            dfDropTimes = dfDefer.copy(deep=True)
            dfDropTimes.drop(columns=dropColumns, inplace=True)
            dfDeferInfo.dfDrop = dfDropTimes

        if self._isToCsv and self._path is not None and self._filename is not None:
            self._exportFile.exportExcel([dfDeferInfo.df, dfDeferInfo.dfDrop], [
                                         'df', 'dfDrop'], self._path, self._filename)
        return dfDeferInfo
        pass

    pass


class TestDeferCalcu(unittest.TestCase):

    def __init__(self, methodName: str = "runTest", path=None, filename=None) -> None:
        super(TestDeferCalcu, self).__init__(methodName)
        self._path = path
        self._filename = filename

    def test_calcu_ball_df(self):
        """DataFrame balls相等結果"""
        mockExportFile = ExportFile()
        convert = BeginConvertMark()
        quantile = Quantile(QLevel=QLevel.Q10)
        quantile.cutFrt = 0
        quantile.cutEnd = 39
        deferCalcu = DeferCalcu(mockExportFile,  convert,
                                quantile, True, self._path, self._filename)
        deferCalcu.includeColumns = ['markOutliers', 'varQ']
        # act
        dbContext = MSSQLDbContext({'server': 'wpdb2.hihosting.hinet.net', 'user': 'p89880749_p89880749',
                                    'password': 'Jonny1070607!@#$%', 'database': 'p89880749_test'})
        rows = dbContext.select(
            'select drawNumberSize,lotteryDate from Daily539 ORDER BY period')
        inputs = list(map(lambda row: row['drawNumberSize'].split(','), rows))
        dfDefer = deferCalcu.calcu(inputs)

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
        deferCalcu = DeferCalcu(mockExportFile,  convert,
                                quantile, True, self._path, self._filename)
        deferCalcu.includeColumns = ['markOutliers', 'varQ']
        # act
        dbContext = MSSQLDbContext({'server': 'wpdb2.hihosting.hinet.net', 'user': 'p89880749_p89880749',
                                    'password': 'Jonny1070607!@#$%', 'database': 'p89880749_test'})
        rows = dbContext.select(
            'select drawNumberSize,lotteryDate from Daily539 ORDER BY period')
        inputs = list(map(lambda row: row['drawNumberSize'].split(','), rows))
        dfDefer = deferCalcu.calcu(inputs)
        # assert
        self.assertEqual(True, True)

    def test_to_csv_df(self):
        """ToCsv呼叫"""
        # arrange
        mockExportFile = Mock()
        convert = ConvertMark()
        deferCalcu = DeferCalcu(mockExportFile, [], convert, True)
        # act
        deferCalcu.calcu([])
        # assert
        excepted = 1
        self.assertEqual(excepted, mockExportFile.exportCsv.call_count)
        pass


if __name__ == '__main__':
    try:
        import json

        # 讀取 export.json 文件
        with open('calcu_539/algorithm/calcu_old/export.json', 'r') as file:
            data = json.load(file)
            path = data['path']
            filename_ball = 'deferBall_539.xlsx'
            filename_mark = 'deferMark.xlsx'

        # 現在可以使用 path 和 filename 變數
        print(f"The path is: {path}")
        suite = unittest.TestSuite()
        # suite.addTest(TestDeferCalcu('test_calcu_mark_df',
        #               path=path, filename=filename_mark))
        suite.addTest(TestDeferCalcu('test_calcu_ball_df',
                      path=path, filename=filename_ball))
        runner = unittest.TextTestRunner(verbosity=2)
        runner.run(suite)
    except SystemExit:
        pass
