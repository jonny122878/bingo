from matplotlib.pylab import normal
from pandas import Series
import pandas as pd
from db.db import MSSQLDbContext
from calcu_539.static.quantile_old import Quantile
from calcu_539.algorithm.calcu_old import TimesCalcu, QLevel
from calcu_539.algorithm.mark_old import BeginConvertMark
from calcu_539.excel.exportFile import ExportFile
import numpy as np
from ballPeriod import DividePeriod, BallGroup539
from typing import Dict, Any, List


class LevelGroup:
    @property
    def hot(self) -> Dict[str, int]:
        return self._hot

    @hot.setter
    def hot(self, value: Dict[str, int]):
        self._hot = value

    @property
    def normal(self) -> Dict[str, int]:
        return self._normal

    @normal.setter
    def normal(self, value: Dict[str, int]):
        self._normal = value

    @property
    def cold(self) -> Dict[str, int]:
        return self._cold

    @cold.setter
    def cold(self, value: Dict[str, int]):
        self._cold = value

    def __init__(self) -> None:
        self._hot = {}
        self._normal = {}
        self._cold = {}
        pass


class BallLevel:
    def __init__(self):
        pass

    @staticmethod
    def calcu(inputs: List[Dict[str, Any]]) -> LevelGroup:
        """
        """
        quantile = Quantile(QLevel=QLevel.Q10)
        quantile.cutFrt = 0
        quantile.cutEnd = 39
        beginConvertMark = BeginConvertMark()
        exportFile = ExportFile()
        timesCalcu = TimesCalcu(exportFile, beginConvertMark, quantile)
        quantile.cutFrt = 0
        quantile.cutEnd = 39
        timesCalcu.includeColumns = ['num', 'asc']
        dfTimesInfo = timesCalcu.calcu(inputs)
        exportFile.exportExcel([dfTimesInfo.df], ['test'],
                               'C:/Programs/bingo/test_data', 'ballLevel.xlsx')

        df39 = dfTimesInfo.df.copy(deep=True)
        df39.drop(columns=['mark', 'num', 'var',
                           'std', 'asc', 'desc'], inplace=True)
        last_row: Series = df39.iloc[-1]
        if isinstance(last_row, Series):
            dictLast = {key: float(value)
                        for key, value in last_row.to_dict().items()}
            # convert to dict array
            Q1 = np.quantile(last_row, 0.25)
            Q3 = np.quantile(last_row, 0.75)
            hotGroup = {key: value for key,
                        value in dictLast.items() if value > Q3}

            normalGroup = {key: value for key,
                           value in dictLast.items() if value <= Q3 and value >= Q1}
            coldGroup = {key: value for key,
                         value in dictLast.items() if value < Q1}
            print(len(hotGroup.keys()) +
                  len(normalGroup.keys()) + len(coldGroup.keys()))
            # print(normalGroup)
            # print(coldGroup)
        else:
            print("The last row is not a Series.")
        levelGroup = LevelGroup()
        levelGroup.cold = coldGroup
        levelGroup.normal = normalGroup
        levelGroup.hot = hotGroup
        return levelGroup
        pass


if __name__ == '__main__':

    data1 = {
        'column1': [1, 2, 3, 4, 5],
        'column2': ['A', 'B', 'C', 'D', 'Z']
    }
    data2 = {
        'column1': [6, 1, 9, 3, 7],
        'column2': ['A', 'B', 'C', 'D', 'E']
    }

    df1 = pd.DataFrame(data1)
    df2 = pd.DataFrame(data2)

    temp1 = pd.merge(df1, df2, on="column1")
    temp2 = pd.merge(df1, df2, on="column2")
    print('')

    # Function to split an array into 5 arrays, each containing 100 elements
    def split_array(arr):
        return [arr[i:i + 100] for i in range(0, len(arr), 100)]

    # 觀察出冷熱門球號連莊數

    # region select top 30 => inputs:[[]]、period str
    dbContext = MSSQLDbContext({'server': 'wpdb2.hihosting.hinet.net', 'user': 'p89880749_p89880749',
                                'password': 'Jonny1070607!@#$%', 'database': 'p89880749_test'})
    rows = dbContext.select(
        'select top 100 drawNumberSize,lotteryDate from Daily539 ORDER BY period')

    # endregion

    # region inputs分成數個群體
    dividePeriod = DividePeriod()
    divide = 50
    ballGroup539s = dividePeriod.calcu(rows, divide)

    inputs = ballGroup539s[0].ball2Ds
    input2s = ballGroup539s[1].ball2Ds
    levelGroup = BallLevel.calcu(inputs)
    levelGroup2 = BallLevel.calcu(input2s)
    hotData1 = {'hot': [], 'normal': [], 'cold': []}
    hotData2 = {'hot': [], 'normal': [], 'cold': []}
    hotData1['hot'] = list(map(lambda e: e, levelGroup.hot.keys()))
    hotData2['hot'] = list(map(lambda e: e, levelGroup2.hot.keys()))
    hotData1['normal'] = list(map(lambda e: e, levelGroup.normal.keys()))
    hotData2['normal'] = list(map(lambda e: e, levelGroup2.normal.keys()))
    hotData1['cold'] = list(map(lambda e: e, levelGroup.cold.keys()))
    hotData2['cold'] = list(map(lambda e: e, levelGroup2.cold.keys()))

    print(hotData1['hot'])
    print(hotData2['hot'])
    print(hotData1['normal'])
    print(hotData2['normal'])
    print(hotData1['cold'])
    print(hotData2['cold'])
    # endregion

    # region 換算號碼出現次數
    # 四分位距
    # 熱門:25
    # 普通:50
    # 冷門:25

    # endregion

    # region calcu rank change

    # calcu ball same rank percent
    # calcu ball change rank percent
    # endregion

    # 前20期開獎號碼
    # insert mongodb
    # export excel
    # 驗證後20期開獎號碼
    # 計算出差異

    pass
