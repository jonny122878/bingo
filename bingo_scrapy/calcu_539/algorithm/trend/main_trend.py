from ast import Not
from matplotlib.pyplot import axis
import pandas as pd
from calcu_539.algorithm.calcu_old import DeferCalcu
from calcu_539.algorithm.mark_old import BeginConvertMark
from calcu_539.excel.exportFile import ExportFile
from calcu_539.static.quantile_old import QLevel, Quantile
from db.db import MSSQLDbContext


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


if __name__ == '__main__':
    # 23.6%、38.2%、61.8%和 78.6%的比率被称为斐波那契比率。

    # region research DataFrame order

    # Load the table into a DataFrame
    data = {
        'order': [1, 2, 3, 4, 5, 6, 7, 8],
        'level': ['Q1', 'Q2', 'Q3', 'Q1', 'Q4', 'Q1', 'Q3', 'Q4']
    }
    df = pd.DataFrame(data)

    # Define high and low points
    high_point = 'Q1'
    low_point = 'Q4'

    # Find the indices of high and low points
    high_indices = df[df['level'] == high_point].index
    low_indices = df[df['level'] == low_point].index

    # Calculate the ranges
    ranges = []
    for high_idx in high_indices:
        for low_idx in low_indices:
            start = df.loc[high_idx, 'order']
            end = df.loc[low_idx, 'order']
            week = WeekInfo()
            week.start = start
            week.end = end
            is_start_exist = any(r.start <= start and r.end >=
                                 start for r in ranges)
            is_end_exist = any(r.start <= end and r.end >= end for r in ranges)
            if not (is_start_exist or is_end_exist):
                ranges.append(week)

    print(ranges)
    # endregion

    # region select db
    dbContext = MSSQLDbContext({'server': 'wpdb2.hihosting.hinet.net', 'user': 'p89880749_p89880749',
                                'password': 'Jonny1070607!@#$%', 'database': 'p89880749_test'})
    rows = dbContext.select(
        'select drawNumberSize,lotteryDate from Daily539 ORDER BY period')
    inputs = list(map(lambda row: row['drawNumberSize'].split(','), rows))
    # endregion

    # region convert num time std
    exportFile = ExportFile()
    quantile = Quantile(QLevel=QLevel.Q10)
    quantile.cutFrt = 0
    quantile.cutEnd = 39
    beginConvertMark = BeginConvertMark()
    deferBallCalcu = DeferCalcu(exportFile, beginConvertMark, quantile)
    deferBallCalcu._path = 'C:/Programs/bingo/test_data'
    deferBallCalcu._filename = 'trend_defer.xlsx'
    deferBallCalcu._isToCsv = True
    deferBallCalcu.includeColumns = ['var', 'varQ']
    dfDeferBallInfo = deferBallCalcu.calcu(inputs)
    # endregion

    # region map Q3、Q4、Q10
    def map_Q3(row: str) -> str:
        if row == 'Q1' or row == 'Q2':
            return 'Q1'
        elif row == 'Q9' or row == 'Q10':
            return 'Q3'
        else:
            return 'Q2'

    def map_Q4(row: str) -> str:
        if row == 'Q1' or row == 'Q2':
            return 'Q1'
        elif row == 'Q3' or row == 'Q4' or row == 'Q5':
            return 'Q2'
        elif row == 'Q6' or row == 'Q7' or row == 'Q8':
            return 'Q3'
        elif row == 'Q9' or row == 'Q10':
            return 'Q4'

    dfTrend = dfDeferBallInfo.dfDrop.copy(deep=True)
    dfTrend['var_Q3'] = dfTrend['varQ'].apply(map_Q3)
    dfTrend['var_Q4'] = dfTrend['varQ'].apply(map_Q4)
    dfTrend.to_excel('C:/Programs/bingo/test_data/QLevel.xlsx')
    print(dfTrend)
    # endregion

    # region find max point search low point how to group Q2 to Q9
    # period range、period

    # endregion

    # region output df and to excel
    # endregion

    # region 計算4個percent位置
    # endregion

    # region 以23.6%計算找出1、2、3回調位置
    # endregion

    # region insert mongo
    # endregion

    pass
