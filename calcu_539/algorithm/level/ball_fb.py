from matplotlib import pyplot as plt
from calcu_539.algorithm.calcu_old import DeferCalcu
from calcu_539.algorithm.mark_old import BeginConvertMark
from calcu_539.excel.exportFile import ExportFile
from calcu_539.static.quantile_old import QLevel, Quantile
from db.db import MSSQLDbContext


if __name__ == '__main__':

    # region select db
    dbContext = MSSQLDbContext({'server': 'wpdb2.hihosting.hinet.net', 'user': 'p89880749_p89880749',
                                'password': 'Jonny1070607!@#$%', 'database': 'p89880749_test'})
    rows = dbContext.select(
        'select Top 50 drawNumberSize,lotteryDate from Daily539 ORDER BY period')
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
    deferBallCalcu.includeColumns = ['01', '02', '03', '04', '05', '06']
    dfDeferBallInfo = deferBallCalcu.calcu(inputs)
    # endregion

    dfTrend = dfDeferBallInfo.dfDrop.copy(deep=True)
    colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k', 'w']
    print(dfTrend.columns.values)
    print(dfTrend['01'].values)
    columns = dfTrend.columns.values
    plt.figure(figsize=(10, 6))
    for color, column in zip(colors, dfTrend.columns.values):
        plt.plot(dfTrend[column], color=color, label=column)
        # fib_sequence1 = [1, 10, 2, 20, 3, 1]
        # fib_sequence2 = [2, 5, 8, 13, 21, 34]
        # # 繪製第一條線，顏色為藍色，名稱為 'Sequence 1'
        # plt.plot(fib_sequence1, marker='o', linestyle='-',
        #         color='b', label='Sequence 1')
        # # 繪製第二條線，顏色為紅色，名稱為 'Sequence 2'
        # plt.plot(fib_sequence2, marker='s', linestyle='--',
        #         color='r', label='Sequence 2')
    plt.title('Fibonacci Sequence')
    plt.xlabel('Index')
    plt.ylabel('Fibonacci Number')
    plt.grid(True)
    plt.show()

    pass
