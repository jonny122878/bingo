import pandas as pd
from collections import defaultdict
from datetime import datetime
from typing import List, Dict

from calcu_539.algorithm.calcu import DeferCalcu, ExportFile, QLevel
from calcu_539.algorithm.mark_old import BeginConvertMark
from calcu_539.static.quantile_old import Quantile
from db.MapBingoModel import MapBingoModel
from db.db import MSSQLDbContext
from db.BingoModel import BingoModel
from datetime import timedelta
from itertools import groupby
import numpy as np
from scipy import stats

# 將1號連續
# 其他80號脫期做皮爾森


if __name__ == '__main__':

    # region select db where between date 2024-11-01 and 2024-11-30
    dbContext = MSSQLDbContext({'server': 'wpdb2.hihosting.hinet.net', 'user': 'p89880749_p89880749',
                                'password': 'Jonny1070607!@#$%', 'database': 'p89880749_test'})

    query = (
        "SELECT [drawTerm], [dDate], [bigShowOrder], [createDate] "
        "FROM [p89880749_test].[p89880749_p89880749].[Bingo] "
        "WHERE dDate BETWEEN '{start_date}' AND '{end_date}' "
        "ORDER BY [dDate] DESC, [drawTerm] DESC"
    ).format(start_date='2024-11-01', end_date='2024-11-30')

    mapBingoModel = MapBingoModel()
    rows_dict: List[dict] = dbContext.select(query)
    rows: List[BingoModel] = [
        mapBingoModel.dict_to_bingo_model(row) for row in rows_dict]

    # endregion

    # region 資料格式處理 split balls and map model to big and to bool big and small
    for row in rows:
        row = mapBingoModel.set_big_show_orders_list(row)
        row = mapBingoModel.set_big_samll_qty(row)
        row = mapBingoModel.qty_big_small_mark(row)
    # endregion

    # region date當key將rows分組
    grouped_results: Dict[datetime, List[BingoModel]] = {
        key: list(group) for key, group in groupby(rows, key=lambda x: x.dDate)}
    # endregion

    for bingoModel in grouped_results[datetime.strptime('2024-11-01', '%Y-%m-%d').date()]:
        pass

    # 初始化一個包含 80 個鍵的字典，每個鍵的值都是一個空列表
    key_count = 80
    initialized_dict = {str(key).zfill(2): []
                        for key in range(1, key_count + 1)}

    # region 先讓1~80Defer給生出來
    mockExportFile = ExportFile()
    convert = BeginConvertMark()
    quantile = Quantile(QLevel=QLevel.Q10)
    quantile.cutFrt = 0
    quantile.cutEnd = 39
    path = 'C:/Programs/bingo/bingo_scrapy/539_calcu'
    filename = 'deferBall.xlsx'
    deferCalcu = DeferCalcu(mockExportFile,  convert,
                            quantile, True, path, filename)
    deferCalcu.includeColumns = ['markOutliers', 'varQ']
    # endregion
    print(initialized_dict)
