import pandas as pd
from collections import defaultdict
from datetime import datetime
from typing import List, Dict

from db.MapBingoModel import MapBingoModel
from db.db import MSSQLDbContext
from db.BingoModel import BingoModel
from datetime import timedelta
from itertools import groupby
import numpy as np
from scipy import stats

# Example usage
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

    # region 用單日當測試單位:大和小一共有288觀測點
    bigContinous = []
    smallContinous = []
    oneContinous = []
    twoContinous = []
    bigStep = 0
    smallStep = 0
    oneStep = 0
    twoStep = 0
    for bingoModel in grouped_results[datetime.strptime('2024-11-01', '%Y-%m-%d').date()]:
        if 1 in bingoModel.bigShowOrders:
            oneStep += 1
        if 2 in bingoModel.bigShowOrders:
            twoStep += 1
        if bingoModel.isBig:
            bigStep += 1
        if bingoModel.isSmall:
            smallStep += 1
        bigContinous.append(bigStep)
        oneContinous.append(oneStep)
        smallContinous.append(smallStep)
        twoContinous.append(twoStep)
        print(bingoModel)
    # endregion
    one_big_correlation_coefficient, one_big_p_value = stats.pearsonr(
        oneContinous, bigContinous)
    one_small_correlation_coefficient, one_small_p_value = stats.pearsonr(
        oneContinous, smallContinous)
    one_two_correlation_coefficient, one_two_p_value = stats.pearsonr(
        oneContinous, twoContinous)
    print(one_big_correlation_coefficient)
    print(one_big_p_value)
    print(one_small_correlation_coefficient)
    print(one_small_p_value)
    print(one_two_correlation_coefficient)
    print(one_two_p_value)
    print('')

    # region 輸出產出次數列表

    # 將這四個陣列合併成一個 DataFrame
    data = {
        'bigContinous': bigContinous,
        'smallContinous': smallContinous,
        'oneContinous': oneContinous,
        'twoContinous': twoContinous
    }
    df = pd.DataFrame(data)

    # 輸出到 Excel 文件
    output_path = 'C:/Programs/bingo/test_data/continous.xlsx'
    df.to_excel(output_path, index=False)

    print(f'DataFrame 已經輸出到 {output_path}')
    # endregion

    # map 30 period list model

    # 30 period sum big

    # 30 period sum 1 where


# 用日期為單位一共30觀測點
# 大次數 當日累加次數
# map 30 period list model

# map model to big
# 30 period sum big

# 30 period sum 1 where


# 用日期為單位一共30觀測點
# 大次數 當日累加次數
# 1號次數 當日累加次數
# 1號拖延1次數 當日累加次數
