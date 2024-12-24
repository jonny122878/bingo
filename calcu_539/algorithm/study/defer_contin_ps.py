import pandas as pd
from collections import defaultdict
from datetime import datetime
from typing import List, Dict

from calcu_539.algorithm.calcu_old import DeferCalcu, ExportFile, QLevel
from calcu_539.algorithm.mark_old import BeginBingoConvertMark
from calcu_539.static.quantile_old import Quantile
from db.MapBingoModel import MapBingoModel
from db.db import MSSQLDbContext
from db.BingoModel import BingoModel
from datetime import timedelta
from itertools import groupby
import numpy as np
from scipy import stats
import json

# 將1號連續
# 其他80號脫期做皮爾森

if __name__ == '__main__':
    # 讀取 export.json 文件
    with open('calcu_539/algorithm/calcu_old/export.json', 'r') as file:
        data = json.load(file)
        ps_path = data['ps_path']
        ps_filename = data['ps_filename']

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
        row = mapBingoModel.set_big_show_orders_str(row)
        row = mapBingoModel.set_big_samll_qty(row)
        row = mapBingoModel.qty_big_small_mark(row)
    # endregion

    # region date當key將rows分組
    grouped_results: Dict[datetime, List[BingoModel]] = {
        key: list(group) for key, group in groupby(rows, key=lambda x: x.dDate)}
    # endregion

    # 2024-11-01當測試資料
    inputs = [row.strBigShowOrders for row in grouped_results[datetime.strptime(
        '2024-11-01', '%Y-%m-%d').date()]]

    # 初始化一個包含 80 個鍵的字典，每個鍵的值都是一個空列表
    key_count = 80
    initialized_dict = {str(key).zfill(2): []
                        for key in range(1, key_count + 1)}

    # region 先讓1~80Defer給生出來
    mockExportFile = ExportFile()
    convert = BeginBingoConvertMark()
    quantile = Quantile(QLevel=QLevel.Q10)
    quantile.cutFrt = 0
    quantile.cutEnd = 80
    deferCalcu = DeferCalcu(mockExportFile,  convert,
                            quantile, True, ps_path, ps_filename)
    deferCalcu.includeColumns = ['01', '26', '44', 'num']
    dfDefer = deferCalcu.calcu(inputs)
    # endregion

    # 獲得 01 欄位並將其 value 轉成 []
    column_01_values = dfDefer.df['01'].tolist()

    # 遍歷 initialized_dict，並將 key 填入 dfDefer，將值寫入
    for key in initialized_dict.keys():
        initialized_dict[key] = dfDefer.df[key].tolist()

    results = []

    # 遍歷 initialized_dict.keys() 排除 01 key
    for key in initialized_dict.keys():
        if key != '01':
            data1 = column_01_values
            data2 = initialized_dict[key]
            correlation_coefficient, p_value = stats.pearsonr(data1, data2)
            result = {
                "key1": '01',
                "key2": key,
                "correlation_coefficient": correlation_coefficient,
                "p_value": p_value
            }
            results.append(result)

    # 將結果輸出成 JSON
    with open('correlation_results.json', 'w') as json_file:
        json.dump(results, json_file, indent=4)

    # 過濾 p_value <= 0.05 的項目
    filtered_results = [
        result for result in results if result['p_value'] <= 0.05]

    # 按 correlation_coefficient 降序排序
    sorted_results = sorted(
        filtered_results, key=lambda x: x['correlation_coefficient'], reverse=True)

    # 將結果輸出到 JSON 文件
    output_file_path = 'filtered_sorted_results.json'
    with open(output_file_path, 'w') as json_file:
        json.dump(sorted_results, json_file, indent=4)

    print(
        f"Filtered and sorted results have been written to {output_file_path}")
    print(initialized_dict)
