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
from datetime import date

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
    rows_dict = dbContext.select(query)

    # Custom JSON encoder to handle date objects

    class DateEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, date):
                return obj.isoformat()
            return super(DateEncoder, self).default(obj)

    # 將結果輸出到 JSON 文件
    output_file_path = f'{ps_path}/db_bingo.json'
    with open(output_file_path, 'w') as json_file:
        json.dump(rows_dict, json_file, indent=4, cls=DateEncoder)
