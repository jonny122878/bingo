import sys
import os


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from MACD.MACD import MACDPlot
from parse.TimesAlgorithm import TimesAlgorithm
from db.db import MSSQLDbContext



if __name__ == '__main__':
    import pandas as pd
    import random
    excel_file = 'MACD.xlsx'  # 新增，參考 TimesMACDFeed.py
    _ExcelPath = r"C:\Programs\test_data"
    sql = MSSQLDbContext({'server': 'wpdb2.hihosting.hinet.net', 'user': 'p89880749_p89880749',
                          'password': 'Jonny1070607!@#$%', 'database': 'p89880749_test'})
    rows = sql.select('select drawTerm, bigShowOrder from Bingo ORDER BY drawTerm DESC ')
    # 增加 bigShowOrders 欄位
    for row in rows:
        if 'bigShowOrder' in row and isinstance(row['bigShowOrder'], str):
            row['bigShowOrders'] = row['bigShowOrder'].split(',')
        else:
            row['bigShowOrders'] = []
    algo = TimesAlgorithm()
    fileNameNoExten = os.path.splitext(os.path.basename(excel_file))[0]
    fileExten = os.path.splitext(os.path.basename(excel_file))[1]
    print(fileNameNoExten)
    print(fileExten)
    takeColumns = ["79", "80"]
    algo.TakeColumns = takeColumns
    # 以 rows['bigShowOrders'] 轉為 list 作為 inputs
    inputs = list(row['bigShowOrders'] for row in rows if 'bigShowOrders' in row)
    algo.LoadData(inputs)
    # 建立一個 df 等於 algo.DfResult，最開頭插入一欄 drawTerm
    df = algo.DfResult.copy()
    if isinstance(rows, list) and len(rows) > 0 and 'drawTerm' in rows[0]:
        draw_terms = [row['drawTerm'] for row in rows]
        df.insert(0, 'drawTerm', draw_terms)
    # print(df)
    # 匯出 df 到 Excel
    export_path = os.path.join(_ExcelPath, f"export.xlsx")  # 移除
    df.to_excel(export_path, index=False)  # 移除
    plot_rows = df.to_dict(orient='records')
    balls = []
    for j in range(1, 81):
        ball_field = str(j).zfill(2)
        import matplotlib.pyplot as plt
        macd = MACDPlot(plot_rows, date_field='drawTerm', close_field=ball_field)
        macd.load()
        if macd.isMACDBig and macd.MACDOffset > 0.2:
            print(f"MACD offset {macd.MACDOffset}")
            balls.append(ball_field)
    print(f"MACD big balls: {balls}")
    pass