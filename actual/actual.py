import sys
import os
import shutil


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
    rows = sql.select('select drawTerm, bigShowOrder from Bingo ORDER BY drawTerm ASC ')
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
    macd_ranges = {
        'MACD <0.05': [],
        'MACD 0.05-0.1': [],
        'MACD 0.1-0.15': [],
        'MACD 0.15-0.2': [],
        'MACD >0.2': []
    }
    for j in range(1, 81):
        ball_field = str(j).zfill(2)
        import matplotlib.pyplot as plt
        macd = MACDPlot(plot_rows, date_field='drawTerm', close_field=ball_field)
        macd.load()
        offset = abs(macd.MACDOffset)
        if macd.isMACDBig:
            if offset < 0.05:
                macd_ranges['MACD <0.05'].append(ball_field)
            elif offset < 0.1:
                macd_ranges['MACD 0.05-0.1'].append(ball_field)
            elif offset < 0.15:
                macd_ranges['MACD 0.1-0.15'].append(ball_field)
            elif offset < 0.2:
                macd_ranges['MACD 0.15-0.2'].append(ball_field)
            else:
                macd_ranges['MACD >0.2'].append(ball_field)
            balls.append(ball_field)
    print(f"MACD big balls: {balls}")

    # 輸出 MACD 統計到 Sheet2，球號以逗號分隔
    macd_stats_df = pd.DataFrame({k: [','.join(v)] for k, v in macd_ranges.items()})

    # 讀取 actual.xlsx，並寫入新一列
    import pandas as pd
    from openpyxl.utils import get_column_letter
    actual_path = os.path.join(_ExcelPath, "actual.xlsx")
    from openpyxl import load_workbook
    import os as _os

    # 取得當前 drawTerm 最大值
    if not df.empty and 'drawTerm' in df.columns:
        last_draw_term = df['drawTerm'].iloc[-1]
        next_draw_term = int(last_draw_term) + 1
    else:
        next_draw_term = 1
    # 新增一列
    new_row = {"drawTerm": next_draw_term,"bigShowOrder":[], "balls": balls}
    new_df = pd.DataFrame([new_row])

    if _os.path.exists(actual_path):
        # 檔案存在，附加寫入
        with pd.ExcelWriter(actual_path, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
            # 取得現有資料行數
            book = writer.book
            if "Sheet1" in book.sheetnames:
                sheet = book["Sheet1"]
                startrow = sheet.max_row
            else:
                startrow = 0
            new_df.to_excel(writer, sheet_name="Sheet1", index=False, header=False, startrow=startrow)
            # Sheet2 也用附加方式
            if "Sheet2" in book.sheetnames:
                sheet2 = book["Sheet2"]
                startrow2 = sheet2.max_row
            else:
                startrow2 = 0
            macd_stats_df.to_excel(writer, sheet_name='Sheet2', index=False, header=False, startrow=startrow2)
        # 自適應欄寬
        wb = load_workbook(actual_path)
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            for col in ws.columns:
                max_length = 0
                col_letter = get_column_letter(col[0].column)
                for cell in col:
                    try:
                        if cell.value:
                            max_length = max(max_length, len(str(cell.value)))
                    except:
                        pass
                ws.column_dimensions[col_letter].width = max_length + 2
        wb.save(actual_path)
        # 直接複製 actual.xlsx 為 actual_view.xlsx
        actual_view_path = os.path.join(_ExcelPath, "actual_view.xlsx")
        shutil.copyfile(actual_path, actual_view_path)
    else:
        # 檔案不存在，建立新檔案
        with pd.ExcelWriter(actual_path, engine='openpyxl', mode='w') as writer:
            new_df.to_excel(writer, sheet_name="Sheet1", index=False)
            # 新增：輸出 MACD 統計到 Sheet2
            macd_stats_df.to_excel(writer, sheet_name='Sheet2', index=False)
        # 自適應欄寬
        wb = load_workbook(actual_path)
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            for col in ws.columns:
                max_length = 0
                col_letter = get_column_letter(col[0].column)
                for cell in col:
                    try:
                        if cell.value:
                            max_length = max(max_length, len(str(cell.value)))
                    except:
                        pass
                ws.column_dimensions[col_letter].width = max_length + 2
        wb.save(actual_path)
        # 直接複製 actual.xlsx 為 actual_view.xlsx
        actual_view_path = os.path.join(_ExcelPath, "actual_view.xlsx")
        shutil.copyfile(actual_path, actual_view_path)

