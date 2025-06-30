import sys
import os
import datetime


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

    # 讀取 actual.xlsx Sheet1
    actual_path = os.path.join(_ExcelPath, "actual.xlsx")
    actual_df = pd.read_excel(actual_path, sheet_name="Sheet1")
    # 檢查 balls 欄位是否存在，否則報錯並列出所有欄位
    if 'balls' not in actual_df.columns:
        print(f"actual.xlsx Sheet1 欄位: {list(actual_df.columns)}")
        raise KeyError("'balls' 欄位不存在，請確認 actual.xlsx Sheet1 的欄位名稱是否正確！")
    # 轉成 dict 方便查詢
    actual_map = {str(row['drawTerm']): row['balls'] if isinstance(row['balls'], list) else eval(row['balls']) for _, row in actual_df.iterrows()}

    # 將 balls 欄位填入
    for row in rows:
        key = str(row['drawTerm'])
        balls_str = actual_map.get(key, "")
        row['balls'] = balls_str if balls_str else []

    # 新增 compare 與 percent 欄位
    # for row in rows:
    #     balls = set(row['balls'])
    #     bigs = set(row['bigShowOrders'])
    #     compare = list(balls & bigs)
    #     row['compare'] = compare
    #     row['percent'] = round(len(compare) / len(balls), 2) if balls else 0.0

    # 增加 showBigSmallSort、showBigPercent、showSmallPercent、bigSmallSort、bigPercent、smallPercent 欄位
    for row in rows:
        bigShowOrders = row['bigShowOrders']
        small_count = sum(1 for b in bigShowOrders if 1 <= int(b) <= 40)
        big_count = sum(1 for b in bigShowOrders if 41 <= int(b) <= 80)
        total = len(bigShowOrders)
        showSmallPercent = small_count / total if total > 0 else 0
        showBigPercent = big_count / total if total > 0 else 0
        if total > 0:
            if showBigPercent >= 0.65:
                showBigSmallSort = "大"
            elif showSmallPercent >= 0.65:
                showBigSmallSort = "小"
            else:
                showBigSmallSort = "合"
        else:
            showBigSmallSort = ""
        row['showBigSmallSort'] = showBigSmallSort
        row['showBigPercent'] = showBigPercent
        row['showSmallPercent'] = showSmallPercent
        # 保留 bigSmallSort、bigPercent、smallPercent
        row['bigSmallSort'] = showBigSmallSort
        row['bigPercent'] = showBigPercent
        row['smallPercent'] = showSmallPercent

    # 從 actual_df 取得 bigSmallSort、bigPercent、smallPercent
    actual_map_bigSmallSort = {str(row['drawTerm']): row['bigSmallSort'] if 'bigSmallSort' in row and not pd.isna(row['bigSmallSort']) else '' for _, row in actual_df.iterrows()}
    actual_map_bigPercent = {str(row['drawTerm']): row['bigPercent'] if 'bigPercent' in row and not pd.isna(row['bigPercent']) else 0 for _, row in actual_df.iterrows()}
    actual_map_smallPercent = {str(row['drawTerm']): row['smallPercent'] if 'smallPercent' in row and not pd.isna(row['smallPercent']) else 0 for _, row in actual_df.iterrows()}
    for row in rows:
        key = str(row['drawTerm'])
        row['bigSmallSort'] = actual_map_bigSmallSort.get(key, '')
        row['bigPercent'] = actual_map_bigPercent.get(key, 0)
        row['smallPercent'] = actual_map_smallPercent.get(key, 0)

    # 以 balls 欄位計算 smallPercent、bigPercent、bigSmallSort
    for row in rows:
        balls = row.get('balls', [])
        small_count = sum(1 for b in balls if 1 <= int(b) <= 40)
        big_count = sum(1 for b in balls if 41 <= int(b) <= 80)
        total = len(balls)
        showSmallPercent = small_count / total if total > 0 else 0
        showBigPercent = big_count / total if total > 0 else 0
        if total > 0:
            if showBigPercent >= 0.54:
                showBigSmallSort = "小"
            elif showSmallPercent >= 0.54:
                showBigSmallSort = "大"
            else:
                showBigSmallSort = "合"
        else:
            showBigSmallSort = ""
        row['smallPercent'] = showSmallPercent
        row['bigPercent'] = showBigPercent
        row['bigSmallSort'] = showBigSmallSort

    # 增加 compare 欄位
    for row in rows:
        showBigSmallSort = row.get('showBigSmallSort', '')
        bigSmallSort = row.get('bigSmallSort', '')
        if bigSmallSort == '合':
            row['compare'] = 0
        elif showBigSmallSort != '合' and bigSmallSort != '合':
            row['compare'] = 175
        else:
            row['compare'] = -50


    # 若需輸出 DataFrame
    df = pd.DataFrame(rows)
    # 只保留 drawTerm 存在於 actual.xlsx 的資料，且 balls 欄位有值
    df = df[df['drawTerm'].astype(str).isin(actual_df['drawTerm'].astype(str)) & df['balls'].astype(bool)]
    # 找出 actual_df 有但 df 無的 drawTerm
    missing_terms = set(actual_df['drawTerm'].astype(str)) - set(df['drawTerm'].astype(str))
    if missing_terms:
        # 只取 balls 欄位有值的列
        extra_rows = actual_df[actual_df['drawTerm'].astype(str).isin(missing_terms) & actual_df['balls'].astype(bool)]
        # 若欄位不一致，補齊缺失欄位
        for col in df.columns:
            if col not in extra_rows.columns:
                extra_rows[col] = None
        # 按 df 欄位順序排列
        extra_rows = extra_rows[df.columns]
        # 合併
        df = pd.concat([df, extra_rows], ignore_index=True)
    print(df)
    # 只輸出 bigShowOrders 欄位，bigShowOrder 不輸出
    output_columns = [col for col in df.columns if col != 'bigShowOrder']
    df = df[output_columns]
    # 回寫到 actual.xlsx Sheet1，若 DataFrame 不為空才寫入
    if not df.empty:
        # 只覆蓋 Sheet1，保留 Sheet2、Sheet3
        from openpyxl import load_workbook
        import shutil
        with pd.ExcelWriter(actual_path, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
            book = writer.book
            if "Sheet1" in book.sheetnames:
                # 清空 Sheet1 內容
                ws = book["Sheet1"]
                ws.delete_rows(1, ws.max_row)
            df.to_excel(writer, sheet_name="Sheet1", index=False, header=True)
        # 直接複製 actual.xlsx 為 actual__HHmmss.xlsx
        now_str = datetime.datetime.now().strftime("%H%M%S")
        actual_view_path = os.path.join(_ExcelPath, f"actual__{now_str}.xlsx")
        shutil.copyfile(actual_path, actual_view_path)
    else:
        print("DataFrame 為空，不進行寫入。")

