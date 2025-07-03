import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db.db import MSSQLDbContext

if __name__ == '__main__':
    import pandas as pd
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
    # 增加 bigSmallSort、OddEvenSort 欄位
    for row in rows:
        bigShowOrders = row['bigShowOrders']
        # bigSmallSort: 01-40為小，41-80為大
        small_count = sum(1 for b in bigShowOrders if 1 <= int(b) <= 40)
        big_count = sum(1 for b in bigShowOrders if 41 <= int(b) <= 80)
        total = len(bigShowOrders)
        if total > 0:
            if big_count / total >= 0.65:
                bigSmallSort = "大"
            elif small_count / total >= 0.65:
                bigSmallSort = "小"
            else:
                bigSmallSort = "合"
        else:
            bigSmallSort = ""
        # OddEvenSort: 奇數個數>=65%為奇，偶數個數>=65%為偶，否則合
        odd_count = sum(1 for b in bigShowOrders if int(b) % 2 == 1)
        even_count = sum(1 for b in bigShowOrders if int(b) % 2 == 0)
        if total > 0:
            if odd_count / total >= 0.65:
                OddEvenSort = "奇"
            elif even_count / total >= 0.65:
                OddEvenSort = "偶"
            else:
                OddEvenSort = "合"
        else:
            OddEvenSort = ""
        row['bigSmallSort'] = bigSmallSort
        row['OddEvenSort'] = OddEvenSort
    # 轉為 DataFrame
    df = pd.DataFrame(rows)
    # 將欄位名稱補零，確保 MACDPlot 能正確取用 '01'~'80'
    df = df.rename(columns={str(i): str(i).zfill(2) for i in range(1, 81)})
    # 輸出到 Excel Sheet1, Sheet2, Sheet3, Sheet4
    actual_path = os.path.join(_ExcelPath, "discrete.xlsx")
    with pd.ExcelWriter(actual_path, engine='openpyxl', mode='w') as writer:
        # 先計算三個 std 欄
        from parse.DeferAlgorithm import DeferAlgorithm
        from parse.ContinAlgorithm import ContinAlgorithm
        from parse.TimesAlgorithm import TimesAlgorithm
        takeColumns = [str(i) for i in range(1, 81)]
        inputs = [row['bigShowOrders'] for row in rows]
        # Defer std
        defer_algo = DeferAlgorithm()
        defer_algo.TakeColumns = takeColumns
        defer_algo.LoadData(inputs)
        df_defer = defer_algo.DfResult
        defer_std_col = df_defer['std'] if 'std' in df_defer.columns else pd.Series([None]*len(df))
        # Contin std
        contin_algo = ContinAlgorithm()
        contin_algo.TakeColumns = takeColumns
        contin_algo.LoadData(inputs)
        df_contin = contin_algo.DfResult
        contin_std_col = df_contin['std'] if 'std' in df_contin.columns else pd.Series([None]*len(df))
        # Times std
        times_algo = TimesAlgorithm()
        times_algo.TakeColumns = takeColumns
        times_algo.LoadData(inputs)
        df_times = times_algo.DfResult
        times_std_col = df_times['std'] if 'std' in df_times.columns else pd.Series([None]*len(df))
        # 將 std 欄補齊長度與 df 一致
        defer_std_col = defer_std_col.reset_index(drop=True).reindex(range(len(df)))
        contin_std_col = contin_std_col.reset_index(drop=True).reindex(range(len(df)))
        times_std_col = times_std_col.reset_index(drop=True).reindex(range(len(df)))
        df['Defer std'] = defer_std_col
        df['Contin std'] = contin_std_col
        df['Times std'] = times_std_col
        # 寫入 Sheet1
        df.to_excel(writer, sheet_name="Sheet1", index=False, header=True)
        # 其餘 Sheet2~4
        df_defer.to_excel(writer, sheet_name="Defer", index=False, header=True)
        df_contin.to_excel(writer, sheet_name="Contin", index=False, header=True)
        df_times.to_excel(writer, sheet_name="Times", index=False, header=True)
    print(f"已將 DB 資料及演算法結果輸出到 {actual_path} 的 Sheet1~4，並將 std 欄整欄搬到 Sheet1")

