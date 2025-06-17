import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from typing import List
from db.db import MSSQLDbContext

class TimesHorizontalStraightBingoFeed:
    def __init__(self) -> None:
        # ...existing code...
        self._ExcelPath = None
        self._ExcelFile = None  # 新增 ExcelFile 屬性

    @property
    def ExcelPath(self):
        return self._ExcelPath

    @ExcelPath.setter
    def ExcelPath(self, value):
        self._ExcelPath = value

    @property
    def ExcelFile(self):
        return self._ExcelFile

    @ExcelFile.setter
    def ExcelFile(self, value):
        self._ExcelFile = value

    def feed(self, rows, sortQty, take_arr, randTimes=None):
        import random
        for i in range(randTimes):
            randStart = random.randint(1, 493)
            randEnd = randStart + 50
            print(f"Random range: randStart={randStart}, randEnd={randEnd}")
            top_rows = rows[randStart:randEnd]
            # 將 bigShowOrder 拆分為陣列，新增欄位 bigShowOrders
            for row in top_rows:
                if 'bigShowOrder' in row and isinstance(row['bigShowOrder'], str):
                    row['bigShowOrders'] = row['bigShowOrder'].split(',')
            # 對每個 take 進行處理
            for take in take_arr:
                skipEnd = (sortQty - take) + 1
                for skip in range(0, skipEnd):
                    self.calcu(top_rows, skip, take, randStart=randStart)

    def calcu(self, top_rows, skip_variant_count=0, take=4, randStart=None):
        # 轉為 DataFrame 並匯出為 Excel，只包含 drawTerm 和 bigShowOrders
        import pandas as pd
        df = pd.DataFrame(top_rows)
        df = df[['drawTerm', 'bigShowOrders']]

        # === 以下搬移 test_calcu 主要邏輯 ===
        from parse.TimesAlgorithm import TimesAlgorithm
        from parse.ball_mark.HorizontalBallMark import HorizontalBallMark
        from parse.ball_mark.StraightBallMark import StraightBallMark
        from parse.calcu.CalcuTimesStraightAndHorizontal import CalcuTimesStraightAndHorizontal

        # 以 top_rows['bigShowOrders'] 作為 inputs
        inputs = [row['bigShowOrders'] for row in top_rows if 'bigShowOrders' in row]

        # Test_LoadDataByStraight variant
        algo_straight = TimesAlgorithm()
        # algo_straight.ExcelPath = r"C:\Programs\test_data\TimesAlgorithmByStraight.xlsx"
        algo_straight.ExcelPath = None
        algo_straight.BallMark = StraightBallMark()
        algo_straight.TakeColumns = ["01S", "02S", "03S", "04S", "05S", "06S", "07S", "08S", "09S", "10S"]
        algo_straight.LoadData(inputs)
        dfStraight = algo_straight.DfExport

        # Test_LoadDataByHorizontal variant
        algo_horizontal = TimesAlgorithm()
        # algo_horizontal.ExcelPath = r"C:\Programs\test_data\TimesAlgorithmByHorizontal.xlsx"
        algo_horizontal.ExcelPath = None
        algo_horizontal.BallMark = HorizontalBallMark()
        algo_horizontal.TakeColumns = ["01H", "11H", "21H", "31H", "41H", "51H", "61H", "71H"]
        algo_horizontal.LoadData(inputs)
        dfHorizontal = algo_horizontal.DfExport

        calcu = CalcuTimesStraightAndHorizontal()
        calcu.SkipVariantCount = skip_variant_count
        calcu.TakeVariantCount = take
        calcu.Calcu(dfStraight, dfHorizontal, Horizontal=HorizontalBallMark(), Straight=StraightBallMark())
        print(dfStraight)
        print(dfHorizontal)

        # 匯出 bigShowOrders 及 calcu.dfDictUniqueGroup 到 Excel 兩個 sheet
        # 新增：merge df 和 calcu.dfDictUniqueGroup
        # 修正：若 'RowIndex' 不存在則用 right_index=True
        if 'RowIndex' in calcu.dfDictUniqueGroup.columns:
            df_merged = pd.merge(df, calcu.dfDictUniqueGroup, left_index=True, right_on='RowIndex', how='left')
        else:
            df_merged = pd.merge(df, calcu.dfDictUniqueGroup, left_index=True, right_index=True, how='left')
        # 確保 'Balls' 欄位存在，若無則補上一個空值欄位
        if 'Balls' not in df_merged.columns:
            df_merged['Balls'] = None

        # 新增條件：所有 Balls array 尾數號碼須一致，否則清空 array
        def filter_balls_tail_consistent(balls):
            if isinstance(balls, list) and balls:
                # 取每個號碼的尾數（字串最後一位）
                tails = [str(b)[-1] for b in balls]
                if all(t == tails[0] for t in tails):
                    return balls
                else:
                    return []
            return balls
        df_merged['Balls'] = df_merged['Balls'].apply(filter_balls_tail_consistent)

        df_merged_simple = df_merged[['drawTerm','bigShowOrders', 'Balls']].copy()
        # 新增 Balls_count 欄位，計算 Balls array 數量
        df_merged_simple['Balls_count'] = df_merged_simple['Balls'].apply(lambda x: len(x) if isinstance(x, list) else 0)
        # 新增 compare 欄位，保留 bigShowOrders 和 Balls 交集
        def compare_elements(row):
            if isinstance(row['bigShowOrders'], list) and isinstance(row['Balls'], list):
                return list(set(row['bigShowOrders']) & set(row['Balls']))
            return []
        df_merged_simple['compare'] = df_merged_simple.apply(compare_elements, axis=1)
        # 新增百分比欄位，分母 Balls 長度，分子 compare 長度
        def calc_percent(row):
            if isinstance(row['Balls'], list):
                if len(row['Balls']) > 0:
                    return round(len(row['compare']) / len(row['Balls']) * 100, 2)
                else:
                    return None  # Balls 為空時回傳 None
            return None
        df_merged_simple['percent'] = df_merged_simple.apply(calc_percent, axis=1)

        # 新增：計算整體 percent 平均（捨去第0列）
        percent_series = df_merged_simple['percent'] if len(df_merged_simple) > 1 else df_merged_simple['percent']
        percent_series = percent_series.dropna()
        if not percent_series.empty:
            percent_avg = percent_series.mean()
        else:
            percent_avg = None
        percent_avg_df = pd.DataFrame({'percent_avg': [percent_avg]})

        # 新增 compare_count 欄位，計算 compare array 數量
        df_merged_simple['compare_count'] = df_merged_simple['compare'].apply(lambda x: len(x) if isinstance(x, list) else 0)
        # line 104: 組合 ExcelPath 與 ExcelFile
        fileNameNoExten = os.path.splitext(os.path.basename(self.ExcelFile))[0]
        fileExten = os.path.splitext(os.path.basename(self.ExcelFile))[1]
        print(fileNameNoExten)
        print(fileExten)
        skip_str = f"{skip_variant_count:02d}"  # 轉為二位數字串
        variant_fileName = f"{fileNameNoExten}_{skip_str}_skip_take{take}{fileExten}"
        # 新增: subfolder 包含 randStart
        if self.ExcelPath and self.ExcelFile:
            if randStart is not None:
                subfolder = os.path.join(self.ExcelPath,f"rand{randStart}", f"take{take}")
            else:
                subfolder = os.path.join(self.ExcelPath, f"take{take}")
            os.makedirs(subfolder, exist_ok=True)
            excel_full_path = os.path.join(subfolder, variant_fileName)
        else:
            excel_full_path = self.ExcelPath  # fallback
        with pd.ExcelWriter(excel_full_path) as writer:
            percent_avg_df.to_excel(writer, sheet_name="PercentAverage", index=False)
            df_merged_simple.to_excel(writer, sheet_name="BigShowOrdersAndBalls", index=False)
            df_merged.to_excel(writer, sheet_name="TopRowsWithElementCount", index=False)
            calcu.dfDictUniqueGroup.to_excel(writer, sheet_name="ElementCountGroupByRow", index=False)
            df.to_excel(writer, sheet_name="TopRows", index=False)

if __name__ == '__main__':
    import pandas as pd  # 匯入 pandas
    import random  # 匯入 random


    sql = MSSQLDbContext({'server': 'wpdb2.hihosting.hinet.net', 'user': 'p89880749_p89880749',
                          'password': 'Jonny1070607!@#$%', 'database': 'p89880749_test'})
    rows = sql.select('select drawTerm, bigShowOrder from Bingo ORDER BY drawTerm DESC ')

    feed_instance = TimesHorizontalStraightBingoFeed()
    feed_instance.ExcelPath = r'C:\Programs\test_data'  # 設定資料夾路徑
    feed_instance.ExcelFile = 'top_rows_bingo.xlsx'           # 設定檔案名稱
    feed_instance.feed(rows, sortQty=18, take_arr=[3,4,5,6,7,8], randTimes=2)
    # feed_instance.feed(rows, sortQty=18, take_arr=[3], randTimes=1)

    print('')# ...copy all contents from TimesHorizontalStraightFeed.py...
