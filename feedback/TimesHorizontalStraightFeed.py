import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from typing import List
from db.db import MSSQLDbContext

class TimesHorizontalStraightFeed:
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

    def feed(self, top_rows, skipStart=0, skipEnd=0, take_variant_count=4):
        # 將 bigShowOrder 拆分為陣列，新增欄位 bigShowOrders
        for row in top_rows:
            if 'bigShowOrder' in row and isinstance(row['bigShowOrder'], str):
                row['bigShowOrders'] = row['bigShowOrder'].split(',')
        # 呼叫 calcu
        for skip in range(skipStart, skipEnd + 1):
            self.calcu(top_rows, skip, take_variant_count)

    def calcu(self, top_rows, skip_variant_count=0, take_variant_count=4):
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
        algo_straight.ExcelPath = r"C:\Programs\test_data\TimesAlgorithmByStraight.xlsx"
        algo_straight.BallMark = StraightBallMark()
        algo_straight.TakeColumns = ["01S", "02S", "03S", "04S", "05S", "06S", "07S", "08S", "09S", "10S"]
        algo_straight.LoadData(inputs)
        dfStraight = algo_straight.DfExport

        # Test_LoadDataByHorizontal variant
        algo_horizontal = TimesAlgorithm()
        algo_horizontal.ExcelPath = r"C:\Programs\test_data\TimesAlgorithmByHorizontal.xlsx"
        algo_horizontal.BallMark = HorizontalBallMark()
        algo_horizontal.TakeColumns = ["01H", "11H", "21H", "31H", "41H", "51H", "61H", "71H"]
        algo_horizontal.LoadData(inputs)
        dfHorizontal = algo_horizontal.DfExport

        calcu = CalcuTimesStraightAndHorizontal()
        calcu.SkipVariantCount = skip_variant_count
        calcu.TakeVariantCount = take_variant_count
        calcu.Calcu(dfStraight, dfHorizontal, Horizontal=HorizontalBallMark(), Straight=StraightBallMark())
        print(dfStraight)
        print(dfHorizontal)

        # 匯出 bigShowOrders 及 calcu.dfDictUniqueGroup 到 Excel 兩個 sheet
        # 新增：merge df 和 calcu.dfDictUniqueGroup
        df_merged = pd.merge(df, calcu.dfDictUniqueGroup, left_index=True, right_on='RowIndex', how='left')
        # 新增：只保留 bigShowOrders 和 Balls 欄位
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
            if isinstance(row['Balls'], list) and len(row['Balls']) > 0:
                return round(len(row['compare']) / len(row['Balls']) * 100, 2)
            return 0.0
        df_merged_simple['percent'] = df_merged_simple.apply(calc_percent, axis=1)

        # 新增：計算整體 percent 平均（捨去第0列）
        if not df_merged_simple.empty and len(df_merged_simple) > 1:
            percent_avg = df_merged_simple['percent'].iloc[1:].mean()
        elif not df_merged_simple.empty:
            percent_avg = df_merged_simple['percent'].iloc[0]
        else:
            percent_avg = 0.0
        percent_avg_df = pd.DataFrame({'percent_avg': [percent_avg]})

        # 新增 compare_count 欄位，計算 compare array 數量
        df_merged_simple['compare_count'] = df_merged_simple['compare'].apply(lambda x: len(x) if isinstance(x, list) else 0)
        # line 104: 組合 ExcelPath 與 ExcelFile
        fileNameNoExten = os.path.splitext(os.path.basename(self.ExcelFile))[0]
        fileExten = os.path.splitext(os.path.basename(self.ExcelFile))[1]
        print(fileNameNoExten)
        print(fileExten)
        skip_str = f"{skip_variant_count:02d}"  # 轉為二位數字串
        variant_fileName = f"{fileNameNoExten}_{skip_str}_skip_take{take_variant_count}{fileExten}"
        if self.ExcelPath and self.ExcelFile:
            excel_full_path = os.path.join(self.ExcelPath, variant_fileName)
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

    sql = MSSQLDbContext({'server': 'wpdb2.hihosting.hinet.net', 'user': 'p89880749_p89880749',
                          'password': 'Jonny1070607!@#$%', 'database': 'p89880749_test'})
    rows = sql.select('select drawTerm, bigShowOrder from Bingo ORDER BY drawTerm DESC ')
    top_rows = rows[:50]  # 只取前4個元素
    # top_rows = rows

    feed_instance = TimesHorizontalStraightFeed()
    feed_instance.ExcelPath = r'C:\Programs\test_data'  # 設定資料夾路徑
    feed_instance.ExcelFile = 'top_rows.xlsx'           # 設定檔案名稱
    feed_instance.feed(top_rows,0,15,3)

    print('')
