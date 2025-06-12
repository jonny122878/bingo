import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from typing import List
from db.db import MSSQLDbContext

class TimesHorizontalStraightFeed:
    def __init__(self) -> None:
        # ...existing code...
        self._ExcelPath = None

    @property
    def ExcelPath(self):
        return self._ExcelPath

    @ExcelPath.setter
    def ExcelPath(self, value):
        self._ExcelPath = value

    def feed(self, top_rows):
        # 將 bigShowOrder 拆分為陣列，新增欄位 bigShowOrders
        for row in top_rows:
            if 'bigShowOrder' in row and isinstance(row['bigShowOrder'], str):
                row['bigShowOrders'] = row['bigShowOrder'].split(',')

        # 轉為 DataFrame 並匯出為 Excel，只包含 drawTerm 和 bigShowOrders
        import pandas as pd
        df = pd.DataFrame(top_rows)
        df = df[['drawTerm', 'bigShowOrders']]
        df.to_excel(self.ExcelPath, index=False)

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
        calcu.Calcu(dfStraight, dfHorizontal, Horizontal=HorizontalBallMark(), Straight=StraightBallMark())
        print(dfStraight)
        print(dfHorizontal)

        # 匯出 bigShowOrders 及 calcu.dfDictUniqueGroup 到 Excel 兩個 sheet
        # 新增：merge df 和 calcu.dfDictUniqueGroup
        df_merged = pd.merge(df, calcu.dfDictUniqueGroup, left_index=True, right_on='RowIndex', how='left')

        with pd.ExcelWriter(self.ExcelPath) as writer:
            df.to_excel(writer, sheet_name="TopRows", index=False)
            calcu.dfDictUniqueGroup.to_excel(writer, sheet_name="ElementCountGroupByRow", index=False)
            df_merged.to_excel(writer, sheet_name="TopRowsWithElementCount", index=False)

if __name__ == '__main__':
    import pandas as pd  # 匯入 pandas

    sql = MSSQLDbContext({'server': 'wpdb2.hihosting.hinet.net', 'user': 'p89880749_p89880749',
                          'password': 'Jonny1070607!@#$%', 'database': 'p89880749_test'})
    rows = sql.select('select TOP 4 drawTerm, bigShowOrder from Bingo ORDER BY drawTerm DESC ')
    top_rows = rows[:4]  # 只取前4個元素

    feed_instance = TimesHorizontalStraightFeed()
    feed_instance.ExcelPath = r'C:\Programs\test_data\top_rows.xlsx'
    feed_instance.feed(top_rows)

    print('')
