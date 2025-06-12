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
