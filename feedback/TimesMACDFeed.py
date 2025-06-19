import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from typing import List
from db.db import MSSQLDbContext

class TimesMACDFeed:
    def __init__(self) -> None:
        self._ExcelPath = None
        self._ExcelFile = None

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

    def feed(self, rows, randTimes=None):
        import random
        for i in range(randTimes):
            randStart = random.randint(1, 493)
            randEnd = randStart + 50
            print(f"Random range: randStart={randStart}, randEnd={randEnd}")
            top_rows = rows[randStart:randEnd]
            for row in top_rows:
                if 'bigShowOrder' in row and isinstance(row['bigShowOrder'], str):
                    row['bigShowOrders'] = row['bigShowOrder'].split(',')
            # 清空邏輯，僅保留參數 rows, randTimes
            pass

if __name__ == '__main__':
    import pandas as pd
    import random
    sql = MSSQLDbContext({'server': 'wpdb2.hihosting.hinet.net', 'user': 'p89880749_p89880749',
                          'password': 'Jonny1070607!@#$%', 'database': 'p89880749_test'})
    rows = sql.select('select drawTerm, bigShowOrder from Bingo ORDER BY drawTerm DESC ')
    feed_instance = TimesMACDFeed()
    feed_instance.ExcelPath = r'C:\Programs\test_data'
    feed_instance.ExcelFile = 'top_rows.xlsx'
    feed_instance.feed(rows, randTimes=30)
    print('')
