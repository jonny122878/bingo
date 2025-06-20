import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from MACD.MACD import MACDPlot
from typing import List
from db.db import MSSQLDbContext
from parse.TimesAlgorithm import TimesAlgorithm

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
            rows = sorted(rows, key=lambda x: x.get('drawTerm'))
            top_rows = rows[randStart:randEnd]
            # 依 drawTerm 排序
            for row in top_rows:
                if 'bigShowOrder' in row and isinstance(row['bigShowOrder'], str):
                    row['bigShowOrders'] = row['bigShowOrder'].split(',')
            # 清空邏輯，僅保留參數 rows, randTimes
            self.calcu(top_rows, randStart)
            pass

    def calcu(self, rows,i):
        algo = TimesAlgorithm()
        fileNameNoExten = os.path.splitext(os.path.basename(self.ExcelFile))[0]
        fileExten = os.path.splitext(os.path.basename(self.ExcelFile))[1]
        print(fileNameNoExten)
        print(fileExten)
        variant_fileName = f"{fileNameNoExten}_{i}{fileExten}"
        algo.ExcelPath = os.path.join(self._ExcelPath, variant_fileName)
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
        export_path = os.path.join(self._ExcelPath, f"export_{i}.xlsx")
        df.to_excel(export_path, index=False)
        print(f"Exported to {export_path}")
        # 取得 df 前面47個元素丟入 plot rows 內
        plot_rows = df.head(47).to_dict(orient='records')
        end_rows = df.tail(3).to_dict(orient='records')
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

        # dfExcel 來源為 rows
        import pandas as pd
        dfExcel = pd.DataFrame(rows)
        if 'bigShowOrder' in dfExcel.columns:
            dfExcel = dfExcel.drop(columns=['bigShowOrder'])
        # 增加 balls 欄位
        dfExcel['balls'] = [balls] * len(dfExcel)
        # 將 balls 欄位 drawTerm 前 45 列清空
        if 'drawTerm' in dfExcel.columns:
            dfExcel.loc[dfExcel.index[:47], 'balls'] = ''
        # 增加 compare 欄位，取 bigShowOrders 與 balls 的交集
        dfExcel['compare'] = dfExcel.apply(
            lambda row: list(set(row['bigShowOrders']) & set(row['balls'])) if isinstance(row['bigShowOrders'], list) else [],
            axis=1
        )
        # 增加 perenct 欄位，為 balls count / bigShowOrders count
        dfExcel['perenct'] = dfExcel.apply(
            lambda row: len(row['compare']) / len(row['balls']) if isinstance(row['balls'], list) and len(row['balls']) > 0 else 0,
            axis=1
        )
        print(dfExcel)
        # 若要輸出到 Excel，可加上：
        dfExcel.to_excel(os.path.join(self._ExcelPath, f"balls_{i}.xlsx"), index=False)
        # macd_plot69.plot(ball_field69)
        pass

if __name__ == '__main__':
    import pandas as pd
    import random
    sql = MSSQLDbContext({'server': 'wpdb2.hihosting.hinet.net', 'user': 'p89880749_p89880749',
                          'password': 'Jonny1070607!@#$%', 'database': 'p89880749_test'})
    rows = sql.select('select drawTerm, bigShowOrder from Bingo ORDER BY drawTerm DESC ')
    feed_instance = TimesMACDFeed()
    feed_instance.ExcelPath = r'C:\Programs\test_data'
    feed_instance.ExcelFile = 'MACD.xlsx'
    feed_instance.feed(rows, randTimes=10)
    print('')
