from typing import List
from pandas import DataFrame

class DeferAlgorithm:
    """計算拖期"""

    def __init__(self) -> None:
        self._TakeColumns: List[str] = []
        self._DfResult: DataFrame = None
        self._DfExport: DataFrame = None

    @property
    def TakeColumns(self):
        return self._TakeColumns

    @TakeColumns.setter
    def TakeColumns(self, value):
        self._TakeColumns = value

    @property
    def DfResult(self):
        return self._DfResult

    @DfResult.setter
    def DfResult(self, value):
        self._DfResult = value

    @property
    def DfExport(self):
        return self._DfExport

    @DfExport.setter
    def DfExport(self, value):
        self._DfExport = value

    def LoadData(self, inputs: List[str]) -> None:
        # 1. 生成 ball2Ds: List[List[str]]
        ball2Ds: List[List[str]] = []

        # 2. 生成 dictBall: dict，key "01"~"80"，value 0
        dictBall = {f"{i:02d}": 0 for i in range(1, 81)}

        # 新增：遍歷 inputs，更新 dictBall 並將其值附加到 ball2Ds
        for row in inputs:
            balls = set(list(row))
            for key in dictBall.keys():
                if key in balls:
                    dictBall[key] += 1
                else:
                    dictBall[key] = 0
            ball2Ds.append(list(dictBall.values()))

        # 3. 實體化 self._DfResult，data=ball2Ds，columns=dictBall 的 keys
        self._DfResult = DataFrame(data=ball2Ds, columns=list(dictBall.keys()))
        # 輸出到 Excel
        self._DfResult.to_excel(r"C:\Programs\test_data\DeferAlgorithm.xlsx", index=False)
