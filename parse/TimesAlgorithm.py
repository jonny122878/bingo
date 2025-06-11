from typing import List
from pandas import DataFrame
import pandas as pd
from ball_mark.IBallMark import IBallMark  # 修改為相對匯入

class TimesAlgorithm:
    """計算次數"""

    def __init__(self) -> None:
        self._TakeColumns: List[str] = []
        self._DfResult: DataFrame = None
        self._DfExport: DataFrame = None
        self._ExcelPath: str = None  # 新增
        self._BallMark: IBallMark = None  # 新增

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

    @property
    def ExcelPath(self):  # 新增
        return self._ExcelPath

    @ExcelPath.setter
    def ExcelPath(self, value: str):  # 新增
        self._ExcelPath = value

    @property
    def BallMark(self):  # 新增
        return self._BallMark

    @BallMark.setter
    def BallMark(self, value: IBallMark):  # 新增
        self._BallMark = value

    def LoadData(self, inputs: List[str]) -> None:
        # 1. 生成 ball2Ds: List[List[str]]
        ball2Ds: List[List[str]] = []

        # 2. 生成 dictBall: dict，key "01"~"80"，value 0
        if self._BallMark is None:
            dictBall = {f"{i:02d}": 0 for i in range(1, 81)}
        else:
            dictBall = self._BallMark.loadStds()

        # 修改：遍歷 inputs，若 key in balls 則累加 1
        for row in inputs:
            if self._BallMark is None:
                balls = list(row)  # 移除 set
            else:
                balls = [self._BallMark.ballToMark(b) for b in row]  # 移除 set
            for key in balls:
                if key in dictBall.keys():
                    dictBall[key] += 1
            ball2Ds.append(list(dictBall.values()))

        # 3. 實體化 self._DfResult，data=ball2Ds，columns=dictBall 的 keys
        self._DfResult = DataFrame(data=ball2Ds, columns=list(dictBall.keys()))
        # --- 新增邏輯 ---
        # 將 self._DfExport 複製自 self._DfResult
        self._DfExport = self._DfResult.copy()

        # 若 self._TakeColumns 非空，則刪除 self._DfExport 中名稱存在於 self._TakeColumns 的欄位
        if self._TakeColumns:
            # 1. 宣告 excludeColumns = 01~80
            if self._BallMark is None:
                excludeColumns = [f"{i:02d}" for i in range(1, 81)]
            else:
                excludeColumns = list(self._BallMark.loadStds().keys())
            # 2. foreach excludeColumns 若不存在 self._TakeColumns 則保留
            excludeColumns = [col for col in excludeColumns if col not in self._TakeColumns]
            # 3. self._DfExport drop method call excludeColumns
            self._DfExport.drop(columns=excludeColumns, inplace=True)

        # 輸出到 Excel，self._DfExport 為第 1 個 sheet，self._DfResult 為第 2 個 sheet
        if self._ExcelPath is not None:
            with pd.ExcelWriter(self._ExcelPath) as writer:
                self._DfExport.to_excel(writer, sheet_name="Export", index=False)
                self._DfResult.to_excel(writer, sheet_name="Result", index=False)
