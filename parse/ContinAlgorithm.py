from typing import List
from pandas import DataFrame
import pandas as pd
import numpy as np

class ContinAlgorithm:
    """計算連期"""

    def __init__(self) -> None:
        self._TakeColumns: List[str] = []
        self._DfResult: DataFrame = None
        self._DfExport: DataFrame = None
        self._IsToExcel: bool = True  # 新增

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
    def IsToExcel(self):
        return self._IsToExcel

    @IsToExcel.setter
    def IsToExcel(self, value: bool):
        self._IsToExcel = value

    def LoadData(self, inputs: List[str]) -> None:
        # 1. 生成 ball2Ds: List[List[str]]
        ball2Ds: List[List[str]] = []

        # 2. 生成 dictBall: dict，key "01"~"80"，value 0
        dictBall = {f"{i:02d}": 0 for i in range(1, 81)}

        # 遍歷 inputs，if 累加1，else 清空成0
        for row in inputs:
            balls = set(list(row))
            for key in dictBall.keys():
                if key in balls:
                    dictBall[key] += 1
                else:
                    dictBall[key] = 0
            row_values = list(dictBall.values())
            # 新增：計算離散分布（標準差）
            std_value = float(np.std(row_values))
            row_values.append(std_value)
            ball2Ds.append(row_values)

        # 3. 實體化 self._DfResult，data=ball2Ds，columns=dictBall 的 keys + ["std"]
        self._DfResult = DataFrame(data=ball2Ds, columns=list(dictBall.keys()) + ["std"])
        # --- 新增邏輯 ---
        # 將 self._DfExport 複製自 self._DfResult
        self._DfExport = self._DfResult.copy()

        # 若 self._TakeColumns 非空，則刪除 self._DfExport 中名稱存在於 self._TakeColumns 的欄位
        if self._TakeColumns:
            # 1. 宣告 excludeColumns = 01~80
            excludeColumns = [f"{i:02d}" for i in range(1, 81)]
            # 2. foreach excludeColumns 若不存在 self._TakeColumns 則保留
            excludeColumns = [col for col in excludeColumns if col not in self._TakeColumns]
            # 3. self._DfExport drop method call excludeColumns
            self._DfExport.drop(columns=excludeColumns, inplace=True)

        # 輸出到 Excel，self._DfExport 為第 1 個 sheet，self._DfResult 為第 2 個 sheet
        if self._IsToExcel:
            with pd.ExcelWriter(r"C:\Programs\test_data\ContinAlgorithm.xlsx") as writer:
                self._DfExport.to_excel(writer, sheet_name="Export", index=False)
                self._DfResult.to_excel(writer, sheet_name="Result", index=False)
