from pandas import DataFrame
import pandas as pd
import numpy as np
from parse.ball_mark.IBallMark import IBallMark

class CalcuTimesStraightAndHorizontal:
    def Calcu(self, dfStraight: DataFrame, dfHorizontal: DataFrame, Horizontal: IBallMark, Straight: IBallMark) -> None:
        # merge DataFrame
        dfMerged = pd.concat([dfStraight, dfHorizontal], axis=1)
        # 取得第一列並排序
        first_row_np = dfMerged.iloc[0].to_numpy()
        sorted_first_row = np.sort(first_row_np)
        # 包裝成 DataFrame 並加上標題
        dfSorted = pd.DataFrame({
            'Key': dfMerged.columns,
            'Sorted': sorted_first_row
        })
        # variant: 跳過前5個元素後取8個元素
        variant_values = sorted_first_row[5:13]
        variant_keys = dfMerged.columns[5:13]
        dfSortedVariant = pd.DataFrame({
            'Key': variant_keys,
            'Sorted': variant_values
        })
        # 輸出到 Excel，dfMerged 為第 1 個 sheet，dfStraight 為第 2 個 sheet，dfHorizontal 為第 3 個 sheet
        with pd.ExcelWriter(r"C:\Programs\test_data\CalcuTimesStraightAndHorizontal.xlsx") as writer:
            dfMerged.to_excel(writer, sheet_name="Merged", index=False)
            dfStraight.to_excel(writer, sheet_name="StraightB", index=False)
            dfHorizontal.to_excel(writer, sheet_name="HorizontalB", index=False)
            # 新增排序後的第一列到新工作表，左側有標題
            dfSorted.to_excel(writer, sheet_name="SortedFirstRow", index=False)
            # 新增 variant 結果到第5個sheet
            dfSortedVariant.to_excel(writer, sheet_name="SortedFirstRowVariant", index=False)