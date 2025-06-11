from pandas import DataFrame
import pandas as pd

class CalcuTimesStraightAndHorizontal:
    def Calcu(self, dfStraight: DataFrame, dfHorizontal: DataFrame) -> None:
        # merge DataFrame
        dfMerged = pd.concat([dfStraight, dfHorizontal], axis=1)
        # 輸出到 Excel，dfMerged 為第 1 個 sheet，dfStraight 為第 2 個 sheet，dfHorizontal 為第 3 個 sheet
        with pd.ExcelWriter(r"C:\Programs\test_data\CalcuTimesStraightAndHorizontal.xlsx") as writer:
            dfMerged.to_excel(writer, sheet_name="Merged", index=False)
            dfStraight.to_excel(writer, sheet_name="Straight", index=False)
            dfHorizontal.to_excel(writer, sheet_name="Horizontal", index=False)
