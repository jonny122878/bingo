from pandas import DataFrame
import pandas as pd
import numpy as np
from parse.ball_mark.IBallMark import IBallMark
from collections import Counter

class CalcuTimesStraightAndHorizontal:
    def Calcu(self, dfStraight: DataFrame, dfHorizontal: DataFrame, Horizontal: IBallMark, Straight: IBallMark) -> None:
        # merge DataFrame
        dfMerged = pd.concat([dfStraight, dfHorizontal], axis=1)
        # 橫向遍歷，整併 dfSortedVariant foreach 到主 for 迴圈，產生 KeyN, SortedN, BallsN 共 9 欄
        sorted_dict = {}
        variant_dict = {}
        for i in range(dfMerged.shape[0]):
            row_np = dfMerged.iloc[i].to_numpy()
            sorted_indices = np.argsort(row_np)
            sorted_keys = dfMerged.columns[sorted_indices]
            sorted_values = np.sort(row_np)
            sorted_dict[f'Row {i+1}'] = sorted_keys
            sorted_dict[f'Sorted{i+1}'] = sorted_values
            # variant: 跳過前5個元素後取8個元素
            variant_values = sorted_values[5:13]
            variant_keys = [dfMerged.columns[idx] for idx in range(5, 13)]
            variant_balls = []
            for k in variant_keys:
                balls = Horizontal.markToBalls(str(k))
                if not balls:
                    balls = Straight.markToBalls(str(k))
                variant_balls.append(balls)
            variant_dict[f'Key{i+1}'] = variant_keys
            variant_dict[f'Sorted{i+1}'] = variant_values
            variant_dict[f'Balls{i+1}'] = variant_balls
        dfSorted = pd.DataFrame(sorted_dict)
        dfSortedVariant = pd.DataFrame(variant_dict)
        first_row_temp = dfMerged.iloc[0].to_numpy()
        # 建立 dictUnique，key 為 Balls 欄位每個元素，value 為出現次數
        dictUnique = {}
        for balls in variant_balls:
            for b in balls:
                dictUnique[b] = dictUnique.get(b, 0) + 1
        # 移除 value == 1 的元素
        dictUnique = {k: v for k, v in dictUnique.items() if v > 1}
        dfDictUnique = pd.DataFrame(list(dictUnique.items()), columns=["Ball", "Count"])
        # 將 dfDictUnique 直向轉橫向，只保留 keys 組成 array，且只有一個 column
        if not dfDictUnique.empty:
            dfDictUniqueHorizontal = pd.DataFrame({'Balls': [dfDictUnique["Ball"].to_list()]})
        else:
            dfDictUniqueHorizontal = pd.DataFrame()
        # 輸出到 Excel，只保留4個sheet
        with pd.ExcelWriter(r"C:\Programs\test_data\CalcuTimesStraightAndHorizontal.xlsx") as writer:
            dfMerged.to_excel(writer, sheet_name="Merged", index=False)
            # 移除 dfStraight 和 dfHorizontal 的輸出
            # dfStraight.to_excel(writer, sheet_name="StraightB", index=False)
            # dfHorizontal.to_excel(writer, sheet_name="HorizontalB", index=False)
            # 新增排序後的第一列到新工作表，左側有標題
            dfSorted.to_excel(writer, sheet_name="SortedFirstRow", index=False)
            # 新增 variant 結果到第3個sheet
            dfSortedVariant.to_excel(writer, sheet_name="SortedFirstRowVariant", index=False)
            # 新增 dictUnique 統計到第4個 sheet
            dfDictUnique.to_excel(writer, sheet_name="ElementCount", index=False)
            # 新增第5個sheet：dfDictUniqueHorizontal
            dfDictUniqueHorizontal.to_excel(writer, sheet_name="ElementCountHorizontal", index=False)