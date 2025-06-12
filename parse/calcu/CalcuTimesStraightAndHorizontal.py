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
        dfDictUnique_list = []
        for i in range(dfMerged.shape[0]):
            row_np = dfMerged.iloc[i].to_numpy()
            sorted_indices = np.argsort(row_np)
            sorted_keys = dfMerged.columns[sorted_indices]
            sorted_values = np.sort(row_np)
            sorted_dict[f'Row {i+1}'] = sorted_keys
            sorted_dict[f'Sorted{i+1}'] = sorted_values
            # variant: 跳過前5個元素後取8個元素
            variant_indices = list(range(5, 13))
            variant_values = sorted_values[5:13]
            # 從 variant_values index 反推 column name sorted_keys
            variant_keys = [sorted_keys[idx] for idx in range(5, 13)]
            variant_balls = []
            for k in variant_keys:
                balls = Horizontal.markToBalls(str(k))
                if not balls:
                    balls = Straight.markToBalls(str(k))
                variant_balls.append(balls)
            variant_dict[f'Key{i+1}'] = variant_keys
            variant_dict[f'Sorted{i+1}'] = variant_values
            variant_dict[f'Balls{i+1}'] = variant_balls
            # 每一列都計算 dictUnique 並存入 list
            dictUnique = {}
            for balls in variant_balls:
                for b in balls:
                    dictUnique[b] = dictUnique.get(b, 0) + 1
            dfDictUnique = pd.DataFrame(list(dictUnique.items()), columns=["Ball", "Count"])
            dfDictUnique = dfDictUnique[dfDictUnique["Count"] > 1]  # 移除 Count == 1
            dfDictUnique["RowIndex"] = i+1
            dfDictUnique_list.append(dfDictUnique)
        dfSorted = pd.DataFrame(sorted_dict)
        dfSortedVariant = pd.DataFrame(variant_dict)
        # 合併所有 rows 的 dfDictUnique
        if dfDictUnique_list:
            dfDictUniqueAll = pd.concat(dfDictUnique_list, ignore_index=True)
        else:
            dfDictUniqueAll = pd.DataFrame()
        # 輸出到 Excel
        with pd.ExcelWriter(r"C:\Programs\test_data\CalcuTimesStraightAndHorizontal.xlsx") as writer:
            dfMerged.to_excel(writer, sheet_name="Merged", index=False)
            dfSorted.to_excel(writer, sheet_name="SortedFirstRow", index=False)
            dfSortedVariant.to_excel(writer, sheet_name="SortedFirstRowVariant", index=False)
            dfDictUniqueAll.to_excel(writer, sheet_name="ElementCountAllRows", index=False)